package translator

import (
	"encoding/binary"
	"hash/fnv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/dmibor/pilosa-ingester/clustering"
	"github.com/dmibor/pilosa-ingester/mappingstorage"
	gopilosa "github.com/pilosa/go-pilosa"
)

//core is copied from Pilosa PDK, simplified and added new functionality

type Translator interface {
	Get(id uint64) (val string, err error)
	GetID(val string) (id uint64, err error)
	Close() error
}

// IndexTranslator is used to translate index fields and column real data values to internal Pilosa IDs
// it is responsible for storing these mappings locally ( the two way val/id mapping) in leveldb.
// it is also responsible for generating new internal Pilosa IDs for values never seen before
type IndexTranslator struct {
	lock   sync.RWMutex
	fields map[string]Translator
	column Translator
}

// NewIndexTranslator gets a new IndexTranslator.
func NewIndexTranslator(dir string, indexName string, client *gopilosa.Client, c *clustering.PilosaCluster,
	icluster *clustering.IngestClusterConfig, postgresConfig *mappingstorage.PostgresConfig, fields ...string) (lt *IndexTranslator, err error) {
	lt = &IndexTranslator{
		fields: make(map[string]Translator),
	}

	lt.column, err = newColumnTranslator(dir, indexName, client, c, icluster)
	if err != nil {
		return nil, err
	}

	for _, field := range fields {
		lft, err := newMapFieldTranslator(field, indexName, postgresConfig)
		if err != nil {
			return nil, errors.Wrap(err, "making FieldTranslator")
		}
		lt.fields[field] = lft
	}
	return lt, nil
}

// Get returns the value mapped to the given id in the given field.
func (lt *IndexTranslator) GetRowVal(field string, id uint64) (val string, err error) {
	lft, err := lt.getFieldTranslator(field)
	if err != nil {
		return "", errors.Wrap(err, "getting field translator")
	}
	return lft.Get(id)
}

// GetID returns the integer id associated with the given value in the given field.
// It allocates a new ID if the value is not found.
func (lt *IndexTranslator) GetRowID(field string, val string) (id uint64, err error) {
	lft, err := lt.getFieldTranslator(field)
	if err != nil {
		return 0, errors.Wrap(err, "getting field translator")
	}
	id, err = lft.GetID(val)
	return
}

func (lt *IndexTranslator) GetColVal(id uint64) (val string, err error) {
	return lt.column.Get(id)
}

func (lt *IndexTranslator) GetColID(val string) (id uint64, err error) {
	id, err = lt.column.GetID(val)
	return
}

// getFieldTranslator retrieves or creates a FieldTranslator for the given field.
func (lt *IndexTranslator) getFieldTranslator(field string) (Translator, error) {
	lt.lock.RLock()
	if tr, ok := lt.fields[field]; ok {
		lt.lock.RUnlock()
		return tr, nil
	}
	return nil, errors.New("getting field translator")
}

// Close closes all of the underlying leveldb instances.
func (lt *IndexTranslator) Close() error {
	errs := make(errorList, 0)
	for f, lft := range lt.fields {
		err := lft.Close()
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "field : %v", f))
		}
	}
	err := lt.column.Close()
	if err != nil {
		errs = append(errs, errors.Wrap(err, "column"))
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}

// FieldTranslator translates fields using leveldb.
type FieldTranslator struct {
	lock   valueLocker
	idMap  *leveldb.DB
	valMap *leveldb.DB

	idGen idGenerator
}

type errorList []error

func (errs errorList) Error() string {
	errstrings := make([]string, len(errs))
	for i, err := range errs {
		errstrings[i] = err.Error()
	}
	return strings.Join(errstrings, "; ")
}

// Close closes the two leveldbs used by the FieldTranslator.
func (lft *FieldTranslator) Close() error {
	errs := make(errorList, 0)
	err := lft.idMap.Close()
	if err != nil {
		errs = append(errs, errors.Wrap(err, "closing idMap"))
	}
	err = lft.valMap.Close()
	if err != nil {
		errs = append(errs, errors.Wrap(err, "closing valMap"))
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

// newFieldTranslator creates a new FieldTranslator which uses LevelDB as
// backing storage.
func newFieldTranslator(dirname string, field string) (Translator, error) {
	var err error
	var idGen idGenerator
	mdbs := &FieldTranslator{
		idGen: idGen,
		lock:  newBucketVLock(),
	}
	mdbs.idMap, err = leveldb.OpenFile(dirname+"/"+field+"-id", &opt.Options{})
	if err != nil {
		return nil, errors.Wrapf(err, "opening leveldb at %v", dirname+"/"+field+"-id")
	}
	iter := mdbs.idMap.NewIterator(nil, nil)
	var initID uint64
	if iter.Last() {
		initID = binary.BigEndian.Uint64(iter.Key()) + 1
		iter.Release()
	}
	mdbs.idGen = newSequentialIDGen(initID)
	mdbs.valMap, err = leveldb.OpenFile(dirname+"/"+field+"-val", &opt.Options{})
	if err != nil {
		return nil, errors.Wrapf(err, "opening leveldb at %v", dirname+"/"+field+"-val")
	}
	return mdbs, nil
}

// Get returns the value mapped to the given id.
func (lft *FieldTranslator) Get(id uint64) (string, error) {
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	data, err := lft.idMap.Get(idBytes, nil)
	if err != nil {
		return "", errors.Wrap(err, "fetching from idMap")
	}
	return string(data), nil
}

// GetID returns the integer id associated with the given value. It allocates a
// new ID if the value is not found.
func (lft *FieldTranslator) GetID(val string) (id uint64, err error) {
	valBytes := []byte(val)
	var data []byte

	// if you're expecting most of the mapping to already be done, this would be faster
	data, err = lft.valMap.Get(valBytes, &opt.ReadOptions{})
	if err != nil && err != leveldb.ErrNotFound {
		return 0, errors.Wrap(err, "trying to read value map")
	} else if err == nil {
		return binary.BigEndian.Uint64(data), nil
	}

	// else, val not found
	lft.lock.Lock(valBytes)
	defer lft.lock.Unlock(valBytes)
	// re-read after locking
	data, err = lft.valMap.Get(valBytes, &opt.ReadOptions{})
	if err != nil && err != leveldb.ErrNotFound {
		return 0, errors.Wrap(err, "trying to read value map")
	} else if err == nil {
		return binary.BigEndian.Uint64(data), nil
	}

	idBytes := make([]byte, 8)
	new := lft.idGen.generateID()
	binary.BigEndian.PutUint64(idBytes, new)
	err = lft.idMap.Put(idBytes, valBytes, &opt.WriteOptions{})
	if err != nil {
		return 0, errors.Wrap(err, "putting new id into idmap")
	}
	err = lft.valMap.Put(valBytes, idBytes, &opt.WriteOptions{})
	if err != nil {
		return 0, errors.Wrap(err, "putting new id into valmap")
	}
	return new, nil
}

type valueLocker interface {
	Lock(val []byte)
	Unlock(val []byte)
}

type bucketVLock struct {
	ms []sync.Mutex
}

func newBucketVLock() bucketVLock {
	return bucketVLock{
		ms: make([]sync.Mutex, 1000),
	}
}

func (b bucketVLock) Lock(val []byte) {
	hsh := fnv.New32a()
	hsh.Write(val) // never returns error for hash
	b.ms[hsh.Sum32()%1000].Lock()
}

func (b bucketVLock) Unlock(val []byte) {
	hsh := fnv.New32a()
	hsh.Write(val) // never returns error for hash
	b.ms[hsh.Sum32()%1000].Unlock()
}
