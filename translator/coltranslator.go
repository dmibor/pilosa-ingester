package translator

import (
	"encoding/binary"
	"log"
	"time"

	"github.com/allegro/bigcache"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/dmibor/pilosa-ingester/clustering"
	gopilosa "github.com/pilosa/go-pilosa"
)

type ColumnTranslator struct {
	lock   valueLocker
	idMap  *leveldb.DB
	valMap *leveldb.DB
	//used to track which IDs in which shard are used already,
	// to continue from where left off after script restart
	shardMap *leveldb.DB
	cache    *bigcache.BigCache

	idGen      idGenerator
	shardWidth uint64
}

func newColumnTranslator(dirname string, indexName string, client *gopilosa.Client, c *clustering.PilosaCluster,
	icluster *clustering.IngestClusterConfig) (Translator, error) {
	var err error
	var idGen idGenerator

	ct := &ColumnTranslator{
		idGen:      idGen,
		lock:       newBucketVLock(),
		shardWidth: c.ShardWidth,
		cache:      setupCache(),
	}
	ct.idMap, err = leveldb.OpenFile(dirname+"/__columns-id", &opt.Options{})
	if err != nil {
		return nil, errors.Wrapf(err, "opening leveldb at %v", dirname+"/__columns-id")
	}
	ct.shardMap, err = leveldb.OpenFile(dirname+"/__columns-shards", &opt.Options{})
	if err != nil {
		return nil, errors.Wrapf(err, "opening leveldb at %v", dirname+"/__columns-shards")
	}

	ct.idGen, err = newMultiShardIDGen(c, indexName, client, ct.shardMap, icluster)
	if err != nil {
		return nil, errors.Wrapf(err, "creating multishard id generator")
	}

	ct.valMap, err = leveldb.OpenFile(dirname+"/__columns-val", &opt.Options{})
	if err != nil {
		return nil, errors.Wrapf(err, "opening leveldb at %v", dirname+"/__columns-val")
	}

	return ct, nil
}

func setupCache() *bigcache.BigCache {
	//90 min eviction
	config := bigcache.DefaultConfig(90 * time.Minute)
	//3gb max size
	config.HardMaxCacheSize = 3072
	config.Verbose = false
	cache, initErr := bigcache.NewBigCache(config)
	if initErr != nil {
		log.Fatal(initErr)
	}

	return cache
}

// Get returns the value mapped to the given id.
func (t *ColumnTranslator) Get(id uint64) (string, error) {
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	data, err := t.idMap.Get(idBytes, nil)
	if err != nil {
		return "", errors.Wrap(err, "fetching from idMap")
	}
	return string(data), nil
}

// GetID returns the integer id associated with the given value. It allocates a
// new ID if the value is not found.
func (t *ColumnTranslator) GetID(val string) (id uint64, err error) {
	valBytes := []byte(val)
	var data []byte

	//// if you're expecting most of the mapping to already be done, this would be faster
	//data, err = t.valMap.Get(valBytes, &opt.ReadOptions{})
	//if err != nil && err != leveldb.ErrNotFound {
	//	return 0, errors.Wrap(err, "trying to read value map")
	//} else if err == nil {
	//	return binary.BigEndian.Uint64(data), nil
	//}
	fromCache, err := t.cache.Get(val)
	if err == nil {
		return binary.BigEndian.Uint64(fromCache), nil
	}

	// else, val not found
	t.lock.Lock(valBytes)
	defer t.lock.Unlock(valBytes)
	// re-read after locking
	data, err = t.valMap.Get(valBytes, &opt.ReadOptions{})
	if err != nil && err != leveldb.ErrNotFound {
		return 0, errors.Wrap(err, "trying to read value map")
	} else if err == nil {
		return binary.BigEndian.Uint64(data), nil
	}
	idBytes := make([]byte, 8)
	shardBytes := make([]byte, 8)

	new := t.idGen.generateID()

	binary.BigEndian.PutUint64(idBytes, new)
	binary.BigEndian.PutUint64(shardBytes, new/t.shardWidth)

	//put to loadedShards
	err = t.cache.Set(val, idBytes)
	if err != nil {
		log.Print(err)
	}

	err = t.idMap.Put(idBytes, valBytes, &opt.WriteOptions{})
	if err != nil {
		return 0, errors.Wrap(err, "putting new id into idmap")
	}
	err = t.valMap.Put(valBytes, idBytes, &opt.WriteOptions{})
	if err != nil {
		return 0, errors.Wrap(err, "putting new id into valmap")
	}
	//override last used id for this shard
	err = t.shardMap.Put(shardBytes, idBytes, &opt.WriteOptions{})
	if err != nil {
		return 0, errors.Wrap(err, "putting new id into valmap")
	}

	return new, nil
}

// Close closes the two leveldbs used by the FieldTranslator.
func (t *ColumnTranslator) Close() error {
	errs := make(errorList, 0)
	err := t.idMap.Close()
	if err != nil {
		errs = append(errs, errors.Wrap(err, "closing idMap"))
	}
	err = t.valMap.Close()
	if err != nil {
		errs = append(errs, errors.Wrap(err, "closing valMap"))
	}
	err = t.shardMap.Close()
	if err != nil {
		errs = append(errs, errors.Wrap(err, "closing shardMap"))
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}
