package indexer

import (
	"io"
	"log"
	"sync"

	"github.com/pkg/errors"

	gopilosa "github.com/pilosa/go-pilosa"
)

//struct to work with Pilosa indexer that can
// - create columns
// - create fields together with import infra around them
//
// this is heavily simplified version of pdk.Index
// that aims to stay in sync with pilosa/go-pilosa client repo
// pilosa/pdk repo seems quite outdated
type Indexer struct {
	client *gopilosa.Client

	// lock        sync.RWMutex
	index       *gopilosa.Index
	importWG    sync.WaitGroup
	recordChans map[string]chanRecordIterator
}

func NewIndexer(client *gopilosa.Client, index *gopilosa.Index) *Indexer {
	return &Indexer{
		recordChans: make(map[string]chanRecordIterator),
		client:      client,
		index:       index,
	}
}

// setupField ensures the existence of a field in Pilosa,
// and starts importers for the field.
// It is not threadsafe - callers must hold i.lock.Lock() or guarantee that they have
// exclusive access to Index before calling.
//
//every field has it's own batchsize and threadcount
func (i *Indexer) SetupField(field *gopilosa.Field, batchSize int, threads int) error {
	fieldName := field.Name()
	if _, ok := i.recordChans[fieldName]; !ok {
		err := i.client.EnsureField(field)
		if err != nil {
			return errors.Wrapf(err, "creating field '%v'", field)
		}
		i.recordChans[fieldName] = newChanRecordIterator()

		i.importWG.Add(1)
		go func(fram *gopilosa.Field, cbi chanRecordIterator) {
			defer i.importWG.Done()
			err := i.client.ImportField(fram, cbi, gopilosa.OptImportBatchSize(batchSize),
				gopilosa.OptImportThreadCount(threads))
			if err != nil {
				log.Println(errors.Wrapf(err, "starting field import for %v", fieldName))
			}
		}(field, i.recordChans[fieldName])
	}
	return nil
}

func (i *Indexer) AddColumnStrings(fieldName string, col string, row string, ts int64) {
	c, ok := i.recordChans[fieldName]
	if !ok {
		log.Fatal("Misconfigured field schema - aborting")
	}
	c <- gopilosa.Column{RowKey: row, ColumnKey: col, Timestamp: ts}
}

func (i *Indexer) AddColumnNumeric(fieldName string, colID uint64, rowID uint64, ts int64) {
	c, ok := i.recordChans[fieldName]
	if !ok {
		log.Fatal("Misconfigured field schema - aborting")
	}
	c <- gopilosa.Column{RowID: rowID, ColumnID: colID, Timestamp: ts}
}

func (i *Indexer) Close() error {
	for _, cbi := range i.recordChans {
		close(cbi)
	}
	i.importWG.Wait()
	return nil
}

type chanRecordIterator chan gopilosa.Record

func newChanRecordIterator() chanRecordIterator {
	return make(chan gopilosa.Record, 2000)
}

func (c chanRecordIterator) NextRecord() (gopilosa.Record, error) {
	b, ok := <-c
	if !ok {
		return b, io.EOF
	}
	return b, nil
}
