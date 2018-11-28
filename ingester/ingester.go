package ingester

import (
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/dmibor/pilosa-ingester/indexer"
	"github.com/dmibor/pilosa-ingester/translator"

	"github.com/pkg/errors"
)

const (
	parseConcurrency = 8 //default goroutines for parsing
)

//indexes of fields in the log
var logFields = map[string]int{
	"ts":          0,
	"idfield":     1,
	"cfield":      2,
	"ofield":      3,
	"sfield_list": 4,
}

type Ingester struct {
	parseConcurrency int
	src              *source
	mp               mapperAndPusher
	index            *indexer.Indexer
}

func NewIngester(sourceCh chan string, index *indexer.Indexer, translator *translator.IndexTranslator, stopSig chan struct{}) *Ingester {
	//source for Pilosa ingester
	src := &source{sourceCh, stopSig}
	var mp mapperAndPusher
	mp.index = index
	mp.translator = translator

	return &Ingester{
		parseConcurrency: parseConcurrency,
		src:              src,
		mp:               mp,
		index:            index,
	}
}

// starts importing pipeline
func (n *Ingester) Run() error {
	pwg := sync.WaitGroup{}
	for i := 0; i < n.parseConcurrency; i++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			var recordErr error
			for {
				// Source
				rec, recordErr := n.src.Record()
				if recordErr != nil {
					break
				}

				//convert and assert string
				line := rec.(string)

				// Map and Push data to go-pilosa field channels for import
				err := n.mp.MapAndPush(line)
				if err != nil {
					log.Fatalf("couldn't map val: %s, err: %v", line, err)
					continue
				}
			}
			if recordErr != io.EOF && recordErr != nil {
				log.Printf("error in ingest run loop: %v", recordErr)
			}
		}()
	}
	pwg.Wait()
	log.Print("############### Finished data import!")

	err := n.index.Close()
	if err != nil {
		return errors.Wrap(err, "closing indexer during ingester shutdown")
	}
	err = n.mp.translator.Close()
	if err != nil {
		return errors.Wrap(err, "closing translator during ingester shutdown")
	}
	return nil
}

//
type mapperAndPusher struct {
	translator *translator.IndexTranslator
	index      *indexer.Indexer
}

//maps log line from logs to pilosa records in format ready for import
func (m *mapperAndPusher) MapAndPush(line string) error {

	fields := strings.Split(line, "\t")

	//translate idfield into Pilosa column sequential number using levelDB translator
	col, err := m.translator.GetColID(fields[logFields["idfield"]])
	if err != nil {
		return errors.Wrap(err, "getting column id from subject")
	}

	//parse ts field in logs that would be used to for pilosa rows
	ts, err := time.Parse("2006-01-02 15:04:05", fields[logFields["ts"]])
	if err != nil {
		log.Println(errors.Wrapf(err, "could not convert time for id %s and time %s", fields[logFields["idfield"]],
			fields[logFields["ts"]]))
	}
	rowID, err := m.translator.GetRowID("ofield", fields[logFields["ofield"]])
	if err != nil {
		return errors.Wrap(err, "translating ofield")
	}
	m.index.AddColumnNumeric("ofield", col, rowID, ts.UnixNano())

	rowID, err = m.translator.GetRowID("cfield", fields[logFields["cfield"]])
	if err != nil {
		return errors.Wrap(err, "translating cfield")
	}
	m.index.AddColumnNumeric("cfield", col, rowID, ts.UnixNano())

	str := fields[logFields["sfield_list"]]
	if str == "" { //don't process empty list
		return nil
	}
	sfields := strings.Split(str, ",")
	for _, v := range sfields {
		rowID, err = m.translator.GetRowID("sfield", v)
		if err != nil {
			return errors.Wrap(err, "translating sfield")
		}
		m.index.AddColumnNumeric("sfield", col, rowID, ts.UnixNano())
	}

	return nil
}

//Ingestion source
type source struct {
	lineCh  chan string
	stopSig chan struct{}
}

func (s source) Record() (interface{}, error) {
	select {
	case r, open := <-s.lineCh:
		if !open {
			return nil, io.EOF
		}
		return r, nil
	case <-s.stopSig:
		//don't read any more lines and shutdown gracefully
		return nil, io.EOF
	}
}
