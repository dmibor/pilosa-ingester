package scanner

import (
	"bufio"
	"compress/gzip"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/dmibor/pilosa-ingester/clustering"
	"github.com/dmibor/pilosa-ingester/file-marker"
	"github.com/dmibor/pilosa-ingester/logstorage"
)

type Scanner struct {
	config              *Config
	logStorage          *logstorage.LogStorage
	marker              *filemarker.FileStateMarker
	parallelBuckets     int
	filesCh             chan logstorage.AwsFile
	fileLoadParallelism int
	ingestCluster       *clustering.IngestClusterConfig
}

type Config struct {
	MaxResults      int    `toml:"max_results"`
	StartDate       string `toml:"start_date"`
	EndDate         string `toml:"end_date"`
	ParallelBuckets int    `toml:"parallel_buckets"`
	BucketMin       int    `toml:"bucket_min"`
	BucketMax       int    `toml:"bucket_max"`
}

type dayBucket struct {
	day    string
	bucket int
}

func NewScanner(conf *Config, logStorage *logstorage.LogStorage, marker *filemarker.FileStateMarker,
	icluster *clustering.IngestClusterConfig) *Scanner {
	return &Scanner{
		config:              conf,
		logStorage:          logStorage,
		marker:              marker,
		filesCh:             make(chan logstorage.AwsFile, 100),
		fileLoadParallelism: 3,
		ingestCluster:       icluster,
	}
}

func (s *Scanner) Scan() chan string {

	scannableDayBucket := make(chan dayBucket, 2000)
	var startDay, endDay time.Time
	var err error

	if startDay, err = time.Parse("2006-01-02", s.config.StartDate); err != nil {
		log.Fatal("parse date")
	}
	if endDay, err = time.Parse("2006-01-02", s.config.EndDate); err != nil {
		log.Fatal("parse date")
	}

	//find days and buckets to be scanned
	go func() {
		for i := startDay; i.Before(endDay) || i.Equal(endDay); i = i.Add(24 * time.Hour) {
			d := i.Format("2006-01-02")
			for j := s.config.BucketMin; j <= s.config.BucketMax; j++ {
				//find bucket owned by this ingester node
				if j%s.ingestCluster.NodesTotal == s.ingestCluster.NodeID {
					db := dayBucket{
						bucket: j,
						day:    d,
					}
					scannableDayBucket <- db
				}
			}
		}
		close(scannableDayBucket)
	}()

	var filesNum int32
	var wg sync.WaitGroup
	for i := 0; i < s.config.ParallelBuckets; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for db := range scannableDayBucket {
				// log.Printf("bucket %d day %s", db.bucket, db.day)
				files, err := s.logStorage.List(db.day, db.bucket)
				if err != nil {
					log.Printf("%s", err)
				}

				markedFiles := s.marker.GetMarkedFiles(db.day, db.bucket)

				for _, f := range files {
					// log.Printf("file from aws %s", f.name)
					if filesNum >= int32(s.config.MaxResults) {
						break
					}
					//check if file is not marked processed yet
					if _, marked := markedFiles[f.Name]; !marked {
						s.filesCh <- f
						atomic.AddInt32(&filesNum, 1)
					}
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(s.filesCh)
	}()

	return s.filesToLinesAsync()
}

func (s *Scanner) filesToLinesAsync() chan string {
	//read lines in several files and put them to linesCh channel for next stage
	var fileNum int32
	var dataSize int64
	linesCh := make(chan string, 2000)

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < s.fileLoadParallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range s.filesCh {
				// log.Printf("file %s", file.name)

				reader, err := s.logStorage.Get(file.Name)
				if err != nil {
					log.Print(errors.Wrap(err, "getting file reader"))
				}
				fz, err := gzip.NewReader(reader)
				if err != nil {
					log.Print(errors.Wrap(err, "getting gzip reader"))
				}
				r := bufio.NewReader(fz)
				lineCount := 0
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						if err != io.EOF {
							log.Print(errors.Wrap(err, "reading file line"))
						}
						break
					} else {
						linesCh <- line
						lineCount++
					}
				}
				fz.Close()
				reader.Close()
				atomic.AddInt32(&fileNum, 1)
				atomic.AddInt64(&dataSize, file.Size)
				log.Printf("Read file %s with %d lines. Total files read %d, total data %d MB, time %s\n", file.Name, lineCount, fileNum,
					dataSize/1024/1024, time.Since(start))

				//mark file as processed in db
				s.marker.Mark(file.Day, file.Bucket, file.Name)

			}
		}()
	}
	go func() {
		wg.Wait()
		close(linesCh)
	}()
	return linesCh
}
