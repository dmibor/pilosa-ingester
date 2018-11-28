package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/dmibor/pilosa-ingester/clustering"
	"github.com/dmibor/pilosa-ingester/file-marker"
	"github.com/dmibor/pilosa-ingester/indexer"
	"github.com/dmibor/pilosa-ingester/ingester"
	"github.com/dmibor/pilosa-ingester/logstorage"
	"github.com/dmibor/pilosa-ingester/mappingstorage"
	"github.com/dmibor/pilosa-ingester/proxy-translator"
	"github.com/dmibor/pilosa-ingester/scanner"
	"github.com/dmibor/pilosa-ingester/translator"
	gopilosa "github.com/pilosa/go-pilosa"

	"github.com/pkg/errors"
)

//NOTE decided keep usage of Pilosa PDK to a minimum, since
//on numerous occasions found out that it's development is at least a couple of months behind
// more frequently used repos like Pilosa and go-pilosa
func main() {
	var linesCh chan string
	stopSignal := make(chan struct{})

	//for pprof profiling
	if flags.enablePprof {
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	config, err := parseConfig(flags.configFile)
	if err != nil {
		log.Fatal(err)
	}

	var mappingDir string
	if config.WorkingDir == "" {
		log.Fatal("Working directory not provided")
	}
	//find dir with local column mappings
	if flags.refresh {
		log.Printf("Deleting all mappings for index %s in working dir %s", flags.indexName, config.WorkingDir)
		deleteColumnMappingDir(flags.indexName, config.WorkingDir)
		mappingDir = createNewDir(flags.indexName, config.WorkingDir)
	} else {
		mappingDir, err = findColumnMappingsDir(config.WorkingDir, flags.indexName)
		if err != nil {
			log.Fatal(err)
		}
		//get mapping dir abs path
		log.Printf("Taking columns mapping from dir %s", mappingDir)
	}
	client, err := setupPilosaClient(config.PilosaHosts)
	if err != nil {
		log.Fatal(err)
	}
	//gather info about Pilosa cluster
	cluster, err := clustering.NewPilosaCluster(client, config.ShardWidth, flags.indexName)
	if err != nil {
		log.Fatal(err)
	}
	convConfig := mappingstorage.PostgresConfig(config.Postgres)
	t, err := translator.NewIndexTranslator(mappingDir, flags.indexName, client, cluster, &config.IngestCluster,
		&convConfig, "sfield", "ofield", "cfield")
	if err != nil {
		log.Fatal(err)
	}

	var proxy *http.Server

	shutdownHooks(proxy, stopSignal)

	switch true {
	case flags.doUpload:
		fileStateMarker := filemarker.NewFileStateMarker(config.Postgres, flags.indexName, config.IngestCluster.NodeID)
		//do refresh
		if flags.refresh {
			log.Printf("Refreshing processed files marks for index %s", flags.indexName)
			//refresh marks owned by this node
			fileStateMarker.RefreshMarks()
		}
		logStorage := logstorage.NewLogStorage(&config.LogStorage)
		scanner := scanner.NewScanner(&config.Scanner, logStorage, fileStateMarker, &config.IngestCluster)
		linesCh = scanner.Scan()
		//get structure that can work with pilosa index, create columns, load data, etc
		indexer, err := setupPilosa(flags.indexName, flags.batchSize, flags.threadsPerField, client)
		if err != nil {
			log.Fatal(err)
		}
		ingester := ingester.NewIngester(linesCh, indexer, t, stopSignal)

		//////////////////////////////////////////
		//start ingesting data
		/////////////////////////////////////////
		err = ingester.Run()
		if err != nil {
			log.Fatal(err)
		}

	case flags.startProxy:
		var wg sync.WaitGroup
		wg.Add(1)
		//init proxy that can accept queries with strings values and then convert them to internal pilosa column and row ids
		proxy = &http.Server{
			Addr:    config.MappingProxyAddr,
			Handler: proxytranslator.NewPilosaForwarder(config.PilosaHosts[0], t),
		}

		go func() {
			defer wg.Done()
			err := proxy.ListenAndServe()
			if err != nil {
				log.Printf("proxy closed: %v", err)
			}
		}()

		wg.Wait()
	}

}
func shutdownHooks(proxy *http.Server, stopSignal chan struct{}) {
	//shutdown hook
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	go func() {
		<-c
		log.Println("Received interrupt signal - shutting down")
		close(stopSignal)
		//shutdown proxy
		if proxy != nil {
			if err := proxy.Shutdown(nil); err != nil {
				log.Fatal(err)
			}
		}
	}()
}

func findColumnMappingsDir(dir string, indexName string) (string, error) {
	dirs, err := filepath.Glob(dir + "/" + indexName + "_mappings*")
	if err != nil || len(dirs) != 1 {
		return "", errors.New("corrupted mappings dirs")
	}
	return dirs[0], nil
}

var flags struct {
	refresh         bool
	indexName       string
	configFile      string
	threadPerField  int
	batchSize       int
	doUpload        bool
	startProxy      bool
	threadsPerField int
	enablePprof     bool
}

func init() {
	flag.BoolVar(&flags.refresh, "refresh", false, "true to delete all stored mappings and processed files marks, use when starting import from scratch")
	flag.StringVar(&flags.indexName, "index", "_test", "index to use, creates new index if does not exist")
	flag.StringVar(&flags.configFile, "conf", "config_test.toml", "toml config file path")
	flag.IntVar(&flags.threadsPerField, "fieldT", 1, "Number of threads used to set up each field import process")
	flag.IntVar(&flags.batchSize, "batch", 1000000, "Number of set bit statements to collect in one batch before sending to pilosa")
	flag.BoolVar(&flags.doUpload, "upload", false, "true to start upload from s3")
	flag.BoolVar(&flags.startProxy, "proxy", false, "true to start translation proxy for pilosa based on existing mappings")
	flag.BoolVar(&flags.enablePprof, "pprof", false, "true to enable pprof profiling endpoint at localhost:6060")
	flag.Parse()

	log.Printf("Flags used:\nrefresh=%t\nindexName=%s\nconfigFile=%s\n"+
		"threadsPerField=%d\nbatchSize=%d\ndoUpload=%t\nstartProxy=%t\nenablePprof=%t",
		flags.refresh, flags.indexName, flags.configFile, flags.threadsPerField,
		flags.batchSize, flags.doUpload, flags.startProxy, flags.enablePprof,
	)
}

func createNewDir(indexName string, workingDir string) string {

	newDir, err := ioutil.TempDir(workingDir, indexName+"_mappings")
	if err != nil {
		log.Fatal(errors.Wrap(err, "creating mapping dir"))
	}
	return newDir
}

func deleteColumnMappingDir(index string, dir string) {

	dirs, err := filepath.Glob(dir + "/" + index + "_mappings*")
	if err != nil {
		panic(err)
	}
	for _, f := range dirs {
		if err := os.RemoveAll(f); err != nil {
			panic(err)
		}
	}
}

func deleteCurrentIndex(indexName string, pilosaHosts []string, client *gopilosa.Client) {
	schema := gopilosa.NewSchema()
	index := schema.Index(indexName)

	//check index exist
	serverSchema, err := client.Schema()
	if err != nil {
		log.Fatal(errors.Wrap(err, "check index exist to delete it"))
	}
	indexes := serverSchema.Indexes()
	if _, ok := indexes[indexName]; !ok {
		return
	}
	err = client.DeleteIndex(index)
	if err != nil {
		log.Fatal(errors.Wrap(err, "deleting index"))
	}
}

func setupPilosaClient(hosts []string) (*gopilosa.Client, error) {
	client, err := gopilosa.NewClient(hosts,
		gopilosa.OptClientSocketTimeout(time.Minute*60),
		gopilosa.OptClientConnectTimeout(time.Second*60))
	if err != nil {
		return nil, errors.Wrap(err, "creating pilosa clustering client")
	}
	return client, nil
}

func setupPilosa(indexName string, batchsize int, threads int, client *gopilosa.Client) (*indexer.Indexer, error) {
	schema := gopilosa.NewSchema()

	//for now not using the feature to do translation of string column IDs and string field row IDs to numberis IDs
	//at the Pilosa clustering itself (i.e. keys option below), since it's not working properly.
	//it can't properly calculate shard in the go-pilosa client, always sends import to shard 0. And Pilosa server
	//don't have any functionality yet to forward imports to different node if receiving node is not the owner of the shard.

	index := schema.Index(indexName)
	//time quantum param determines what views pilosa would store for the field. Using
	sfield := index.Field("sfield", gopilosa.OptFieldTypeTime(gopilosa.TimeQuantumMonthDay))
	cfield := index.Field("cfield", gopilosa.OptFieldTypeTime(gopilosa.TimeQuantumMonthDay))
	ofield := index.Field("ofield", gopilosa.OptFieldTypeTime(gopilosa.TimeQuantumMonthDay))

	indexer := indexer.NewIndexer(client, index)
	err := client.SyncSchema(schema)
	if err != nil {
		return nil, errors.Wrap(err, "synchronizing schema")
	}

	err = indexer.SetupField(sfield, batchsize, threads)
	if err != nil {
		return nil, errors.Wrapf(err, "setting up field '%s'", sfield.Name())
	}
	err = indexer.SetupField(cfield, batchsize, threads)
	if err != nil {
		return nil, errors.Wrapf(err, "setting up field '%s'", cfield.Name())
	}
	err = indexer.SetupField(ofield, batchsize, threads)
	if err != nil {
		return nil, errors.Wrapf(err, "setting up field '%s'", ofield.Name())
	}

	return indexer, nil

}
