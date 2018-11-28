package translator

import (
	"log"
	"sync"

	"github.com/pkg/errors"

	"github.com/dmibor/pilosa-ingester/mappingstorage"
)

//- this translator stores all mappings in memory and saves to mappings store every time new mappings found
//- after startup loads all existing mappings from mappings store
// MapFieldTranslator is an in-memory implementation of Translator using
// sync.Map.
type MapFieldTranslator struct {
	m sync.Map
	l sync.RWMutex
	s map[uint64]string

	idGen idGenerator
	store *mappingstorage.PostgresStore
}

// NewMapFieldTranslator creates a new MapFrameTranslator.
func newMapFieldTranslator(field string, indexName string, postgresConfig *mappingstorage.PostgresConfig) (Translator, error) {
	var initID uint64

	t := &MapFieldTranslator{
		s:     make(map[uint64]string, 0),
		store: mappingstorage.NewPostgresStore(postgresConfig, field, indexName),
	}

	initID = initMappingsFromMappingStore(t)
	t.idGen = newSequentialIDGen(initID)
	return t, nil
}

func initMappingsFromMappingStore(t *MapFieldTranslator) uint64 {
	//recreate mappings from local file
	idMap := t.store.GetAll()
	if len(idMap) == 0 {
		return 0 //if empty should start from 0
	}
	var max uint64
	for k, v := range idMap {
		t.m.Store(k, v)
		t.s[v] = k
		if max < v {
			max = v
		}
	}
	return max + 1
}

// Get returns the value mapped to the given id.
func (m *MapFieldTranslator) Get(pilosaID uint64) (string, error) {
	m.l.RLock()
	defer m.l.RUnlock()
	return m.s[pilosaID], nil
}

// GetID returns the integer id associated with the given value. It allocates a
// new ID if the value is not found.
func (m *MapFieldTranslator) GetID(externalID string) (id uint64, err error) {
	if idv, ok := m.m.Load(externalID); ok {
		if id, ok = idv.(uint64); !ok {
			return 0, errors.Errorf("Got non uint64 value back from MapTranslator: %v", idv)
		}
		return id, nil
	}
	m.l.Lock()
	defer m.l.Unlock()
	//save mapping to mappings store, try until successful, fail hard after 10000 tries
	for i := 0; i < 10000; i++ {
		id, err = m.store.Get(externalID)
		if err != nil && err != mappingstorage.ErrNoRows {
			return 0, errors.Wrap(err, "getting pilosa ID from mappings store")
		}
		if err == nil {
			m.s[id] = externalID
			m.m.Store(externalID, id)
			return id, nil
		}
		//nothing found in DB
		//if err == ErrNoRows
		nextID := m.idGen.generateID()

		//NOTE
		//this logic heavily relies on postgresDB guarantee not to insert duplicate values into both pilosaIDs(as part of primary key)
		// and externalIDs (as UNIQUE constraint)
		//TODO low priority - improve this, long warm up after startup, very db dependent

		if m.store.Put(nextID, externalID) {
			m.s[nextID] = externalID
			m.m.Store(externalID, nextID)
			return nextID, nil
		}
		//TODO remove temp monitoring
		log.Printf("Temp_mappings_monitor: failed to commit id, try = %d, id = %d, external_id=%s", i, nextID, externalID)
	}
	return 0, errors.New("Couldn't commit new id to mappings store after 10000 tries")
}

func (m *MapFieldTranslator) Close() error {
	return m.store.Close()
}
