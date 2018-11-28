package translator

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"

	"github.com/dmibor/pilosa-ingester/clustering"
	gopilosa "github.com/pilosa/go-pilosa"
)

type idGenerator interface {
	generateID() uint64
}

//simple sequential threadsafe id generator
type sequentialIDGen struct {
	id uint64
}

func newSequentialIDGen(initID uint64) idGenerator {
	return &sequentialIDGen{
		id: initID,
	}
}

func (g *sequentialIDGen) generateID() uint64 {
	new := atomic.AddUint64(&g.id, 1)
	return new - 1
}

//multiShardIDGen is generating IDs that belong to several Pilosa shards at
//the same time in round-robin.
//Shards for ids generation are chosen in a way that when these shards are submitted to the Pilosa cluster one
//after another - they should all go to different nodes
type multiShardIDGen struct {
	idsCh        chan uint64
	shardWidth   uint64
	generatedIDs []*IDInShard
	r            nodeRRobinRotator
	sn           shardNexter
}

func newMultiShardIDGen(cluster *clustering.PilosaCluster, indexName string, client *gopilosa.Client,
	shards *leveldb.DB, icluster *clustering.IngestClusterConfig) (idGenerator, error) {

	m := &multiShardIDGen{
		idsCh:      make(chan uint64, 1024),
		shardWidth: cluster.ShardWidth,
		r: nodeRRobinRotator{
			curIndex:   0,
			nodes:      cluster.NodeIDs,
			replFactor: cluster.ReplFactor,
		},
		sn: shardNexter{
			endpoint:      "/internal/fragment/nodes?shard=%d&index=%s",
			indexName:     indexName,
			client:        client,
			loadedShards:  make(map[string][]uint64),
			shardsCh:      make(chan *nodesOfShard, 128),
			ingestCluster: icluster,
		},
		//will generate ids in shards that would go to all nodes in the clustering simultaneously
		//nil slice elements would get appropriate shards during initialization or in m.start()
		generatedIDs: make([]*IDInShard, len(cluster.NodeIDs)),
	}

	//find unfinished shards
	iter := shards.NewIterator(nil, nil)
	initShardState, excludeSet := initIDsFromLocalStore(iter, cluster.ShardWidth)
	m.sn.excludeSet = excludeSet

	m.init(initShardState)
	m.start()

	//TODO remove this temp monitoring
	go func() {
		for {
			fmt.Print("\nShard IDS status\n")
			for i, v := range m.generatedIDs {
				if v == nil {
					continue
				}
				fmt.Printf("Shard%dnum = %d, id = %d\n", i, v.Shard, v.CurID)
			}
			time.Sleep(10 * time.Second)
		}
	}()

	return m, nil
}

//init Shard state, so that id generation can start from where it left off
func (m *multiShardIDGen) init(initShardState []IDInShard) {
	s := make(map[uint64]struct{})

	for _, v := range initShardState {
		s[v.Shard] = struct{}{}
	}

	var i int
	for i = range m.generatedIDs {
		if i > len(initShardState)-1 {
			break
		}
		m.generatedIDs[i] = &initShardState[i]
	}
	//log if there are more unfinished shards that nodes in current clustering
	//we will drop extra unfinished shards
	for i := len(initShardState) - 1; i > len(m.generatedIDs)-1; i-- {
		log.Printf("ERROR shard %d is unfinished, last id=%d, but dropped since not enough nodes", initShardState[i].Shard, initShardState[i].CurID)
	}
}

func (m *multiShardIDGen) start() {
	//func that would calculate ids for loaded shards
	go func() {
		//round-robin id generation for all chosen shards
		for {
			for i, v := range m.generatedIDs {
				if v == nil {
					//replace nil shards
					var err error
					v, err = m.nextShard()
					if err != nil {
						//TODO handle this properly
						panic(err)
					}
					m.generatedIDs[i] = v
				}
				if v.CurID >= m.shardWidth {
					//out of ids, mark for replacement
					m.generatedIDs[i] = nil
					continue
				}

				m.idsCh <- v.CurID + v.Shard*m.shardWidth
				v.CurID++
			}

		}
	}()

	//start shard loading from pilosa clustering async
	go m.sn.loadShards()
}

//not thread-safe
//uses nodeRRobinRotator to make sure that next Shard for id creation is taken so that load to clustering
//distributed evenly at least during new ids creation
//e.g. if there are 3 nodes: node1, node2, node3, with replication factor 2
//first shards chosen goes to node1, node2,
//second Shard chosen - node2, node3
//third Shard chosen - node3,node1
func (m *multiShardIDGen) nextShard() (*IDInShard, error) {
	nextNodes := m.r.nextNodeBunch()
	shard := m.sn.nextShardForNodes(nextNodes)

	//TODO remove this temp monitoring
	log.Printf("Temp_monitor: Shard returned = %d, for nodes = %s", shard, strings.Join(nextNodes, ","))

	return &IDInShard{
		Shard: shard,
		CurID: 0,
	}, nil
}

func (m *multiShardIDGen) generateID() uint64 {
	id := <-m.idsCh
	return id
}

func initIDsFromLocalStore(iter iterator.Iterator, shardWidth uint64) ([]IDInShard, map[uint64]struct{}) {
	exclude := make(map[uint64]struct{})
	var unfinished []IDInShard

	if !iter.Last() {
		return nil, nil
	}
	var shard, id uint64
	for {
		if iter.Key() == nil {
			//done
			break
		}
		shard = binary.BigEndian.Uint64(iter.Key())
		exclude[shard] = struct{}{}
		id = (binary.BigEndian.Uint64(iter.Value()) + 1) % shardWidth //next available id in shard
		if id == 0 {
			iter.Prev()
			continue
		}
		t := IDInShard{
			Shard: shard,
			CurID: id,
		}
		unfinished = append(unfinished, t)

		iter.Prev()
	}
	var str string
	for _, v := range unfinished {
		str = str + fmt.Sprintf("Shard=%d with ID=%d\n", v.Shard, v.CurID)
	}
	log.Printf("Unfinished shards loaded from local store:\n%s", str)

	return unfinished, exclude
}

type fragmentNodeRoot struct {
	ID string `json:"id"`
}

type IDInShard struct {
	Shard uint64
	CurID uint64
}

//would return replFactor number of nodes sequence from []nodes, rotating it in round-robin fashion
//every time nextNodeBunch() is called
type nodeRRobinRotator struct {
	curIndex   int
	nodes      []string
	replFactor int
}

func (ns *nodeRRobinRotator) nextNodeBunch() []string {
	var nodeIDs []string
	nodeNum := len(ns.nodes)
	for i := 0; i < ns.replFactor; i++ {
		nodeIDs = append(nodeIDs, ns.nodes[(ns.curIndex+i)%nodeNum])
	}
	ns.curIndex = (ns.curIndex + 1) % nodeNum
	return nodeIDs
}

type nodesOfShard struct {
	shard uint64
	nodes []string
}

type shardNexter struct {
	loadedShards map[string][]uint64
	endpoint     string
	indexName    string
	client       *gopilosa.Client
	shardsCh     chan *nodesOfShard

	//params used to restore state and be able to start from where previously left off.
	//need to explicitly store detailed state here since can not rely on any
	//previous Shard / node ownerships, since that could've changed, clustering might've
	//changed its configuration
	excludeSet map[uint64]struct{}

	ingestCluster *clustering.IngestClusterConfig
}

//returns next unused empty shard that belongs to nodes (nodes)
func (s *shardNexter) nextShardForNodes(nodes []string) uint64 {
	getNext := func(nodes []string) uint64 {
		//sort for correct lookup
		sort.Strings(nodes)
		key := strings.Join(nodes, ",")
		if v, ok := s.loadedShards[key]; ok {
			//return first elem and return
			if len(v) == 1 {
				delete(s.loadedShards, key)
			} else {
				s.loadedShards[key] = v[1:]
			}
			return v[0]
		}
		//if not found in loadedShards - churn through next shards until needed Shard found
		for {
			nodesOfShard := <-s.shardsCh

			sort.Strings(nodesOfShard.nodes)
			//is it what we are looking for?
			if sliceEqual(nodesOfShard.nodes, nodes) {
				//yes
				return nodesOfShard.shard
			}
			//no, save and load nodes for next Shard
			key = strings.Join(nodesOfShard.nodes, ",")

			if v, ok := s.loadedShards[key]; !ok {
				var t []uint64
				t = append(t, nodesOfShard.shard)
				s.loadedShards[key] = t
			} else {
				v = append(v, nodesOfShard.shard)
				s.loadedShards[key] = v
			}
		}
	}

	//determine shard ownership in the ingest cluster
	//always take only (nodeID)-th shard, since that is the one owned by this ingester node
	var myShard uint64
	for {
		for i := 0; i < s.ingestCluster.NodesTotal; i++ {
			if i == s.ingestCluster.NodeID {
				myShard = getNext(nodes)
				continue
			}
			getNext(nodes)
		}
		//skip shards used in previous runs
		if _, ok := s.excludeSet[myShard]; !ok {
			break
		}
	}
	return myShard
}

//func that would load shards from pilosa clustering
func (s *shardNexter) loadShards() {

	var shardToLoad uint64
	for {

		forThisShard, err := s.loadNodesForShard(shardToLoad)
		if err != nil {
			//TODO handle this better
			panic(err)
		}
		s.shardsCh <- &nodesOfShard{
			shardToLoad,
			forThisShard,
		}
		shardToLoad++
	}
}

func sliceEqual(s1 []string, s2 []string) bool {
	if s1 == nil || s2 == nil {
		panic("can be no nil slices")
	}
	if len(s1) != len(s2) {
		return false
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}

func (s *shardNexter) loadNodesForShard(shard uint64) ([]string, error) {
	path := fmt.Sprintf(s.endpoint, shard, s.indexName)
	_, body, err := s.client.HttpRequest("GET", path, []byte{}, nil)
	if err != nil {
		return nil, errors.Wrap(err, "clustering querying")
	}
	var fragmentNodes []fragmentNodeRoot
	err = json.Unmarshal(body, &fragmentNodes)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling fragment node URIs")
	}
	var res []string
	for _, node := range fragmentNodes {
		res = append(res, node.ID)
	}
	return res, nil
}
