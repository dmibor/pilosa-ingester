package clustering

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	gopilosa "github.com/pilosa/go-pilosa"
)

type PilosaCluster struct {
	ReplFactor int
	NodeIDs    []string
	ShardWidth uint64
}

type Status struct {
	Nodes []Node `json:"nodes"`
}

type Node struct {
	ID string `json:"id"`
}

func NewPilosaCluster(client *gopilosa.Client, shardWidth uint64, indexName string) (*PilosaCluster, error) {

	nodes, err := fetchClusterNodes(client)
	if err != nil {
		return nil, errors.Wrap(err, "creating new cluster")
	}

	rf, err := fetchReplFactor(client, indexName)

	return &PilosaCluster{
		NodeIDs:    nodes,
		ReplFactor: rf,
		ShardWidth: shardWidth,
	}, nil
}

func fetchClusterNodes(client *gopilosa.Client) ([]string, error) {
	s, err := client.Status()
	if err != nil {
		return nil, errors.Wrap(err, "executing client Status()")
	}

	var nodes []string
	for _, node := range s.Nodes {
		nodes = append(nodes, node.ID)
	}
	return nodes, nil
}

//this is a hacky way to get replication factor - no better way in cluster API for now
func fetchReplFactor(client *gopilosa.Client, indexName string) (int, error) {
	path := fmt.Sprintf("/internal/fragment/nodes?shard=%d&index=%s", 0, indexName)
	_, body, err := client.HttpRequest("GET", path, []byte{}, nil)
	if err != nil {
		return 0, errors.Wrap(err, "cluster querying")
	}
	var fragmentNodes []struct{}
	err = json.Unmarshal(body, &fragmentNodes)
	if err != nil {
		return 0, errors.Wrap(err, "unmarshaling fragment node URIs")
	}

	return len(fragmentNodes), nil
}
