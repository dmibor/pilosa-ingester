package main

import (
	"github.com/BurntSushi/toml"

	"github.com/dmibor/pilosa-ingester/clustering"
	"github.com/dmibor/pilosa-ingester/file-marker"
	"github.com/dmibor/pilosa-ingester/logstorage"
	"github.com/dmibor/pilosa-ingester/scanner"
)

type Config struct {
	ShardWidth       uint64                         `toml:"shard_width"`
	WorkingDir       string                         `toml:"working_dir"`
	PilosaHosts      []string                       `toml:"pilosa_hosts"`
	MappingProxyAddr string                         `toml:"mapping_proxy_addr"`
	Scanner          scanner.Config                 `toml:"scanner"`
	LogStorage       logstorage.Config              `toml:"logstorage"`
	System           System                         `toml:"system"`
	Postgres         filemarker.PostgresConfig      `toml:"postgres"`
	IngestCluster    clustering.IngestClusterConfig `toml:"ingest_cluster"`
}

type System struct {
	GCPercent int `toml:"gc_percent"`
	DebugPort int `toml:"debug_port"`
}

func parseConfig(file string) (*Config, error) {
	conf := &Config{}
	_, err := toml.DecodeFile(file, conf)
	if err != nil {
		return nil, err
	}
	return conf, nil
}
