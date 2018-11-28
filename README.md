This repo was created as a part of our attempts to use Pilosa for our real-world dataset with real-world patterns and problems hidden inside it. 
It contains some of the features we needed to make that happen.

## Features:
- New Pilosa column ids can be generated in several shards that would go to different Pilosa nodes at the same time. 
Helps to avoid hot nodes when mapping string data to sequential Pilosa column ids.
- Clustering logic for Ingester nodes, based on Pilosa shards. Data should be routed based on Pilosa column id to particular Ingester node.
Data can be ingested by the cluster of Ingester nodes - each node would own particular set of shards and mappings of these shards. Field mappings would be stored in common DB.
- Some additional caching to improve Ingester node throughput limited by local storage latency.
Our experience shows that time to query local storage is a real bottleneck when pushing for high upload throughput for data that is hard to just hash to Pilosa internal ID and has a lot of duplicate set bit operations for same Pilosa column ID.
- Ability to continue stopped or crashed ingestion from where it was left off. 

## To start:
```shell 
./pilosa-ingester -upload -refresh -pprof -conf=<config_file_path> -batch=1000000 -fieldT=64 -index=<index_name>
```

## Config file

Required config file looks like this 
```
shard_width=1048576 #2^20
working_dir="/tmp/" #working dir for mappings files
pilosa_hosts=["localhost:10101","localhost:10102"] #list of pilosa cluster hosts
mapping_proxy_addr="localhost:13999" #if mappings proxy for queries needed

#scanner part is specific to our S3 formats 
[scanner]
start_date="2018-07-01"
end_date="2018-07-01"
max_results=600000000

#S3 data log files storage configs 
[logstorage]
bucket_name="sample-bucket"
bucket_region="ap-southeast-1"
aws_key=""
aws_secret=""

#postgres is used to store field mappings of string ids to pilosa row IDs + to store processed files marks
[postgres]
hosts="localhost"
port=5432
user="postgres"
database_name="postgres"

#basic ingester cluster configs
[ingest_cluster]
node_id=0 #id of current node
nodes_total=1 #total number of nodes in ingestion cluster
```

## Postgres setup

This repo uses two postgres tables:
```
create table pilosa_field_mappings (index text, field CHAR(10), pilosa_id int, external_id text, UNIQUE (index,field,external_id), primary key(index,field,pilosa_id));
create table pilosa_marker (day CHAR(10), bucket int, index text, file text, owner_node int, primary key(index,day, bucket,file));
```