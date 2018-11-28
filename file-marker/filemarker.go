package filemarker

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

type PostgresConfig struct {
	Host         string `toml:"hosts"`
	Port         int    `toml:"port"`
	User         string `toml:"user"`
	DatabaseName string `toml:"database_name"`
}

//handles interaction with DB to mark processed files
type FileStateMarker struct {
	pilosaIndex string
	db          *sql.DB
	//node id in the ingestion cluster that owns this state marker
	ownerNode int
}

func NewFileStateMarker(config PostgresConfig, pilosaIndex string, ownerNode int) *FileStateMarker {

	db, err := newPostgresSetup(&config)
	if err != nil {
		log.Fatal(err)
	}

	m := &FileStateMarker{
		db:          db,
		pilosaIndex: pilosaIndex,
		ownerNode:   ownerNode,
	}

	return m
}

//returns "set" of files that are marked in this day and bucket
func (m *FileStateMarker) GetMarkedFiles(day string, bucket int) map[string]struct{} {
	res := make(map[string]struct{})
	rows, err := m.db.Query(`SELECT file FROM pilosa_marker WHERE day=$1  and bucket=$2 and index=$3`, day, bucket, m.pilosaIndex)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var file string
		rows.Scan(&file)
		res[file] = struct{}{}
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	return res
}

func (m *FileStateMarker) Mark(day string, bucket int, file string) {
	_, err := m.db.Exec(`INSERT INTO pilosa_marker (day, bucket, index, file, owner_node) VALUES ($1, $2, $3, $4, $5)`,
		day, bucket, m.pilosaIndex, file, m.ownerNode)
	if err != nil {
		log.Fatal(err)
		// log.Printf("Error marking %s:%d:%s:%s because %v", day, bucket, file, m.pilosaIndex, err)
		// return
	}
}

func (m *FileStateMarker) RefreshMarks() {
	_, err := m.db.Exec(`DELETE FROM pilosa_marker WHERE index=$1 and owner_node=$2`, m.pilosaIndex, m.ownerNode)
	if err != nil {
		log.Fatal(err)
		// log.Printf("Error marking %s:%d:%s:%s because %v", day, bucket, file, m.pilosaIndex, err)
		// return
	}
}

func (m *FileStateMarker) Close() {
	err := m.db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func newPostgresSetup(config *PostgresConfig) (*sql.DB, error) {
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.Host, config.Port, config.User, "", config.DatabaseName))
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}
