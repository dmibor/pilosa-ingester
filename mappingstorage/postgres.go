package mappingstorage

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"log"
)

type PostgresConfig struct {
	Host         string `toml:"hosts"`
	Port         int    `toml:"port"`
	User         string `toml:"user"`
	DatabaseName string `toml:"database_name"`
}

type PostgresStore struct {
	db        *sql.DB
	indexName string
	field     string
}

var ErrNoRows = errors.New("No Rows found")

func NewPostgresStore(config *PostgresConfig, field string, indexName string) *PostgresStore {

	db, err := newPostgresSetup(config)
	if err != nil {
		log.Fatal(err)
	}

	m := &PostgresStore{
		db:        db,
		indexName: indexName,
		field:     field,
	}

	return m
}

func (p *PostgresStore) Put(pilosaID uint64, externalID string) bool {
	_, err := p.db.Exec(`INSERT INTO pilosa_field_mappings (index,field,pilosa_id,external_id) VALUES ($1, $2, $3, $4)`, p.indexName, p.field, pilosaID, externalID)
	if err == nil {
		return true
	}
	pqe, ok := err.(*pq.Error)
	if ok != true {
		log.Fatalf("unexpected type, err=%s", err)
	}

	//duplicate entry error code for columns with UNIQUE postgres guarantee
	if string(pqe.Code) == "23505" {
		return false
	} else {
		log.Fatal("unexpected error code when inserting into Postgres store")
	}
	return false
}

func (p *PostgresStore) Get(externalID string) (uint64, error) {
	rows, err := p.db.Query(`SELECT pilosa_id FROM pilosa_field_mappings WHERE index=$1 and field=$2 and external_id=$3`, p.indexName, p.field, externalID)
	if err != nil {
		return 0, err
	}
	var id uint64
	//for pq driver this query does not return ErrNoRows - just returns empty rows, so special handling
	var hasData bool
	defer rows.Close()
	//take only first one
	if rows.Next() {
		hasData = true
		rows.Scan(&id)
	}
	err = rows.Err()
	if err != nil {
		return 0, err
	}
	if !hasData {
		return 0, ErrNoRows
	}
	return id, nil
}

func (p *PostgresStore) GetAll() map[string]uint64 {
	res := make(map[string]uint64)
	rows, err := p.db.Query(`SELECT pilosa_id,external_id FROM pilosa_field_mappings WHERE index=$1 and field=$2`, p.indexName, p.field)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var (
		pilosaID   uint64
		externalID string
	)
	for rows.Next() {
		rows.Scan(&pilosaID, &externalID)
		res[externalID] = pilosaID
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	return res
}

func (p *PostgresStore) DeleteAll() {

	_, err := p.db.Exec(`DELETE FROM pilosa_field_mappings WHERE index=$1`, p.indexName)
	if err != nil {
		log.Fatal(err)
		// log.Printf("Error marking %s:%d:%s:%s because %v", day, bucket, file, m.pilosaIndex, err)
		// return
	}
}

func (p *PostgresStore) Close() error {
	return p.db.Close()
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
