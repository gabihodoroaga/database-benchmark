package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/lib/pq"
)

// PostgresStorageServer server
// docker run --name postgres-test -e POSTGRES_PASSWORD=example -e POSTGRES_USER=postgres -d -p 5432:5432 postgres
// docker exec -it postgres-test psql -U postgres -c 'create database test;'
type PostgresStorageServer struct {
	db *sql.DB
}

func (s *PostgresStorageServer) connect() func() {

	if connStr == "" {
		connStr = "dbname=test user=postgres password=example sslmode=disable"
	}
	var err error
	s.db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Println("docker run --name postgres -e POSTGRES_PASSWORD=example -e POSTGRES_USER=postgres -d -p 5432:5432 postgres")
		log.Println("docker exec -it postgres psql -U postgres -c 'create database test;'")
		log.Fatal(err)
	}

	queryTemplate = "SELECT * FROM " + tableName + " WHERE item1 = $1 AND load < $2 AND item2 = $3 AND item3 = $4 AND item4 = $5 ORDER BY load LIMIT 1;"

	log.Printf("Begin POSTGRES load test")
	log.Printf("Connection string: %v", connStr)

	return func() { s.db.Close() }
}

func (s *PostgresStorageServer) setup() {

	// create table
	stmnt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s  (
		id INTEGER PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		ip VARCHAR(100) NOT NULL,
		item1 INTEGER NOT NULL,
		item2 INTEGER NOT NULL,
		item3 INTEGER NOT NULL,
		item4 INTEGER NOT NULL,
		item5 INTEGER NOT NULL,
		load INTEGER NOT NULL
	);`, tableName)

	_, err := s.db.Exec(stmnt)
	if err != nil {
		log.Panic(err)
	}

	// create index if not exists
	stmnt = "CREATE INDEX IF NOT EXISTS item_idx_1 ON " + tableName + " (item1,item2,item3,item4,item5,load);"
	_, err = s.db.Exec(stmnt)
	if err != nil {
		log.Panic(err)
	}

	var count int
	err = s.db.QueryRow("SELECT count(id) FROM " + tableName).Scan(&count)

	if count >= noOfRecords {
		log.Printf("Table '%s' contains %d records; skiping loading data", tableName, count)
		return
	}

	start := count
	end := noOfRecords

	log.Printf("Begin loading records from %d to %d", start, end)
	tx, err := s.db.Begin()
	stmt, err := tx.Prepare(pq.CopyIn(tableName, "id", "name", "ip", "item1", "item2", "item3", "item4", "item5", "load"))
	if err != nil {
		log.Fatal(err)
	}

	rand.Seed(time.Now().UnixNano())

	for i := start; i < end; i++ {
		_, err = stmt.Exec(
			i,                      // id
			"item"+strconv.Itoa(i), // name
			generateRandomIP(),     // IP
			rand.Intn(noOfItems1),  // item1
			rand.Intn(noOfItems2),  // item2
			rand.Intn(noOfItems3),  // item3
			rand.Intn(noOfItems4),  // item4
			rand.Intn(noOfItems5),  // item5
			rand.Intn(90)+10,       // load
		)
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		fmt.Printf("Error commit: %v\n", err)
	}

}

func (s *PostgresStorageServer) queryExec(query string, args ...interface{}) (interface{}, error) {

	var item itemSQL
	err := s.db.QueryRow(query, args...,
	).Scan(&item.ID, &item.ItemName, &item.IP, &item.Item1, &item.Item2, &item.Item3, &item.Item4, &item.Item5, &item.Load)
	if err != nil {
		fmt.Printf("Error: %v", err)
	}
	return item, err
}

func (s *PostgresStorageServer) queryFormat(rowByID itemSQL) (string, []interface{}) {
	return queryTemplate, []interface{}{
		rowByID.Item1,
		90,
		rowByID.Item2,
		rowByID.Item3,
		rowByID.Item4,
	}
}

func (s *PostgresStorageServer) queryByID(docID int) itemSQL {
	var rowByID itemSQL
	err := s.db.QueryRow("SELECT * FROM "+tableName+" WHERE id = $1", docID).Scan(&rowByID.ID,
		&rowByID.ItemName, &rowByID.IP, &rowByID.Item1, &rowByID.Item2, &rowByID.Item3, &rowByID.Item4, &rowByID.Item5, &rowByID.Load)

	if err != nil {
		panic(err)
	}
	return rowByID
}
