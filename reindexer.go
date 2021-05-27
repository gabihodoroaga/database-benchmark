package main

import (
	"errors"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/restream/reindexer"
)

// ReindexerStorageServer reindexer implementation
// docker run -p9088:9088 -p6534:6534 -it reindexer/reindexer
type ReindexerStorageServer struct {
	db *reindexer.Reindexer
}

func (s *ReindexerStorageServer) connect() func() {

	if connStr == "" {
		connStr = "cproto://user:pass@127.0.0.1:6534/testdb"
	}
	s.db = reindexer.NewReindex(connStr, reindexer.WithCreateDBIfMissing())
	err := s.db.OpenNamespace(tableName, reindexer.DefaultNamespaceOptions(), itemSQL{})
	if err != nil {
		log.Println("docker run -p9088:9088 -p6534:6534 -it reindexer/reindexer")
		log.Panic(err)
	}

	queryTemplate = "load<90,item1={},item2={},item3={},item4={}"

	log.Printf("Begin REINDEXER load test")
	log.Printf("Connection string: %v", connStr)

	return func() {}
}

func (s *ReindexerStorageServer) setup() {
	log.Printf("Begin create namespace '%s'...", tableName)

	q := s.db.Query(tableName)
	q.AggregateMax("id")
	iterator := q.Exec()
	defer iterator.Close()
	maxID := iterator.AggResults()[0]

	count := int(maxID.Value) + 1
	if count >= noOfRecords {
		log.Printf("Table '%s' contains %d records; skiping loading data", tableName, count)
		return
	}
	start := count
	end := noOfRecords

	log.Printf("Begin loading records from %d to %d", start, end)

	rand.Seed(time.Now().UnixNano())
	for i := start; i < end; i++ {

		err := s.db.Upsert(tableName, &itemSQL{
			ID:       i,
			ItemName: "item" + strconv.Itoa(i),
			IP:       generateRandomIP(),
			Item1:    rand.Intn(noOfItems1),
			Item2:    rand.Intn(noOfItems2),
			Item3:    rand.Intn(noOfItems3),
			Item4:    rand.Intn(noOfItems4),
			Item5:    rand.Intn(noOfItems1),
			Load:     rand.Intn(90) + 10,
		})
		if err != nil {
			panic(err)
		}
	}
}

func (s *ReindexerStorageServer) queryFormat(fields itemSQL) (string, []interface{}) {
	return "",
		[]interface{}{
			90, fields.Item1, fields.Item2, fields.Item3, fields.Item4,
		}
}

func (s *ReindexerStorageServer) queryExec(query string, args ...interface{}) (interface{}, error) {

	doc, found := s.db.Query(tableName).
		Sort("load", false).
		WhereInt("load", reindexer.LT, args[0].(int)).
		WhereInt("item1", reindexer.EQ, args[1].(int)).
		WhereInt("item2", reindexer.EQ, args[2].(int)).
		WhereInt("item3", reindexer.EQ, args[3].(int)).
		WhereInt("item4", reindexer.EQ, args[4].(int)).
		Limit(1).
		Offset(0).
		Get()

	if !found {
		return nil, errors.New("not found")
	}
	return doc, nil
}

func (s *ReindexerStorageServer) queryByID(docID int) itemSQL {

	elem, found := s.db.Query(tableName).
		Where("id", reindexer.EQ, docID).
		Get()

	if !found {
		log.Panic("id not found " + strconv.Itoa(docID))
	}
	doc := elem.(*itemSQL)
	return *doc
}
