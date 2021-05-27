package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/RediSearch/redisearch-go/redisearch"
)

// RedisStorageServer redis implementation
// docker run -it --name redis-search-2 -p 6379:6379 redislabs/redisearch:2.0.0
type RedisStorageServer struct {
	client *redisearch.Client
}

func (s *RedisStorageServer) connect() func() {

	if connStr == "" {
		connStr = "localhost:6379"
	}
	s.client = redisearch.NewClient(connStr, tableName)

	queryTemplate = "@item1:[%d %d] @item2:[%d %d] @item3:[%d %d] @item4:[%d %d] @load:[0 90]"

	log.Printf("Begin REDIS load test")
	log.Printf("Connection string: %v", connStr)

	return func() {}
}

func (s *RedisStorageServer) setup() {
	log.Printf("Begin create index '%s'...", tableName)
	// check if index exists
	indexInfo, err := s.client.Info()
	if err == nil {
		log.Printf("Index '%s' exists; skiping create index", tableName)
	} else {

		// Create a schema
		sc := redisearch.NewSchema(redisearch.DefaultOptions).
			AddField(redisearch.NewTextField("name")).
			AddField(redisearch.NewTextField("ip")).
			AddField(redisearch.NewNumericField("item1")).
			AddField(redisearch.NewNumericField("item2")).
			AddField(redisearch.NewNumericField("item3")).
			AddField(redisearch.NewNumericField("item4")).
			AddField(redisearch.NewNumericField("item5")).
			AddField(redisearch.NewNumericField("load"))

		// Create the index with the given schema
		if err := s.client.CreateIndex(sc); err != nil {
			log.Panic(err)
		}
		log.Printf("Done\n")
	}

	log.Printf("Begin load index '%s' ...", tableName)
	if indexInfo != nil && indexInfo.DocCount >= uint64(noOfRecords) {
		log.Printf("Index '%s' contains %d records; skiping loading data", tableName, int(indexInfo.DocCount))
		return
	}
	start := 0
	if indexInfo != nil {
		start = int(indexInfo.DocCount)
	}
	end := noOfRecords
	log.Printf("Begin loading records from %d to %d", start, end)
	rand.Seed(time.Now().UnixNano())
	batchSize := 5000
	docs := make([]redisearch.Document, 0, batchSize)
	for i := start; i < end; i++ {
		doc := redisearch.NewDocument("item:"+strconv.Itoa(i), 1.0)

		doc.Set("name", "item"+strconv.Itoa(i)).
			Set("ip", generateRandomIP()).
			Set("item1", rand.Intn(noOfItems1)).
			Set("item2", rand.Intn(noOfItems2)).
			Set("item3", rand.Intn(noOfItems3)).
			Set("item4", rand.Intn(noOfItems4)).
			Set("item5", rand.Intn(noOfItems5)).
			Set("load", rand.Intn(90)+10)

		docs = append(docs, doc)
		if len(docs) == batchSize {
			if err := s.client.Index(docs...); err != nil {
				log.Panic(err)
			}
			docs = make([]redisearch.Document, 0, batchSize)
		}
	}
	// add remaining document
	if err := s.client.Index(docs...); err != nil {
		log.Panic(err)
	}

	log.Printf("Done\n")
}

func (s *RedisStorageServer) queryFormat(fields itemSQL) (string, []interface{}) {
	return fmt.Sprintf(queryTemplate,
		fields.Item1, fields.Item1, fields.Item2, fields.Item2, fields.Item3, fields.Item3, fields.Item4, fields.Item4), []interface{}{}
}

func (s *RedisStorageServer) queryExec(query string, args ...interface{}) (interface{}, error) {
	doc, _, err := s.client.Search(
		redisearch.NewQuery(query).
			SetSortBy("load", true).
			Limit(0, 1).
			SetReturnFields("name", "item1", "item2", "load"))
	return doc, err
}

func (s *RedisStorageServer) queryByID(docID int) itemSQL {
	doc, err := s.client.Get("item:" + strconv.Itoa(docID))

	if err != nil {
		log.Panic(err)
	}
	var rowByID itemSQL
	rowByID.ID = docID
	rowByID.ItemName = doc.Properties["name"].(string)
	rowByID.Item1, _ = strconv.Atoi(doc.Properties["item1"].(string))
	rowByID.Item2, _ = strconv.Atoi(doc.Properties["item2"].(string))
	rowByID.Item3, _ = strconv.Atoi(doc.Properties["item3"].(string))
	rowByID.Item4, _ = strconv.Atoi(doc.Properties["item4"].(string))
	rowByID.Item5, _ = strconv.Atoi(doc.Properties["item5"].(string))
	rowByID.Load, _ = strconv.Atoi(doc.Properties["load"].(string))
	return rowByID
}
