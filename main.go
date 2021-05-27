package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	tableName       string = "demo"
	noOfItems1      int    = 2
	noOfItems2      int    = 65
	noOfItems3      int    = noOfItems2 * 11
	noOfItems4      int    = noOfItems2 * 5
	noOfItems5      int    = 20000
	noOfSamples     int    = 30
	noOfSamplesLoad int    = 10000
)

var (
	connStr       string
	queryTemplate string
	noOfRecords   int = 1_000_000
	noOfWorkers   int = 10
	batchDuration int = 5  // in seconds
	loadDuration  int = 30 // in seconds

	queryDataChan chan dbQuery
	statsChan     chan stats

	wgWorker sync.WaitGroup
	wgStats  sync.WaitGroup
	server   storageServer
)

type storageServer interface {
	connect() func()
	setup()
	queryExec(string, ...interface{}) (interface{}, error)
	queryFormat(itemSQL) (string, []interface{})
	queryByID(int) itemSQL
}

func main() {

	// setup logger
	defer setupLogger().Close()

	// read the flags
	var typeRedis = flag.Bool("redis", false, "Test redis server")
	var typePostgres = flag.Bool("postgres", false, "Test postgres server")
	var typeReindexer = flag.Bool("reindexer", false, "Test reindexer server")
	flag.StringVar(&connStr, "conn", "", "Connection string - value depends o the server type")
	flag.IntVar(&noOfRecords, "records", 1_000_000, "Total number of records. If the targer does not have the total number of records it will trigger a load")
	flag.IntVar(&noOfWorkers, "workers", 10, "Total number of workers")
	flag.IntVar(&batchDuration, "batch", 5, "The batch duration in seconds")
	flag.IntVar(&loadDuration, "load", 30, "The load duration in seconds")
	flag.Parse()

	if *typeRedis {
		server = &RedisStorageServer{}
	} else if *typePostgres {
		server = &PostgresStorageServer{}
	} else if *typeReindexer {
		server = &ReindexerStorageServer{}
	} else {
		flag.PrintDefaults()
		os.Exit(1)
	}

	// connect
	closeFunc := server.connect()
	defer closeFunc()

	server.setup()
	queryData()
	queryDataWithLoad(1)
	queryDataWithLoad(noOfWorkers)
}

func setupLogger() *os.File {
	logFile, err := os.Create(fmt.Sprintf("storage_%d.log", time.Now().Unix()))
	if err != nil {
		panic(err)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	return logFile
}

func queryDataWorker() {
	wgWorker.Add(1)
	for q := range queryDataChan {
		startTime := time.Now()

		_, err := server.queryExec(q.query, q.args...)
		if err != nil {
			log.Print(err)
		}

		duration := float64(time.Since(startTime)) / float64(time.Millisecond)
		statsChan <- stats{Duration: duration, Error: err != nil}
	}
	wgWorker.Done()
}

func printStarts(workerCount int) {
	wgStats.Add(1)

	data := make([]float64, 0)
	var errors, totalCount int64

	resetStats := func() {
		data = make([]float64, 0)
	}
	resetStats()
	warmUp := true

	ticker := time.NewTicker(time.Duration(batchDuration) * time.Second)
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-done:
				return
			case t := <-ticker.C:
				if !warmUp {
					dataCopy := data[:]
					count := len(dataCopy)
					sort.Sort(sort.Float64Slice(dataCopy))
					minTime := dataCopy[0]
					maxTime := dataCopy[count-1]
					totalTime := 0.0
					for i := range dataCopy {
						totalTime += dataCopy[i]
					}
					avgTime := totalTime / float64(count)
					maxTime99 := dataCopy[int(float64(count)*0.99)]
					maxTime90 := dataCopy[int(float64(count)*0.90)]

					log.Println("Tick at", t.Format("2020-01-01 11:00:00.000"))
					log.Println("================================")
					log.Printf("Query template        : %s", queryTemplate)
					log.Printf("No of workers         : %d", workerCount)
					log.Println("--------------------------------")
					log.Printf("No of queries batch   : %d\n", count)
					log.Printf("No of queries / sec   : %d\n", count/batchDuration)
					log.Printf("Min query time        : %.3fms\n", minTime)
					log.Printf("Max query time(100%%)  : %.3fms\n", maxTime)
					log.Printf("Max query time(99%%)   : %.3fms\n", maxTime99)
					log.Printf("Max query time(90%%)   : %.3fms\n", maxTime90)
					log.Printf("Avg query time        : %.3fms\n", avgTime)
					log.Printf("Total no of queries   : %d\n", totalCount)
					log.Printf("Total no of error     : %d\n", errors)
				}
				warmUp = false
				resetStats()
			}
		}
	}()

	for s := range statsChan {
		if !warmUp {
			data = append(data, s.Duration)
			totalCount++
			if s.Error {
				errors++
			}
		}
	}
	done <- true
	wgStats.Done()
}

func queryDataWithLoad(workerCount int) {
	log.Printf("Begin query data with load - %d worker(s) ...", workerCount)

	queryDataChan = make(chan dbQuery)

	for i := 0; i < workerCount; i++ {
		go queryDataWorker()
	}

	var testRows []itemSQL
	for i := 0; i < noOfSamplesLoad; i++ {
		docID := rand.Intn(noOfRecords)
		rowByID := server.queryByID(docID)

		if rowByID.Load >= 90 {
			continue
		}
		testRows = append(testRows, rowByID)
	}

	statsChan = make(chan stats)
	// start the stats worker
	go printStarts(workerCount)

	counter := 0
	for start := time.Now(); ; {
		fields := testRows[counter%(len(testRows)-1)]

		query, args := server.queryFormat(fields)

		queryDataChan <- dbQuery{query: query, args: args}
		counter++

		if counter&0x0f == 0 { // Check in every 16th iteration
			if time.Since(start) > time.Duration(loadDuration)*time.Second {
				break
			}
		}
	}
	close(queryDataChan)
	wgWorker.Wait()
	close(statsChan)
	wgStats.Wait()
}

func queryData() {
	log.Printf("Begin query data - 1 query / sec ...")
	var testRows []itemSQL
	for i := 0; i < noOfSamples+10; i++ {

		docID := rand.Intn(noOfRecords)
		rowByID := server.queryByID(docID)

		if rowByID.Load >= 90 {
			continue
		}
		testRows = append(testRows, rowByID)
	}

	mind := float64(-1)
	maxd := float64(-1)
	count := float64(0)
	total := float64(0)

	for range time.Tick(time.Second * 1) {
		if int(count) > noOfSamples {
			break
		}

		fields := testRows[int(count)]
		query, args := server.queryFormat(fields)

		startTime := time.Now()

		doc, err := server.queryExec(query, args...)
		if err != nil {
			log.Panic(err)
		}
		duration := float64(time.Since(startTime)) / float64(time.Millisecond)
		if mind < 0 || mind > duration {
			mind = duration
		}
		if maxd < 0 || maxd < duration {
			maxd = duration
		}

		log.Printf("time: %f ms, doc %v,\n", duration, doc)
		total += duration
		count++
	}
	log.Printf("No of samples: %d\n", int(count-1))
	log.Printf("Min query time: %fms\n", mind)
	log.Printf("Max query time: %fms\n", maxd)
	log.Printf("Avg query time: %fms\n", total/count)
	log.Printf("Done")
}

// TODO: reindexer does not work with mutiple tags
type itemSQL struct {
	ID       int    `reindex:"id,,pk"`
	ItemName string `reindex:"itemName"`
	IP       string `reindex:"ip"`
	Item1    int    `reindex:"item1,hash"`
	Item2    int    `reindex:"item2,hash"`
	Item3    int    `reindex:"item3,hash"`
	Item4    int    `reindex:"item4,hash"`
	Item5    int    `reindex:"item5,hash"`
	Load     int    `reindex:"load,tree"`
}

func (item *itemSQL) DeepCopy() interface{} {
	copyItem := &itemSQL{
		ID:       item.ID,
		ItemName: item.ItemName,
		IP:       item.IP,
		Item1:    item.Item1,
		Item2:    item.Item2,
		Item3:    item.Item3,
		Item4:    item.Item4,
		Item5:    item.Item5,
		Load:     item.Load,
	}
	return copyItem
}

type stats struct {
	Duration float64
	Error    bool
}

type dbQuery struct {
	query string
	args  []interface{}
}

func generateRandomIP() string {
	ip := ""
	rand.Seed(time.Now().UnixNano())
	ip += strconv.Itoa(rand.Intn(254)+1) + "." + strconv.Itoa(rand.Intn(254)+1) + "." + strconv.Itoa(rand.Intn(254)+1) + "." + strconv.Itoa(rand.Intn(254)+1)
	return ip
}
