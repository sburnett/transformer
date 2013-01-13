package transformer

import (
	"bytes"
	"container/heap"
	"expvar"
	"fmt"
	"github.com/jmhodges/levigo"
	"log"
	"math"
)

type LevelDbRecord struct {
	Key           []byte
	Value         []byte
	DatabaseIndex uint8
}

type LevelDbRecordSlice []*LevelDbRecord

func (p LevelDbRecordSlice) Len() int           { return len(p) }
func (p LevelDbRecordSlice) Less(i, j int) bool { return bytes.Compare(p[i].Key, p[j].Key) < 0 }
func (p LevelDbRecordSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type Transformer interface {
	Do(inputChan chan *LevelDbRecord, outputChan ...chan *LevelDbRecord)
}

var recordsRead, bytesRead, recordsWritten, bytesWritten *expvar.Int

func init() {
	recordsRead = expvar.NewInt("RecordsRead")
	recordsWritten = expvar.NewInt("RecordsWritten")
	bytesRead = expvar.NewInt("BytesRead")
	bytesWritten = expvar.NewInt("BytesWritten")
}

func readRecords(db *levigo.DB, databaseIndex uint8, firstKey, lastKey []byte, recordsChan chan *LevelDbRecord) {
	defer close(recordsChan)

	readOpts := levigo.NewReadOptions()
	defer readOpts.Close()
	it := db.NewIterator(readOpts)
	if firstKey == nil {
		it.SeekToFirst()
	} else {
		it.Seek(firstKey)
	}
	for ; it.Valid(); it.Next() {
		if lastKey == nil && !bytes.HasPrefix(it.Key(), firstKey) {
			break
		}
		if lastKey != nil && bytes.Compare(it.Key(), lastKey) > 0 {
			break
		}
		recordsChan <- &LevelDbRecord{Key: it.Key(), Value: it.Value()}
		recordsRead.Add(1)
		bytesRead.Add(int64(len(it.Key()) + len(it.Value())))
	}
	if err := it.GetError(); err != nil {
		log.Fatalf("Error iterating through database: %v", err)
	}
}

func demuxInputsSorted(inputChans []chan *LevelDbRecord, outputChan chan *LevelDbRecord) {
	defer close(outputChan)

	currentRecords := make(PriorityQueue, 0, len(inputChans))
	readRecord := func(inputChan chan *LevelDbRecord) {
		if record, ok := <-inputChan; ok {
			item := &Item{
				record:   record,
				channel:  inputChan,
				priority: Priority{key: record.Key, databaseIndex: record.DatabaseIndex},
			}
			heap.Push(&currentRecords, item)
		}
	}
	for _, inputChan := range inputChans {
		readRecord(inputChan)
	}
	for currentRecords.Len() > 0 {
		item := heap.Pop(&currentRecords).(*Item)
		outputChan <- item.record
		readRecord(item.channel)
	}
}

func writeRecords(db *levigo.DB, recordsChan chan *LevelDbRecord) {
	writeOpts := levigo.NewWriteOptions()
	defer writeOpts.Close()
	for record := range recordsChan {
		if err := db.Put(writeOpts, record.Key, record.Value); err != nil {
			log.Fatalf("Error writing to channel")
		}
		recordsWritten.Add(1)
		bytesWritten.Add(int64(len(record.Key) + len(record.Value)))
	}
}

// Run a transformer on the given input database, writing any emitted results to
// an output database. You may read and write to the same database.
//
// Only read keys between firstKey and lastKey, inclusive. If firstKey is nil,
// start at the first key. If lastKey is nil, only read keys that are prefixes
// of firstKey.  If both firstKey and lastKey are nil, read all keys.
func RunTransformer(transformer Transformer, inputDbPaths, outputDbPaths []string, firstKey, lastKey []byte) {
	if len(inputDbPaths) > math.MaxUint8 {
		panic(fmt.Errorf("Cannot read from more than %d databases", math.MaxUint8))
	}

	inputOpts := levigo.NewOptions()
	inputOpts.SetMaxOpenFiles(128)
	defer inputOpts.Close()
	databases := make(map[string]*levigo.DB)
	for _, inputDbPath := range inputDbPaths {
		inputDb, err := levigo.Open(inputDbPath, inputOpts)
		if err != nil {
			log.Fatalf("Error opening leveldb database %v: %v", inputDbPath, err)
		}
		defer inputDb.Close()
		databases[inputDbPath] = inputDb
	}
	outputOpts := levigo.NewOptions()
	outputOpts.SetMaxOpenFiles(128)
	outputOpts.SetCreateIfMissing(true)
	for _, outputDbPath := range outputDbPaths {
		_, ok := databases[outputDbPath]
		if !ok {
			outputDb, err := levigo.Open(outputDbPath, outputOpts)
			if err != nil {
				log.Fatalf("Error opening leveldb database %v: %v", outputDbPath, err)
			}
			defer outputDb.Close()
			databases[outputDbPath] = outputDb
		}
	}

	inputChan := make(chan *LevelDbRecord)
	outputChans := make([]chan *LevelDbRecord, len(outputDbPaths))
	for i := 0; i < len(outputDbPaths); i++ {
		outputChans[i] = make(chan *LevelDbRecord)
	}

	if len(inputDbPaths) == 1 {
		go readRecords(databases[inputDbPaths[0]], 0, firstKey, lastKey, inputChan)
	} else {
		inputChans := make([]chan *LevelDbRecord, len(inputDbPaths))
		for idx, inputDbPath := range inputDbPaths {
			inputChans[idx] = make(chan *LevelDbRecord)
			go readRecords(databases[inputDbPath], uint8(idx), firstKey, lastKey, inputChans[idx])
		}
		go demuxInputsSorted(inputChans, inputChan)
	}
	for idx, outputDbPath := range outputDbPaths {
		go writeRecords(databases[outputDbPath], outputChans[idx])
	}

	transformer.Do(inputChan, outputChans...)
	for _, outputChan := range outputChans {
		close(outputChan)
	}
}
