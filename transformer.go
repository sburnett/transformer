package transformer

import (
	"bytes"
	"expvar"
	"github.com/jmhodges/levigo"
	"log"
)

type LevelDbRecord struct {
	Key   []byte
	Value []byte
}

type LevelDbRecordSlice []*LevelDbRecord

func (p LevelDbRecordSlice) Len() int           { return len(p) }
func (p LevelDbRecordSlice) Less(i, j int) bool { return bytes.Compare(p[i].Key, p[j].Key) < 0 }
func (p LevelDbRecordSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type Transformer interface {
	Do(inputChan, outputChan chan *LevelDbRecord)
}

var recordsRead, bytesRead, recordsWritten, bytesWritten *expvar.Int

func init() {
	recordsRead = expvar.NewInt("RecordsRead")
	recordsWritten = expvar.NewInt("RecordsWritten")
	bytesRead = expvar.NewInt("BytesRead")
	bytesWritten = expvar.NewInt("BytesWritten")
}

func readRecords(db *levigo.DB, firstKey, lastKey []byte, recordsChan chan *LevelDbRecord) {
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
func RunTransformer(transformer Transformer, inputDbPath, outputDbPath string, firstKey, lastKey []byte) {
	inputOpts := levigo.NewOptions()
	inputOpts.SetMaxOpenFiles(128)
	defer inputOpts.Close()
	inputDb, err := levigo.Open(inputDbPath, inputOpts)
	if err != nil {
		log.Fatalf("Error opening leveldb database %v: %v", inputDbPath, err)
	}
	defer inputDb.Close()

	var outputDb *levigo.DB
	if outputDbPath == inputDbPath {
		outputDb = inputDb
	} else {
		outputOpts := levigo.NewOptions()
		outputOpts.SetMaxOpenFiles(128)
		outputOpts.SetCreateIfMissing(true)
		defer outputOpts.Close()
		outputDb, err = levigo.Open(outputDbPath, outputOpts)
		if err != nil {
			log.Fatalf("Error opening leveldb database %v: %v", outputDbPath, err)
		}
		defer outputDb.Close()
	}

	inputChan := make(chan *LevelDbRecord)
	outputChan := make(chan *LevelDbRecord)

	go readRecords(inputDb, firstKey, lastKey, inputChan)
	go writeRecords(outputDb, outputChan)

	transformer.Do(inputChan, outputChan)
	close(outputChan)
}
