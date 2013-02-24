package transformer

import (
	"expvar"
	"fmt"
	"github.com/jmhodges/levigo"
	"sync"
)

var recordsRead, bytesRead, recordsWritten, bytesWritten *expvar.Int

func init() {
	recordsRead = expvar.NewInt("RecordsRead")
	recordsWritten = expvar.NewInt("RecordsWritten")
	bytesRead = expvar.NewInt("BytesRead")
	bytesWritten = expvar.NewInt("BytesWritten")
}

type LevelDbStore struct {
	recordReader LevelDbReader
	recordWriter LevelDbWriter

	dbPath                         string
	dbOpenLock                     sync.Mutex
	openForReading, openForWriting bool
	db                             *levigo.DB
	dbOpts                         *levigo.Options
}

type LevelDbReader func(recordsChan chan *LevelDbRecord, db *levigo.DB) error
type LevelDbWriter func(recordsChan chan *LevelDbRecord, db *levigo.DB) error

func NewLevelDbStore(dbPath string, recordReader LevelDbReader, recordWriter LevelDbWriter) *LevelDbStore {
	return &LevelDbStore{
		dbPath:       dbPath,
		recordReader: recordReader,
		recordWriter: recordWriter,
	}
}

func (store *LevelDbStore) openDatabase() error {
	if store.openForReading || store.openForWriting {
		return nil
	}
	dbOpts := levigo.NewOptions()
	dbOpts.SetMaxOpenFiles(128)
	dbOpts.SetCreateIfMissing(true)
	dbOpts.SetBlockSize(1 << 22) // 4 MB
	db, err := levigo.Open(store.dbPath, dbOpts)
	if err != nil {
		dbOpts.Close()
		return err
	}
	store.db = db
	store.dbOpts = dbOpts
	return nil
}

func (store *LevelDbStore) closeDatabase() {
	if store.openForReading && store.openForWriting {
		return
	}
	store.db.Close()
	store.dbOpts.Close()
}

func (store *LevelDbStore) Read(recordsChan chan *LevelDbRecord) error {
	defer close(recordsChan)

	store.dbOpenLock.Lock()
	if store.openForReading {
		panic("Only one routine may read from a LevelDB at a time.")
	}
	if err := store.openDatabase(); err != nil {
		store.dbOpenLock.Unlock()
		return err
	}
	store.openForReading = true
	store.dbOpenLock.Unlock()
	defer func() {
		store.dbOpenLock.Lock()
		store.closeDatabase()
		store.openForReading = false
		store.dbOpenLock.Unlock()
	}()

	return store.recordReader(recordsChan, store.db)
}

func (store *LevelDbStore) Write(recordsChan chan *LevelDbRecord) error {
	store.dbOpenLock.Lock()
	if store.openForWriting {
		panic("Only one routine may write to a LevelDB at a time.")
	}
	if err := store.openDatabase(); err != nil {
		store.dbOpenLock.Unlock()
		return err
	}
	store.openForWriting = true
	store.dbOpenLock.Unlock()
	defer func() {
		store.dbOpenLock.Lock()
		store.closeDatabase()
		store.openForWriting = false
		store.dbOpenLock.Unlock()
	}()

	return store.recordWriter(recordsChan, store.db)
}

func ReadAllRecords(recordsChan chan *LevelDbRecord, db *levigo.DB) error {
	readOpts := levigo.NewReadOptions()
	defer readOpts.Close()
	it := db.NewIterator(readOpts)
	it.SeekToFirst()
	for ; it.Valid(); it.Next() {
		recordsChan <- &LevelDbRecord{Key: it.Key(), Value: it.Value()}
		recordsRead.Add(1)
		bytesRead.Add(int64(len(it.Key()) + len(it.Value())))
	}
	if err := it.GetError(); err != nil {
		return fmt.Errorf("Error iterating through database: %v", err)
	}
	return nil
}

func WriteAllRecords(recordsChan chan *LevelDbRecord, db *levigo.DB) error {
	writeOpts := levigo.NewWriteOptions()
	defer writeOpts.Close()
	for record := range recordsChan {
		if err := db.Put(writeOpts, record.Key, record.Value); err != nil {
			return fmt.Errorf("Error writing to database: %v", err)
		}
		recordsWritten.Add(1)
		bytesWritten.Add(int64(len(record.Key) + len(record.Value)))
	}
	return nil
}
