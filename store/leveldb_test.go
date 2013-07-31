package store

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
)

func ExampleLevelDbStore_readWrite() {
	dbPath, err := ioutil.TempDir("", "transformer-leveldb-test")
	if err != nil {
		panic(err)
	}

	store := NewLevelDbStore(dbPath)

	if err := store.BeginWriting(); err != nil {
		panic(err)
	}
	writeRecord := func(record *Record) {
		if err := store.WriteRecord(record); err != nil {
			panic(err)
		}
	}
	writeRecord(NewRecord("a", "x", 0))
	writeRecord(NewRecord("c", "z", 0))
	writeRecord(NewRecord("b", "y", 0))
	if err := store.EndWriting(); err != nil {
		panic(err)
	}

	if err := store.BeginReading(); err != nil {
		panic(err)
	}
	for {
		record, err := store.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}
	if err := store.EndReading(); err != nil {
		panic(err)
	}

	if err := os.RemoveAll(dbPath); err != nil {
		panic(err)
	}

	// Output:
	// a: x
	// b: y
	// c: z
}

func ExampleLevelDbStore_seek() {
	dbPath, err := ioutil.TempDir("", "transformer-leveldb-test")
	if err != nil {
		panic(err)
	}

	store := NewLevelDbStore(dbPath)

	if err := store.BeginWriting(); err != nil {
		panic(err)
	}
	writeRecord := func(record *Record) {
		if err := store.WriteRecord(record); err != nil {
			panic(err)
		}
	}
	writeRecord(NewRecord("a", "x", 0))
	writeRecord(NewRecord("b", "y", 0))
	writeRecord(NewRecord("c", "z", 0))
	writeRecord(NewRecord("d", "x", 0))
	writeRecord(NewRecord("f", "y", 0))
	if err := store.EndWriting(); err != nil {
		panic(err)
	}

	if err := store.BeginReading(); err != nil {
		panic(err)
	}
	for {
		record, err := store.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s: %s\n", record.Key, record.Value)

		if bytes.Equal(record.Key, []byte("a")) {
			store.Seek([]byte("c"))
		} else if bytes.Equal(record.Key, []byte("c")) {
			store.Seek([]byte("e"))
		}
	}
	if err := store.EndReading(); err != nil {
		panic(err)
	}

	if err := os.RemoveAll(dbPath); err != nil {
		panic(err)
	}

	// Output:
	// a: x
	// c: z
	// f: y
}

func ExampleLevelDbStore_deleteAll() {
	dbPath, err := ioutil.TempDir("", "transformer-leveldb-test")
	if err != nil {
		panic(err)
	}

	store := NewLevelDbStore(dbPath)

	if err := store.BeginWriting(); err != nil {
		panic(err)
	}
	writeRecord := func(record *Record) {
		if err := store.WriteRecord(record); err != nil {
			panic(err)
		}
	}
	writeRecord(NewRecord("a", "x", 0))
	writeRecord(NewRecord("c", "z", 0))
	writeRecord(NewRecord("b", "y", 0))
	if err := store.EndWriting(); err != nil {
		panic(err)
	}

	if err := store.BeginWriting(); err != nil {
		panic(err)
	}
	if err := store.DeleteAllRecords(); err != nil {
		panic(err)
	}
	if err := store.EndWriting(); err != nil {
		panic(err)
	}

	if err := store.BeginReading(); err != nil {
		panic(err)
	}
	for {
		record, err := store.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s: %s\n", record.Key, record.Value)
	}
	fmt.Printf("End of records\n")
	if err := store.EndReading(); err != nil {
		panic(err)
	}

	if err := os.RemoveAll(dbPath); err != nil {
		panic(err)
	}

	// Output:
	// End of records
}
