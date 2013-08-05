package store

import (
	"fmt"
)

func ExampleSliceStore() {
	store := SliceStore{}
	store.BeginWriting()
	store.WriteRecord(NewRecord("b", "x", 0))
	store.WriteRecord(NewRecord("c", "y", 0))
	store.WriteRecord(NewRecord("a", "z", 0))
	store.EndWriting()

	store.BeginReading()
	for {
		record, err := store.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("[0] %s: %s\n", record.Key, record.Value)
	}
	store.EndReading()

	// Output:
	// [0] a: z
	// [0] b: x
	// [0] c: y
}

func ExampleSliceStore_seek() {
	store := SliceStore{}
	store.BeginWriting()
	store.WriteRecord(NewRecord("a", "x", 0))
	store.WriteRecord(NewRecord("b", "y", 0))
	store.WriteRecord(NewRecord("c", "z", 0))
	store.WriteRecord(NewRecord("d", "y", 0))
	store.WriteRecord(NewRecord("e", "x", 0))
	store.EndWriting()

	store.BeginReading()
	store.Seek([]byte("c"))
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
	store.EndReading()

	// Output:
	// c: z
	// d: y
	// e: x
}

func ExampleSliceManager() {
	manager := NewSliceManager()

	firstStore := manager.Writer("store")
	secondStore := manager.Writer("store")
	thirdStore := manager.Writer("another")

	firstStore.BeginWriting()
	firstStore.WriteRecord(NewRecord("hello", "world", 0))
	firstStore.EndWriting()

	secondStore.BeginWriting()
	secondStore.WriteRecord(NewRecord("test", "record", 0))
	secondStore.EndWriting()

	thirdStore.BeginWriting()
	thirdStore.WriteRecord(NewRecord("another", "test", 0))
	thirdStore.EndWriting()

	for _, name := range []string{"store", "another"} {
		fmt.Printf("Records from %s:\n", name)
		backingStore := manager.GetSlice(name)
		backingStore.BeginReading()
		for {
			record, err := backingStore.ReadRecord()
			if err != nil {
				panic(err)
			}
			if record == nil {
				break
			}
			fmt.Printf("%s: %s\n", record.Key, record.Value)
		}
		backingStore.EndReading()
		fmt.Println()
	}

	// Output:
	//
	// Records from store:
	// hello: world
	// test: record
	//
	// Records from another:
	// another: test
}
