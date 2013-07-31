package store

import (
	"fmt"
)

func ExampleRangeExcludingReader() {
	store := SliceStore{}
	store.BeginWriting()
	store.WriteRecord(NewRecord("a", "x", 0))
	store.WriteRecord(NewRecord("b", "y", 0))
	store.WriteRecord(NewRecord("c", "z", 0))
	store.WriteRecord(NewRecord("d", "y", 0))
	store.WriteRecord(NewRecord("e", "x", 0))
	store.WriteRecord(NewRecord("f", "a", 0))
	store.WriteRecord(NewRecord("g", "b", 0))
	store.WriteRecord(NewRecord("h", "c", 0))
	store.WriteRecord(NewRecord("j", "e", 0))
	store.WriteRecord(NewRecord("k", "f", 0))
	store.EndWriting()

	excludedStore := SliceStore{}
	excludedStore.BeginWriting()
	excludedStore.WriteRecord(NewRecord("c", "e", 0))
	excludedStore.WriteRecord(NewRecord("h", "i", 0))
	excludedStore.EndWriting()

	excludingReader := NewRangeExcludingReader(&store, &excludedStore)
	excludingReader.BeginReading()
	for {
		record, err := excludingReader.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s ", record.Key)
	}
	excludingReader.EndReading()

	// Output:
	// a b f g j k
}

func ExampleRangeExcludingReader_empty() {
	store := SliceStore{}
	store.BeginWriting()
	store.WriteRecord(NewRecord("a", "x", 0))
	store.WriteRecord(NewRecord("b", "y", 0))
	store.WriteRecord(NewRecord("c", "z", 0))
	store.WriteRecord(NewRecord("d", "y", 0))
	store.EndWriting()

	excludedStore := SliceStore{}
	excludedStore.BeginWriting()
	excludedStore.EndWriting()

	excludingReader := NewRangeExcludingReader(&store, &excludedStore)
	excludingReader.BeginReading()
	for {
		record, err := excludingReader.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s ", record.Key)
	}
	excludingReader.EndReading()

	// Output:
	// a b c d
}
