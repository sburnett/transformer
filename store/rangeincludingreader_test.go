package store

import (
	"fmt"
)

func ExampleRangeIncludingReader() {
	store := SliceStore{}
	store.BeginWriting()
	store.WriteRecord(NewRecord("a", "x", 0))
	store.WriteRecord(NewRecord("b", "y", 0))
	store.WriteRecord(NewRecord("c", "z", 0))
	store.WriteRecord(NewRecord("d", "y", 0))
	store.WriteRecord(NewRecord("e", "x", 0))
	store.WriteRecord(NewRecord("f", "a", 0))
	store.WriteRecord(NewRecord("g", "b", 0))
	store.WriteRecord(NewRecord("i", "d", 0))
	store.WriteRecord(NewRecord("k", "f", 0))
	store.EndWriting()

	includedStore := SliceStore{}
	includedStore.BeginWriting()
	includedStore.WriteRecord(NewRecord("c", "e", 0))
	includedStore.WriteRecord(NewRecord("h", "j", 0))
	includedStore.EndWriting()

	includingReader := NewRangeIncludingReader(&store, &includedStore)
	includingReader.BeginReading()
	for {
		record, err := includingReader.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s ", record.Key)
	}
	includingReader.EndReading()

	// Output:
	// c d e i
}

func ExampleRangeIncludingReader_empty() {
	store := SliceStore{}
	store.BeginWriting()
	store.WriteRecord(NewRecord("a", "x", 0))
	store.WriteRecord(NewRecord("b", "y", 0))
	store.WriteRecord(NewRecord("c", "z", 0))
	store.EndWriting()

	includedStore := SliceStore{}
	includedStore.BeginWriting()
	includedStore.EndWriting()

	includingReader := NewRangeIncludingReader(&store, &includedStore)
	includingReader.BeginReading()
	for {
		record, err := includingReader.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s ", record.Key)
	}
	includingReader.EndReading()

	fmt.Printf("End of output\n")

	// Output:
	// End of output
}
