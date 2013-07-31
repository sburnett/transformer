package store

import (
	"fmt"
)

func ExampleTruncatingWriter() {
	store := &SliceStore{}
	truncatingStore := NewTruncatingWriter(store)
	truncatingStore.BeginWriting()
	truncatingStore.WriteRecord(NewRecord("b", "x", 0))
	truncatingStore.EndWriting()

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

	truncatingStore.BeginWriting()
	truncatingStore.WriteRecord(NewRecord("c", "y", 0))
	truncatingStore.WriteRecord(NewRecord("a", "z", 0))
	truncatingStore.EndWriting()

	store.BeginReading()
	for {
		record, err := store.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("[1] %s: %s\n", record.Key, record.Value)
	}
	store.EndReading()

	// Output:
	// [0] b: x
	// [1] a: z
	// [1] c: y
}
