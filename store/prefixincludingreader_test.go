package store

import (
	"fmt"
)

func ExamplePrefixIncludingReader() {
	store := SliceStore{}
	store.BeginWriting()
	store.WriteRecord(NewRecord("aaa", "x", 0))
	store.WriteRecord(NewRecord("aab", "y", 0))
	store.WriteRecord(NewRecord("abc", "z", 0))
	store.WriteRecord(NewRecord("acc", "y", 0))
	store.WriteRecord(NewRecord("baa", "x", 0))
	store.WriteRecord(NewRecord("bac", "a", 0))
	store.WriteRecord(NewRecord("bbb", "b", 0))
	store.WriteRecord(NewRecord("dab", "l", 0))
	store.WriteRecord(NewRecord("eaa", "z", 0))
	store.WriteRecord(NewRecord("eab", "z", 0))
	store.WriteRecord(NewRecord("eba", "z", 0))
	store.WriteRecord(NewRecord("ebb", "z", 0))
	store.WriteRecord(NewRecord("ebc", "z", 0))
	store.EndWriting()

	includedStore := SliceStore{}
	includedStore.BeginWriting()
	includedStore.WriteRecord(NewRecord("aa", "", 0))
	includedStore.WriteRecord(NewRecord("b", "", 0))
	includedStore.WriteRecord(NewRecord("c", "", 0))
	includedStore.WriteRecord(NewRecord("ea", "", 0))
	includedStore.WriteRecord(NewRecord("eb", "", 0))
	includedStore.EndWriting()

	includingReader := NewPrefixIncludingReader(&store, &includedStore)
	includingReader.BeginReading()
	for {
		record, err := includingReader.ReadRecord()
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		fmt.Printf("%s\n", record.Key)
	}
	includingReader.EndReading()

	// Output:
	// aaa
	// aab
	// baa
	// bac
	// bbb
	// eaa
	// eab
	// eba
	// ebb
	// ebc
}
