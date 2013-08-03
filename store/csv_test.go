package store

import (
	"bytes"
	"fmt"

	"github.com/sburnett/lexicographic-tuples"
)

func ExampleCsvStore_write() {
	var first, last, party string
	var birth_year int32

	writer := bytes.NewBuffer([]byte{})
	store := NewCsvStore(writer, []string{"first_name", "last_name"}, []string{"party", "birth_year"}, &first, &last, &party, &birth_year)

	if err := store.BeginWriting(); err != nil {
		panic(err)
	}
	writeRecord := func(record *Record) {
		if err := store.WriteRecord(record); err != nil {
			panic(err)
		}
	}
	writeRecord(&Record{
		Key:   lex.EncodeOrDie("George", "Washington"),
		Value: lex.EncodeOrDie("Independent", int32(1732)),
	})
	writeRecord(&Record{
		Key:   lex.EncodeOrDie("John", "Adams"),
		Value: lex.EncodeOrDie("Federalist", int32(1735)),
	})
	writeRecord(&Record{
		Key:   lex.EncodeOrDie("Thomas", "Jefferson"),
		Value: lex.EncodeOrDie("Democratic-Republican", int32(1743)),
	})
	if err := store.EndWriting(); err != nil {
		panic(err)
	}

	fmt.Print(writer.String())

	// Output:
	//
	// first_name,last_name,party,birth_year
	// George,Washington,Independent,1732
	// John,Adams,Federalist,1735
	// Thomas,Jefferson,Democratic-Republican,1743
}
