package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/sburnett/lexicographic-tuples"
)

func ExampleCsvStore_write() {
	writer := NewBufferCloser()

	var first, last, party string
	var birth_year int32

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

func ExampleCsvFileManager() {
	csvPath, err := ioutil.TempDir("", "transformer-csv-manager-test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(csvPath)

	manager := NewCsvFileManager(csvPath)

	var k, v string
	writer := manager.Writer("test.csv", []string{"word"}, []string{"reverse"}, &k, &v)
	writer.BeginWriting()
	writeRecord := func(record *Record) {
		if err := writer.WriteRecord(record); err != nil {
			panic(err)
		}
	}
	writeRecord(&Record{
		Key:   lex.EncodeOrDie("but"),
		Value: lex.EncodeOrDie("tub"),
	})
	writeRecord(&Record{
		Key:   lex.EncodeOrDie("so"),
		Value: lex.EncodeOrDie("os"),
	})
	writer.EndWriting()

	contents, err := ioutil.ReadFile(filepath.Join(csvPath, "test.csv"))
	if err != nil {
		panic(err)
	}
	fmt.Print(string(contents))

	// Output:
	//
	// word,reverse
	// but,tub
	// so,os
}

func ExampleCsvStdoutManager() {
	manager := NewCsvStdoutManager()

	var k, v string
	writer := manager.Writer("words.csv", []string{"word"}, []string{"reverse"}, &k, &v)
	writer.BeginWriting()
	writeRecord := func(record *Record) {
		if err := writer.WriteRecord(record); err != nil {
			panic(err)
		}
	}
	writeRecord(&Record{
		Key:   lex.EncodeOrDie("but"),
		Value: lex.EncodeOrDie("tub"),
	})
	writeRecord(&Record{
		Key:   lex.EncodeOrDie("so"),
		Value: lex.EncodeOrDie("os"),
	})
	writer.EndWriting()

	manager.PrintToStdout("words.csv")

	// Output:
	//
	// word,reverse
	// but,tub
	// so,os
}
