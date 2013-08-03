package store

import (
	"encoding/csv"
	"fmt"
	"io"
	"reflect"

	"github.com/sburnett/lexicographic-tuples"
)

type CsvStore struct {
	writer                           *csv.Writer
	keyColumnNames, valueColumnNames []string
	keyVariables, valueVariables     []interface{}
}

func NewCsvStore(writer io.Writer, keyColumnNames, valueColumnNames []string, columns ...interface{}) *CsvStore {
	if len(keyColumnNames)+len(valueColumnNames) != len(columns) {
		panic("Number of column names must match the number of columns")
	}
	return &CsvStore{
		writer:           csv.NewWriter(writer),
		keyColumnNames:   keyColumnNames,
		valueColumnNames: valueColumnNames,
		keyVariables:     columns[:len(keyColumnNames)],
		valueVariables:   columns[len(keyColumnNames):],
	}
}

func (store *CsvStore) BeginWriting() error {
	if err := store.writer.Write(append(store.keyColumnNames, store.valueColumnNames...)); err != nil {
		return err
	}
	return nil
}

func (store *CsvStore) WriteRecord(record *Record) error {
	if _, err := lex.Decode(record.Key, store.keyVariables...); err != nil {
		return err
	}
	if _, err := lex.Decode(record.Value, store.valueVariables...); err != nil {
		return err
	}
	var values []string
	for _, pointer := range append(store.keyVariables, store.valueVariables...) {
		value := reflect.ValueOf(pointer)
		values = append(values, fmt.Sprintf("%v", value.Elem().Interface()))
	}
	if err := store.writer.Write(values); err != nil {
		return err
	}
	return nil
}

func (store *CsvStore) EndWriting() error {
	store.writer.Flush()
	return nil
}
