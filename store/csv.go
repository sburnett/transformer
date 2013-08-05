package store

import (
	"encoding/csv"
	"fmt"
	"io"
	"path/filepath"
	"reflect"

	"github.com/sburnett/lexicographic-tuples"
)

type CsvStore struct {
	writer                           io.WriteCloser
	csvWriter                        *csv.Writer
	keyColumnNames, valueColumnNames []string
	keyVariables, valueVariables     []interface{}
}

func NewCsvStore(writer io.WriteCloser, keyColumnNames, valueColumnNames []string, columns ...interface{}) *CsvStore {
	if len(keyColumnNames)+len(valueColumnNames) != len(columns) {
		panic("Number of column names must match the number of columns")
	}
	return &CsvStore{
		writer:           writer,
		keyColumnNames:   keyColumnNames,
		valueColumnNames: valueColumnNames,
		keyVariables:     columns[:len(keyColumnNames)],
		valueVariables:   columns[len(keyColumnNames):],
	}
}

func (store *CsvStore) BeginWriting() error {
	store.csvWriter = csv.NewWriter(store.writer)
	if err := store.csvWriter.Write(append(store.keyColumnNames, store.valueColumnNames...)); err != nil {
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
	if err := store.csvWriter.Write(values); err != nil {
		return err
	}
	return nil
}

func (store *CsvStore) EndWriting() error {
	store.csvWriter.Flush()
	return store.writer.Close()
}

type csvFileManager string

func NewCsvFileManager(csvRoot string) Manager {
	return csvFileManager(csvRoot)
}

func (dirname csvFileManager) open(params ...interface{}) *CsvStore {
	if len(params) < 4 {
		panic(fmt.Errorf("NewCsvStore requires at least 2 arguments."))
	}
	basename := params[0].(string)
	keyColumnNames := params[1].([]string)
	valueColumnNames := params[2].([]string)
	columns := params[3:]

	filename := filepath.Join(string(dirname), basename)
	writer := NewLazyFileCreator(filename)

	return NewCsvStore(writer, keyColumnNames, valueColumnNames, columns...)
}

func (m csvFileManager) Reader(params ...interface{}) Reader                 { panic("Unimplemented") }
func (m csvFileManager) Writer(params ...interface{}) Writer                 { return m.open(params...) }
func (m csvFileManager) Seeker(params ...interface{}) Seeker                 { panic("Unimplemented") }
func (m csvFileManager) Deleter(params ...interface{}) Deleter               { panic("Unimplemented") }
func (m csvFileManager) ReadingWriter(params ...interface{}) ReadingWriter   { panic("Unimplemented") }
func (m csvFileManager) SeekingWriter(params ...interface{}) SeekingWriter   { panic("Unimplemented") }
func (m csvFileManager) ReadingDeleter(params ...interface{}) ReadingDeleter { panic("Unimplemented") }
func (m csvFileManager) SeekingDeleter(params ...interface{}) SeekingDeleter { panic("Unimplemented") }

type CsvStdoutManager struct {
	buffers map[string]BufferCloser
}

func NewCsvStdoutManager() CsvStdoutManager {
	return CsvStdoutManager{
		buffers: make(map[string]BufferCloser),
	}
}

func (manager CsvStdoutManager) PrintToStdout(name string) {
	fmt.Print(manager.buffers[name])
}

func (manager CsvStdoutManager) open(params ...interface{}) *CsvStore {
	if len(params) < 4 {
		panic(fmt.Errorf("NewCsvStore requires at least 2 arguments."))
	}
	name := params[0].(string)
	keyColumnNames := params[1].([]string)
	valueColumnNames := params[2].([]string)
	columns := params[3:]

	manager.buffers[name] = NewBufferCloser()
	return NewCsvStore(manager.buffers[name], keyColumnNames, valueColumnNames, columns...)
}

func (m CsvStdoutManager) Reader(params ...interface{}) Reader                 { panic("Unimplemented") }
func (m CsvStdoutManager) Writer(params ...interface{}) Writer                 { return m.open(params...) }
func (m CsvStdoutManager) Seeker(params ...interface{}) Seeker                 { panic("Unimplemented") }
func (m CsvStdoutManager) Deleter(params ...interface{}) Deleter               { panic("Unimplemented") }
func (m CsvStdoutManager) ReadingWriter(params ...interface{}) ReadingWriter   { panic("Unimplemented") }
func (m CsvStdoutManager) SeekingWriter(params ...interface{}) SeekingWriter   { panic("Unimplemented") }
func (m CsvStdoutManager) ReadingDeleter(params ...interface{}) ReadingDeleter { panic("Unimplemented") }
func (m CsvStdoutManager) SeekingDeleter(params ...interface{}) SeekingDeleter { panic("Unimplemented") }
