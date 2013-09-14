package store

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sburnett/lexicographic-tuples"
)

type SqliteStore struct {
	filename, table string
	db              *sql.DB
	tx              *sql.Tx
	stmt            *sql.Stmt

	columnNames                  []string
	keyVariables, valueVariables []interface{}
}

func NewSqliteStore(filename, table string, keyColumnNames, valueColumnNames []string, columns ...interface{}) *SqliteStore {
	if len(keyColumnNames)+len(valueColumnNames) != len(columns) {
		panic("Number of column names must match the number of columns")
	}
	return &SqliteStore{
		filename:       filename,
		table:          table,
		columnNames:    append(keyColumnNames, valueColumnNames...),
		keyVariables:   columns[:len(keyColumnNames)],
		valueVariables: columns[len(keyColumnNames):],
	}
}

func (store *SqliteStore) BeginWriting() error {
	db, err := sql.Open("sqlite3", store.filename)
	if err != nil {
		return err
	}
	store.db = db

	columns := strings.Join(store.columnNames, ",")
	createQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", store.table, columns)
	if _, err := db.Exec(createQuery); err != nil {
		db.Close()
		return err
	}
	deleteQuery := fmt.Sprintf("DELETE FROM %s", store.table)
	if _, err := db.Exec(deleteQuery); err != nil {
		db.Close()
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		db.Close()
		return err
	}
	store.tx = tx

	var valuePlaceholders []string
	for i := 0; i < len(store.columnNames); i++ {
		valuePlaceholders = append(valuePlaceholders, "?")
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", store.table, columns, strings.Join(valuePlaceholders, ","))
	stmt, err := store.tx.Prepare(insertQuery)
	if err != nil {
		db.Close()
		return err
	}
	store.stmt = stmt
	return nil
}

func (store *SqliteStore) WriteRecord(record *Record) error {
	if _, err := lex.Decode(record.Key, store.keyVariables...); err != nil {
		return err
	}
	if _, err := lex.Decode(record.Value, store.valueVariables...); err != nil {
		return err
	}
	if _, err := store.stmt.Exec(append(store.keyVariables, store.valueVariables...)...); err != nil {
		return err
	}
	return nil
}

func (store *SqliteStore) EndWriting() error {
	if err := store.tx.Commit(); err != nil {
		return err
	}
	if err := store.stmt.Close(); err != nil {
		return err
	}
	return store.db.Close()
}

type sqliteManager string

// Write to tables in the provided a Sqlite database.
func NewSqliteManager(dbFilename string) Manager {
	return sqliteManager(dbFilename)
}

func (filename sqliteManager) open(params ...interface{}) *SqliteStore {
	if len(params) < 4 {
		panic(fmt.Errorf("NewSqliteStore requires at least 4 arguments."))
	}
	table := params[0].(string)
	keyColumnNames := params[1].([]string)
	valueColumnNames := params[2].([]string)
	columns := params[3:]
	return NewSqliteStore(string(filename), table, keyColumnNames, valueColumnNames, columns...)
}

func (m sqliteManager) Reader(params ...interface{}) Reader                 { panic("Unimplemented") }
func (m sqliteManager) Writer(params ...interface{}) Writer                 { return m.open(params...) }
func (m sqliteManager) Seeker(params ...interface{}) Seeker                 { panic("Unimplemented") }
func (m sqliteManager) Deleter(params ...interface{}) Deleter               { panic("Unimplemented") }
func (m sqliteManager) ReadingWriter(params ...interface{}) ReadingWriter   { panic("Unimplemented") }
func (m sqliteManager) SeekingWriter(params ...interface{}) SeekingWriter   { panic("Unimplemented") }
func (m sqliteManager) ReadingDeleter(params ...interface{}) ReadingDeleter { panic("Unimplemented") }
func (m sqliteManager) SeekingDeleter(params ...interface{}) SeekingDeleter { panic("Unimplemented") }
