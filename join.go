package transformer

import (
    "bytes"
    "github.com/sburnett/transformer/key"
)

type Joiner struct {
    inputChan chan *LevelDbRecord
    prefixValues []interface{}
    currentKeyPrefix []byte
    currentRecord, nextRecord *LevelDbRecord
}

// Construct a new Joiner by reading records from the provided input channel and
// grouping them by identical prefixValues.
//
// For example, given a set of records of names, years, months, and dollar amounts:
//
//     "John Doe", 2013, 01, 200
//     "John Doe", 2013, 01, 10
//     "John Doe", 2013, 02, 30
//     "John Doe", 2013, 02, 50
//     "John Doe", 2013, 03, 100
//     "Bob Smith", 2013, 01, 100
//     "Bob Smith", 2013, 02, 20
//
// If we wanted to print the aggregate spending for each person per month:
//
//    var name string
//    var year, month int32
//    joiner := NewJoiner(records, &name, &year, &month)
//    for joiner.NextPrefix() {
//      var monthlySpending int32
//      for joiner.NextRecord() {
//        record := joiner.Read()
//        var spending
//        key.DecodeOrDie(record.Key, &spending)
//        monthlySpending += spending
//      }
//      fmt.Printf("%s spent $%d in %d-%d", name, monthlySpending, year, month)
//    }
//
// This will print:
//
//     John Doe spent $210 in 2013-01
//     John Doe spent $80 in 2013-02
//     John Doe spent $100 in 2013-03
//     Bob Smith spent $100 in 2013-01
//     Bob Smith spent $20 in 2013-01
//
// If we instead wanted to aggregated spending per person per year:
//
//    var name string
//    var year int32
//    joiner := NewJoiner(records, &name, &year)
//    for joiner.NextPrefix() {
//      var yearlySpending int32
//      for joiner.NextRecord() {
//        record := joiner.Read()
//        var month, spending
//        key.DecodeOrDie(record.Key, &month, &spending)
//        yearlySpending += spending
//      }
//      fmt.Printf("%s spent $%d in %d", name, yearlySpending, year)
//    }
//
// In each case, the Joiner groups records together when they share the same
// values for each argument passed to NewJoiner.
func NewJoiner(inputChan chan *LevelDbRecord, prefixValues ...interface{}) *Joiner {
    return &Joiner{
        inputChan: inputChan,
        prefixValues: prefixValues,
    }
}

func (joiner *Joiner) readRecord() *LevelDbRecord {
    newRecord, ok := <-joiner.inputChan
    if !ok {
        return nil
    }
    return newRecord
}

// Advance the Joiner so we can read a new group of records with identical
// prefixes. You should only call this method once NextRecord() has returned
// false. (You cannot skip records within a prefix by calling NextPrefix.)
//
// Return true iff there is another prefix to read.
func (joiner *Joiner) NextGroup() bool {
    if joiner.currentKeyPrefix == nil {
        joiner.nextRecord = joiner.readRecord()
    }
    if joiner.nextRecord == nil {
        return false
    }
    newPrefix, _ := key.DecodeAndSplitOrDie(joiner.nextRecord.Key, joiner.prefixValues...)
    joiner.currentKeyPrefix = newPrefix
    return true
}

// Advance the Joiner to the next record within the current prefix, or return
// false if there are no more records. Once this method returns false it is
// safe to advance to the next prefix using NextPrefix().
func (joiner *Joiner) NextRecord() bool {
    joiner.currentRecord = nil
    if joiner.nextRecord != nil {
        joiner.currentRecord = joiner.nextRecord
        joiner.currentRecord.Key = joiner.nextRecord.Key[len(joiner.currentKeyPrefix):]
        joiner.nextRecord = nil
        return true
    }
    newRecord := joiner.readRecord()
    if newRecord == nil {
        return false
    }
    if !bytes.HasPrefix(newRecord.Key, joiner.currentKeyPrefix) {
        joiner.nextRecord = newRecord
        return false
    }
    joiner.currentRecord = newRecord
    joiner.currentRecord.Key = newRecord.Key[len(joiner.currentKeyPrefix):]
    return true
}

// Read the current record in the current group. This will return nil if there
// is no record, which can happen when we reach the end of a group and haven't
// called NextGroup to advance to the next group.
func (joiner *Joiner) Read() *LevelDbRecord {
    return joiner.currentRecord
}
