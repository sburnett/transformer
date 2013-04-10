package transformer

import (
    "bytes"
    "github.com/sburnett/transformer/key"
)

type Grouper struct {
    inputChan chan *LevelDbRecord
    prefixValues []interface{}
    CurrentGroupPrefix []byte
    currentRecord, nextRecord *LevelDbRecord
}

// Construct a new Grouper by reading records from the provided input channel and
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
//    grouper := GroupRecords(records, &name, &year, &month)
//    for grouper.NextPrefix() {
//      var monthlySpending int32
//      for grouper.NextRecord() {
//        record := grouper.Read()
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
//    grouper := GroupRecords(records, &name, &year)
//    for grouper.NextPrefix() {
//      var yearlySpending int32
//      for grouper.NextRecord() {
//        record := grouper.Read()
//        var month, spending
//        key.DecodeOrDie(record.Key, &month, &spending)
//        yearlySpending += spending
//      }
//      fmt.Printf("%s spent $%d in %d", name, yearlySpending, year)
//    }
//
// In each case, the Grouper groups records together when they share the same
// values for each argument passed to GroupRecords.
func GroupRecords(inputChan chan *LevelDbRecord, prefixValues ...interface{}) *Grouper {
    return &Grouper{
        inputChan: inputChan,
        prefixValues: prefixValues,
    }
}

func (grouper *Grouper) readRecord() *LevelDbRecord {
    newRecord, ok := <-grouper.inputChan
    if !ok {
        return nil
    }
    if newRecord == nil {
        panic("LevelDbRecords should never be nil")
    }
    return newRecord
}

// Advance the Grouper so we can read a new group of records with identical
// prefixes. You should only call this method once NextRecord() has returned
// false. (You cannot skip records within a prefix by calling NextPrefix.)
//
// Return true iff there is another prefix to read.
func (grouper *Grouper) NextGroup() bool {
    if grouper.CurrentGroupPrefix == nil {
        grouper.nextRecord = grouper.readRecord()
    }
    if grouper.nextRecord == nil {
        return false
    }
    newPrefix, _ := key.DecodeAndSplitOrDie(grouper.nextRecord.Key, grouper.prefixValues...)
    grouper.CurrentGroupPrefix = newPrefix
    return true
}

// Advance the Grouper to the next record within the current prefix, or return
// false if there are no more records. Once this method returns false it is
// safe to advance to the next prefix using NextPrefix().
func (grouper *Grouper) NextRecord() bool {
    grouper.currentRecord = nil
    if grouper.nextRecord != nil {
        grouper.currentRecord = grouper.nextRecord
        grouper.currentRecord.Key = grouper.currentRecord.Key[len(grouper.CurrentGroupPrefix):]
        grouper.nextRecord = nil
        return true
    }
    newRecord := grouper.readRecord()
    if newRecord == nil {
        return false
    }
    if !bytes.HasPrefix(newRecord.Key, grouper.CurrentGroupPrefix) {
        grouper.nextRecord = newRecord
        return false
    }
    grouper.currentRecord = newRecord
    grouper.currentRecord.Key = grouper.currentRecord.Key[len(grouper.CurrentGroupPrefix):]
    return true
}

// Read the current record in the current group. This will return nil if there
// is no record, which can happen when we reach the end of a group and haven't
// called NextGroup to advance to the next group.
func (grouper *Grouper) Read() *LevelDbRecord {
    return grouper.currentRecord
}
