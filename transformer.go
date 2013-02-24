package transformer

import (
	"bytes"
)

type LevelDbRecord struct {
	Key           []byte
	Value         []byte
	DatabaseIndex uint8
}

type LevelDbRecordSlice []*LevelDbRecord

func (p LevelDbRecordSlice) Len() int           { return len(p) }
func (p LevelDbRecordSlice) Less(i, j int) bool { return bytes.Compare(p[i].Key, p[j].Key) < 0 }
func (p LevelDbRecordSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func RunTransformer(transformer Transformer, reader StoreReader, writer StoreWriter) {
	inputChan := make(chan *LevelDbRecord)
	outputChan := make(chan *LevelDbRecord)
	go func() {
		if err := reader.Read(inputChan); err != nil {
			panic(err)
		}
	}()
	go func() {
		transformer.Do(inputChan, outputChan)
		close(outputChan)
	}()
	if err := writer.Write(outputChan); err != nil {
		panic(err)
	}
}
