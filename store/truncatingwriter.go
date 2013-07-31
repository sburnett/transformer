package store

type TruncatingWriter struct {
	writer Deleter
}

// Delete the contents of a Deleter before writing any records to it.
func NewTruncatingWriter(writer Deleter) *TruncatingWriter {
	return &TruncatingWriter{writer: writer}
}

func (store *TruncatingWriter) BeginWriting() error {
	if err := store.writer.BeginWriting(); err != nil {
		return err
	}
	return store.writer.DeleteAllRecords()
}

func (store *TruncatingWriter) WriteRecord(record *Record) error {
	return store.writer.WriteRecord(record)
}

func (store *TruncatingWriter) EndWriting() error {
	return store.writer.EndWriting()
}
