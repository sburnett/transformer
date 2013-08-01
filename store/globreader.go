package store

import (
	"path/filepath"
	"sort"

	"github.com/sburnett/transformer/key"
)

type GlobReader struct {
	path      string
	filenames []string
	cursor    int
}

func NewGlobReader(path string) *GlobReader {
	return &GlobReader{
		path: path,
	}
}

func (reader *GlobReader) BeginReading() error {
	filenames, err := filepath.Glob(reader.path)
	if err != nil {
		return err
	}
	for _, filename := range filenames {
		absFilename, err := filepath.Abs(filename)
		if err != nil {
			return err
		}
		reader.filenames = append(reader.filenames, absFilename)
	}
	sort.Sort(sort.StringSlice(reader.filenames))
	reader.cursor = -1
	return nil
}

func (reader *GlobReader) ReadRecord() (*Record, error) {
	reader.cursor++
	if reader.cursor >= len(reader.filenames) {
		return nil, nil
	}
	record := &Record{Key: key.EncodeOrDie(reader.filenames[reader.cursor])}
	return record, nil
}

func (reader *GlobReader) EndReading() error {
	return nil
}
