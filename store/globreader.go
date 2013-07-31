package store

import (
	"path/filepath"
)

func NewGlobReader(path string) (*SliceStore, error) {
	filenames, err := filepath.Glob(path)
	if err != nil {
		return nil, err
	}
	filenamesStore := SliceStore{}
	filenamesStore.BeginWriting()
	for _, filename := range filenames {
		absFilename, err := filepath.Abs(filename)
		if err != nil {
			return nil, err
		}
		filenamesStore.WriteRecord(NewRecord(absFilename, "", 0))
	}
	filenamesStore.EndWriting()
	return &filenamesStore, nil
}
