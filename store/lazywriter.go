package store

import (
	"bytes"
	"io"
	"os"
)

type lazyFileCreator struct {
	filename string
	handle   *os.File
}

func NewLazyFileCreator(filename string) io.WriteCloser {
	return &lazyFileCreator{
		filename: filename,
	}
}

func (fileCreator *lazyFileCreator) Write(p []byte) (int, error) {
	if fileCreator.handle == nil {
		handle, err := os.Create(fileCreator.filename)
		if err != nil {
			return 0, err
		}
		fileCreator.handle = handle
	}
	return fileCreator.handle.Write(p)
}

func (fileCreator *lazyFileCreator) Close() error {
	err := fileCreator.handle.Close()
	if err != nil {
		return err
	}
	fileCreator.handle = nil
	return nil
}

type BufferCloser struct {
	*bytes.Buffer
}

func (BufferCloser) Close() error {
	return nil
}

func NewBufferCloser() BufferCloser {
	return BufferCloser{bytes.NewBuffer([]byte{})}
}
