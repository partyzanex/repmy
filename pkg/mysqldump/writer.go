package mysqldump

import (
	"io"
)

type chanWriter struct {
	results chan<- []byte
}

func (w *chanWriter) Write(b []byte) (int, error) {
	w.results <- b

	return len(b), nil
}

func (w *chanWriter) Close() error {
	close(w.results)

	return nil
}

func NewWriter(results chan<- []byte) io.WriteCloser {
	return &chanWriter{
		results: results,
	}
}
