package dump

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	insertPrefix = []byte("INSERT INTO `")
	fileExt      = []byte(".sql")
	endSuffix    = []byte("\n--\n-- end of data\n--\n")

	prefixLength = len(insertPrefix)
)

type DirWriter interface {
	io.WriteCloser

	GetFile(name string) (io.WriteCloser, error)
}

type dirWriter struct {
	dir  string
	gzip bool

	files *sync.Map
	mu    *sync.RWMutex
}

func NewDirWriter(dir string, gz bool) (DirWriter, error) {
	err := createDir(dir)
	if err != nil {
		return nil, err
	}

	writer := &dirWriter{
		dir:   dir,
		gzip:  gz,
		files: &sync.Map{},
		mu:    &sync.RWMutex{},
	}

	return writer, nil
}

func (w *dirWriter) Write(b []byte) (int, error) {
	file, err := w.parseFileName(b)
	if err != nil {
		return 0, err
	}

	fileName := string(file)

	f, err := w.getFile(fileName)
	if err != nil {
		return 0, err
	}

	n, err := f.Write(b)
	if err != nil {
		return n, err
	}

	err = w.mustClosed(b, f)
	if err != nil {
		return n, err
	}

	return n, err
}

func (w *dirWriter) Close() (err error) {
	w.files.Range(func(key, value interface{}) bool {
		file, ok := value.(io.WriteCloser)
		if !ok {
			err = fmt.Errorf("key %v, value %v is not a file descriptor", key, value)
			return false
		}

		err = file.Close()

		return err == nil
	})

	return
}

func (w *dirWriter) GetFile(name string) (io.WriteCloser, error) {
	return w.getFile(filepath.Base(name))
}

func (w *dirWriter) mustClosed(b []byte, f io.Closer) error {
	if i := bytes.Index(b, endSuffix); i <= 0 {
		return nil
	}

	return f.Close()
}

func (w *dirWriter) getFile(fileName string) (io.WriteCloser, error) {
	entry, ok := w.files.Load(fileName)
	if !ok {
		file, err := NewFileWriter(w.dir, fileName, w.gzip)
		if err != nil {
			return nil, fmt.Errorf("unable to create file %s: %s", fileName, err)
		}

		w.files.Store(fileName, file)

		return file, nil
	}

	return entry.(io.WriteCloser), nil
}

func (*dirWriter) parseFileName(b []byte) ([]byte, error) {
	start, end := bytes.Index(b, insertPrefix), 0

	if start < 0 {
		return nil, fmt.Errorf("unable to parse start of file name")
	}

	start += prefixLength

	for i, char := range b[start:] {
		if char == '`' {
			end = i + start
			break
		}
	}

	if end == 0 {
		return nil, fmt.Errorf("unable to parse end of file name")
	}

	n := make([]byte, end-start)
	copy(n, b[start:end])

	return append(n, fileExt...), nil
}

type fileWriter struct {
	file io.WriteCloser
}

func NewFileWriter(dir, file string, gz bool) (io.WriteCloser, error) {
	err := createDir(dir)
	if err != nil {
		return nil, err
	}

	if gz {
		file += ".gz"
	}

	filePath := filepath.Join(dir, file)

	if _, err := os.Stat(filePath); err == nil {
		err := os.Remove(filePath)
		if err != nil && os.IsNotExist(err) {
			return nil, err
		}
	}

	var wc io.WriteCloser

	wc, err = os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("unable to create file %s: %s", filePath, err)
	}

	if gz {
		wc, err = gzip.NewWriterLevel(wc, gzip.BestSpeed)
		if err != nil {
			return nil, fmt.Errorf("unable to create gzip writer: %s", err)
		}
	}

	writer := &fileWriter{
		file: wc,
	}

	return writer, nil
}

func (w *fileWriter) Write(b []byte) (int, error) {
	return w.file.Write(b)
}

func (w *fileWriter) Close() error {
	return w.file.Close()
}

func createDir(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		if !os.IsExist(err) {
			return fmt.Errorf("unable to create directory %s: %s", dir, err)
		}
	}

	return nil
}

func closeWriter(w io.WriteCloser, err error) {
	errCl := w.Close()
	if errCl != nil {
		if err != nil {
			err = fmt.Errorf("when DumpDLL was executed an occured writer closing error: %s and %s", errCl, err)
			return
		}

		err = fmt.Errorf("writer closing error: %s", errCl)
	}
}
