package dump

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	insertPrefix = []byte("INSERT INTO `")
	fileExt      = []byte(".sql")

	prefixLength = len(insertPrefix)
)

type dirWriter struct {
	dir string

	files *sync.Map
	//fileMap map[string]int
	//files   []*os.File
	mu *sync.RWMutex
}

func NewDirWriter(dir string) (io.WriteCloser, error) {
	err := createDir(dir)
	if err != nil {
		return nil, err
	}

	writer := &dirWriter{
		dir:   dir,
		files: &sync.Map{},
		//files:   make([]*os.File, 0),
		//fileMap: make(map[string]int),
		mu: &sync.RWMutex{},
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

	return f.Write(b)
}

func (w *dirWriter) Close() (err error) {
	w.files.Range(func(key, value interface{}) bool {
		file, ok := value.(*os.File)
		if !ok {
			err = fmt.Errorf("key %v, value %v is not a file descriptor", key, value)
			return false
		}

		err = file.Close()

		return err == nil
	})

	return
}

func (w *dirWriter) getFile(fileName string) (*os.File, error) {
	entry, ok := w.files.Load(fileName)
	if !ok {
		file, err := os.Create(filepath.Join(w.dir, fileName))
		if err != nil {
			return nil, fmt.Errorf("unable to create file %s: %s", fileName, err)
		}

		w.files.Store(fileName, file)

		return file, nil
	}

	return entry.(*os.File), nil
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
	file *os.File
}

func NewFileWriter(dir, file string) (io.WriteCloser, error) {
	err := createDir(dir)
	if err != nil {
		return nil, err
	}

	filePath := filepath.Join(dir, file)

	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("unable to create file %s: %s", filePath, err)
	}

	writer := &fileWriter{
		file: f,
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
