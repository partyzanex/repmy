package dump

type DBWriter struct {
}

func (w *DBWriter) Write(b []byte) (int, error) {
	return 0, nil
}

func (w *DBWriter) Close() error {
	return nil
}
