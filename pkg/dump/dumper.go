package dump

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"sync"

	"github.com/partyzanex/repmy/pkg/pool"
	"github.com/sirupsen/logrus"
)

var (
	ErrNoTableRows = fmt.Errorf("no table rows")
)

type Dumper struct {
	Source *sql.DB

	Dest   io.WriteCloser
	Output string

	Threads int
	Workers int
	MaxRows int
	Buffer  int

	NoHeaders   bool
	NoDropTable bool
	NoData      bool
	Verbose     bool

	repo *Repository
}

func (d *Dumper) Repo() *Repository {
	if d.repo == nil {
		d.repo = New(d.Source)
	}

	return d.repo
}

func (d *Dumper) DumpDLL(ctx context.Context, w io.WriteCloser, tables ...string) (err error) {
	defer closeWriter(w, err)

	buf := &bytes.Buffer{}

	toDump, err := d.getTablesForDump(ctx, tables...)
	if err != nil {
		return
	}

	if d.Verbose {
		logrus.Infof("create DLL dump for %d tables", len(toDump))
	}

	for _, table := range toDump {
		err = d.writeTableHeaders(buf, table)
		if err != nil {
			return
		}

		err = d.writeDropTable(buf, table)
		if err != nil {
			return
		}

		err = d.writeCreateTable(ctx, buf, table)
		if err != nil {
			return
		}
	}

	_, err = buf.WriteTo(w)
	if err != nil {
		return
	}

	if d.Verbose {
		logrus.Infof("DLL was been successfully created for %d tables", len(toDump))
	}

	return
}

func (d *Dumper) DumpData(ctx context.Context, w io.WriteCloser, tables ...string) (err error) {
	defer closeWriter(w, err)

	toDump, err := d.getTablesForDump(ctx, tables...)
	if err != nil {
		return
	}

	if d.Verbose {
		logrus.Infof("flush tables with read lock")
	}

	_, err = d.Repo().FlushTablesWithReadLock(ctx)
	if err != nil {
		err = fmt.Errorf("flush tables with read lock failed: %s", err)
		return
	}

	defer func() {
		_, err := d.Repo().UnlockTables(ctx)
		if err != nil {
			logrus.Error(err)
			return
		}

		if d.Verbose {
			logrus.Infof("unlock tables for read")
		}
	}()

	d.dumpData(ctx, w, toDump...)

	return
}

func (d *Dumper) getTablesForDump(ctx context.Context, tables ...string) ([]*Table, error) {
	tbs, err := d.Repo().GetTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get tables: %s", err)
	}

	var (
		uniq   = make(map[string]*Table)
		toDump = make([]*Table, 0)
	)

	if len(tables) > 0 {
		for _, tbl := range tbs {
			uniq[tbl.Name] = tbl
		}

		for _, table := range tables {
			tbl, ok := uniq[table]
			if !ok {
				return nil, fmt.Errorf("table %s is not exists", table)
			}

			toDump = append(toDump, tbl)
		}
	} else {
		toDump = tbs
	}

	return toDump, nil
}

func (d *Dumper) dumpData(ctx context.Context, w io.Writer, tables ...*Table) {
	if d.Verbose {
		logrus.Infof("runs dump for %d tables", len(tables))
	}

	tch := make(chan *Table, len(tables))

	for _, table := range tables {
		tch <- table
	}

	close(tch)

	processes := &pool.ProcessPool{}
	errors := &pool.ProcessPool{}

	errors.RunProcess(ctx, func(ctx context.Context) error {
		errs := processes.Errors()

		for err := range errs {
			logrus.Errorf("process error: %s", err)
		}

		return nil
	}, nil)

	if d.Verbose {
		logrus.Infof("runs %d processes", d.Threads)
	}

	for i := 0; i < d.Threads; i++ {
		processes.RunProcess(ctx, d.processDump(tch, w), nil)
	}

	processes.Wait()
	errors.Wait()
}

func (d *Dumper) processDump(tables <-chan *Table, w io.Writer) pool.Process {
	return func(ctx context.Context) error {
		for table := range tables {
			if d.Verbose {
				logrus.Infof("starting dump for table '%s'", table.Name)
			}

			columns, err := d.Repo().GetTableColumns(ctx, *table)
			if err != nil {
				return err
			}

			table.Columns = columns

			err = d.dumpTable(ctx, w, table)
			if err != nil {
				return err
			}

			if d.Verbose {
				logrus.Infof("finished dump for table '%s'", table.Name)
			}
		}

		return nil
	}
}

var (
	openParenthesis   = []byte("(")
	closedParenthesis = []byte(")")
	comma             = []byte(",")
	commaSpace        = []byte(", ")
	eol               = []byte(";\n")
)

func (d *Dumper) dumpTable(ctx context.Context, w io.Writer, table *Table) (err error) {
	var (
		wg  = &sync.WaitGroup{}
		buf = &bytes.Buffer{}

		insert  = []byte(fmt.Sprintf("INSERT INTO `%s` VALUES ", table.Name))
		max     = d.MaxRows
		current = 0
	)

	err = d.writeDataHeaders(ctx, buf, table)
	if err != nil && err != ErrNoTableRows {
		return
	}

	if err == ErrNoTableRows {
		return nil
	}

	logrus.Debugf("gets values from repo for table %s", table.Name)

	values, errors := d.Repo().GetValues(ctx, *table, d.Buffer, d.Workers)

	buf.Write(insert)
	wg.Add(1)

	go func() {
		for err := range errors {
			logrus.Errorf("err: %s", err)
		}

		wg.Done()
	}()

Results:
	for {
		select {
		case raw, ok := <-values:
			if !ok && raw == nil {
				break Results
			}

			if current > 0 {
				buf.Write(commaSpace)
			}

			buf.Write(openParenthesis)
			buf.Write(bytes.Join(raw, comma))
			buf.Write(closedParenthesis)
			current++

			if current == max {
				buf.Write(eol)

				err = d.writeBuffer(buf, w)
				if err != nil {
					return
				}

				buf.Reset()
				buf.Write(insert)
				current = 0
			}
		}
	}

	if current > 0 {
		buf.Write(eol)

		err = d.writeBuffer(buf, w)
		if err != nil {
			return
		}
	}

	wg.Wait()

	return nil
}

func (d *Dumper) writeTableHeaders(w io.Writer, table *Table) error {
	if !d.NoHeaders {
		str := fmt.Sprintf("--\n-- Structure for table `%s`\n--\n\n", table.Name)

		_, err := io.WriteString(w, str)
		if err != nil {
			return fmt.Errorf("unable to write %s to file: %s", str, err)
		}
	}

	return nil
}

func (d *Dumper) writeDropTable(w io.Writer, table *Table) error {
	if !d.NoDropTable {
		str := fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table.Name)

		_, err := io.WriteString(w, str)
		if err != nil {
			return fmt.Errorf("unable to write %s to file: %s", str, err)
		}
	}

	return nil
}

func (d *Dumper) writeCreateTable(ctx context.Context, w io.Writer, table *Table) error {
	logrus.Debugf("getting DLL for table %s", table.Name)

	dll, err := d.Repo().GetCreateTable(ctx, *table)
	if err != nil {
		return err
	}

	str := fmt.Sprintf("%s;\n\n", dll)

	_, err = io.WriteString(w, str)
	if err != nil {
		return fmt.Errorf("unable to write %s to file: %s", str, err)
	}

	return nil
}

func (d *Dumper) writeDataHeaders(ctx context.Context, w io.Writer, table *Table) error {
	if d.NoData {
		return ErrNoTableRows
	}

	count, err := d.Repo().Count(ctx, *table)
	if err != nil {
		return err
	}

	table.Count = count

	dataHeader := ""

	if !d.NoHeaders {
		dataHeader += fmt.Sprintf("-- %s's data [count=%d]\n", table.Name, count)
	}

	_, err = io.WriteString(w, dataHeader)
	if err != nil {
		return fmt.Errorf("unable to write %s to file: %s", dataHeader, err)
	}

	if count == 0 {
		return ErrNoTableRows
	}

	return nil
}

func (Dumper) writeBuffer(buf *bytes.Buffer, dst io.Writer) error {
	_, err := buf.WriteTo(dst)
	if err != nil {
		n := buf.Len()
		if n > 100 {
			n = 100
		}

		return fmt.Errorf("writing %s... failed: %s", buf.Bytes()[0:n], err)
	}

	return nil
}
