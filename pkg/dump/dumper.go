package dump

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/partyzanex/repmy/pkg/pool"
	"github.com/sirupsen/logrus"
)

type Dumper struct {
	DB *sql.DB

	Output        string
	Workers       int
	Limit         int
	NoHeaders     bool
	NoDropTable   bool
	NoCreateTable bool
	NoData        bool
	Verbose       bool
}

func (d *Dumper) Dump(ctx context.Context, tables ...string) error {
	toDump, err := d.getTablesForDump(ctx, tables...)
	if err != nil {
		return err
	}

	repo := New(d.DB)

	if d.Verbose {
		logrus.Infof("flush tables with read lock")
	}

	_, err = repo.FlushTablesWithReadLock(ctx)
	if err != nil {
		return fmt.Errorf("flush tables with read lock failed: %s", err)
	}

	defer func() {
		_, err = repo.UnlockTables(ctx)
		if err != nil {
			logrus.Error(err)
			return
		}

		if d.Verbose {
			logrus.Infof("unlock tables for read")
		}
	}()

	d.Run(ctx, toDump...)

	return nil
}

func (d *Dumper) getTablesForDump(ctx context.Context, tables ...string) ([]*Table, error) {
	repo := New(d.DB)

	tbs, err := repo.GetTables(ctx)
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

func (d *Dumper) Run(ctx context.Context, tables ...*Table) {
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
			logrus.Error(err)
		}

		return nil
	}, nil)

	if d.Verbose {
		logrus.Infof("runs %d processes", d.Workers)
	}

	for i := 0; i < d.Workers; i++ {
		processes.RunProcess(ctx, d.processDump(tch), nil)
	}

	processes.Wait()
	errors.Wait()
}

func (d *Dumper) processDump(tables <-chan *Table) pool.Process {
	return func(ctx context.Context) error {
		for table := range tables {
			if d.Verbose {
				logrus.Infof("starting dump for table '%s'", table.Name)
			}

			err := d.dumpTable(ctx, table)
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

func (d *Dumper) dumpTable(ctx context.Context, table *Table) error {
	repo := New(d.DB)

	fileName := filepath.Join(d.Output, table.Name+".sql")
	logrus.Debugf("creating file %s", fileName)

	file, err := os.Create(fileName)
	if err != nil {
		return err
	}

	defer file.Close()

	if !d.NoHeaders {
		str := fmt.Sprintf("--\n-- Structure for table `%s`\n--\n\n", table.Name)

		_, err := io.WriteString(file, str)
		if err != nil {
			return fmt.Errorf("unable to write %s to file: %s", str, err)
		}
	}

	if !d.NoDropTable {
		str := fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table.Name)

		_, err := io.WriteString(file, str)
		if err != nil {
			return fmt.Errorf("unable to write %s to file: %s", str, err)
		}
	}

	if !d.NoCreateTable {
		logrus.Debugf("getting DLL for table %s", table.Name)

		dll, err := repo.GetCreateTable(ctx, *table)
		if err != nil {
			return err
		}

		str := fmt.Sprintf("%s;\n", dll)

		_, err = io.WriteString(file, str)
		if err != nil {
			return fmt.Errorf("unable to write %s to file: %s", str, err)
		}
	}

	if d.NoData {
		return nil
	}

	count, err := repo.Count(ctx, table.Name)
	if err != nil {
		return err
	}

	logrus.Debugf("count rows of table %s - %d", table.Name, count)

	dataHeader := "\n"

	if !d.NoHeaders {
		dataHeader += fmt.Sprintf("-- %s's data [count=%d]\n", table.Name, count)
	}

	_, err = io.WriteString(file, dataHeader)
	if err != nil {
		return fmt.Errorf("unable to write %s to file: %s", dataHeader, err)
	}

	if count == 0 {
		return nil
	}

	logrus.Debugf("gets values from repo for table %s", table.Name)

	values, errors := repo.GetValues(ctx, *table, 5000000)

	go func() {
		for err := range errors {
			logrus.Error(err)
		}
	}()

	insert := []byte(fmt.Sprintf("INSERT INTO `%s` VALUES ", table.Name))
	max, current := d.Limit, 0

	buf := &bytes.Buffer{}
	buf.Write(insert)

	var (
		openParenthesis   = []byte("(")
		closedParenthesis = []byte(")")
		comma             = []byte(",")
		commaSpace        = []byte(", ")
		eol               = []byte(";\n")
	)

	for raw := range values {
		if current > 0 {
			buf.Write(commaSpace)
		}

		buf.Write(openParenthesis)
		buf.Write(bytes.Join(raw, comma))
		buf.Write(closedParenthesis)
		current++

		if current == max {
			//logrus.Debugf("writing %d rows to dump for table %s", current, table.Name)
			buf.Write(eol)

			_, err := buf.WriteTo(file)
			if err != nil {
				n := buf.Len()
				if n > 100 {
					n = 100
				}

				return fmt.Errorf("writing %s... failed: %s", buf.Bytes()[0:n], err)
			}

			buf.Reset()
			buf.Write(insert)
			current = 0
		}
	}

	if current > 0 {
		buf.Write(eol)

		_, err := buf.WriteTo(file)
		if err != nil {
			n := buf.Len()
			if n > 100 {
				n = 100
			}

			return fmt.Errorf("writing %s... failed: %s", buf.Bytes()[0:n], err)
		}
	}

	return nil
}
