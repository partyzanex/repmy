package dump

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"

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

const sep = "\n-- separator --\n"

func (d *Dumper) Dump(ctx context.Context, tables ...string) error {
	repo := New(d.DB)

	tbs, err := repo.GetTables(ctx)
	if err != nil {
		return fmt.Errorf("unable to get tables: %s", err)
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
				return fmt.Errorf("table %s is not exists", table)
			}

			toDump = append(toDump, tbl)
		}
	} else {
		toDump = tbs
	}

	tch := make(chan *Table, len(toDump))

	for _, table := range toDump {
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

	for i := 0; i < d.Workers; i++ {
		processes.RunProcess(ctx, func(ctx context.Context) error {
			for table := range tch {
				_, err := repo.FlushTable(ctx, table.Name)
				if err != nil {
					return err
				}

				_, err = repo.LockRead(ctx, table.Name)
				if err != nil {
					return err
				}

				if d.Verbose {
					logrus.Infof("start dump table '%s'", table.Name)
				}

				results, errs := d.DumpTable(ctx, *table)

				file, err := os.Create(filepath.Join(d.Output, table.Name+".sql"))
				if err != nil {
					return err
				}

				go func() {
					for err := range errs {
						logrus.Error(err)
					}
				}()

				for result := range results {
					_, err := io.WriteString(file, result+sep)
					if err != nil {
						n := len(result)
						if n > 100 {
							n = 100
						}

						return fmt.Errorf("write %s... failed: %s", result[0:n], err)
					}
				}

				err = file.Close()
				if err != nil {
					return err
				}

				if d.Verbose {
					logrus.Infof("finished dump table '%s'", table.Name)
				}

				_, err = repo.UnlockTable(ctx, table.Name)
				if err != nil {
					return err
				}
			}

			return nil
		}, nil)
	}

	processes.Wait()
	errors.Wait()

	return nil
}

func (d *Dumper) DumpTable(ctx context.Context, table Table) (<-chan string, <-chan error) {
	results := make(chan string)
	errors := make(chan error)

	go func() {
		defer func() {
			close(results)
			close(errors)
		}()

		repo := New(d.DB)
		workers := pool.NewWorkersPool(pool.Size(1))

		if !d.NoHeaders {
			results <- fmt.Sprintf("\n--\n-- Structure for table `%s`\n--\n\n", table.Name)
		}

		if !d.NoDropTable {
			results <- fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", table.Name)
		}

		if !d.NoCreateTable {
			dll, err := repo.GetCreateTable(ctx, table)
			if err != nil {
				errors <- err
				return
			}

			results <- fmt.Sprintf("%s;\n", dll)
		}

		if d.NoData {
			return
		}

		count, err := repo.Count(ctx, table.Name)
		if err != nil {
			errors <- err
			return
		}

		n := 1
		limit := 0

		if d.Limit > 0 {
			n = int(math.Ceil(float64(count) / float64(d.Limit)))
			limit = d.Limit
		}

		columns, err := repo.GetTableColumns(ctx, table.Name)
		if err != nil {
			errors <- err
			return
		}

		dataHeader := "\n"

		if !d.NoHeaders {
			dataHeader += fmt.Sprintf("-- %s's data\n", table.Name)
		}

		for i := 0; i < n; i++ {
			workers.AddTask(&task{
				UID:    i + 1,
				Limit:  limit,
				Offset: i * limit,
				RunFunc: func(t *task) error {
					defer logrus.Debugf("Task %d finished for table '%s'", t.ID(), table.Name)

					query := repo.GetSelectQuery(columns, table.Name, t.Limit, t.Offset)

					inserts, err := repo.GetInserts(ctx, query, table)
					if err != nil {
						return fmt.Errorf("unable to get inserts: %s", err)
					}

					results <- dataHeader + strings.Join(inserts, "\n")

					runtime.Gosched()

					return nil
				},
			})
		}

		workers.Wait()
	}()

	return results, errors
}

type Formatter struct {
	logrus.Formatter
}

var prefix = []byte("\n\n--\n-- ")
var postfix = []byte("--\n")

func (f *Formatter) Format(entry *logrus.Entry) ([]byte, error) {
	b, err := f.Formatter.Format(entry)
	if err != nil {
		return b, err
	}

	b = append(prefix, b...)
	b = append(b, postfix...)

	return b, err
}
