package dump_test

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/partyzanex/repmy/pkg/dump"
	"github.com/partyzanex/testutils"
)

func TestRepository_LockRead(t *testing.T) {
	db, mock, err := sqlmock.New()
	testutils.FatalErr(t, "sqlmock.New()", err)

	repo := dump.New(db)
	ctx := context.Background()

	mock.ExpectExec("LOCK TABLES `table` READ").WillReturnResult(sqlmock.NewResult(0, 1))

	_, err = repo.LockRead(ctx, "table")
	testutils.FatalErr(t, "repo.LockRead", err)
}

func TestRepository_FlushTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	testutils.FatalErr(t, "sqlmock.New()", err)

	repo := dump.New(db)
	ctx := context.Background()

	mock.ExpectExec("FLUSH TABLES `table`").WillReturnResult(sqlmock.NewResult(0, 1))

	_, err = repo.FlushTable(ctx, "table")
	testutils.FatalErr(t, "repo.FlushTable", err)
}

func TestRepository_UnlockTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	testutils.FatalErr(t, "sqlmock.New()", err)

	repo := dump.New(db)
	ctx := context.Background()

	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(sqlmock.NewResult(0, 1))

	_, err = repo.UnlockTables(ctx)
	testutils.FatalErr(t, "repo.UnlockTables", err)
}

func TestRepository_GetTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	testutils.FatalErr(t, "sqlmock.New()", err)

	repo := dump.New(db)
	ctx := context.Background()

	table := dump.Table{
		Name: "table",
		Type: "BASE TABLE",
	}

	view := dump.Table{
		Name: "view",
		Type: "VIEW",
	}

	rows := sqlmock.NewRows([]string{"Tables_in_database", "Table_type"})
	rows.AddRow(table.Name, table.Name)
	rows.AddRow("view", "VIEW")

	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(rows)

	tables, err := repo.GetTables(ctx)
	testutils.FatalErr(t, "repo.GetTables(ctx)", err)

	testutils.AssertEqual(t, "len(tables)", len(tables), 2)

	f := 0

	for _, tb := range tables {
		if tb.Type == table.Name {
			testutils.AssertEqual(t, "table.Name", tb.Name, table.Name)
			f++
		}

		if tb.Type == view.Type {
			testutils.AssertEqual(t, "view.Name", tb.Name, view.Name)
			f++
		}
	}

	testutils.AssertEqual(t, "found", f, 2)

	exp := errors.New("expected error")
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnError(exp)

	tables, err = repo.GetTables(ctx)

	testutils.AssertEqual(t, "err", exp, err)
	testutils.AssertEqual(t, "len(tables)", len(tables), 0)
}

func TestRepository_Count(t *testing.T) {
	db, mock, err := sqlmock.New()
	testutils.FatalErr(t, "sqlmock.New()", err)

	repo := dump.New(db)
	ctx := context.Background()

	rows := sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(999)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table`").WillReturnRows(rows)

	table := dump.Table{
		Name: "table",
		Type: dump.BaseTable,
	}

	count, err := repo.Count(ctx, table)
	testutils.FatalErr(t, "repo.Count", err)

	testutils.AssertEqual(t, "count", uint64(999), count)
}

type expect struct {
	ID    int
	Name  string
	Hash  string
	Descr string
	Data  int64
}

func TestRepository_GetValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	testutils.FatalErr(t, "sqlmock.New()", err)

	repo := dump.New(db)
	ctx := context.Background()

	columns := []string{"id", "name", "hash", "descr", "data"}
	rows := sqlmock.NewRows(columns)
	n := 1000
	expected := make([]expect, n)

	for i := 0; i < n; i++ {
		row := expect{
			ID:    i,
			Name:  testutils.RandomString(30),
			Hash:  testutils.RandomString(48),
			Descr: testutils.RandomString(500),
			Data:  testutils.RandInt64(0, 100000),
		}

		rows.AddRow(row.ID, row.Name, row.Hash, row.Descr, row.Data)
		expected[i] = row
	}

	mock.ExpectQuery("SELECT `id`, `name`, `hash`, `descr`, `data` FROM `tbl`").WillReturnRows(rows)
	table := dump.Table{
		Name:    "tbl",
		Type:    dump.BaseTable,
		Columns: columns,
	}

	results, errs := repo.GetValues(ctx, table, 1000, 1)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		for err := range errs {
			testutils.Err(t, "err", err)
		}

		wg.Done()
	}()

	i := 0
	wrap := func(str string) string {
		return "'" + str + "'"
	}

	for raw := range results {
		testutils.AssertEqual(t, "ID", string(raw[0]), wrap(strconv.Itoa(expected[i].ID)))
		testutils.AssertEqual(t, "Name", string(raw[1]), wrap(expected[i].Name))
		testutils.AssertEqual(t, "Hash", string(raw[2]), wrap(expected[i].Hash))
		testutils.AssertEqual(t, "Descr", string(raw[3]), wrap(expected[i].Descr))
		testutils.AssertEqual(t, "Data", string(raw[4]), wrap(strconv.FormatInt(expected[i].Data, 10)))
		i++
	}

	wg.Wait()
}

func BenchmarkRepository_GetValues(b *testing.B) {
	db, mock, err := sqlmock.New()
	testutils.FatalErr(b, "sqlmock.New()", err)

	repo := dump.New(db)
	ctx := context.Background()

	columns := []string{"id", "name", "hash", "descr", "data"}
	rows := sqlmock.NewRows(columns)
	n := 10000
	expected := make([]expect, n)

	for i := 0; i < n; i++ {
		row := expect{
			ID:    i,
			Name:  testutils.RandomString(30),
			Hash:  testutils.RandomString(48),
			Descr: testutils.RandomString(500),
			Data:  testutils.RandInt64(0, 100000),
		}

		rows.AddRow(row.ID, row.Name, row.Hash, row.Descr, row.Data)
		expected[i] = row
	}

	mock.ExpectQuery("SELECT `id`, `name`, `hash`, `descr`, `data` FROM `tbl`").WillReturnRows(rows)
	table := dump.Table{
		Name:    "tbl",
		Type:    dump.BaseTable,
		Columns: columns,
	}

	results, _ := repo.GetValues(ctx, table, 1000, 1)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		//<-results
		for raw := range results {
			_ = raw
		}
	}
}

var (
	null  = []byte("NULL")
	quote = []byte("'")
)

func BenchmarkRepository_GetValues2(b *testing.B) {
	b.StopTimer()
	db, mock, err := sqlmock.New()
	testutils.FatalErr(b, "sqlmock.New()", err)

	repo := dump.New(db)
	ctx := context.Background()

	columns := []string{"id", "name", "hash", "descr", "data"}
	rows := sqlmock.NewRows(columns)
	n := 10000
	expected := make([]expect, n)

	for i := 0; i < n; i++ {
		row := expect{
			ID:    i,
			Name:  testutils.RandomString(30),
			Hash:  testutils.RandomString(48),
			Descr: testutils.RandomString(500),
			Data:  testutils.RandInt64(0, 100000),
		}

		rows.AddRow(row.ID, row.Name, row.Hash, row.Descr, row.Data)
		expected[i] = row
	}

	table := dump.Table{
		Name:    "tbl",
		Type:    dump.BaseTable,
		Columns: columns,
	}

	query := repo.GetSelectQuery(table, 0, 0)

	k := len(table.Columns)
	values := make([]*sql.RawBytes, k)
	args := make([]interface{}, k)

	for i := range values {
		args[i] = &values[i]
	}

	b.ResetTimer()
	b.StartTimer()

	//b.RunParallel(func(pb *testing.PB) {
	//	for pb.Next() {
	//		<-results
	//	}
	//})

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mock.ExpectQuery("SELECT `id`, `name`, `hash`, `descr`, `data` FROM `tbl`").WillReturnRows(rows)
		b.StartTimer()

		results := getChannel()
		wg := &sync.WaitGroup{}
		wg.Add(1)

		go func() {
			for result := range results {
				b.StopTimer()
				_ = result
				b.StartTimer()
			}

			b.StopTimer()
			wg.Done()
			b.StartTimer()
		}()

		raws, _ := db.QueryContext(ctx, query)
		//testutils.FatalErr(b, "db.QueryContext(ctx, query)", err)

		rowsScanAndWrite(raws, results, args, k, values)

		_ = raws.Close()

		close(results)

		wg.Wait()
	}
}

func rowsScanAndWrite(rows *sql.Rows, results chan<- [][]byte, args []interface{}, k int, values []*sql.RawBytes) {
	for rows.Next() {
		_ = rows.Scan(args...)
		//testutils.FatalErr(b, "raws.Scan(args...)", err)

		raw := make([][]byte, k)

		for i, col := range values {
			val := null

			if col != nil {
				val = append(quote, dump.Escape(*col)...)
				val = append(val, quote...)
			}

			raw[i] = val
		}

		results <- raw
	}
}

func getChannel() chan [][]byte {
	return make(chan [][]byte)
}

func TestRepository_GetValues3(t *testing.T) {
	results := make(chan []byte, 100)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		//for result := range results {
		//	_ = result
		//}
	Loop:
		for {
			v, ok := <-results
			_ = v
			//t.Logf("%s %v", v, ok)
			if !ok {
				break Loop
			}

			//select {
			//case v, ok := <-results:
			//	t.Logf("%s %v", v, ok)
			//	if !ok {
			//		break Loop
			//	}
			//}
		}

		wg.Done()
	}()

	for i := 0; i < 100; i++ {
		results <- []byte(strconv.Itoa(i))
	}

	close(results)

	wg.Wait()
}

func BenchmarkRepository_GetValues3(b *testing.B) {
	results := make(chan []byte, 1000)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		//for result := range results {
		//	_ = result
		//}
	Loop:
		for {
			//_, ok := <-results
			//if !ok {
			//	break Loop
			//}

			select {
			case _, ok := <-results:
				if !ok {
					break Loop
				}
			}
		}

		wg.Done()
	}()

	str := []byte("a12dsd")

	for i := 0; i < b.N; i++ {
		results <- str
	}

	close(results)

	wg.Wait()
}
