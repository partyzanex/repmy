package dump_test

import (
	"context"
	"errors"
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

	_, err = repo.UnlockTable(ctx, "table")
	testutils.FatalErr(t, "repo.UnlockTable", err)
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

	count, err := repo.Count(ctx, "table")
	testutils.FatalErr(t, "repo.Count", err)

	testutils.AssertEqual(t, "count", uint64(999), count)
}

func TestRepository_GetInserts(t *testing.T) {
	db, mock, err := sqlmock.New()
	testutils.FatalErr(t, "sqlmock.New()", err)

	repo := dump.New(db)
	ctx := context.Background()

	expected := map[int]struct {
		ID   int
		Name string
	}{
		1: {ID: 1, Name: "Test 1"},
		2: {ID: 2, Name: "Test 2"},
		3: {ID: 3, Name: "Test 3"},
		4: {ID: 4, Name: "Test 4"},
		5: {ID: 5, Name: "Test 5"},
	}

	rows := sqlmock.NewRows([]string{"id", "name"})

	for _, row := range expected {
		rows.AddRow(row.ID, row.Name)
	}

	expectedSQL := "SELECT id, name FROM table LIMIT 10 OFFSET 0"
	mock.ExpectQuery(expectedSQL).WillReturnRows(rows)

	inserts, err := repo.GetInserts(ctx, expectedSQL, dump.Table{
		Name: "table",
		Type: "BASE TABLE",
	})
	testutils.FatalErr(t, "repo.GetInserts", err)

	testutils.AssertEqual(t, "", len(expected), len(inserts))

	for _, insert := range inserts {
		t.Log(insert)
	}
}

func TestEscape(t *testing.T) {
	input := string([]byte{0, '\n', '\r', '\\', '\'', '"', '\032', 'a'})
	expected := `\0\n\r\\\'\"\Za`
	result := dump.Escape(input)
	testutils.AssertEqual(t, "escape", expected, result)
}
