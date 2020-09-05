package dump

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/partyzanex/repmy/pkg/pool"
	"github.com/sirupsen/logrus"
	"math"
)

type Repository struct {
	db *sql.DB
}

func New(db *sql.DB) *Repository {
	return &Repository{
		db: db,
	}
}

func (repo *Repository) LockRead(ctx context.Context, table string) (sql.Result, error) {
	return repo.db.ExecContext(ctx, fmt.Sprintf("LOCK TABLES `%s` READ", table))
}

func (repo *Repository) FlushTable(ctx context.Context, table string) (sql.Result, error) {
	return repo.db.ExecContext(ctx, fmt.Sprintf("FLUSH TABLES `%s`", table))
}

func (repo *Repository) UnlockTables(ctx context.Context) (sql.Result, error) {
	return repo.db.ExecContext(ctx, "UNLOCK TABLES")
}

func (repo *Repository) FlushTablesWithReadLock(ctx context.Context) (sql.Result, error) {
	return repo.db.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK")
}

func (repo *Repository) GetTables(ctx context.Context) ([]*Table, error) {
	var (
		tables []*Table
		rows   *sql.Rows
	)

	rows, err := repo.db.QueryContext(ctx, "SHOW FULL TABLES")
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var tableName, tableType string

		err = rows.Scan(&tableName, &tableType)
		if err != nil {
			return nil, err
		}

		tables = append(tables, &Table{
			Name: tableName,
			Type: tableType,
		})
	}

	return tables, nil
}

type Config struct {
	TableName   string
	Limit       int
	Headers     bool
	DropTable   bool
	CreateTable bool
	NoData      bool
}

func (repo *Repository) Count(ctx context.Context, table Table) (count uint64, err error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table.Name)
	row := repo.db.QueryRowContext(ctx, query)
	err = row.Scan(&count)

	return
}

func (repo *Repository) GetCreateTable(ctx context.Context, table Table) (string, error) {
	row := repo.db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`", table.Name))

	var tableName, dll string

	if table.Type == BaseTable {
		err := row.Scan(&tableName, &dll)

		if err != nil {
			return "", err
		}
	}

	if table.Type == View {
		var character, collation string

		err := row.Scan(&tableName, &dll, &character, &collation)
		if err != nil {
			return "", err
		}
	}

	if dll == "" {
		return "", fmt.Errorf("no DLL for table: %s (%s)", table.Name, table.Type)
	}

	return dll, nil

}

func (repo *Repository) GetTableColumns(ctx context.Context, table Table) ([]string, error) {
	query := fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table.Name)

	rows, err := repo.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	return columns, nil
}

// todo: replace query to SHOW COLUMNS FROM table
func (repo *Repository) GetSelectQuery(table Table, limit, offset int) string {
	query := fmt.Sprintf("SELECT %s FROM `%s`", table.GetColumns(), table.Name)

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	}

	return query
}

var (
	null  = []byte("NULL")
	quote = []byte("'")
)

func (repo *Repository) GetValues(ctx context.Context, table Table, buffer, workers int) (<-chan [][]byte, <-chan error) {
	if table.Type != BaseTable {
		return nil, nil
	}

	results := make(chan [][]byte, buffer)
	errors := make(chan error)

	go func() {
		defer func() {
			close(results)
			close(errors)
		}()

		limit, size := 1, 0

		if buffer > 0 {
			limit = int(math.Ceil(float64(table.Count) / float64(workers)))

			if limit > buffer {
				size = workers
			}
		}

		workers := pool.NewWorkersPool(pool.Size(size), pool.WithCtx(ctx))

		logrus.Debugf("runs %d workers with limit=%d and buffer=%d", size, limit, buffer)

		for i := 0; i < size; i++ {
			workers.AddTask(&task{
				Num:     i + 1,
				Table:   table,
				Repo:    repo,
				Limit:   limit,
				Offset:  i * limit,
				Results: results,
				Errors:  errors,
			})
		}

		workers.Wait()
	}()

	return results, errors
}
