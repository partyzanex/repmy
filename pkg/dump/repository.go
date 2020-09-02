package dump

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
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

func (repo *Repository) UnlockTable(ctx context.Context, table string) (sql.Result, error) {
	return repo.db.ExecContext(ctx, "UNLOCK TABLES")
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

func (repo *Repository) Count(ctx context.Context, table string) (count uint64, err error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
	row := repo.db.QueryRowContext(ctx, query)
	err = row.Scan(&count)

	return
}

func (repo *Repository) GetCreateTable(ctx context.Context, table Table) (string, error) {
	row := repo.db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE `%s`", table.Name))

	var tableName, dll string

	if table.Type == "BASE TABLE" {
		err := row.Scan(&tableName, &dll)

		if err != nil {
			return "", err
		}
	}

	if table.Type == "VIEW" {
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

func (repo *Repository) GetTableColumns(ctx context.Context, table string) ([]string, error) {
	query := fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table)

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

func (repo *Repository) GetSelectQuery(columns []string, table string, limit, offset int) string {
	cols := strings.Join(columns, ",")
	query := fmt.Sprintf("SELECT %s FROM `%s`", cols, table)

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	}

	return query
}

func (repo *Repository) GetInserts(ctx context.Context, query string, table Table) ([]string, error) {
	if table.Type != "BASE TABLE" {
		return []string{}, nil
	}

	rows, err := repo.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("unable to execute query '%s': %s", query, err)
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("unable to get columnt: %s", err)
	}

	n := len(columns)
	values := make([]*sql.RawBytes, n)
	args := make([]interface{}, n)

	for i := range values {
		args[i] = &values[i]
	}

	var (
		data   = make([]string, 0)
		raw    = make([]string, n)
		insert = fmt.Sprintf("INSERT INTO `%s` VALUES", table.Name)
		null   = "NULL"
	)

	for rows.Next() {
		err := rows.Scan(args...)
		if err != nil {
			return nil, fmt.Errorf("unable to scan row: %s", err)
		}

		for i, col := range values {
			val := null

			if col != nil {
				val = "'" + Escape(string(*col)) + "'"
			}

			raw[i] = val
		}

		data = append(data, insert+" ("+strings.Join(raw, ", ")+");")
	}

	return data, nil
}

func Escape(str string) string {
	var (
		esc  string
		buf  bytes.Buffer
		last int
	)

	for i, c := range str {
		switch c {
		case 0:
			esc = `\0`
		case '\n':
			esc = `\n`
		case '\r':
			esc = `\r`
		case '\\':
			esc = `\\`
		case '\'':
			esc = `\'`
		case '"':
			esc = `\"`
		case '\032':
			esc = `\Z`
		default:
			continue
		}

		buf.WriteString(str[last:i])
		buf.WriteString(esc)
		last = i + 1
	}

	buf.WriteString(str[last:])

	return buf.String()
}
