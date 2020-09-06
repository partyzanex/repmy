package dump

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sirupsen/logrus"
)

type task struct {
	Num           int
	Limit, Offset int
	Table         Table
	Repo          *Repository
	Results       chan<- [][]byte
	Errors        chan<- error
}

func (t task) ID() int {
	return t.Num
}

func (t *task) Run(ctx context.Context) error {
	logrus.Debugf("task %d for table %s started", t.ID(), t.Table.Name)

	repo := t.Repo
	table := t.Table
	errors := t.Errors
	results := t.Results

	query := repo.GetSelectQuery(table, t.Limit, t.Offset)

	rows, err := repo.db.QueryContext(ctx, query)
	if err != nil {
		errors <- fmt.Errorf("unable to execute query '%s': %s", query, err)
		return nil
	}

	defer func() {
		err := rows.Close()
		if err != nil {
			errors <- fmt.Errorf("closing rows failed: %s", err)
		}
	}()

	var (
		n      = len(table.Columns)
		values = make([]*sql.RawBytes, n)
		args   = make([]interface{}, n)
	)

	for i := range values {
		args[i] = &values[i]
	}

	for rows.Next() {
		err := rows.Scan(args...)
		if err != nil {
			errors <- fmt.Errorf("unable to scan row: %s", err)
			return nil
		}

		raw := make([][]byte, n)

		for i, col := range values {
			val := null

			if col != nil {
				val = append(quote, Escape(*col)...)
				val = append(val, quote...)
			}

			raw[i] = val
		}

		//logrus.Debug(len(raw))
		results <- raw
	}

	return nil
}
