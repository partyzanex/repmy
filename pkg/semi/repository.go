package semi

import (
	"context"
	"database/sql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type Repository struct {
	db *sqlx.DB
}

// show variables for semi-synchronous replication
func (repo *Repository) Show(ctx context.Context) (results []Variable, err error) {
	err = repo.db.SelectContext(ctx, &results, `show status like '%semi%'`)
	if err != nil {
		return nil, errors.Wrap(err, "executing query failed")
	}

	return
}

// create new repository
func New(db *sql.DB) *Repository {
	return &Repository{
		db: sqlx.NewDb(db, "mysql"),
	}
}
