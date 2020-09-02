package master

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/partyzanex/repmy/pkg/mysql"
	"github.com/pkg/errors"
)

// Repository represents repository layer for master
type Repository struct {
	db *sqlx.DB
}

// ShowStatus returns Status (parsed result of `show master status`)
func (repo *Repository) ShowStatus(ctx context.Context) (status *Status, err error) {
	status = &Status{}

	err = repo.db.GetContext(ctx, status, `show master status`)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get master status")
	}

	return
}

// SetReplUser sets replication user
func (repo *Repository) SetReplUser(ctx context.Context, user mysql.ReplUser) error {
	q := `grant replication slave on %s.* to '%s'@'%s' identified by '%s';`
	q = fmt.Sprintf(q, user.GetDatabase(), user.Name, user.GetHost(), user.Password)

	_, err := repo.db.ExecContext(ctx, q)
	if err != nil {
		return errors.Wrap(err, "unable to execute query")
	}

	_, err = repo.db.ExecContext(ctx, `flush privileges;`)
	if err != nil {
		return errors.Wrap(err, "unable to flush privileges")
	}

	return nil
}

// ReadLock locks read for table in database
// returns channel for done
func (repo *Repository) ReadLock(ctx context.Context, dbName string) (chan<- struct{}, error) {
	// prepare sql queries
	q := fmt.Sprintf(`use %s; flush tables with read lock;`, dbName)

	_, err := repo.db.ExecContext(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, "unable to execute query")
	}

	done := make(chan struct{})

	go func() {
		// copy database connection
		db := *repo.db

		// waiting for unlock tables
		select {
		case <-ctx.Done():
		case <-done:
			_, err := db.Exec(`unlock tables`)
			if err != nil {
				log.Printf(`unable to execute query: 'unlock tables', error: %s`, err)
			}
		}
	}()

	return done, nil
}

// New creates a new repository
func New(db *sql.DB) *Repository {
	return &Repository{
		db: sqlx.NewDb(db, "mysql"),
	}
}
