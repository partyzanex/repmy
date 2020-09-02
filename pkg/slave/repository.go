package slave

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/partyzanex/repmy/pkg/master"
	"github.com/partyzanex/repmy/pkg/mysql"
	"github.com/pkg/errors"
)

// Repository represents repository layer for slave
type Repository struct {
	db *sqlx.DB
}

// New creates a new repository
func New(db *sql.DB) *Repository {
	return &Repository{
		db: sqlx.NewDb(db, "mysql"),
	}
}

// ShowStatus returns Status (parsed result of `show slave status`)
func (repo *Repository) ShowStatus(ctx context.Context) (status *Status, err error) {
	status = &Status{}

	err = repo.db.GetContext(ctx, status, `show slave status`)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get slave status")
	}

	return
}

// ChangeMaster executes CHANGE MASTER TO query
func (repo *Repository) ChangeMaster(ctx context.Context, status master.Status, user mysql.ReplUser) error {
	q := `
CHANGE MASTER TO 
	MASTER_HOST='%s', 
	MASTER_USER='%s', 
	MASTER_PASSWORD='%s', 
	MASTER_LOG_FILE='%s', 
	MASTER_LOG_POS=%d;
`

	q = fmt.Sprintf(q, user.MasterHost, user.Name, user.Password, status.File, status.Position)

	_, err := repo.db.ExecContext(ctx, q)
	if err != nil {
		return errors.Wrap(err, "unable to change master")
	}

	return nil
}

// Start executes START SLAVE
func (repo *Repository) Start(ctx context.Context) error {
	_, err := repo.db.ExecContext(ctx, `START SLAVE`)
	if err != nil {
		return errors.Wrap(err, "unable to start slave")
	}

	return nil
}

// Stop executes STOP SLAVE
func (repo *Repository) Stop(ctx context.Context) error {
	_, err := repo.db.ExecContext(ctx, `STOP SLAVE`)
	if err != nil {
		return errors.Wrap(err, "unable to stop slave")
	}

	return nil
}

// Reset executes RESET SLAVE
func (repo *Repository) Reset(ctx context.Context) error {
	_, err := repo.db.ExecContext(ctx, `RESET SLAVE`)
	if err != nil {
		return errors.Wrap(err, "unable to reset slave")
	}

	return nil
}
