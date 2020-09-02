package mysql

import (
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"

	_ "github.com/davecgh/go-spew/spew"
)

// ReplUser represents user for replication
type ReplUser struct {
	Name       string
	Host       string
	Password   string
	Database   string
	MasterHost string
}

// GetDatabase returns valid database name or '*'
func (u ReplUser) GetDatabase() string {
	if u.Database == "" {
		return "*"
	}

	return u.Database
}

// GetHost valid hostname for mysql user or '%'
func (u ReplUser) GetHost() string {
	if u.Host == "" {
		return "%"
	}

	return u.Host
}

// SetMasterHost parses master host from DSN and sets to MasterHost
func (u *ReplUser) SetMasterHost(dsn string) error {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return errors.Wrap(err, "unable to parse DSN")
	}

	// split hostname and port
	parts := strings.Split(cfg.Addr, ":")
	if len(parts) < 1 {
		return errors.New("invalid DSN address")
	}

	// set only hostname
	u.MasterHost = parts[0]

	return nil
}
