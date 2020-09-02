package master_test

import (
	"context"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/partyzanex/repmy/pkg/master"
	"github.com/partyzanex/repmy/pkg/mysql"
	"github.com/partyzanex/testutils"
)

func TestRepository_ShowStatus(t *testing.T) {
	db := testutils.NewSqlDB(t, "mysql", "MASTER_TEST")

	repo := master.New(db)
	ctx := context.Background()

	status, err := repo.ShowStatus(ctx)
	testutils.FatalErr(t, "repo.ShowStatus(ctx)", err)

	//t.Log(status)

	testutils.AssertEqualFatal(t, "status.Position", status.Position > 0, true)
}

func TestRepository_SetReplUser(t *testing.T) {
	db := testutils.NewSqlDB(t, "mysql", "MYSQL_TEST")

	repo := master.New(db)
	ctx := context.Background()

	err := repo.SetReplUser(ctx, mysql.ReplUser{
		Name:     "repl_test",
		Password: "123456",
	})
	testutils.FatalErr(t, "repo.SetReplUser(ctx, user.User{})", err)
}
