package semi_test

import (
	"context"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/partyzanex/repmy/pkg/semi"
	"github.com/partyzanex/testutils"
)

func TestRepository_Show(t *testing.T) {
	db := testutils.NewSqlDB(t, "mysql", "MASTER_TEST")

	repo := semi.New(db)
	ctx := context.Background()

	results, err := repo.Show(ctx)
	testutils.FatalErr(t, "repo.Show(ctx)", err)

	//t.Log(results)

	testutils.AssertEqualFatal(t, "count", len(results) > 0, true)
}
