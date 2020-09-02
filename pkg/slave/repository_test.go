package slave_test

import (
	"context"
	"os"
	"testing"

	_ "github.com/davecgh/go-spew/spew"
	_ "github.com/partyzanex/repmy/pkg/mysql"

	"github.com/partyzanex/repmy/pkg/master"
	"github.com/partyzanex/repmy/pkg/mysql"
	"github.com/partyzanex/repmy/pkg/slave"
	"github.com/partyzanex/testutils"
)

func TestRepository_ShowStatus(t *testing.T) {
	m := testutils.NewSqlDB(t, "mysql", "MASTER_TEST")
	s := testutils.NewSqlDB(t, "mysql", "SLAVE_TEST")

	repoMaster := master.New(m)
	repoSlave := slave.New(s)
	ctx := context.Background()

	masterStatus, err := repoMaster.ShowStatus(ctx)
	testutils.FatalErr(t, "repoMaster.ShowStatus(ctx)", err)

	slaveStatus, err := repoSlave.ShowStatus(ctx)
	testutils.FatalErr(t, "repoSlave.ShowStatus(ctx)", err)

	//t.Log(spew.Sdump(masterStatus))
	//t.Log(spew.Sdump(slaveStatus))

	masterHost := os.Getenv("MASTER_HOST")
	if masterHost == "" {
		t.Fatal("empty MASTER_HOST")
	}

	_ = slaveStatus
	replUser := mysql.ReplUser{
		Name:       "repl_test",
		Password:   "123456",
		MasterHost: masterHost,
	}

	//err = replUser.SetMasterHost(os.Getenv("MASTER_TEST"))
	//testutils.FatalErr(t, `replUser.SetMasterHost(os.Getenv("MASTER_TEST"))`, err)

	err = repoMaster.SetReplUser(ctx, replUser)
	testutils.FatalErr(t, "repoMaster.SetReplUser", err)

	err = repoSlave.Stop(ctx)
	testutils.FatalErr(t, "repoSlave.Stop(ctx)", err)

	err = repoSlave.Reset(ctx)
	testutils.FatalErr(t, "repoSlave.Reset(ctx)", err)

	err = repoSlave.ChangeMaster(ctx, *masterStatus, replUser)
	testutils.FatalErr(t, "repoSlave.ChangeMaster(ctx, *masterStatus, replUser)", err)

	err = repoSlave.Start(ctx)
	testutils.FatalErr(t, "repoSlave.Start(ctx)", err)

	slaveStatus, err = repoSlave.ShowStatus(ctx)
	testutils.FatalErr(t, "repoSlave.ShowStatus(ctx)", err)

	testutils.AssertEqual(t, "SlaveIORunning", slaveStatus.SlaveIORunning, "Yes")
	testutils.AssertEqual(t, "SlaveSQLRunning", slaveStatus.SlaveSQLRunning, "Yes")
}
