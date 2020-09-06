package dump_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/partyzanex/repmy/pkg/dump"
	"github.com/partyzanex/testutils"
)

func TestFileWriter_Write(t *testing.T) {
	data := []struct {
		Input []byte
		Err   bool
	}{
		{Input: []byte("INSERT INTO `"), Err: true},
		{Input: []byte("--\n--\nCREATE TABLE IF NOT EXISTS"), Err: true},
		{Input: []byte(""), Err: true},
		{Input: []byte("INSERT INTO `table` VALUES (1, 'test')"), Err: false},
		{Input: []byte("--\ntable's data [count=8876]\n\nINSERT INTO `table` VALUES (1, 'test');\n"), Err: false},
		{Input: []byte("UPDATE `table` SET name = 'test'"), Err: true},
	}

	dirName := testutils.RandomString(12)

	defer func() {
		err := os.RemoveAll(dirName)
		if err != nil && !os.IsNotExist(err) {
			testutils.FatalErr(t, "os.RemoveAll(dirName)", err)
		}
	}()

	dir, err := dump.NewDirWriter(dirName, false)
	testutils.FatalErr(t, "dump.NewDirWriter", err)

	defer func() {
		err := dir.Close()
		testutils.FatalErr(t, "dir.Close()", err)
	}()

	for i, item := range data {
		_, err := dir.Write(item.Input)
		testutils.AssertEqual(t, fmt.Sprintf("Err %d", i), item.Err, err != nil)
		//t.Log(err)
	}
}
