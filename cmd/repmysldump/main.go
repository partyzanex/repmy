package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/partyzanex/repmy/pkg/dump"
	"github.com/partyzanex/repmy/pkg/mysqldump"
	"github.com/partyzanex/repmy/pkg/pool"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"io"
	"os"
	"os/signal"
	"strings"
)

var (
	args    = pflag.StringP("args", "-a", "", "mysqldump arguments")
	tables  = pflag.StringSlice("tables", []string{}, "tables list")
	threads = pflag.IntP("threads", "t", 1, "the number of tables read at the same time")
	source  = pflag.StringP("source", "s", "", "source DSN, ex. 'user:password@tcp(localhost:3306)/source_db'")
	output  = pflag.StringP("output", "o", "dump", "output dir")
)

func main() {
	pflag.Parse()

	cfg, err := mysql.ParseDSN(*source)
	if err != nil {
		exit(err.Error())
	}

	src, err := sql.Open("mysql", *source)
	if err != nil {
		exit(fmt.Sprintf("unable to open source database: %s", err))
	}

	//repo := dump.New(src)
	dumper := &dump.Dumper{Source: src}

	ctx, cancel := context.WithCancel(context.Background())
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, os.Kill)

	go func() {
		<-quit
		cancel()
	}()

	workers := pool.NewWorkersPool(pool.Size(*threads))
	processes := &pool.ProcessPool{}
	errors := &pool.ProcessPool{}

	errors.RunProcess(ctx, func(ctx context.Context) error {
		errs := processes.Errors()

		for err := range errs {
			logrus.Fatal(err)
		}

		return nil
	}, nil)

	tbs, err := dumper.GetTablesForDump(ctx, *tables...)
	if err != nil {
		logrus.Error(err)
		return
	}

	dir, err := dump.NewDirWriter(*output, false)
	if err != nil {
		logrus.Error(err)
		return
	}

	defer func() {
		err := dir.Close()
		if err != nil {
			logrus.Error(err)
		}
	}()

	results := make([]chan []byte, len(tbs))
	writers := make([]io.WriteCloser, len(tbs))

	host, port := parseAddr(cfg.Addr)
	creds := []string{"-u", cfg.User, "-p" + cfg.Passwd, "-h", host, "-P", port}
	a := append(creds, strings.Split(*args, " ")...)

	for i, table := range tbs {
		results[i] = make(chan []byte)

		processes.RunProcess(ctx, func(ctx context.Context) error {
			result := results[i]
			tbl := table

		WriteToFile:
			for {
				select {
				case v, ok := <-result:
					if !ok {
						break WriteToFile
					}

					file, err := dir.GetFile(tbl.Name + ".sql")
					if err != nil {
						return err
					}

					_, err = file.Write(v)
					if err != nil {
						return err
					}
				}
			}

			return nil
		}, nil)

		writers[i] = mysqldump.NewWriter(results[i])

		task := mysqldump.NewExecutor(writers[i], a...)
		workers.AddTask(task)
	}

	workers.Wait()

	for _, w := range writers {
		err := w.Close()
		if err != nil {
			logrus.Error(err)
		}
	}

	processes.Wait()
	errors.Wait()
}

func exit(msg string) {
	logrus.Info(msg)
	os.Exit(0)
}

func parseAddr(addr string) (host, port string) {
	p := strings.Split(addr, ":")
	n := len(p)

	if n > 0 {
		host = p[0]
	}

	if n > 1 {
		port = p[1]
	}

	return
}

type Result chan []byte
