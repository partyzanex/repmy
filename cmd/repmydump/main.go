package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"

	"github.com/partyzanex/repmy/pkg/dump"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"

	_ "github.com/go-sql-driver/mysql"
)

var (
	source  = pflag.StringP("source", "s", "", "source DSN, ex. 'user:password@tcp(localhost:3306)/source_db'")
	dest    = pflag.StringP("dest", "d", "", "destination DSN, ex. 'user:password@tcp(localhost:3306)/dest_db'")
	threads = pflag.IntP("threads", "t", 1, "the number of tables read at the same time")
	workers = pflag.IntP("workers", "w", 1, "number of simultaneous reads from one table")
	buffer  = pflag.IntP("buffer", "b", 100000, "max buffer size in rows, affects memory allocation")
	max     = pflag.IntP("max-rows", "m", 1000, "number of rows written in one insert")
	verbose = pflag.BoolP("verbose", "v", false, "verbose progress")
	output  = pflag.StringP("output", "o", "dump", "output dir")

	tables = pflag.StringSlice("tables", []string{}, "tables list")

	noHeaders   = pflag.Bool("no-headers", false, "dump tables without headers")
	noDropTable = pflag.Bool("no-drop-table", false, "dump tables without DROP TABLE IF EXISTS ...")
	noData      = pflag.Bool("no-data", false, "dump only DLL (without data)")

	debug = pflag.Bool("debug", false, "debug mode")
)

func main() {
	pflag.Parse()

	if *debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	if *source == "" {
		exit("flag --source [-s] is required")
	}

	//if *output == "" {
	//	exit("flag --output is required")
	//}

	var (
		src, dst *sql.DB
		err      error
	)

	_ = dst

	src, err = sql.Open("mysql", *source)
	if err != nil {
		exit(fmt.Sprintf("unable to open source database: %s", err))
	}

	if *dest != "" {
		dst, err = sql.Open("mysql", *dest)
		if err != nil {
			exit(fmt.Sprintf("unable to open destination database: %s", err))
		}
	}

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, os.Kill)

	go func() {
		<-quit
		exit("exited")
	}()

	d := dump.Dumper{
		Source:      src,
		Output:      *output,
		Threads:     *threads,
		Workers:     *workers,
		Buffer:      *buffer,
		MaxRows:     *max,
		NoHeaders:   *noHeaders,
		NoDropTable: *noDropTable,
		NoData:      *noData,
		Verbose:     *verbose,
	}

	ctx := context.Background()

	dll, err := dump.NewFileWriter(*output, "__dll.sql")
	if err != nil {
		exit(err.Error())
	}

	err = d.DumpDLL(ctx, dll, *tables...)
	if err != nil {
		_ = dll.Close()
		exit(err.Error())
	}

	dir, err := dump.NewDirWriter(*output)
	if err != nil {
		exit(err.Error())
	}

	err = d.DumpData(ctx, dir, *tables...)
	if err != nil {
		exit(err.Error())
	}
}

func exit(msg string) {
	logrus.Info(msg)
	os.Exit(0)
}
