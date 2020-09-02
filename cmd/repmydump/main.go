package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/partyzanex/repmy/pkg/dump"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"

	_ "github.com/go-sql-driver/mysql"
)

var (
	user    = pflag.StringP("user", "u", "", "user")
	pass    = pflag.StringP("password", "p", "", "password")
	host    = pflag.StringP("host", "h", "localhost", "hostname")
	port    = pflag.Uint16P("port", "P", 3306, "port")
	dbname  = pflag.StringP("database", "d", "", "database")
	workers = pflag.IntP("treads", "t", 1, "number of treads")
	limit   = pflag.IntP("limit", "l", 0, "limit of rows")
	verbose = pflag.BoolP("verbose", "v", false, "verbose progress")
	file    = pflag.StringP("file", "f", "", "file path")

	tables = pflag.StringSlice("tables", []string{}, "tables list")

	noHeaders     = pflag.Bool("no-headers", false, "dump tables without headers")
	noDropTable   = pflag.Bool("no-drop-table", false, "dump tables without DROP TABLE IF EXISTS ...")
	noCreateTable = pflag.Bool("no-create-table", false, "dump tables without DLL (data only)")
	noData        = pflag.Bool("no-data", false, "dump only DLL (without data)")
)

func main() {
	pflag.Parse()

	if *user == "" {
		fatal("flag --user is required")
	}

	if *dbname == "" {
		fatal("flag --db is required")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", *user, *pass, *host, *port, *dbname)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fatal(err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, os.Kill)

	go func() {
		<-quit
		cancel()
	}()

	logrus.SetLevel(logrus.DebugLevel)

	if *file == "" {
		logrus.SetFormatter(&dump.Formatter{
			Formatter: &logrus.TextFormatter{},
		})
	}

	d := dump.Dumper{
		DB:            db,
		Workers:       *workers,
		Limit:         *limit,
		NoHeaders:     *noHeaders,
		NoDropTable:   *noDropTable,
		NoCreateTable: *noCreateTable,
		NoData:        *noData,
		Verbose:       *verbose,
	}

	var w io.Writer = os.Stdout

	if *file != "" {
		dst, err := os.Create(*file)
		if err != nil {
			fatal(err.Error())
		}

		defer func() {
			err := dst.Close()
			if err != nil {
				fatal(err.Error())
			}
		}()

		w = dst
	}

	err = d.Dump(ctx, w, *tables...)
	if err != nil {
		fatal(err.Error())
	}
}

func fatal(msg string) {
	fmt.Println(msg)
	os.Exit(0)
}
