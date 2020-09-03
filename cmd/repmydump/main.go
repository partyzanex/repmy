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
	user    = pflag.StringP("user", "u", "", "user")
	pass    = pflag.StringP("password", "p", "", "password")
	host    = pflag.StringP("host", "h", "localhost", "hostname")
	port    = pflag.Uint16P("port", "P", 3306, "port")
	dbname  = pflag.StringP("database", "d", "", "database")
	workers = pflag.IntP("treads", "t", 1, "number of treads")
	limit   = pflag.IntP("limit", "l", 10, "limit of rows")
	verbose = pflag.BoolP("verbose", "v", false, "verbose progress")
	output  = pflag.StringP("output", "o", "dump", "output dir")

	tables = pflag.StringSlice("tables", []string{}, "tables list")

	noHeaders     = pflag.Bool("no-headers", false, "dump tables without headers")
	noDropTable   = pflag.Bool("no-drop-table", false, "dump tables without DROP TABLE IF EXISTS ...")
	noCreateTable = pflag.Bool("no-create-table", false, "dump tables without DLL (data only)")
	noData        = pflag.Bool("no-data", false, "dump only DLL (without data)")

	debug = pflag.Bool("debug", false, "debug mode")
)

func main() {
	pflag.Parse()

	if *debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	if *user == "" {
		exit("flag --user is required")
	}

	if *dbname == "" {
		exit("flag --db is required")
	}

	if *output == "" {
		exit("flag --output is required")
	}

	if err := os.MkdirAll(*output, 0755); err != nil {
		if !os.IsExist(err) {
			exit(err.Error())
		}
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", *user, *pass, *host, *port, *dbname)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		exit(err.Error())
	}

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, os.Kill)

	go func() {
		<-quit
		exit("exited")
	}()

	d := dump.Dumper{
		DB:            db,
		Output:        *output,
		Workers:       *workers,
		Limit:         *limit,
		NoHeaders:     *noHeaders,
		NoDropTable:   *noDropTable,
		NoCreateTable: *noCreateTable,
		NoData:        *noData,
		Verbose:       *verbose,
	}

	err = d.Dump(context.Background(), *tables...)
	if err != nil {
		exit(err.Error())
	}
}

func exit(msg string) {
	logrus.Info(msg)
	os.Exit(0)
}
