package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/gajohnson/pg2stream/replication"
)

func main() {
	fmt.Println("starting pg2stream ...")
	dbname := flag.String("dbname", "test", "database name")
	dbhost := flag.String("host", "localhost", "database host")
	dbport := flag.Int("dbport", 5432, "database port")
	dbuser := flag.String("user", "root", "database user")
	drop := flag.Bool("drop", false, "drop replication slot on start up")
	flag.Parse()

	ctx := context.Context(context.Background())

	r := replication.Replication{
		Database:          *dbname,
		Host:              *dbhost,
		Port:              uint16(*dbport),
		User:              *dbuser,
		KeepaliveInterval: 10,
		SlotName:          "pg2stream",
		CreateSlot:        true,
		DropSlot:          *drop,
		Options:           []string{"\"include-schemas\" 'off'", "\"include-types\" 'off'"},
	}

	r.Connect(ctx)
	r.Read(ctx)
}
