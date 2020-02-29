package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/gajohnson/pg2stream/replication"
	"github.com/gajohnson/pg2stream/stream"
	"github.com/jackc/pgx"
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

	s := stream.StdoutStream{}

	wal := make(chan pgx.WalMessage)
	ack := make(chan uint64)

	go s.Handle(wal, ack)

	r.Connect(ctx)
	r.Read(ctx, wal, ack)
}
