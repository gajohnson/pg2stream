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

	var s stream.Stream

	dbname := flag.String("dbname", "test", "database name")
	dbhost := flag.String("host", "localhost", "database host")
	dbport := flag.Int("dbport", 5432, "database port")
	dbuser := flag.String("user", "root", "database user")
	drop := flag.Bool("drop", false, "drop replication slot on start up")
	buffer := flag.Int("buffer", 1, "internal buffer size")
	kinesis := flag.String("kinesis", "", "kinesis stream name")
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
		Options:           []string{
			"\"include-schemas\" 'off'",
			"\"include-types\" 'off'",
			"\"include-xids\" 'on'",
			"\"format-version\" '2'",
		},
	}

	if *kinesis != "" {
		s = &stream.KinesisStream{StreamName: *kinesis, BatchSize: 500, MaxWait: 10}
	} else {
		s = &stream.StdoutStream{}
	}

	wal := make(chan pgx.WalMessage, *buffer)
	ack := make(chan uint64)

	go s.Handle(wal, ack)

	r.Connect(ctx)
	r.Read(ctx, wal, ack)
}
