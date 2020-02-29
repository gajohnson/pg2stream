package main

import "context"
import "flag"
import "fmt"
import "github.com/jackc/pgx"

func main() {
	fmt.Println("starting pg2stream ...")
	dbname := flag.String("dbname", "test", "database name")
	dbhost := flag.String("host", "localhost", "database host")
	dbport := flag.Int("dbport", 5432, "database port")
	dbuser := flag.String("user", "root", "database user")
	flag.Parse()

	ctx := context.Context(context.Background())

	config := pgx.ConnConfig{}
	config.Database = *dbname
	config.Host = *dbhost
	config.Port = uint16(*dbport)
	config.User = *dbuser

	if conn, err := pgx.ReplicationConnect(config); err != nil {
		fmt.Println("ReplicationConnect error: ", err)
	} else {
		defer conn.Close()

		if err := conn.DropReplicationSlot("test_slot"); err != nil {
			fmt.Println("DropReplicationSlot error: ", err)
		}

		if consistentPoint, snapshotName, err := conn.CreateReplicationSlotEx("test_slot", "wal2json"); err != nil {
			fmt.Println("CreateReplicationSlotEx error: ", err)
		} else {
			fmt.Println(consistentPoint, snapshotName)

			lsn, _ := pgx.ParseLSN(consistentPoint)
			if err := conn.StartReplication("test_slot", lsn, -1); err != nil {
				fmt.Println("StartReplication error: ", err)
			}

			for {
				if msg, err := conn.WaitForReplicationMessage(ctx); err != nil {
					fmt.Println("WaitForReplicationMessage error: ", err)
				} else {
					if msg.WalMessage != nil {
						fmt.Println(string(msg.WalMessage.WalData))
					}
				}
			}
		}
	}
}
