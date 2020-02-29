package main

import "context"
import "flag"
import "fmt"
import "time"
import "github.com/jackc/pgx"

func keepalive(ctx context.Context, conn *pgx.ReplicationConn) {
	for {
		select {
		case <- ctx.Done():
			return
		case <- time.Tick(time.Duration(10 * time.Second)):
			status(conn, 0)
		}
	}
}

func status(conn *pgx.ReplicationConn, lsn uint64) {
	if status, err := pgx.NewStandbyStatus(lsn); err != nil {
		fmt.Println("NewStandbyStatus error: ", err)
	} else {
		status.ReplyRequested = 0
		conn.SendStandbyStatus(status)
	}
}


func main() {
	fmt.Println("starting pg2stream ...")
	dbname := flag.String("dbname", "test", "database name")
	dbhost := flag.String("host", "localhost", "database host")
	dbport := flag.Int("dbport", 5432, "database port")
	dbuser := flag.String("user", "root", "database user")
	drop := flag.Bool("drop", false, "drop replication slot on start up")
	flag.Parse()

	ctx := context.Context(context.Background())

	config := pgx.ConnConfig{}
	config.Database = *dbname
	config.Host = *dbhost
	config.Port = uint16(*dbport)
	config.User = *dbuser

	wal2jsonOpts := []string{"\"include-lsn\" 'on'", "\"include-schemas\" 'off'", "\"include-types\" 'off'"}

	if conn, err := pgx.ReplicationConnect(config); err != nil {
		fmt.Println("ReplicationConnect error: ", err)
	} else {
		defer conn.Close()

		go keepalive(ctx, conn)

		if *drop {
			if err := conn.DropReplicationSlot("test_slot"); err != nil {
				fmt.Println("DropReplicationSlot error: ", err)
			}
		}

		consistentPoint, snapshotName, err := conn.CreateReplicationSlotEx("test_slot", "wal2json")
		if err != nil {
			fmt.Println("CreateReplicationSlotEx error: ", err)
		}
		fmt.Println(consistentPoint, snapshotName)

		lsn, _ := pgx.ParseLSN(consistentPoint)
		if err := conn.StartReplication("test_slot", lsn, -1, wal2jsonOpts...); err != nil {
			fmt.Println("StartReplication error: ", err)
		}

		for {
			if msg, err := conn.WaitForReplicationMessage(ctx); err != nil {
				fmt.Println("WaitForReplicationMessage error: ", err)
			} else {
				if msg.WalMessage != nil {
					fmt.Println(string(msg.WalMessage.WalData))
					status(conn, msg.WalMessage.WalStart)
				}
				if msg.ServerHeartbeat != nil {
					fmt.Println("heartbeat: ", msg.ServerHeartbeat)
					if msg.ServerHeartbeat.ReplyRequested == 1 {
						fmt.Println("heartbeat reply: ", lsn)
						status(conn, lsn)
					}
				}
			}
		}
	}
}
