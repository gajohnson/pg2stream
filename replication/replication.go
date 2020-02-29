package replication

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	"time"
)

type Replication struct {
	Database          string
	Host              string
	Port              uint16
	User              string
	Connection        *pgx.ReplicationConn
	KeepaliveInterval time.Duration
	SlotName          string
	CreateSlot        bool
	DropSlot          bool
	Options           []string
}

func (r *Replication) Connect(ctx context.Context) {
	var lsn uint64

	config := pgx.ConnConfig{}
	config.Database = r.Database
	config.Host = r.Host
	config.Port = r.Port
	config.User = r.User

	if conn, err := pgx.ReplicationConnect(config); err != nil {
		panic(err)
	} else {
		r.Connection = conn
	}

	if r.DropSlot {
		if err := r.Connection.DropReplicationSlot(r.SlotName); err != nil {
			panic(err)
		}
	}

	if r.CreateSlot {
		consistentPoint, _, _ := r.Connection.CreateReplicationSlotEx(r.SlotName, "wal2json")
		lsn, _ = pgx.ParseLSN(consistentPoint)
	} else {
		lsn = 0
	}

	if err := r.Connection.StartReplication(r.SlotName, lsn, -1, r.Options...); err != nil {
		panic(err)
	}

	go r.keepalive(ctx)
}

func (r *Replication) Read(ctx context.Context) {
	for {
		if msg, err := r.Connection.WaitForReplicationMessage(ctx); err != nil {
			panic(err)
		} else {
			if msg.WalMessage != nil {
				fmt.Println(string(msg.WalMessage.WalData))
				r.status(msg.WalMessage.WalStart)
			}
			if msg.ServerHeartbeat != nil {
				if msg.ServerHeartbeat.ReplyRequested == 1 {
					r.status(0)
				}
			}
		}
	}
}

func (r *Replication) keepalive(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.Tick(time.Duration(r.KeepaliveInterval * time.Second)):
			r.status(0)
		}
	}
}

func (r *Replication) status(lsn uint64) {
	if status, err := pgx.NewStandbyStatus(lsn); err != nil {
		panic(err)
	} else {
		status.ReplyRequested = 0
		r.Connection.SendStandbyStatus(status)
	}
}
