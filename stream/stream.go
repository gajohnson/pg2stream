package stream

import "github.com/jackc/pgx"

type Stream interface {
	Handle(recieve chan pgx.WalMessage, ack chan uint64)
}
