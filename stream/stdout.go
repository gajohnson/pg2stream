package stream

import (
	"fmt"
	"github.com/jackc/pgx"
)

type StdoutStream struct{}

func (s *StdoutStream) Handle(recieve chan pgx.WalMessage, ack chan uint64) {
	for {
		select {
		case msg := <-recieve:
			fmt.Println(string(msg.WalData))
			ack <- msg.WalStart
		}
	}
}
