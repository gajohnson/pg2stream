package stream

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/jackc/pgx"
)

type KinesisStream struct {
	StreamName string
}

func (s *KinesisStream) Handle(recieve chan pgx.WalMessage, ack chan uint64) {
	ks := session.New(&aws.Config{Region: aws.String("us-west-2")})
	kc := kinesis.New(ks)

	for {
		select {
		case msg := <-recieve:
			putOutput, err := kc.PutRecord(&kinesis.PutRecordInput{
				Data:         msg.WalData,
				StreamName:   &s.StreamName,
				PartitionKey: aws.String("key1"),
			})
			if err != nil {
				panic(err)
			} else {
				fmt.Println(putOutput)
				ack <- msg.WalStart
			}
		}
	}
}
