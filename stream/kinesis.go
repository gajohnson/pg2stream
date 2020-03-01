package stream

import (
	"crypto/md5"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/jackc/pgx"
	"time"
)

type KinesisStream struct {
	StreamName string
	BatchSize uint64
	MaxWait time.Duration

	batch []*kinesis.PutRecordsRequestEntry
	batchBytes int
	batchIndex uint64
	maxWal uint64
}

func (s *KinesisStream) Handle(recieve chan pgx.WalMessage, ack chan uint64) {
	ks := session.New(&aws.Config{Region: aws.String("us-west-2")})
	client := kinesis.New(ks)

	s.init()

	maxRecordSize := 1 << 20 // 1MB
	magicNumber := []byte{0xF3, 0x89, 0x9A, 0xC2}
	partitionKeyIndexSize := 8
	maxSize := maxRecordSize - len(magicNumber) - partitionKeyIndexSize

	for {
		select {
		case msg := <-recieve:
			fmt.Println(string(msg.WalData))
			currentLsn := pgx.FormatLSN(msg.WalStart)
			size := len(msg.WalData) + len([]byte(currentLsn)) + md5.Size + 1
			s.maxWal = msg.WalStart

			if s.batchBytes + size >= maxSize {
				fmt.Println("bytes (", s.batchBytes + size, ", ", s.batchIndex,  "): ", currentLsn)
				s.drain(*client, ack)
			} else if s.batchIndex >= s.BatchSize {
				fmt.Println("records (", s.batchBytes + size, ", ", s.batchIndex,  "): ", currentLsn)
				s.drain(*client, ack)
			}

			record := &kinesis.PutRecordsRequestEntry{
				Data:         msg.WalData,
				PartitionKey: aws.String(currentLsn),
			}
			s.batch = append(s.batch, record)
			s.batchIndex++
			s.batchBytes += size
		case <-time.Tick(time.Duration(s.MaxWait * time.Second)):
			if s.batchIndex > 0 {
				s.drain(*client, ack)
				fmt.Println("timeout: ", pgx.FormatLSN(s.maxWal))
			}
		}
	}
}

func (s *KinesisStream) init() {
	s.batch = make([]*kinesis.PutRecordsRequestEntry, 0)
	s.batchIndex = 0
	s.batchBytes = 0
}

func (s *KinesisStream) drain(client kinesis.Kinesis, ack chan uint64) {
	putsOutput, err := client.PutRecords(&kinesis.PutRecordsInput{
		Records:    s.batch,
		StreamName: aws.String(s.StreamName),
	})

	if err != nil {
		// TODO
		panic(err)
	}

	fmt.Println(putsOutput)

	ack <- s.maxWal
	s.init()
}
