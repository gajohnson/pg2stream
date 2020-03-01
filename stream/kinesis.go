package stream

import (
	"fmt"
	"github.com/a8m/kinesis-producer"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/jackc/pgx"
	"time"
)

type KinesisStream struct {
	StreamName string
	BatchCount int
	BatchSize  int
	RecordSize int
	MaxWait    time.Duration

	client      *kinesis.Kinesis
	ack         chan uint64
	records     []*kinesis.PutRecordsRequestEntry
	currentSize int
	currentLSN  uint64
}

func (s *KinesisStream) Handle(recieve chan pgx.WalMessage, ack chan uint64) {
	s.init()
	s.client = kinesis.New(session.New(&aws.Config{Region: aws.String("us-west-2")}))
	s.ack = ack
	aggregator := new(producer.Aggregator)

	for {
		select {
		case msg := <-recieve:
			data := []byte(string(msg.WalData) + "\n")
			pkey := pgx.FormatLSN(msg.WalStart)
			s.currentLSN = msg.WalStart

			size := len(data) + len([]byte(pkey))
			if size+aggregator.Size() >= s.RecordSize {
				if drainedRecord, err := aggregator.Drain(); err != nil {
					panic(err)
				} else if drainedRecord != nil {
					s.addRecord(drainedRecord)
				}
			}
			aggregator.Put(data, pkey)

		case <-time.Tick(time.Duration(s.MaxWait * time.Second)):
			if drainedRecord, err := aggregator.Drain(); err != nil {
				panic(err)
			} else if drainedRecord != nil {
				s.addRecord(drainedRecord)
			}
			s.flush()
		}
	}
}

func (s *KinesisStream) init() {
	s.currentSize = 0
	s.records = make([]*kinesis.PutRecordsRequestEntry, 0, s.BatchCount)
}

func (s *KinesisStream) addRecord(record *kinesis.PutRecordsRequestEntry) {
	recordSize := len(record.Data) + len(*record.PartitionKey)
	if s.currentSize+recordSize >= s.BatchSize {
		s.flush()
	}
	s.currentSize += recordSize
	s.records = append(s.records, record)
	if len(s.records) >= s.BatchCount {
		s.flush()
	}
}

func (s *KinesisStream) flush() {
	if len(s.records) == 0 {
		return
	}
	size := 0
	for _, r := range s.records {
		size += len(r.Data) + len(*r.PartitionKey)
	}
	fmt.Println("flush: ", len(s.records), size)
	out, err := s.client.PutRecords(&kinesis.PutRecordsInput{
		Records:    s.records,
		StreamName: &s.StreamName,
	})
	fmt.Println("failed: ", *out.FailedRecordCount)
	if err != nil {
		panic(err)
	}
	s.ack <- s.currentLSN
	s.init()
}
