# pg2stream

At-least-once delivery of changes from PostgreSQL to AWS Kinesis.

### Requirements

* go (tested on 1.14)
* postgresql 10+ (tested on 12.2)
* wal2json

### Test Usage

#### Setup (MacOS)

    brew install go postgresql wal2json
    createdb test
    echo "wal_level = logical" >> /usr/local/var/postgres/postgresql.conf
    brew services restart postgresql

#### Build

    make build

#### Run

    ./pg2stream -host localhost -user $(whoami)

#### Command line options

	-dbname=DB            database name (default: test)
	-dbhost=HOST          database host (default: localhost)
	-dbport=PORT          database port (default: 5432)
	-dbuser=NAME          database user (default: root)
	-drop:                drop replication slot on start up (default: false)
	-buffer=SIZE          internal buffer size (default: 1)
	-kinesis=STREAM_NAME  kinesis stream name (optional)

If `-kinesis` is specified, output will be sent to that stream. Otherwise, changes are logged to stdout
for testing purposes.

#### Test Local DB Changes

    make test-sql

### Todo

* Partitioning by tx id
* Batch puts to kinesis
* Config file support
* Logging
* Error handling
* Kafka streaming
