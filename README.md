# pg2stream

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

	-dbname    database name (default: test)
	-dbhost:   database host (default: localhost)
	-dbport:   database port (default: 5432)
	-dbuser:   database user (default: root)
	-drop:     drop replication slot on start up (default false)
	-buffer:   internal buffer size (default: 1)
	-kinesis:  kinesis stream name (optional)

If -kinesis is specified, output will be sent to that stream. Otherwise, changes are logged to stdout
for testing purposes.

#### Test Local DB Changes

    make test-sql

### Todo

* Batch puts to kinesis
* Config file support
* Kafka streaming
