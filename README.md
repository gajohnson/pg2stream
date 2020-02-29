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

### Todo

* Kinesis
* Kafka
