## ton-kafka-producer

This project is designed for TVM-compatible blockchains, including Everscale,
Venom, Gosh, and TON. It consists of three main components: a blockchain node, a
traditional database, and a Kafka instance. The blockchain node is responsible
for interacting with the blockchain network and providing the necessary data.
The traditional database stores the data that has been received from the
blockchain network and organized by the Kafka instance. The Kafka instance, in
turn, is responsible for receiving data from the blockchain node via the Kafka
producer.

The Kafka producer is a software component that connects to the blockchain node
and produces messages that are then sent to Kafka brokers, which are responsible
for storing and replicating data across a Kafka cluster. The messages produced
by the Kafka producer can include information about transactions, blocks, and
other relevant data from the blockchain network.

By organizing data in Kafka topics, the system ensures that data is properly
ordered and available to indexers that optimize queries to the traditional
database. This means that the system can handle heavy loads, ensuring that data
from the blockchain network is properly synchronized with the database, and can
be easily accessed by dapps and frontend applications.

### Archives Scanner

The Archives Scanner module is responsible for scanning and processing archived
data from the blockchain node. The module is implemented in the
archives_scanner/mod.rs file. This module uses RocksDB as a local storage to
store the scanned data, and it also uses the Kafka producer to produce messages
containing the scanned data, which are sent to Kafka brokers.

Here's a brief breakdown of the code:

- `ArchivesScanner`: Defines a struct that represents the ArchivesScanner
  component. It contains a BlocksHandler instance and a string representing the
  path to a list of archive files.

- `new`: A function that creates a new `ArchivesScanner` instance. It takes a
  `KafkaConfig` instance and a `PathBuf` that specifies the path to a list of
  archive files as arguments.

- `run`: A function that runs the `ArchivesScanner`. It reads the list of
  archive files, parses them to extract data from the blockchain network, and
  sends the data to the `BlocksHandler` instance. It uses a progress bar to
  display the progress of the scanning operation.

- `start_writing_blocks`: A helper function that writes blocks to the
  `BlocksHandler` instance. It takes a progress bar, an atomic counter, a
  BlocksHandler instance, and a `BlockTaskRx` as arguments.

### Block Handler

The Blocks Handler module includes three producer components: Broxus producer,
GQL producer, and Kafka producer. The Broxus producer is responsible for
producing messages containing data about transactions, blocks, and other
relevant information from the blockchain node. The GQL producer is responsible
for producing messages containing GraphQL queries that can be used to retrieve
specific data from the blockchain node. The Kafka producer, as mentioned
earlier, is responsible for producing messages that are sent to Kafka brokers.

### broxus_producer.rs

The implementation of a `BroxusProducer` struct, which is responsible for
producing raw transaction records to a Kafka instance. The struct has a
compressor field of type `ZstdWrapper` for compressing data, and a
`raw_transaction_producer` field of type `KafkaProducer` for producing records
to Kafka.

- The `handle_block` method of the struct is used to prepare and write raw
  transaction records to Kafka. It takes a block ID, block data, and a boolean
  flag to ignore prepare errors. The method prepares records using the
  prepare_records method, and writes them to Kafka using the
  raw_transaction_producer field.

- The `prepare_records` method prepares transaction records by iterating over
  account blocks and their transactions, compressing the raw transaction data,
  and creating a TransactionRecord struct for each transaction. The method
  returns a vector of transaction records.

- The `prepare_raw_transaction_record` method is used by prepare_records to
  prepare a single transaction record. It takes a partition, compressor, and raw
  transaction data, and returns an Option<TransactionRecord>. The method creates
  a Transaction struct from the raw transaction data, checks if it is an
  ordinary transaction, and creates a TransactionRecord struct containing the
  partition, key (the hash of the raw transaction data), and compressed value
  (the compressed binary data of the transaction). The method returns None if
  the transaction is not ordinary.

#### gql_producer.rs

The code defines a `GqlProducer` struct that implements methods for handling and
producing data related to a blockchain. The struct contains fields for
KafkaProducer instances that are responsible for sending different types of data
such as blocks, block proofs, messages, transactions, accounts, and raw blocks
to Kafka.

- The `new` method initializes a `GqlProducer` instance by creating
  `KafkaProducer` instances using the configuration provided as an argument.

- The `handle_state` method sends account data from a ShardStateStuff object to
  Kafka using the account_producer field.

- The `handle_block` method prepares and sends block-related data to Kafka based
  on the input parameters.

- The `prepare_records` method generates a vector of DbRecord instances for a
  given block. DbRecord is an enum that represents different types of data that
  can be produced and sent to Kafka.

- The `in_msg`, `out_msg`, `transaction`, `account`, `deleted_account`,
  `block_proof`, and `block` functions generate `DbRecord` instances for their
  respective types of data.

- The code also defines some helper functions for creating and processing
  blockchain addresses and hashes, and includes an error enum
  `TonSubscriberError`that is used in the code.

#### kafka_producer.rs

The `KafkaProducer` module contains a `KafkaProducer` struct, which is used to
produce messages to Kafka. It takes in a configuration and a partition, and
provides functions for writing messages to the specified partition. The module
uses a `FutureProducer` from the `rdkafka` crate to send messages asynchronously
to Kafka. It also uses a `Batch` struct to batch messages before sending them to
Kafka, which can improve performance. The `PendingRecord` struct is used to
represent messages that have not yet been sent to Kafka.

- `use` statements to import external dependencies, such as `std` and `tokio`. A
  `KafkaProducer` struct that contains configuration information for the
  producer, as well as a `FutureProducer` from the `rdkafka` library, a map of
  `Batch` objects, and a boolean flag indicating whether the producer is fixed
  to specific partitions.

- An `enum` for specifying partition behavior, with options for fixed partitions
  or any partition.

- Implementation of the `KafkaProducer` struct with methods for initializing a
  new producer, sending a record to a specified partition, and writing a batch
  of records.

- An implementation of the `Drop` trait to handle cleanup when the producer is
  dropped.

- A `Batch` struct that contains a `Mutex`-protected `VecDeque` of
  `PendingRecord` objects.

- A `PendingRecord` struct that represents a single record to be sent to the
  Kafka cluster, containing the record key, value, creation time, and a
  `DeliveryFuture` that resolves when the record is successfully delivered.

### Runtime requirements

- CPU: 4 cores, 2 GHz
- RAM: 8 GB
- Storage: 100 GB fast SSD
- Network: 100 MBit/s

### How to run

1. Build all binaries and prepare services
   ```bash
   ./scripts/setup.sh
   ```
2. Edit `/etc/ton-kafka-producer/config.yaml`
3. Enable and start the service:
   ```bash
   systemctl enable ton-kafka-producer
   systemctl start ton-kafka-producer
   ```

### Config example

The example configuration includes settings that specify how the Kafka producer
should connect to Kafka brokers, as well as options for securing the connection
using SASL/SSL protocols. It also includes settings for the scan type, which
determines how the producer retrieves data from the TON node.

```yaml
---
# Optional states endpoint (see docs below)
rpc_config:
  # States RPC endpoint
  address: "0.0.0.0:8081"

metrics_settings:
  # Listen address of metrics. Used by the client to gather prometheus metrics.
  # Default: "127.0.0.1:10000"
  listen_address: "0.0.0.0:10000"
  # Metrics update interval in seconds. Default: 10
  collection_interval_sec: 10

# # Scan from local archives
# scan_type:
#   kind: FromArchives
#   # Example how to prepare: `find path/to/archives > path/to/archives_list`
#   list_path: path/to/archives_list

scan_type:
  kind: FromNetwork
  node_config:
    # Root directory for node DB. Default: "./db"
    db_path: "/var/db/ton-kafka-producer"

    # UDP port, used for ADNL node. Default: 30303
    adnl_port: 30000

    # Path to temporary ADNL keys.
    # NOTE: Will be generated if it was not there.
    # Default: "./adnl-keys.json"
    temp_keys_path: "/etc/ton-kafka-producer/adnl-keys.json"

    # Archives map queue. Default: 16
    parallel_archive_downloads: 32

    # archive_options:
    #   # Archives S3 uploader options
    #   uploader_options:
    #     name: ""
    #     endpoint: "http://127.0.0.1:9000"
    #     bucket: "archives"
    #     credentials:
    #       access_key: "example_key"
    #       secret_key: "example_password"

    # # Specific block from which to run the indexer
    # start_from: 12365000

    # # Allowed DB size in bytes. Default: one third of all machine RAM
    # max_db_memory_usage: 3000000000

kafka_settings:
  mode: broxus
  raw_transaction_producer:
    topic: everscale-transactions
    brokers: "kafka1.my.website:20001, kafka1.my.website:20002, kafka1.my.website:20003"
    attempt_interval_ms: 100
    security_config:
      Sasl:
        security_protocol: "SASL_SSL"
        ssl_ca_location: "client.pem"
        sasl_mechanism: "sasl mechanism"
        sasl_username: "your sasl username"
        sasl_password: "your sasl password"
# OR gql kafka producer
#
#kafka_settings:
#  mode: gql
#  requests_consumer:
#    topic: gql.requests
#    brokers: "1.2.3.4:20001, 1.2.3.4:20002, 1.2.3.4:20003"
#    group_id: gql-mainnet
#    session_timeout_ms: 6000
#  block_producer:
#    topic: gql.blocks
#    brokers: "1.2.3.4:20001, 1.2.3.4:20002, 1.2.3.4:20003"
#    attempt_interval_ms: 100
#  message_producer:
#    topic: gql.messages
#    brokers: "1.2.3.4:20001, 1.2.3.4:20002, 1.2.3.4:20003"
#    attempt_interval_ms: 100
#  transaction_producer:
#    topic: gql.transactions
#    brokers: "1.2.3.4:20001, 1.2.3.4:20002, 1.2.3.4:20003"
#    attempt_interval_ms: 100
#  account_producer:
#    topic: gql.accounts
#    brokers: "1.2.3.4:20001, 1.2.3.4:20002, 1.2.3.4:20003"
#    attempt_interval_ms: 100
#  block_proof_producer:
#    topic: gql.blocks_signatures
#    brokers: "1.2.3.4:20001, 1.2.3.4:20002, 1.2.3.4:20003"
#    attempt_interval_ms: 100
```
