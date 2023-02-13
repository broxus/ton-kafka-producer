## ton-kafka-producer

The indexing infrastructure for TVM-compatible blockchains includes a node
available via jRPC (link to jrpc repo) and indexer services with some high-level
APIs for each dApp we want to index. The latter doesn’t fetch needed messages
from the former. Instead, we use Kafka to organize a stable, consistent and
ordered queue of records from the node and deliver it to arbitrary number of
indexing services.

The Kafka producer is a software component that connects to the blockchain node
and deliver data to Kafka brokers, which are responsible for storing and
replicating it across a Kafka cluster. Resulting stream includes information
about transactions, blocks, and other relevant data from the blockchain network.

By organizing data in Kafka topics, the system ensures that data is properly
ordered and available to indexer services. Thus, system can handle heavy loads,
ensuring that each indexer database is in sync with the blockchain network

### Main.rs

This is a implementing a service to stream blockchain data into Kafka. It uses
various crates such as `anyhow`, `tokio`, `pomfrit`, and others for different
purposes like error handling, asynchronous programming, metrics collection, and
formatting.

- The `main` function initializes the configuration, sets up a logging
  subscriber, and creates a future for running the actual service. It also sets
  up a signal listener for termination signals to gracefully shutdown the
  service.

- The `run` function creates the actual engine based on the configuration.
  Depending on the type of scan specified in the configuration, it creates an
  instance of either `NetworkScanner`, `ArchivesScanner`, or `S3Scanner`. It
  also sets up a metrics writer and spawns a future to collect and write metrics
  periodically. If the configuration includes RPC settings, it also sets up an
  RPC server to expose the service API.

- The `Metrics` struct implements the `std::fmt::Display` trait to generate a
  custom metrics output. It uses the `pomfrit::formatter` crate for formatting
  the metrics in the Prometheus exposition format.

- The `memory_profiler` function is a helper function to dump the heap memory
  usage statistics on a user-defined signal.

### Archives Scanner

The Archives Scanner module is responsible for scanning and processing archived
data from the blockchain node. The module is implemented in the
archives_scanner/mod.rs file, utilizing RocksDB as local storage to store
scanned data and Kafka producer to send messages containing the scanned data to
Kafka brokers.

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

The Blocks Handler module includes three producer components: Broxus, GQL, and
Kafka producers. The Broxus producer produces messages containing data about
transactions, blocks, and other relevant information from the blockchain node.
The GQL producer produces messages containing GraphQL queries that retrieve
specific data from the blockchain node. The Kafka producer is responsible for
producing messages sent to Kafka brokers.

### broxus_producer.rs

The implementation of a `BroxusProducer` struct, which is responsible for
producing raw transaction records to a Kafka instance. The struct has a
compressor field of type `ZstdWrapper` for compressing data, and a
`raw_transaction_producer` field of type `KafkaProducer` for producing records
to Kafka.

- The `handle_block` method prepares and writes raw transaction records to
  Kafka, using block ID, block data, and a boolean flag to ignore prepare
  errors. The method prepares records using `prepare_records` and writes them to
  Kafka using the `raw_transaction_producer` field.

- The `prepare_records` method prepares transaction records by iterating over
  account blocks and their transactions, compressing the raw transaction data,
  and creating a `TransactionRecord` struct for each transaction. The method
  returns a vector of transaction records.

- The `prepare_raw_transaction_record` method is used by `prepare_records` to
  prepare a single transaction record. It takes a partition, compressor, and raw
  transaction data, and returns an Option<TransactionRecord>. The method creates
  a Transaction struct from the raw transaction data, checks if it is an
  ordinary transaction, and creates a TransactionRecord struct containing the
  partition, key (the hash of the raw transaction data), and compressed value
  (the compressed binary data of the transaction). The method returns None if
  the transaction isn't ordinary.

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

### Network Scanner

Network scanner consists of two main components:

- `message_consumer.rs`: This module contains a `MessageConsumer` struct that is
  responsible for consuming messages from a Kafka topic and passing them to the
  `send_external_message` function defined in the same module. The
  send_external_message function deserializes and broadcasts the message to the
  network.

- `mod.rs`: This module defines the `NetworkScanner` struct, which is the main
  entry point for the network scanner. It also defines the `BlocksSubscriber`
  struct, which is responsible for handling blocks and updating the JRPC state.
  The NetworkScanner struct uses the ton_indexer Rust crate to index blocks,
  while the BlocksSubscriber struct uses the `blocks_handler` module to handle
  blocks.

#### message_consumer.rs

The struct has two fields: `engine`, which is an `Arc` reference to a TON
indexer engine, and `consumer`, which is an `Arc` reference to a Kafka stream
consumer.

- The `new` function takes an Arc reference to a TON indexer engine and a
  `KafkaConsumerConfig` struct, and returns a `Result` containing a
  `MessageConsumer` instance. This function sets up a Kafka client configuration
  and creates a Kafka stream consumer with the provided configuration. It
  subscribes the consumer to the specified topic and returns a new
  `MessageConsumer` instance.

- The `start` function takes no arguments and returns `Result<()>`. It starts a
  Tokio task that consumes messages from the Kafka stream, checks the message
  payload for validity, and broadcasts the message to the TON node using the
  `broadcast_external_message` function provided by the engine. If the message
  is valid and has been broadcast, the consumer commits the message.

- The `send_external_message` function takes an `Arc` reference to a TON indexer
  engine and a byte slice `data`, and returns a Result<()>. This function checks
  the length and level of the provided data, deserializes a message from the
  data, extracts the destination workchain ID from the message header, and
  broadcasts the original data to the specified workchain using the
  `broadcast_external_message` function provided by the `engine`.

- The `MessageBroadcastError` enum defines several error variants that can occur
  during the message broadcasting process, including `TooLarge`, `InvalidBoc`,
  `InvalidLevel`, `TooDeep`, `InvalidMessage`, `InvalidHeader`, and
  `OverlayBroadcastFailed`. The maximum size and depth of an external message
  are defined by the `MAX_EXTERNAL_MESSAGE_SIZE` and
  `MAX_EXTERNAL_MESSAGE_DEPTH` constants, respectively.

#### mod.rs

`NetworkScanner` struct has the following methods:

- `new`: creates a new instance of `NetworkScanner`. It takes `kafka_settings`,
  which is an optional configuration for a Kafka instance, node_settings, a
  configuration for a node, `global_config`, a configuration for the global
  indexer, and `jrpc_state`, a JSON-RPC state. This method creates an instance
  of `ton_indexer::Engine` and an instance of `MessageConsumer` if
  `kafka_settings` is not `None`, and returns a `Result` containing a new
  instance of NetworkScanner.

- `start`: starts the indexer and the message_consumer if the latter is not
  None.

`BlocksSubscriber` is a struct that implements the `ton_indexer::Subscriber`
trait and has the following methods:

- `new`: creates a new instance of `BlocksSubscriber`. It takes config, which is
  an optional configuration for a Kafka instance, and `jrpc_state`, a JSON-RPC
  state.

- `handle_block`: handles a block. It takes block_stuff, which is an instance of
  `BlockStuff`, `block_data`, which is an optional instance of `Bytes` that
  contains the block data, block_proof, which is an optional reference to
  `BlockProofStuff` that contains the block proof, and shard_state, which is an
  optional reference to `ShardStateStuff`. This method updates the JSON-RPC
  state and handles the block.

- `process_block`: processes a block asynchronously. It takes a
  `ProcessBlockContext`. This method loads the block data and block proof, and
  calls the `handle_block` method with the loaded data and proof.

- `process_full_state`: processes a full state asynchronously. It takes a
  `ShardStateStuff` instance. This method handles the state.

### S3 Scanner

This is the implementation of the `S3Scanner` struct, which is responsible for
downloading archives of TON blockchain data from an S3 bucket, parsing the
contained block data, and passing it to a BlocksHandler instance for processing
and broadcasting.

- The `S3Scanner` struct has three fields: handler, which is an `Arc` reference
  to a `BlocksHandler` instance; downloader, which is an instance of the
  `ArchiveDownloader` struct; and `retry_on_error`, which is a boolean value
  indicating whether to retry downloading and processing archives when errors
  occur.

- The `new` function takes a `KafkaConfig` struct and an `S3ScannerConfig`
  struct, and returns a Result containing a `S3Scanner` instance. This function
  creates a new `ArchiveDownloader` instance and returns a new S3Scanner
  instance.

- The `run` function takes no arguments and returns a `Result` containing a unit
  value. This function runs an infinite loop that fetches new archives from the
  `ArchiveDownloader` stream, parses the archives, and processes the contained
  blocks using the `handler` field. The progress of archive processing is
  displayed using a progress bar. If an error occurs while processing a block,
  the function prints an error message and, depending on the value of
  `retry_on_error`, either continues processing or returns an error. When all
  archives have been processed, the function prints a completion message and
  returns.

### Archive.rs

The archive module contains functions and structures for parsing and reading
blockchain Archive packages, which are collections of serialized blockchain
blocks and their accompanying proofs, used by the node Indexer.

The main function in the module is

- The `parse_archive`, which takes a vector of bytes representing the archive
  data and returns a vector of tuples containing `ton_block::BlockIdExt`
  instances as keys and `ParsedEntry` instances as values.

- `ParsedEntry` is a structure containing two fields: `block_stuff`, which is a
  tuple containing a `BlockStuff` instance and a `Bytes` instance representing
  the serialized block data, and `block_proof_stuff`, which is an optional
  `BlockProofStuff` instance containing the proof data for the block.

- The `PackageEntryId` and `PackageEntryIdError` enums are used to represent the
  types of files that can be present in an archive package, and to parse the
  file names of entries in the package. The module also contains several helper
  functions and error types.

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
