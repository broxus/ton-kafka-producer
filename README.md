<p align="center">
  <a href="https://github.com/venom-blockchain/developer-program">
    <img src="https://raw.githubusercontent.com/venom-blockchain/developer-program/main/vf-dev-program.png" alt="Logo" width="366.8" height="146.4">
  </a>
</p>

# ton-kafka-producer

The indexing infrastructure for TVM-compatible blockchains includes a node
available via [jRPC](https://github.com/broxus/everscale-jrpc) and indexer
services with some high-level APIs for each dApp we want to index. The latter
doesn’t fetch needed messages from the former. Instead, we use Kafka to organize
a stable, consistent and ordered queue of records from the node and deliver it
to arbitrary number of indexing services.

The Kafka producer is a software component that connects to the blockchain node
and deliver data to Kafka brokers, which are responsible for storing and
replicating it across a Kafka cluster. Resulting stream includes information
about transactions, blocks, and other relevant data from the blockchain network.

By organizing data in Kafka topics, the system ensures that data is properly
ordered and available to indexer services. Thus, system can handle heavy loads,
ensuring that each indexer database is in sync with the blockchain network

It provides three different methods of scanning blockchain data:

- `NetworkScanner` scans data from a running node. It uses Indexer to retrieve
  the blockchain data and scans the data using various network protocols, such
  as ADNL, RLD, and DHT. It then sends the scanned data to a Kafka broker. This
  method requires a running TON node and access to its data.

- `ArchivesScanner` scans data from local disk archives. It reads the blockchain
  data from the archive files and sends the data to a Kafka broker. This method
  requires a local copy of the blockchain archives.

- `S3Scanner` scans data from S3 storage. It reads the blockchain data from the
  specified S3 bucket and sends the data to a Kafka broker. This method requires
  access to an S3 bucket containing blockchain data.

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
    brokers:
      "kafka1.my.website:20001, kafka1.my.website:20002, kafka1.my.website:20003"
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
#    message_max_size: 4000000
#  message_producer:
#    topic: gql.messages
#    brokers: "1.2.3.4:20001, 1.2.3.4:20002, 1.2.3.4:20003"
#    attempt_interval_ms: 100
#    message_max_size: 4000000
#  transaction_producer:
#    topic: gql.transactions
#    brokers: "1.2.3.4:20001, 1.2.3.4:20002, 1.2.3.4:20003"
#    attempt_interval_ms: 100
#    message_max_size: 4000000
#  account_producer:
#    topic: gql.accounts
#    brokers: "1.2.3.4:20001, 1.2.3.4:20002, 1.2.3.4:20003"
#    attempt_interval_ms: 100
#    message_max_size: 4000000
#  block_proof_producer:
#    topic: gql.blocks_signatures
#    brokers: "1.2.3.4:20001, 1.2.3.4:20002, 1.2.3.4:20003"
#    attempt_interval_ms: 100
#    message_max_size: 4000000
```

## Contributing

We welcome contributions to the project! If you notice any issues or errors, feel free to open an issue or submit a pull request.

## License

Licensed under GPL-3.0 license ([LICENSE](/LICENSE) or https://opensource.org/license/gpl-3-0/).
