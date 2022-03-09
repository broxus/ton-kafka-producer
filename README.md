## ton-kafka-producer

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
```yaml
---
# Optional states endpoint (see docs below)
rpc_config:
  # States RPC endpoint
  address: "0.0.0.0:8081"

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

# log4rs settings.
# See https://docs.rs/log4rs/1.0.0/log4rs/ for more details
logger_settings:
  appenders:
    stdout:
      kind: console
      encoder:
        pattern: "{h({l})} {M} = {m} {n}"
  root:
    level: warn
    appenders:
      - stdout
  loggers:
    ton_indexer:
      level: info
      appenders:
        - stdout
      additive: false
    ton_kafka_producer:
      level: info
      appenders:
        - stdout
      additive: false
```

### States RPC

Endpoint: `http://0.0.0.0:8081` (can be configured by `rpc_config.address`)

- POST `/account`
  ```typescript
  type Request = {
    // Address of the contract in format `(?:-1|0):[0-9a-fA-F]{64}`
    address: string,
  };
  
  type Response = {
    // BOC encoded `ton_block::AccountStuff`
    account: string,
    timings: {
      // Shard state lt
      genLt: string,
      // Shard state utime
      genUtime: number,
    },
    lastTransactionId: {
      // Account last transaction lt
      lt: string,
      // Account last transaction hash in format `[0-9a-f]{64}`
      hash: string,
    }
  } | null;
  ```
