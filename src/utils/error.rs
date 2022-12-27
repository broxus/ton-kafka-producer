use thiserror::Error;

pub type Result<T, E = StorageError> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Transaction tree is too deep. Depth: `{0}` ")]
    TransactionTreeTooDeep(u32),
    #[error("Depth is missing for transaction `{0}` ")]
    TransactionDepthMissing(String),
    #[error("No parent transaction found for message `{0}` ")]
    ParentTransactionMissing(String),
    #[error("Transaction does not contains ExtIn or IntIn message")]
    BadTransaction,
    #[error("Generic rocksdb error occurred")]
    RocksDb(#[from] rocksdb::Error),
    #[error("Failed to deserialize object from binary representation")]
    Bincode(#[from] bincode::Error),
    #[error("Generic error occured")]
    Anyhow(#[from] anyhow::Error),
}
