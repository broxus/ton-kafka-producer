use thiserror::Error;

pub type Result<T, E = StorageError> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("No parent transaction found for message `{0}` ")]
    ParentTransactionMissing(String),
    #[error("No child transaction found for message `{0}` ")]
    ChildTransactionMissing(String),
    #[error("Transaction `{0}` does not contain ExtIn or IntIn message")]
    BadTransaction(String),
    #[error("Data inconsistency")]
    DataInconsistency,
    #[error("Generic rocksdb error occurred")]
    RocksDb(#[from] rocksdb::Error),
    #[error("Failed to deserialize object from binary representation")]
    Bincode(#[from] bincode::Error),
    #[error("Generic error occured")]
    Anyhow(#[from] anyhow::Error),
    #[error("Hex error occured")]
    FromHexError(#[from] hex::FromHexError),
}
