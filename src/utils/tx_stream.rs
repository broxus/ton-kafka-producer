use futures_util::Stream;
use futures_util::StreamExt;
use sqlx::MySqlPool;
use std::io::Write;
use std::path::Path;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use ton_block::Deserializable;

pub struct Db {
    pool: MySqlPool,
}

impl Db {
    pub async fn new(db_string: String) -> Self {
        let pool = MySqlPool::connect(&db_string).await.unwrap();
        Self { pool }
    }

    async fn fetch_raw_transactions(&self) -> anyhow::Result<impl Stream<Item = Vec<u8>> + '_> {
        let mut tx_stream = sqlx::query!(
            "SELECT data
         FROM raw_transactions
         ORDER BY wc, account_id, lt"
        )
        .fetch(&self.pool)
        .map(|row| row.unwrap().data);

        Ok(tx_stream)
    }

    async fn count(&self) -> anyhow::Result<u64> {
        let count = sqlx::query!("SELECT COUNT(*) as count FROM raw_transactions")
            .fetch_one(&self.pool)
            .await
            .unwrap()
            .count;

        Ok(count as u64)
    }

    pub async fn cache_transactions(&self, path: &Path) -> anyhow::Result<()> {
        let out_file = std::fs::File::create(path)?;
        let mut stream = self.fetch_raw_transactions().await?;
        let mut writer = std::io::BufWriter::new(out_file);

        let mut ctr = 0;
        let tot = self.count().await.unwrap();

        while let Some(tx) = stream.next().await {
            let len = (tx.len() as u32).to_le_bytes();
            writer.write_all(len.as_ref()).unwrap();
            writer.write_all(&tx).unwrap();
            ctr += 1;
            if ctr % 1000 == 0 {
                println!("Cached {ctr} of {tot} transactions");
            }
        }

        Ok(())
    }

    pub async fn read_cached(
        &self,
        cache_path: &Path,
    ) -> anyhow::Result<mpsc::Receiver<ton_block::Transaction>> {
        if cache_path.exists() {
            let (tx, rx) = mpsc::channel(100);
            let file = tokio::fs::File::open(cache_path).await?;
            let mut reader = tokio::io::BufReader::new(file);

            tokio::spawn(async move {
                let mut buf = Vec::with_capacity(1024 * 1024);
                loop {
                    let len = reader.read_u32_le().await.unwrap();
                    buf.resize(len as usize, 0);
                    reader.read_exact(&mut buf).await.unwrap();
                    let transaction = ton_block::Transaction::construct_from_bytes(&buf).unwrap();

                    tx.send(transaction).await.unwrap();
                }
            });
            return Ok(rx);
        }

        anyhow::bail!("Cache file not found")
    }
}

#[cfg(test)]
mod test {
    use crate::utils::tx_stream::Db;
    use std::path::Path;

    #[tokio::test]
    async fn cache_tranactions() {
        let db = get_db().await;
        db.cache_transactions(Path::new("transactions.cache"))
            .await
            .unwrap();
    }

    async fn get_db() -> Db {
        let db = super::Db::new("".to_string()).await;
        db
    }

    #[tokio::test]
    async fn read_cached() {
        let db = get_db().await;
        let mut rx = db
            .read_cached(Path::new("transactions.cache"))
            .await
            .unwrap();

        while let Some(tx) = rx.recv().await {
            println!("{:?}", tx);
        }
    }
}
