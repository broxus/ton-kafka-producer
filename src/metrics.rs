use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct RpcMetrics {
    pub requests_processed: AtomicU64,
    pub first_time: AtomicU64,
}

pub struct RpcPublicMetrics {
    pub requests_processed: u64,
    pub rps: u64,
}

impl RpcMetrics {
    pub fn new() -> Arc<RpcMetrics> {
        Arc::new(Self {
            requests_processed: AtomicU64::new(0),
            first_time: AtomicU64::new(chrono::Utc::now().timestamp_millis() as u64),
        })
    }
    pub fn processed(&self) {
        self.requests_processed.fetch_add(1, Ordering::SeqCst);
    }

    pub fn take_metrics(&self) -> RpcPublicMetrics {
        let requests_processed = self.requests_processed.load(Ordering::Acquire);
        self.requests_processed.store(0, Ordering::Release);
        let start_time = self.first_time.load(Ordering::Acquire);
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let rps = requests_processed / (now - start_time).max(1);
        self.first_time.store(now, Ordering::Release);

        RpcPublicMetrics {
            requests_processed,
            rps,
        }
    }
}
