use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct RpcMetrics {
    pub requests_processed: AtomicU64,
    pub since: AtomicU64,
}

pub struct RpcPublicMetrics {
    pub requests_processed: u64,
    pub rps: u64,
}

impl RpcMetrics {
    pub fn new() -> Arc<RpcMetrics> {
        Arc::new(Self {
            requests_processed: AtomicU64::new(0),
            since: AtomicU64::new(chrono::Utc::now().timestamp_millis() as u64),
        })
    }

    pub fn processed(&self) {
        self.requests_processed.fetch_add(1, Ordering::SeqCst);
    }

    pub fn take_metrics(&self) -> RpcPublicMetrics {
        let now = chrono::Utc::now().timestamp_millis() as u64;

        let requests_processed = self.requests_processed.swap(0, Ordering::AcqRel);
        let since = self.since.swap(now, Ordering::AcqRel);

        let rps = requests_processed / now.saturating_sub(since).max(1);

        RpcPublicMetrics {
            requests_processed,
            rps,
        }
    }
}
