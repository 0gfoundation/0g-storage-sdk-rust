use anyhow::Result;
use log::{error, warn};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tokio::time;

type AsyncFunc = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;
type SyncFunc = Arc<dyn Fn() -> Result<()> + Send + Sync>;

pub struct Scheduler;

impl Scheduler {
    pub fn new() -> Self {
        Self
    }

    pub fn schedule<F, Fut>(&self, action: F, interval: Duration, error_message: &'static str)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let action = Arc::new(move || {
            Box::pin(action()) as Pin<Box<dyn Future<Output = Result<()>> + Send>>
        });
        tokio::spawn(run_scheduled_task(action, interval, error_message));
    }

    pub fn schedule_sync<F>(&self, action: F, interval: Duration, error_message: &'static str)
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        let action = Arc::new(action);
        tokio::spawn(run_scheduled_sync_task(action, interval, error_message));
    }

    pub fn schedule_now<F, Fut>(&self, action: F, interval: Duration, error_message: &'static str)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let action = Arc::new(move || {
            Box::pin(action()) as Pin<Box<dyn Future<Output = Result<()>> + Send>>
        });
    
        tokio::spawn(async move {
            if let Err(err) = action().await {
                error!("{}: {}", error_message, err);
            }
            run_scheduled_task(action, interval, error_message).await;
        });
    }

    pub fn schedule_sync_now<F>(&self, action: F, interval: Duration, error_message: &'static str)
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        let action = Arc::new(action);
        tokio::spawn(async move {
            if let Err(err) = action() {
                error!("{}: {}", error_message, err);
            }
            run_scheduled_sync_task(action, interval, error_message).await;
        });
    }

    pub async fn wait_until<F, Fut>(&self, action: F, timeout: Duration) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        tokio::time::timeout(timeout, action()).await?
    }
}

async fn run_scheduled_task(action: AsyncFunc, interval: Duration, error_message: &'static str) {
    let mut interval = time::interval(interval);
    loop {
        interval.tick().await;
        if let Err(err) = action().await {
            warn!("{}: {}", error_message, err);
        }
    }
}

async fn run_scheduled_sync_task(
    action: SyncFunc,
    interval: Duration,
    error_message: &'static str,
) {
    let mut interval = time::interval(interval);
    loop {
        interval.tick().await;
        let action_clone = action.clone();
        if let Err(e) = task::spawn_blocking(move || action_clone())
            .await
            .expect("Task panicked")
        {
            warn!("{}: {}", error_message, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_schedule() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        scheduler.schedule(
            move || {
                let counter = Arc::clone(&counter_clone);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            Duration::from_millis(100),
            "Test error",
        );

        sleep(Duration::from_millis(550)).await;
        assert!(counter.load(Ordering::SeqCst) >= 5);
    }

    #[tokio::test]
    async fn test_schedule_sync() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        scheduler.schedule_sync(
            move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            Duration::from_millis(100),
            "Test error",
        );

        sleep(Duration::from_millis(550)).await;
        assert!(counter.load(Ordering::SeqCst) >= 5);
    }

    #[tokio::test]
    async fn test_compare_sync_and_async() {
        let scheduler = Scheduler::new();
        let sync_counter = Arc::new(AtomicUsize::new(0));
        let async_counter = Arc::new(AtomicUsize::new(0));

        scheduler.schedule_sync(
            {
                let counter = sync_counter.clone();
                move || {
                    std::thread::sleep(Duration::from_millis(100));
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            Duration::from_millis(10),
            "Sync error",
        );

        scheduler.schedule(
            {
                let counter = async_counter.clone();
                move || {
                    let counter_clone = counter.clone();
                    async move {
                        sleep(Duration::from_millis(100)).await;
                        counter_clone.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }
            },
            Duration::from_millis(10),
            "Async error",
        );

        for i in 0..12 {
            sleep(Duration::from_secs(1)).await;
            log::info!(
                "Progress after {}s - Sync: {}, Async: {}",
                i + 1,
                sync_counter.load(Ordering::SeqCst),
                async_counter.load(Ordering::SeqCst)
            );
        }

        let sync_count = sync_counter.load(Ordering::SeqCst);
        let async_count = async_counter.load(Ordering::SeqCst);

        log::info!("Final - Sync task executed {} times", sync_count);
        log::info!("Final - Async task executed {} times", async_count);

        assert!(
            (sync_count as i32 - async_count as i32).abs() <= 2,
            "Sync and async execution counts should be similar. Sync: {}, Async: {}",
            sync_count,
            async_count
        );
    }

    #[tokio::test]
    async fn test_schedule_now() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        scheduler.schedule_now(
            move || {
                let counter = Arc::clone(&counter_clone);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            Duration::from_millis(100),
            "Test error",
        );

        sleep(Duration::from_millis(550)).await;
        assert!(counter.load(Ordering::SeqCst) >= 6);
    }

    #[tokio::test]
    async fn test_schedule_sync_now() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        scheduler.schedule_sync_now(
            move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            Duration::from_millis(100),
            "Test error",
        );

        sleep(Duration::from_millis(550)).await;
        assert!(counter.load(Ordering::SeqCst) >= 6);
    }

    #[tokio::test]
    async fn test_wait_until_success() {
        let scheduler = Scheduler::new();
        let result = scheduler
            .wait_until(|| async { Ok(()) }, Duration::from_secs(1))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wait_until_timeout() {
        let scheduler = Scheduler::new();
        let result = scheduler
            .wait_until(
                || async {
                    sleep(Duration::from_secs(2)).await;
                    Ok(())
                },
                Duration::from_secs(1),
            )
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("deadline has elapsed"));
    }

    #[tokio::test]
    async fn test_wait_until_error() {
        let scheduler = Scheduler::new();
        let result = scheduler
            .wait_until(
                || async { anyhow::bail!("Test error") },
                Duration::from_secs(1),
            )
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Test error");
    }
}
