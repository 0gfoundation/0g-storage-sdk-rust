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

    // 异步函数的立即执行并调度
    pub fn schedule_now<F, Fut>(&self, action: F, interval: Duration, error_message: &'static str)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let action = Arc::new(move || {
            Box::pin(action()) as Pin<Box<dyn Future<Output = Result<()>> + Send>>
        });
        tokio::spawn(run_scheduled_task_now(action, interval, error_message));
    }

    // 同步函数的立即执行并调度
    pub fn schedule_sync_now<F>(&self, action: F, interval: Duration, error_message: &'static str)
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        let action = Arc::new(action);
        tokio::spawn(run_scheduled_sync_task_now(action, interval, error_message));
    }

    pub async fn wait_until<F, Fut>(&self, action: F, timeout: Duration) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        tokio::select! {
            result = action() => result,
            _ = tokio::time::sleep(timeout) => anyhow::bail!("Operation timed out"),
        }
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
        match task::spawn_blocking(move || action_clone()).await {
            Ok(result) => {
                if let Err(err) = result {
                    warn!("{}: {}", error_message, err);
                }
            }
            Err(e) => warn!("Failed to spawn blocking task: {}", e),
        }
    }
}

async fn run_scheduled_task_now(
    action: AsyncFunc,
    interval: Duration,
    error_message: &'static str,
) {
    if let Err(err) = action().await {
        error!("{}: {}", error_message, err);
    }
    run_scheduled_task(action, interval, error_message).await;
}

async fn run_scheduled_sync_task_now(
    action: SyncFunc,
    interval: Duration,
    error_message: &'static str,
) {
    if let Err(err) = action() {
        error!("{}: {}", error_message, err);
    }
    run_scheduled_sync_task(action, interval, error_message).await;
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

        tokio::time::sleep(Duration::from_millis(550)).await;
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

        tokio::time::sleep(Duration::from_millis(550)).await;
        assert!(counter.load(Ordering::SeqCst) >= 5);
    }

    #[tokio::test]
    async fn test_compare_sync_and_async() {
        let scheduler = Scheduler::new();
        let sync_counter = Arc::new(AtomicUsize::new(0));
        let async_counter = Arc::new(AtomicUsize::new(0));

        // 同步调度
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

        // 异步调度
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

        // 等待并定期打印进度
        for i in 0..12 {
            sleep(Duration::from_secs(1)).await;
            println!(
                "Progress after {}s - Sync: {}, Async: {}",
                i + 1,
                sync_counter.load(Ordering::SeqCst),
                async_counter.load(Ordering::SeqCst)
            );
        }

        let sync_count = sync_counter.load(Ordering::SeqCst);
        let async_count = async_counter.load(Ordering::SeqCst);

        println!("Final - Sync task executed {} times", sync_count);
        println!("Final - Async task executed {} times", async_count);

        // 由于任务执行时间和调度间隔相近，我们期望它们的执行次数大致相同
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

        tokio::time::sleep(Duration::from_millis(550)).await;
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

        tokio::time::sleep(Duration::from_millis(550)).await;
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
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    Ok(())
                },
                Duration::from_secs(1),
            )
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Operation timed out");
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
