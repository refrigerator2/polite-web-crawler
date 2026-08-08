use redis::{AsyncCommands, Connection, aio::ConnectionManager};

use crate::error::crawler_error::CrawlerError;

#[derive(Clone)]
pub struct TaskQueue {
    queue: ConnectionManager,
    name: String,
    timeout_secs: f64,
}
impl TaskQueue {
    pub async fn new(
        redis_url: &str,
        queue_name: &str,
        timeout_secs: f64,
    ) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(redis_url)?;
        let manager = ConnectionManager::new(client).await?;

        Ok(Self {
            queue: manager,
            name: queue_name.to_string(),
            timeout_secs,
        })
    }
    pub async fn push(&self, url: &str) -> Result<(), CrawlerError> {
        let mut queue_clone = self.queue.clone();
        let _: () = queue_clone.rpush(&self.name, url).await?;
        Ok(())
    }
    pub async fn pop_front(&self) -> Result<Option<String>, CrawlerError> {
        let mut queue_clone = self.queue.clone();
        let line: Option<(String, String)> =
            queue_clone.blpop(&self.name, self.timeout_secs).await?;
        Ok(line.map(|tuple| tuple.1))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    const REDIS_TEST_URL: &str = "redis://127.0.0.1:6379";

    async fn setup_test_queue() -> TaskQueue {
        let random_queue_name = format!("test_queue_{}", Uuid::new_v4());
        TaskQueue::new(REDIS_TEST_URL, &random_queue_name, 0.1)
            .await
            .expect("Coudnt connect to redis while testing")
    }

    #[tokio::test]
    async fn test_push_and_pop_single_item() {
        let queue = setup_test_queue().await;
        let test_url = "https://example.com/rust";

        queue.push(test_url).await.expect("Error while pushing");

        let popped_url = queue.pop_front().await.expect("Error while popping");

        assert_eq!(popped_url.expect("Mustnt be None"), test_url);
    }

    #[tokio::test]
    async fn test_fifo_order() {
        let queue = setup_test_queue().await;
        let url1 = "https://example.com/1";
        let url2 = "https://example.com/2";
        let url3 = "https://example.com/3";

        queue.push(url1).await.unwrap();
        queue.push(url2).await.unwrap();
        queue.push(url3).await.unwrap();

        assert_eq!(queue.pop_front().await.unwrap(), Some(url1.to_string()));
        assert_eq!(queue.pop_front().await.unwrap(), Some(url2.to_string()));
        assert_eq!(queue.pop_front().await.unwrap(), Some(url3.to_string()));
    }

    #[tokio::test]
    async fn test_pop_empty_queue() {
        let queue = setup_test_queue().await;
        let result = queue.pop_front().await.expect("Error while popping");
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_concurrent_push_and_pop() {
        let queue = setup_test_queue().await;
        let num_workers = 10;
        let items_per_worker = 20;

        let mut handles = Vec::new();

        for worker_id in 0..num_workers {
            let q = queue.clone();
            let handle = tokio::spawn(async move {
                for i in 0..items_per_worker {
                    let url = format!("https://example.com/worker/{}/item/{}", worker_id, i);
                    q.push(&url).await.unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let mut count = 0;
        while let Ok(Some(_)) = queue.pop_front().await {
            count += 1;
        }

        assert_eq!(count, num_workers * items_per_worker);
    }
}
