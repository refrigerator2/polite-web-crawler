use crate::error::crawler_error::CrawlerError;
use redis::{AsyncCommands, aio::ConnectionManager};
use url::Url;

#[derive(Clone)]
pub struct TaskQueue {
    push_queue: ConnectionManager,
    pop_queue: ConnectionManager,
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

        let push_queue = ConnectionManager::new(client.clone()).await?;
        let pop_queue = ConnectionManager::new(client).await?;

        Ok(Self {
            push_queue,
            pop_queue,
            name: queue_name.to_string(),
            timeout_secs,
        })
    }

    pub async fn push(&self, url: &str) -> Result<(), CrawlerError> {
        let mut conn = self.push_queue.clone();
        let _: usize = conn.rpush(&self.name, url).await?;
        Ok(())
    }

    pub async fn pop_front(&self) -> Result<Option<Url>, CrawlerError> {
        let mut conn = self.pop_queue.clone();
        let line: Option<(String, String)> = conn.blpop(&self.name, self.timeout_secs).await?;

        if let Some((_key, url_str)) = line {
            let url = Url::parse(&url_str)?;
            return Ok(Some(url));
        }

        Ok(None)
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
            .expect("Couldn't connect to redis while testing")
    }

    #[tokio::test]
    async fn test_push_and_pop_single_item() {
        let queue = setup_test_queue().await;
        let test_url = Url::parse("https://example.com/rust").unwrap();

        queue
            .push(test_url.as_str())
            .await
            .expect("Error while pushing");

        let popped_url = queue.pop_front().await.expect("Error while popping");

        assert_eq!(popped_url.expect("Mustn't be None"), test_url);
    }

    #[tokio::test]
    async fn test_fifo_order() {
        let queue = setup_test_queue().await;
        let url1 = Url::parse("https://example.com/1").unwrap();
        let url2 = Url::parse("https://example.com/2").unwrap();
        let url3 = Url::parse("https://example.com/3").unwrap();

        queue.push(url1.as_str()).await.unwrap();
        queue.push(url2.as_str()).await.unwrap();
        queue.push(url3.as_str()).await.unwrap();

        // Поправлены типы (сравниваем Url c Url, а не со String)
        assert_eq!(queue.pop_front().await.unwrap(), Some(url1));
        assert_eq!(queue.pop_front().await.unwrap(), Some(url2));
        assert_eq!(queue.pop_front().await.unwrap(), Some(url3));
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
