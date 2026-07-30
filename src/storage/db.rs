use crate::{
    error::crawler_error::CrawlerError, network::link_fetcher::DomainData,
    parsers::html_parser::ParsedPage, storage::domain_cache::CachedData,
};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode},
};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

const MAX_DB_RECONNECTS: usize = 3;

#[derive(Debug, PartialEq, Eq)]
pub enum UrlAccess {
    Allowed,
    Disallowed,
    UnknownDomain,
    URLWithoutHost,
}

#[derive(Clone)]
pub struct CrawlerDB {
    pool: SqlitePool,
}

impl CrawlerDB {
    pub async fn new(db_name: &str) -> Result<Self, CrawlerError> {
        let connection_options = SqliteConnectOptions::from_str(db_name)?
            .create_if_missing(true)
            .pragma("foreign_keys", "ON")
            .journal_mode(SqliteJournalMode::Wal);

        let pool = SqlitePool::connect_with(connection_options).await?;

        Self::create_tables(&pool).await?;

        Ok(Self { pool })
    }

    async fn create_tables(pool: &SqlitePool) -> Result<(), CrawlerError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS domains (
                dom_id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain_string TEXT NOT NULL UNIQUE,
                delay REAL,
                allowed_urls TEXT
            )",
        )
        .execute(pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dom_id INTEGER,
                url TEXT NOT NULL UNIQUE,
                title TEXT,
                description TEXT,
                clean_text TEXT,
                FOREIGN KEY(dom_id) REFERENCES domains(dom_id) ON DELETE CASCADE
            )",
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn save_parsed_page(
        &self,
        dom_id: i64,
        page: &ParsedPage,
    ) -> Result<(), CrawlerError> {
        let mut backoff = Duration::from_secs(1);
        for i in 0..MAX_DB_RECONNECTS {
            let res = sqlx::query(
                "INSERT INTO pages (dom_id, url, title, clean_text, description) 
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(url) DO UPDATE SET
                    dom_id = excluded.dom_id,
                    title = excluded.title,
                    clean_text = excluded.clean_text,
                    description = excluded.description",
            )
            .bind(dom_id)
            .bind(page.url.as_str())
            .bind(page.title.as_deref())
            .bind(page.clean_text.as_deref())
            .bind(page.description.as_deref())
            .execute(&self.pool)
            .await;

            match res {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if i == MAX_DB_RECONNECTS - 1 {
                        return Err(CrawlerError::DbError(e));
                    }
                    eprintln!("attempt {}: Error during saving page: {}", i + 1, e);
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        }
        Err(CrawlerError::DbError(sqlx::Error::RowNotFound))
    }

    pub async fn save_domain(&self, domain_data: &DomainData) -> Result<i64, CrawlerError> {
        let mut backoff = Duration::from_secs(1);

        let robots_str = domain_data.robots.as_deref().map(|s| s.as_str());

        for i in 0..MAX_DB_RECONNECTS {
            let res: Result<i64, sqlx::Error> = sqlx::query_scalar(
                "INSERT INTO domains (domain_string, delay, allowed_urls) 
                 VALUES (?, ?, ?)
                 ON CONFLICT(domain_string) DO UPDATE SET 
                    delay = excluded.delay,
                    allowed_urls = excluded.allowed_urls
                 RETURNING dom_id",
            )
            .bind(&domain_data.domain_string)
            .bind(domain_data.delay)
            .bind(robots_str)
            .fetch_one(&self.pool)
            .await;

            match res {
                Ok(domain_id) => return Ok(domain_id),
                Err(e) => {
                    if i == MAX_DB_RECONNECTS - 1 {
                        return Err(CrawlerError::DbError(e));
                    }
                    eprintln!("attempt {}: Error during saving domain: {}", i + 1, e);
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        }

        Err(CrawlerError::DbError(sqlx::Error::RowNotFound))
    }

    pub async fn get_cache_info_about_domain(
        &self,
        domain: &str,
        agent: &str,
    ) -> Result<Option<CachedData>, CrawlerError> {
        let mut backoff = Duration::from_secs(1);
        for i in 0..MAX_DB_RECONNECTS {
            let res = sqlx::query(
                "SELECT dom_id, allowed_urls, delay FROM domains WHERE domain_string = ?",
            )
            .bind(domain)
            .fetch_optional(&self.pool)
            .await;

            match res {
                Ok(record) => {
                    if let Some(row) = record {
                        let id: i64 = row.try_get("dom_id")?;
                        let allowed_urls: Option<String> = row.try_get("allowed_urls")?;
                        let delay_f32: f32 = row.try_get("delay")?;

                        let cached_data =
                            CachedData::new(id, allowed_urls.map(Arc::new), delay_f32, agent)?;

                        return Ok(Some(cached_data));
                    }
                    return Ok(None);
                }
                Err(e) => {
                    if i == MAX_DB_RECONNECTS - 1 {
                        return Err(CrawlerError::DbError(e));
                    }
                    eprintln!(
                        "attempt {}: Error during getting domain info by name: {}",
                        i + 1,
                        e
                    );
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        }
        Err(CrawlerError::DbError(sqlx::Error::RowNotFound))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::link_fetcher::NotParsedPageData;
    use std::sync::Arc;
    use std::time::Duration;
    use url::Url;

    fn create_domain(domain_string: String, delay: f32, robots: Option<Arc<String>>) -> DomainData {
        DomainData {
            domain_string,
            robots,
            delay,
        }
    }

    #[tokio::test]
    async fn test_new_db_fn() {
        let res = CrawlerDB::new("sqlite::memory:").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_inserting_and_getting_domain_cache_info() -> Result<(), CrawlerError> {
        let db = CrawlerDB::new("sqlite::memory:").await?;

        let robots_content = Arc::new("User-agent: * Disallow: /admin".to_string());

        let dom1 = create_domain(
            "test1.com".to_string(),
            4.2,
            Some(Arc::clone(&robots_content)),
        );
        let id1 = db.save_domain(&dom1).await?;

        let dom2 = create_domain("test2.com".to_string(), 5.2, None);
        let id2 = db.save_domain(&dom2).await?;

        let cache_info1 = db
            .get_cache_info_about_domain("test1.com", "ronaldo")
            .await?
            .expect("Domain should be in db");

        assert_eq!(cache_info1.id, id1);

        assert_eq!(
            cache_info1.delay.as_millis(),
            Duration::from_secs_f32(4.2).as_millis()
        );

        assert!(cache_info1.robot.is_some());

        let cache_info2 = db
            .get_cache_info_about_domain("test2.com", "ronaldo")
            .await?
            .expect("Second domain should be in db");

        assert_eq!(cache_info2.id, id2);
        assert_eq!(
            cache_info2.delay.as_millis(),
            Duration::from_secs_f32(5.2).as_millis()
        );
        assert!(cache_info2.robot.is_none());

        let not_found = db
            .get_cache_info_about_domain("unknown.com", "ronaldo")
            .await?;
        assert!(not_found.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_save_parsed_page() -> Result<(), CrawlerError> {
        let db = CrawlerDB::new("sqlite::memory:").await?;

        let dom = create_domain(
            "ronaldo.com".to_string(),
            1.0,
            Some(Arc::new("".to_string())),
        );
        let dom_id = db.save_domain(&dom).await?;

        let page = ParsedPage::parse(
            NotParsedPageData {
                content: "<h1>CR7</h1>".to_string(),
                url: Url::parse("https://www.ronaldo.com").map_err(CrawlerError::InvalidUrl)?,
            },
            Arc::default(),
        );

        let res = db.save_parsed_page(dom_id, &page).await;
        assert!(res.is_ok());

        Ok(())
    }
}
