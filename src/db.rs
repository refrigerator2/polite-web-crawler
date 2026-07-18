use crate::{crawler_error::CrawlerError, html_parser::ParsedPage, link_fetcher::DomainData};
use std::str::FromStr;
use texting_robots::Robot;

use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode},
};

#[derive(Debug, PartialEq, Eq)]
pub enum UrlAccess {
    Allowed,
    Disallowed,
    UnknownDomain,
    URLWithoutHost,
}

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
        sqlx::query(
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
        .await?;

        Ok(())
    }

    pub async fn save_domain(&self, domain_data: DomainData) -> Result<(), CrawlerError> {
        sqlx::query(
            "INSERT OR REPLACE INTO domains (domain_string, delay, allowed_urls) 
         VALUES (?, ?, ?)",
        )
        .bind(domain_data.domain_string)
        .bind(domain_data.delay)
        .bind(domain_data.robots)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn is_url_allowed(
        &self,
        url: &url::Url,
        user_agent: &str,
    ) -> Result<UrlAccess, CrawlerError> {
        let host = match url.host_str() {
            Some(h) => h,
            None => return Ok(UrlAccess::URLWithoutHost),
        };
        let path = url.path();

        let record = sqlx::query("SELECT allowed_urls FROM domains WHERE domain_string = ?")
            .bind(host)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(row) = record {
            if let Some(raw_robots) = row.get::<Option<String>, _>("allowed_urls") {
                let robot = Robot::new(user_agent, raw_robots.as_bytes())?;

                if robot.allowed(path) {
                    return Ok(UrlAccess::Allowed);
                } else {
                    return Ok(UrlAccess::Disallowed);
                }
            } else {
                return Ok(UrlAccess::Allowed);
            }
        }

        Ok(UrlAccess::UnknownDomain)
    }

    pub async fn get_delay(&self, host: &str) -> Result<Option<f32>, CrawlerError> {
        let record = sqlx::query("SELECT delay FROM domains WHERE domain_string = ?")
            .bind(host)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(row) = record {
            if let Some(delay) = row.get::<Option<f32>, _>("delay") {
                return Ok(Some(delay));
            }
        }

        Ok(None)
    }

    pub async fn check_if_url_is_already_parsed(&self, url: &str) -> Result<bool, CrawlerError> {
        let record = sqlx::query("SELECT 1 FROM pages WHERE url = ? LIMIT 1")
            .bind(url)
            .fetch_optional(&self.pool)
            .await?;

        if record.is_some() {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn get_domain_id_by_domain_name(
        &self,
        host: &str,
    ) -> Result<Option<i64>, CrawlerError> {
        let record = sqlx::query("SELECT dom_id FROM domains WHERE domain_string = ?")
            .bind(host)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(row) = record {
            if let Some(id) = row.get::<Option<i64>, _>("dom_id") {
                return Ok(Some(id));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::link_fetcher::{DomainData, NotParsedPageData};
    use url::Url;

    fn create_domain(domain_string: String, delay: f32, robots: Option<String>) -> DomainData {
        DomainData {
            domain_string,
            robots,
            delay,
        }
    }

    #[tokio::test]
    async fn test_new_db_fn() {
        let res = CrawlerDB::new("sqlite::memory:").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_inserting_and_getting_domains() {
        let db = CrawlerDB::new("sqlite::memory:").await;
        assert!(db.is_ok());
        let db = db.unwrap();

        let dom1 = create_domain(
            "test1".to_string(),
            4.2,
            Some("https://www.ronaldo.com".to_string()),
        );
        let res = db.save_domain(dom1).await;
        assert!(res.is_ok());

        let dom2 = create_domain(
            "test2".to_string(),
            5.2,
            Some("https://www.messi.com".to_string()),
        );
        let res = db.save_domain(dom2).await;
        assert!(res.is_ok());

        let dom3 = create_domain(
            "test3".to_string(),
            6.7,
            Some("https://www.other.com".to_string()),
        );
        let res = db.save_domain(dom3).await;
        assert!(res.is_ok());

        let get_res = db.get_delay("test1").await;
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap().unwrap(), 4.2);

        let get_res = db.get_delay("test2").await;
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap().unwrap(), 5.2);

        let get_res = db.get_delay("test3").await;
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap().unwrap(), 6.7)
    }

    #[tokio::test]
    async fn test_inserting_and_checking_pages() {
        let db = CrawlerDB::new("sqlite::memory:").await;
        assert!(db.is_ok());
        let db = db.unwrap();

        let dom = create_domain("ronaldo.com".to_string(), 1.0, Some("".to_string()));
        db.save_domain(dom).await.unwrap();

        let page = ParsedPage::parse(
            NotParsedPageData {
                content: "smt".to_string(),
                url: Url::parse("https://www.ronaldo.com").unwrap(),
            },
            None,
        )
        .unwrap();

        let res = db.save_parsed_page(1, &page).await;
        assert!(res.is_ok());

        let check_res = db
            .check_if_url_is_already_parsed("https://www.ronaldo.com/")
            .await;
        assert!(check_res.is_ok());
        assert_eq!(check_res.unwrap(), true);
    }

    #[tokio::test]
    async fn test_is_url_allowed_scenarios() {
        let db = CrawlerDB::new("sqlite::memory:").await.unwrap();
        let user_agent = "MyBot";

        let unknown_url = Url::parse("https://github.com/trending").unwrap();
        let access = db.is_url_allowed(&unknown_url, user_agent).await.unwrap();
        assert_eq!(access, UrlAccess::UnknownDomain);

        let raw_robots = "User-agent: *\nDisallow: /admin/\nAllow: /public/";
        let dom = create_domain("example.com".to_string(), 1.0, Some(raw_robots.to_string()));
        db.save_domain(dom).await.unwrap();

        let allowed_url = Url::parse("https://example.com/public/index.html").unwrap();
        let access = db.is_url_allowed(&allowed_url, user_agent).await.unwrap();
        assert_eq!(access, UrlAccess::Allowed);

        let disallowed_url = Url::parse("https://example.com/admin/settings").unwrap();
        let access = db
            .is_url_allowed(&disallowed_url, user_agent)
            .await
            .unwrap();
        assert_eq!(access, UrlAccess::Disallowed);

        let no_host_url = Url::parse("data:text/plain,hello").unwrap();
        let access = db.is_url_allowed(&no_host_url, user_agent).await.unwrap();
        assert_eq!(access, UrlAccess::URLWithoutHost);
    }

    #[tokio::test]
    async fn test_domain_without_robots_txt() {
        let db = CrawlerDB::new("sqlite::memory:").await.unwrap();

        let dom = create_domain("nobots.com".to_string(), 0.0, Some("".to_string()));
        db.save_domain(dom).await.unwrap();

        let url = Url::parse("https://nobots.com/any-path").unwrap();
        let access = db.is_url_allowed(&url, "MyBot").await.unwrap();

        assert_eq!(access, UrlAccess::Allowed);
    }

    #[tokio::test]
    async fn test_check_if_url_is_already_parsed_not_found() {
        let db = CrawlerDB::new("sqlite::memory:").await.unwrap();

        let exists = db
            .check_if_url_is_already_parsed("https://notfound.com")
            .await
            .unwrap();
        assert_eq!(exists, false);
    }

    #[tokio::test]
    async fn test_get_domain_id() {
        let db = CrawlerDB::new("sqlite::memory:").await.unwrap();

        let dom = create_domain("nobots.com".to_string(), 0.0, Some("".to_string()));
        let res = db.save_domain(dom).await;
        assert!(res.is_ok());

        let res = db.get_domain_id_by_domain_name("nobots.com").await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap().unwrap(), 1);

        let res = db.get_domain_id_by_domain_name("smt.com").await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), None);
    }
}
