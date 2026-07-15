use crate::{crawler_error::CrawlerError, html_parser::ParsedPage};
use std::str::FromStr;
use texting_robots::Robot;

use sqlx::{
    ConnectOptions, Row, SqlitePool, query,
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
            "INSERT OR REPLACE INTO pages (dom_id, url, title, clean_text, description) 
         VALUES (?, ?, ?, ?, ?)",
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
    pub async fn save_domain(
        &self,
        domain_string: String,
        delay: f32,
        allowed_urls: String,
    ) -> Result<(), CrawlerError> {
        sqlx::query(
            "INSERT OR REPLACE INTO domains (domain_string, delay, allowed_urls) 
         VALUES (?, ?, ?)",
        )
        .bind(domain_string)
        .bind(delay)
        .bind(allowed_urls)
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

                if (robot.allowed(path)) {
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
}
