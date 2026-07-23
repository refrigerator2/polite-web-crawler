# Polite Async Web Crawler in Rust

An asynchronous, multi-threaded web crawler written in Rust, built on top of `tokio` and `reqwest`. Designed with performance, reliable URL deduplication, and server politeness (adherence to `robots.txt` and `Crawl-Delay`) in mind.

---

## ✨ Key Features

* **⚡ High Performance:** Asynchronous architecture utilizing a `tokio::spawn` worker pool coordinated via an `mpsc` channel.
* **🛡️ Politeness First:**
  * Checks URL access permissions against `robots.txt` for every domain.
  * Configurable per-domain request delays (`Crawl-Delay`) to prevent IP bans.
* **🔍 Keyword Filtering:** Downloads and persists only pages matching specific target keywords.
* **🧠 Efficient Deduplication:**
  * In-memory filtering of visited URLs using thread-safe state wrappers (`RwLock`).
  * Two-tier URL verification (Memory + Database).
* **💾 State Persistence:** Caches domain rules and parsed page metadata in a database.
