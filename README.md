# Polite Web Crawler

A microservice web crawler written in Rust. It respects `robots.txt`, rate-limits requests per domain, deduplicates pages, and stores the results in SQLite.

## Architecture

```
flowchart LR
    W["worker_service<br/>downloads and parses pages"]
    S["storage_service<br/>database, caches, deduplication"]
    Q[("Redis List<br/>task queue")]
    R[("Redis Stream<br/>crawler_events")]
    D[("SQLite<br/>./data")]

    W -->|"gRPC request: is URL allowed / seen?"| S
    S -->|"gRPC response"| W
    W -->|"parsed pages and domains"| R
    R -->|"consumed + XACK"| S
    W <-->|"pop URLs / push found links"| Q
    S -->|"push sitemap URLs"| Q
    S --> D
```

- **`worker_service`** downloads pages, parses HTML and sitemaps, and extracts links.
- **`storage_service`** stores pages and domains, caches `robots.txt`, and deduplicates content and URLs.
- **`common`** contains shared types, DTOs, error handling, and the task queue.
- **`proto/`** contains the gRPC contract between the services.

### Why two transports

| Operation | Transport | Reason |
|---|---|---|
| URL access check, domain delay, URL deduplication | gRPC | the worker needs an answer immediately to continue |
| Saving pages and domains | Redis Streams | the worker doesn't wait, at-least-once delivery, acknowledged with `XACK` |
| URL queue for crawling | Redis List | shared queue that both workers and storage (sitemaps) push to |

## Features

- `robots.txt` and `Crawl-delay` compliance
- Per-domain rate limiting
- Sitemap crawling
- Page deduplication using SimHash with an LSH index
- URL deduplication using a Bloom filter
- Graceful shutdown for both services
- One-command startup with Docker Compose

## Tech stack

Rust, Tokio, tonic (gRPC), Redis (Streams and Lists), SQLite (sqlx), Docker Compose

## Quick start

You only need **Docker** and **Docker Compose**. Rust is not required.

```bash
git clone https://github.com/refrigerator2/web_crawler.git
cd web_crawler
```

Run:

```bash
SEED_URL=https://docs.rs WORKERS=4 LIMIT=10 docker-compose up --abort-on-container-exit
```

The first build takes a few minutes because the Rust project is compiled inside the images. Later builds use the layer cache.

### Parameters

| Variable | Description | If not set |
|---|---|---|
| `SEED_URL` | starting URL | URLs are read from `seed.txt` |
| `WORKERS` | number of parallel workers | default value from the code |
| `LIMIT` | number of pages to save before stopping | crawls until the task queue is empty |

All three parameters are optional. To run without them:

```bash
docker-compose up --abort-on-container-exit
```

### Stopping

The crawler exits on its own when `LIMIT` is reached or the queue is empty. `--abort-on-container-exit` then stops the remaining containers. You can also interrupt it manually with `Ctrl+C`.

To remove containers and networks afterwards:

```bash
docker-compose down
```

To also wipe Redis data:

```bash
docker-compose down -v
```

## Output

Collected data is stored in the SQLite file `./data/crawler.db` in the project root.

```bash
sqlite3 ./data/crawler.db ".tables"
```

## Project structure

```
.
├── crates/
│   ├── common/            # shared types, errors, task queue
│   ├── storage_service/   # gRPC server, Redis Stream consumer, SQLite
│   └── worker_service/    # crawler
├── proto/                 # gRPC contract
├── seed.txt               # default starting URLs
└── docker-compose.yml
```

## Design decisions

- **LSH for SimHash.** The hash is split into 4 bands of 16 bits. Duplicate candidates are looked up only among hashes that share a band, so the check does not degrade to O(N) as the database grows.
- **Graceful shutdown.** Both services handle `SIGINT` and `SIGTERM`. The gRPC server waits for in-flight requests, and the stream consumer finishes its current message.
- **At-least-once processing.** A message is acknowledged in Redis Streams only after a successful database write. Unprocessed messages stay in the pending list.

## Tests

```bash
cargo test --workspace
cargo test --workspace -- --ignored   # integration tests, require Docker
```

Building locally requires `rustup`, `protobuf`, and `cmake`.

## Ethical use

The crawler respects `robots.txt` and limits load on domains, but only run it against sites you are allowed to crawl. Avoid high `WORKERS` values on third-party sites.
