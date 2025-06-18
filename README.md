# Pasta Pipeline – Telegram \& Telegraph ETL with Apache Airflow

> An end-to-end, container-first pipeline that scrapes messages from **@mrakopedia**, extracts Telegraph articles, cleans duplicates and stores everything in Supabase-hosted PostgreSQL via **Apache Airflow 2.7** [^1].

---

## ✨ Key Features

* **Multi-stage ETL** – scrape Telegram, enrich with hashtags, crawl Telegraph pages, upsert into Postgres and run weekly duplicate cleanup in one DAG family [^1][^2].
* **Four processing modes** – `incremental`, `refresh_old`, `daily`, `full` – selectable through Airflow params for flexible back-filling and re-processing [^1].
* **Robust connection handling** – SQLAlchemy pools with `pool_pre_ping`, `pool_recycle` and retry context managers keep long-lived Supabase SSL connections healthy [^1].
* **Batch + async fetching** – Telegraph links processed in configurable batches with `aiohttp` + `asyncio` for fast throughput while respecting rate limits [^1].
* **Automatic duplicate remover** – weekly DAG removes duplicate Telegram messages \& Telegraph rows and clears orphaned links for data hygiene [^1].
* **Docker-first deployment** – single `docker-compose.yml` spins up web-server, scheduler and health-checks with 1 GB memory limits per service [^1].

---

## 📂 Repository Layout

```
pasta-pipeline/
├── dags/
│   ├── dag.py               # DAG factory – creates 4 DAG variants
│   ├── processors/
│   │   ├── message_scraper.py   # Telethon scraper
│   │   ├── db_loader.py         # Batched upserts into Postgres
│   │   └── telegraph_processor.py# Async Telegraph crawler
│   ├── utils/               # Regex helpers
│   └── duplicate_remover.py # Weekly cleanup tasks
├── docker-compose.yml       # Two-service Airflow stack
├── Dockerfile               # Extends apache/airflow:2.7.2
└── README.md                # You are here
```


---

## 🏗 Architecture \& Flow

```
Telegram ➜ Message Scraper ➜ telegram_messages
                 │
                 ▼
        Link Extractor (SQL)
                 │
                 ▼
        Telegraph Processor ➜ telegraph_content
                 │
                 ▼
      Duplicate Cleanup DAG  (weekly)
```

1. **Scrape messages** with Telethon and save raw JSON/CSV artefacts [^2].
2. **Load into Postgres** with batched `INSERT … ON CONFLICT` upserts .
3. **Select links** according to DAG parameters (`incremental`, `refresh_old`, etc.) [^1].
4. **Async fetch + parse** Telegraph pages, compute MD5 hashes and store content .
5. **Deduplicate weekly** to keep only freshest message rows and unify identical content hashes .

---

## 🛠 Tech Stack

| Layer | Tool / Library | Why it’s used |
| :-- | :-- | :-- |
| Orchestration | Apache Airflow 2.7 | DAG scheduling \& retries [^1] |
| Messaging API | Telethon 1.34 | Full Telegram MTProto access [^2] |
| HTML Parsing | BeautifulSoup 4 | Resilient content extraction |
| Async HTTP | aiohttp 3.9 | Non-blocking Telegraph pulls |
| Storage | PostgreSQL 15 (on Supabase) | ACID + JSONB support |
| Data Frames | pandas 2.x | CSV/JSON transform utilities |
| Containers | Docker \& Compose | Repeatable local dev \& prod |


---

## ⚙️ Quick Start

1. **Clone \& configure**

```bash
git clone https://github.com/your-org/pasta-pipeline.git
cd pasta-pipeline
cp .env.example .env  # fill Telegram & Supabase creds
```

2. **Build \& run**

```bash
docker compose up ‑-build
```

3. **Open Airflow UI** – http://localhost:8080, login `admin / $AIRFLOW_ADMIN_PASSWORD` [^1].
4. **Trigger DAGs**
    * `pasta_pipeline` – main daily incremental run [^1].
    * `pasta_pipeline_full` – manual full back-fill without limits [^1].
    * `pasta_duplicate_cleanup` – weekly duplicate purge .

---

## 🔑 Environment Variables (.env)

| Name | Description |
| :-- | :-- |
| `API_ID`, `API_HASH`, `PHONE_NUMBER` | Telegram API credentials [^2] |
| `SESSION_STRING` | Saved Telethon session for headless login [^2] |
| `AIRFLOW_SQL_CONN` | Supabase Postgres URI (session mode 5432) |
| `AIRFLOW_SECRET_KEY` | Flask secret key for Airflow UI [^1] |
| `AIRFLOW_ADMIN_PASSWORD` | Initial Airflow admin password [^1] |


---

## 📝 Database Schema (simplified)

### telegram_messages

| Column | Type | Purpose |
| :-- | :-- | :-- |
| message_id | BIGINT PK | Telegram ID |
| text / date / views / forwards | — | Raw metadata |
| hashtags | TEXT[] | Extracted hashtags |
| telegraph_link | TEXT | First Telegraph URL |
| reactions | JSONB | Emoji → count |
| scraped_at / processed_at / last_updated | TIMESTAMP | Data lineage |

### telegraph_content

| Column | Type | Purpose |
| :-- | :-- | :-- |
| url PK | TEXT | Telegraph URL |
| title / content / description | — | Parsed article fields |
| content_hash / description_hash | TEXT | Change detection |
| processed_at / last_checked | TIMESTAMP | Versioning |
| word_count / status / retry_count | — | Monitoring helpers |


---

## 🧹 Duplicate Removal Strategy

* **telegram_messages** – keep latest `processed_at`, drop older duplicates per `message_id` .
* **telegraph_content** – keep first URL per identical `content_hash`, delete rest .
* **Orphan cleaner** – nullifies stale `telegraph_link` values not present in content table .

Weekly DAG `pasta_duplicate_cleanup` executes these SQL scripts automatically [^1].

---

Hot-reload Airflow code changes by simply restarting the affected container:

```bash
docker compose restart airflow-webserver
```


---
