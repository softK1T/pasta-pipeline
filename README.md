# Pasta Pipeline – Telegram & Telegraph ETL with Apache Airflow

> An end-to-end, container-first pipeline that scrapes messages from **@mrakopedia**, extracts Telegraph articles, cleans duplicates and stores everything in Supabase-hosted PostgreSQL via **Apache Airflow 2.7**.

---

## ✨ Key Features

- **Multi-stage ETL** – scrape Telegram, enrich with hashtags, crawl Telegraph pages, upsert into Postgres and run weekly duplicate cleanup in one DAG family.
- **Four processing modes** – `incremental`, `refresh_old`, `daily`, `full` – selectable through Airflow params for flexible back-filling and re-processing.
- **Robust error handling** – comprehensive retry logic, rate limiting, and graceful failure recovery.
- **Batch + async fetching** – Telegraph links processed in configurable batches with `aiohttp` + `asyncio` for fast throughput while respecting rate limits.
- **Automatic duplicate remover** – weekly DAG removes duplicate Telegram messages & Telegraph rows and clears orphaned links for data hygiene.
- **Configuration management** – centralized configuration system with validation and environment-based settings.
- **Docker-first deployment** – single `docker-compose.yml` spins up web-server, scheduler and health-checks with resource limits.
- **Monitoring & observability** – optional Prometheus and Grafana integration for metrics and visualization.

---

## 📂 Repository Layout

```
pasta-pipeline/
├── dags/
│   ├── dag.py                    # DAG factory – creates 3 DAG variants
│   ├── processors/
│   │   ├── message_scraper.py    # Telethon scraper with error handling
│   │   ├── db_loader.py          # Batched upserts into Postgres
│   │   ├── telegraph_processor.py # Async Telegraph crawler with retries
│   │   └── duplicate_remover.py  # Weekly cleanup tasks
│   └── utils/
│       └── utils.py              # Regex helpers
├── configs/
│   └── config.py                 # Configuration management system
├── docker-compose.yml            # Multi-service Airflow stack
├── Dockerfile                    # Extends apache/airflow:2.7.2
├── requirements.txt              # Python dependencies
├── env.example                   # Environment variables template
└── README.md                     # You are here
```

---

## 🏗 Architecture & Flow

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

1. **Scrape messages** with Telethon and save raw JSON/CSV artefacts with comprehensive error handling.
2. **Load into Postgres** with batched `INSERT … ON CONFLICT` upserts and connection pooling.
3. **Select links** according to DAG parameters (`incremental`, `refresh_old`, etc.).
4. **Async fetch + parse** Telegraph pages with retry logic, rate limiting, and content validation.
5. **Deduplicate weekly** to keep only freshest message rows and unify identical content hashes.

---

## 🛠 Tech Stack

| Layer         | Tool / Library              | Why it's used                |
| :------------ | :-------------------------- | :--------------------------- |
| Orchestration | Apache Airflow 2.7          | DAG scheduling & retries     |
| Messaging API | Telethon 1.34               | Full Telegram MTProto access |
| HTML Parsing  | BeautifulSoup 4             | Resilient content extraction |
| Async HTTP    | aiohttp 3.9                 | Non-blocking Telegraph pulls |
| Storage       | PostgreSQL 15 (on Supabase) | ACID + JSONB support         |
| Data Frames   | pandas 2.x                  | CSV/JSON transform utilities |
| Containers    | Docker & Compose            | Repeatable local dev & prod  |
| Monitoring    | Prometheus + Grafana        | Metrics and visualization    |

---

## ⚙️ Quick Start

1. **Clone & configure**

```bash
git clone https://github.com/your-org/pasta-pipeline.git
cd pasta-pipeline
cp env.example .env  # fill Telegram & Supabase creds
```

2. **Build & run**

```bash
# Basic setup
docker compose up --build

# With monitoring
docker compose --profile monitoring up --build
```

3. **Open Airflow UI** – http://localhost:8080, login `admin / $AIRFLOW_ADMIN_PASSWORD`.
4. **Trigger DAGs**
   - `pasta_pipeline` – main daily incremental run.
   - `pasta_pipeline_full` – manual full back-fill without limits.
   - `pasta_cleanup` – weekly duplicate purge and data maintenance.

---

## 🔑 Environment Variables (.env)

| Name                                 | Description                                 | Default     |
| :----------------------------------- | :------------------------------------------ | :---------- |
| `API_ID`, `API_HASH`, `PHONE_NUMBER` | Telegram API credentials                    | Required    |
| `SESSION_STRING`                     | Saved Telethon session for headless login   | Required    |
| `AIRFLOW_SQL_CONN`                   | Supabase Postgres URI (session mode 5432)   | Required    |
| `AIRFLOW_SECRET_KEY`                 | Flask secret key for Airflow UI             | Required    |
| `AIRFLOW_ADMIN_PASSWORD`             | Initial Airflow admin password              | Required    |
| `CHANNEL_NAME`                       | Telegram channel to scrape                  | @mrakopedia |
| `PROCESSING_MODE`                    | Pipeline mode (incremental/full)            | incremental |
| `TIME_LIMIT_DAYS`                    | Days to look back for messages              | 7           |
| `MAX_MESSAGES_PER_BATCH`             | Max messages per scraping batch             | 1000        |
| `DB_BATCH_SIZE`                      | Database batch size for inserts             | 100         |
| `TELEGRAPH_MAX_RETRIES`              | Max retries for Telegraph requests          | 3           |
| `TELEGRAPH_TIMEOUT`                  | Timeout for Telegraph requests (seconds)    | 30          |
| `TELEGRAPH_RATE_LIMIT`               | Rate limit delay between requests (seconds) | 1           |
| `DATA_RETENTION_DAYS`                | Days to retain data                         | 90          |
| `LOG_LEVEL`                          | Logging level                               | INFO        |

---

## 📝 Database Schema

### telegram_messages

| Column         | Type      | Purpose                    |
| :------------- | :-------- | :------------------------- |
| message_id     | BIGINT PK | Telegram ID                |
| date           | TIMESTAMP | Message timestamp          |
| text           | TEXT      | Message content            |
| views          | INTEGER   | View count                 |
| forwards       | INTEGER   | Forward count              |
| hashtags       | TEXT[]    | Extracted hashtags         |
| telegraph_link | TEXT      | First Telegraph URL        |
| reactions      | JSONB     | Emoji → count              |
| scraped_at     | TIMESTAMP | When message was scraped   |
| processed_at   | TIMESTAMP | When message was processed |

### telegraph_content

| Column           | Type      | Purpose                       |
| :--------------- | :-------- | :---------------------------- |
| url              | TEXT PK   | Telegraph URL                 |
| title            | TEXT      | Article title                 |
| content          | TEXT      | Full HTML content             |
| description      | TEXT      | Article description           |
| content_hash     | TEXT      | MD5 hash for change detection |
| description_hash | TEXT      | Description hash              |
| date_published   | TIMESTAMP | Publication date              |
| word_count       | INTEGER   | Content word count            |
| status           | TEXT      | Processing status             |
| retry_count      | INTEGER   | Number of retry attempts      |
| processed_at     | TIMESTAMP | Processing timestamp          |
| last_checked     | TIMESTAMP | Last check timestamp          |

---

## 🧹 Duplicate Removal Strategy

- **telegram_messages** – keep latest `processed_at`, drop older duplicates per `message_id`.
- **telegraph_content** – keep first URL per identical `content_hash`, delete rest.
- **Orphan cleaner** – nullifies stale `telegraph_link` values not present in content table.
- **Failed link cleanup** – removes links that have exceeded retry limits.
- **Data retention** – automatically removes old data based on retention policy.

Weekly DAG `pasta_cleanup` executes these SQL scripts automatically.

---

## 🔧 Configuration Management

The pipeline uses a centralized configuration system (`configs/config.py`) that:

- **Validates** all required settings on startup
- **Supports** environment variable overrides
- **Provides** type-safe configuration access
- **Handles** fallback values gracefully

```python
from configs.config import get_telegram_config, get_database_config

telegram_config = get_telegram_config()
db_config = get_database_config()
```

---

## 📊 Monitoring & Observability

### Optional Monitoring Stack

```bash
# Start with monitoring
docker compose --profile monitoring up --build
```

- **Prometheus** (http://localhost:9090) - Metrics collection
- **Grafana** (http://localhost:3000) - Visualization dashboards

### Key Metrics

- Message processing rates
- Telegraph link success/failure rates
- Database performance metrics
- Pipeline execution times
- Error rates and retry counts

---

## 🚀 Performance Optimizations

### Database Optimizations

- **Connection pooling** with configurable pool size
- **Batch processing** for large datasets
- **Indexed queries** for better performance
- **Upsert operations** to handle duplicates efficiently

### Network Optimizations

- **Async processing** for Telegraph requests
- **Rate limiting** to respect API limits
- **Retry logic** with exponential backoff
- **Timeout handling** for failed requests

### Memory Optimizations

- **Streaming processing** for large message batches
- **Content size limits** to prevent memory issues
- **Resource limits** in Docker containers

---

## 🔒 Security Features

- **Environment-based** configuration (no hardcoded secrets)
- **Connection encryption** for database connections
- **Rate limiting** to prevent API abuse
- **Input validation** for all external data
- **Error sanitization** to prevent information leakage

---

## 🐛 Troubleshooting

### Common Issues

1. **Telegram Authentication**

   ```bash
   # Generate session string
   python -c "from telethon.sync import TelegramClient; print(TelegramClient('session', api_id, api_hash).start().session.save())"
   ```

2. **Database Connection**

   ```bash
   # Test connection
   docker compose exec airflow-webserver airflow db check
   ```

3. **DAG Import Errors**
   ```bash
   # Restart webserver
   docker compose restart airflow-webserver
   ```

### Logs

```bash
# View logs
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler

# Check specific task logs
docker compose exec airflow-webserver airflow tasks logs pasta_pipeline scrape_telegram_messages latest
```

---

## 🔄 Development

### Hot Reload

```bash
# Restart services after code changes
docker compose restart airflow-webserver airflow-scheduler
```

### Testing

```bash
# Run tests (if implemented)
docker compose exec airflow-webserver python -m pytest tests/

# Validate DAGs
docker compose exec airflow-webserver airflow dags test pasta_pipeline
```

---

## 📈 Scaling Considerations

- **Horizontal scaling** with Celery executor
- **Database sharding** for large datasets
- **CDN integration** for static assets
- **Load balancing** for multiple Airflow instances

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

---

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---
