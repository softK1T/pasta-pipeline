# Pasta Pipeline: Telegram to Telegraph ETL with Airflow

An automated data pipeline that extracts creepypasta stories from the Telegram channel @mrakopedia, processes Telegraph links, and stores content in a PostgreSQL database using Apache Airflow.

## Overview

This pipeline scrapes messages from the Mrakopedia Telegram channel, extracts Telegraph links containing creepypasta stories, and processes them to build a searchable database of horror stories.

## Features

- Scrapes messages from Telegram channels using Telethon
- Extracts Telegraph links and hashtags using regex patterns
- Stores message data in PostgreSQL with proper error handling
- Processes Telegraph content with dynamic task mapping
- Implements proper data cleaning and transformation


## Pipeline Flow

```
Telegram Channel → Message Scraping → Database Storage → Link Extraction → Content Processing
```

1. **Extract**: Scrapes recent messages from Telegram channel
2. **Load**: Stores message data in PostgreSQL database
3. **Transform**: Extracts and cleans Telegraph links
4. **Process**: Fetches content from each Telegraph link in parallel

## Technical Stack

- **Apache Airflow**: Workflow orchestration
- **PostgreSQL**: Data storage
- **Telethon**: Telegram API integration
- **BeautifulSoup**: HTML parsing
- **Pandas**: Data manipulation
- **aiohttp**: Asynchronous HTTP requests


## Project Structure

```
pasta-pipeline/
├── dags/
│   ├── dag.py                    # Main Airflow DAG definition
│   ├── processors/
│   │   ├── message_scraper.py    # Telegram message scraping
│   │   ├── db_loader.py          # Database operations
│   │   └── telegraph_processor.py # Telegraph content processing
│   └── utils/
│       └── utils.py              # Utility functions for extraction
```


## Setup Instructions

1. Clone the repository
2. Create a `.env` file with your Telegram credentials:

```
API_ID=your_telegram_api_id
API_HASH=your_telegram_api_hash
PHONE_NUMBER=your_phone_number
SESSION_STRING=your_telethon_session_string
```

3. Configure your PostgreSQL connection in Airflow
4. Run the pipeline using Airflow

## Database Schema

### telegram_messages

- `message_id`: Unique identifier for the message (Primary Key)
- `date`: Timestamp of the message
- `text`: Message content
- `views`: Number of views
- `forwards`: Number of forwards
- `hashtags`: Array of hashtags
- `telegraph_link`: Link to Telegraph page
- `reactions`: JSON object with emoji reactions


### telegraph_content

- `url`: Telegraph URL (Primary Key)
- `title`: Title of the article
- `content`: Full HTML content
- `date_published`: Publication date
- `processed_at`: Processing timestamp


## Usage

The pipeline is scheduled to run daily, collecting new stories from the Mrakopedia channel. You can also trigger it manually from the Airflow UI.
