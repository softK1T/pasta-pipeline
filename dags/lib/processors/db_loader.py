import json
import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_SIZE = 100
MAX_RETRIES = 3


def create_tables_if_not_exist(conn) -> None:
    """Create database tables if they don't exist."""
    try:
        with conn.cursor() as cur:
            # Create telegram_messages table
            create_telegram_table_query = """
                                          CREATE TABLE IF NOT EXISTS telegram_messages
                                          (
                                              message_id     BIGINT PRIMARY KEY,
                                              date           TIMESTAMP,
                                              text           TEXT,
                                              views          INTEGER,
                                              forwards       INTEGER,
                                              hashtags       TEXT[],
                                              telegraph_link TEXT,
                                              reactions      JSONB,
                                              scraped_at     TIMESTAMP,
                                              processed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                          ); \
                                          """
            cur.execute(create_telegram_table_query)

            # Create index for better query performance
            create_index_query = """
                                 CREATE INDEX IF NOT EXISTS idx_telegram_messages_date
                                     ON telegram_messages (date); \
                                 """
            cur.execute(create_index_query)

            conn.commit()
            logger.info("Database tables created/verified successfully")

    except SQLAlchemyError as e:
        logger.error(f"Database error creating tables: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating tables: {str(e)}")
        raise


def process_telegraph_link(telegraph_link) -> Optional[str]:
    """Process and clean telegraph link."""
    if pd.isna(telegraph_link) or not telegraph_link:
        return None

    if isinstance(telegraph_link, (list, pd.Series, np.ndarray)):
        if len(telegraph_link) > 0:
            link = telegraph_link[0]
        else:
            return None
    else:
        link = telegraph_link

    # Clean the link
    if isinstance(link, str):
        link = link.strip()
        if link.startswith('{') and link.endswith('}'):
            link = link[1:-1]
        if link and link != '{}':
            return link

    return None


def process_hashtags(hashtags) -> Optional[str]:
    """Process hashtags into PostgreSQL array format."""
    if not hashtags or pd.isna(hashtags):
        return None

    try:
        if isinstance(hashtags, list):
            if not hashtags:
                return None
            return "{" + ",".join(f'"{tag}"' for tag in hashtags) + "}"
        else:
            return None
    except Exception as e:
        logger.warning(f"Error processing hashtags: {str(e)}")
        return None


def process_reactions(reactions) -> Optional[str]:
    """Process reactions into JSON format."""
    if pd.isna(reactions) or not reactions:
        return None

    try:
        if isinstance(reactions, dict):
            # Clean NaN values from reactions
            cleaned_reactions = {
                k: None if isinstance(v, float) and np.isnan(v) else v
                for k, v in reactions.items()
            }
            return json.dumps(cleaned_reactions)
        return None
    except Exception as e:
        logger.warning(f"Error processing reactions: {str(e)}")
        return None


def load_messages_to_db(**context) -> Dict:
    """Load scraped messages to database with improved error handling and batch processing."""
    logger.info("Starting database load process")

    try:
        ti = context['ti']
        scrape_result = ti.xcom_pull(task_ids='scrape_telegram_messages')

        if not scrape_result:
            raise ValueError("No scrape result found from previous task")

        json_file = scrape_result['json_file']
        logger.info(f"Loading data from: {json_file}")

        # Load and validate data
        df = pd.read_json(json_file)
        df = df.replace({np.nan: None})

        if df.empty:
            logger.warning("No data to load")
            return {"message_count": 0, "status": "no_data"}

        logger.info(f"Loaded {len(df)} messages for processing")

        # Initialize database connection
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = postgres_hook.get_conn()

        # Create tables if they don't exist
        create_tables_if_not_exist(conn)

        # Process data in batches
        successful_inserts = 0
        failed_inserts = 0

        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            logger.info(f"Processing batch {i // BATCH_SIZE + 1}/{(len(df) + BATCH_SIZE - 1) // BATCH_SIZE}")

            try:
                with conn.cursor() as cur:
                    for _, row in batch.iterrows():
                        try:
                            # Process data fields
                            telegraph_link = process_telegraph_link(row['telegraph_link'])
                            hashtags = process_hashtags(row['hashtags'])
                            reactions = process_reactions(row.get('reactions'))

                            # Prepare upsert query
                            upsert_query = """
                                           INSERT INTO telegram_messages
                                           (message_id, date, text, views, forwards, hashtags,
                                            telegraph_link, reactions, scraped_at, processed_at)
                                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                                           ON CONFLICT (message_id) DO UPDATE SET date           = EXCLUDED.date,
                                                                                  text           = EXCLUDED.text,
                                                                                  views          = EXCLUDED.views,
                                                                                  forwards       = EXCLUDED.forwards,
                                                                                  hashtags       = EXCLUDED.hashtags,
                                                                                  telegraph_link = EXCLUDED.telegraph_link,
                                                                                  reactions      = EXCLUDED.reactions,
                                                                                  scraped_at     = EXCLUDED.scraped_at,
                                                                                  processed_at   = CURRENT_TIMESTAMP; \
                                           """

                            cur.execute(upsert_query, (
                                row['message_id'],
                                row['date'],
                                row['text'],
                                row['views'],
                                row['forwards'],
                                hashtags,
                                telegraph_link,
                                reactions,
                                row.get('scraped_at')
                            ))

                            successful_inserts += 1

                        except Exception as e:
                            logger.error(f"Error processing message {row.get('message_id', 'unknown')}: {str(e)}")
                            failed_inserts += 1
                            continue

                    # Commit batch
                    conn.commit()
                    logger.info(f"Batch {i // BATCH_SIZE + 1} committed successfully")

            except SQLAlchemyError as e:
                logger.error(f"Database error in batch {i // BATCH_SIZE + 1}: {str(e)}")
                conn.rollback()
                failed_inserts += len(batch)
                continue
            except Exception as e:
                logger.error(f"Unexpected error in batch {i // BATCH_SIZE + 1}: {str(e)}")
                conn.rollback()
                failed_inserts += len(batch)
                continue

        logger.info(f"Database load completed. Successful: {successful_inserts}, Failed: {failed_inserts}")

        return {
            "message_count": successful_inserts,
            "failed_count": failed_inserts,
            "total_processed": len(df),
            "status": "completed"
        }

    except Exception as e:
        logger.error(f"Critical error in load_messages_to_db: {str(e)}")
        raise
