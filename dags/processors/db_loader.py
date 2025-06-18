import json
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from airflow.providers.postgres.hooks.postgres import PostgresHook
from contextlib import contextmanager


@contextmanager
def get_postgres_connection():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = None
    try:
        conn = postgres_hook.get_conn()
        yield conn, postgres_hook
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def load_messages_to_db(**context):
    ti = context['ti']
    scrape_result = ti.xcom_pull(task_ids='scrape_telegram_messages')
    json_file = scrape_result['json_file']

    processing_started_at = datetime.now(timezone.utc)
    print(f"Database processing started at: {processing_started_at.isoformat()}")

    df = pd.read_json(json_file)
    df = df.replace({np.nan: None})

    with get_postgres_connection() as (conn, postgres_hook):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS telegram_messages (
            message_id BIGINT PRIMARY KEY,
            date TIMESTAMP,
            text TEXT,
            views INTEGER,
            forwards INTEGER,
            hashtags TEXT[],
            telegraph_link TEXT,
            reactions JSONB,
            scraped_at TIMESTAMP DEFAULT NOW(),
            processed_at TIMESTAMP DEFAULT NOW(),
            last_updated TIMESTAMP DEFAULT NOW()
        );
        """

        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()

            # Batch insert for better performance
            batch_size = 100
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]

                for _, row in batch.iterrows():
                    # Process row data
                    if isinstance(row['telegraph_link'], (list, pd.Series, np.ndarray)):
                        telegraph_link = row['telegraph_link'][0] if len(row['telegraph_link']) > 0 else None
                    else:
                        telegraph_link = row['telegraph_link'] if not pd.isna(row['telegraph_link']) else None

                    hashtags = "{" + ",".join(f'"{tag}"' for tag in row['hashtags']) + "}" if row['hashtags'] else None

                    reactions = row.get('reactions', None)
                    if isinstance(reactions, dict):
                        reactions = {k: None if isinstance(v, float) and np.isnan(v) else v
                                     for k, v in reactions.items()}

                    reactions_json = json.dumps(reactions) if reactions else None

                    # Handle timestamp fields
                    scraped_at = row.get('scraped_at', processing_started_at.isoformat())
                    processed_at = processing_started_at.isoformat()
                    last_updated = processing_started_at.isoformat()

                    upsert_query = """
                    INSERT INTO telegram_messages 
                        (message_id, date, text, views, forwards, hashtags, telegraph_link, reactions, 
                         scraped_at, processed_at, last_updated)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (message_id) DO UPDATE SET
                        date=EXCLUDED.date,
                        text=EXCLUDED.text,
                        views=EXCLUDED.views,
                        forwards=EXCLUDED.forwards,
                        hashtags=EXCLUDED.hashtags,
                        telegraph_link=EXCLUDED.telegraph_link,
                        reactions=EXCLUDED.reactions,
                        processed_at=EXCLUDED.processed_at,
                        last_updated=EXCLUDED.last_updated;                       
                    """

                    cur.execute(upsert_query, (
                        row['message_id'],
                        row['date'],
                        row['text'],
                        row['views'],
                        row['forwards'],
                        hashtags,
                        telegraph_link,
                        reactions_json,
                        scraped_at,
                        processed_at,
                        last_updated
                    ))

                conn.commit()
                print(f"Processed batch {i // batch_size + 1}/{(len(df) - 1) // batch_size + 1}")

    processing_completed_at = datetime.now(timezone.utc)
    print(f"Database processing completed at: {processing_completed_at.isoformat()}")
    print(f"Total processing time: {processing_completed_at - processing_started_at}")

    return {
        "message_count": len(df),
        "processing_started_at": processing_started_at.isoformat(),
        "processing_completed_at": processing_completed_at.isoformat(),
        "total_processing_time": str(processing_completed_at - processing_started_at)
    }
