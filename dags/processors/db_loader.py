# dags/processors/db_loader.py
import json
import logging
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
import os

logger = logging.getLogger(__name__)


def get_sql_connection_string():
    return os.getenv('BUSINESS_SQL_CONN')


def create_tables(**context):
    """Create tables in SQL Server database"""
    connection_string = get_sql_connection_string()
    engine = create_engine(connection_string)

    try:
        with engine.connect() as conn:
            # Create telegram_messages table
            telegram_messages_query = text("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'telegram_messages')
            CREATE TABLE telegram_messages (
                message_id BIGINT PRIMARY KEY,
                date DATETIME2,
                text NVARCHAR(MAX),
                views INT,
                forwards INT,
                hashtags NVARCHAR(MAX),
                telegraph_link NVARCHAR(MAX),
                reactions NVARCHAR(MAX)
            );
            """)
            conn.execute(telegram_messages_query)

            # Create telegraph_content table
            telegraph_content_query = text("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'telegraph_content')
            CREATE TABLE telegraph_content (
                url NVARCHAR(MAX) PRIMARY KEY,
                title NVARCHAR(MAX),
                content NVARCHAR(MAX),
                description NVARCHAR(MAX),
                author NVARCHAR(MAX),
                image_url NVARCHAR(MAX),
                source_url NVARCHAR(MAX),
                date_published DATETIME2,
                processed_at DATETIME2
            );
            """)
            conn.execute(telegraph_content_query)

            conn.commit()
            logger.info("Database tables created successfully")
            return {"status": "success"}
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise


def load_messages_to_db(**context):
    """Load messages to SQL Server database"""
    logger.info("Starting to load messages to database")
    ti = context['ti']
    scrape_result = ti.xcom_pull(task_ids='scrape_telegram_messages')

    if not scrape_result:
        logger.warning("No scrape results found")
        return {"message_count": 0}

    json_file = scrape_result['json_file']

    try:
        df = pd.read_json(json_file)
        df = df.replace({np.nan: None})

        connection_string = get_sql_connection_string()
        engine = create_engine(connection_string)

        with engine.connect() as conn:
            for _, row in df.iterrows():
                # Handle telegraph_link
                if isinstance(row['telegraph_link'], (list, pd.Series, np.ndarray)):
                    telegraph_link = row['telegraph_link'][0] if len(row['telegraph_link']) > 0 else None
                else:
                    telegraph_link = row['telegraph_link'] if not pd.isna(row['telegraph_link']) else None

                # Handle hashtags - convert to JSON string for SQL Server
                hashtags = json.dumps(row['hashtags']) if row['hashtags'] else None

                # Handle reactions
                reactions = row.get('reactions', None)
                if isinstance(reactions, dict):
                    reactions = {k: None if isinstance(v, float) and np.isnan(v) else v
                                 for k, v in reactions.items()}

                reactions_json = json.dumps(reactions) if reactions else None

                # Use MERGE for upsert in SQL Server
                upsert_query = text("""
                MERGE telegram_messages AS target
                USING (VALUES (:message_id, :date, :text, :views, :forwards, :hashtags, :telegraph_link, :reactions)) 
                AS source (message_id, date, text, views, forwards, hashtags, telegraph_link, reactions)
                ON target.message_id = source.message_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        date = source.date,
                        text = source.text,
                        views = source.views,
                        forwards = source.forwards,
                        hashtags = source.hashtags,
                        telegraph_link = source.telegraph_link,
                        reactions = source.reactions
                WHEN NOT MATCHED THEN
                    INSERT (message_id, date, text, views, forwards, hashtags, telegraph_link, reactions)
                    VALUES (source.message_id, source.date, source.text, source.views, source.forwards, source.hashtags, source.telegraph_link, source.reactions);
                """)

                conn.execute(upsert_query, {
                    'message_id': row['message_id'],
                    'date': row['date'],
                    'text': row['text'],
                    'views': row['views'],
                    'forwards': row['forwards'],
                    'hashtags': hashtags,
                    'telegraph_link': telegraph_link,
                    'reactions': reactions_json
                })

            conn.commit()
            logger.info(f"Successfully loaded {len(df)} messages to database")
            return {"message_count": len(df)}
    except Exception as e:
        logger.error(f"Error loading messages to database: {str(e)}")
        raise
