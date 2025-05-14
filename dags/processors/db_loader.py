import json

import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_messages_to_db(**context):
    ti = context['ti']
    scrape_result = ti.xcom_pull(task_ids='scrape_telegram_messages')
    json_file = scrape_result['json_file']

    df = pd.read_json(json_file)
    df = df.replace({np.nan: None})

    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS telegram_messages (
    message_id BIGINT PRIMARY KEY,
    date TIMESTAMP,
    text TEXT,
    views INTEGER,
    forwards INTEGER,
    hashtags TEXT[],
    telegraph_link TEXT,
    reactions JSONB
    );
    """

    with conn.cursor() as cur:
        cur.execute(create_table_query)

        for _, row in df.iterrows():
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

            upsert_query = """
            INSERT INTO telegram_messages 
                (message_id, date, text, views, forwards, hashtags, telegraph_link, reactions)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (message_id) DO UPDATE SET
                date=EXCLUDED.date,
                text=EXCLUDED.text,
                views=EXCLUDED.views,
                forwards=EXCLUDED.forwards,
                hashtags=EXCLUDED.hashtags,
                telegraph_link=EXCLUDED.telegraph_link,
                reactions=EXCLUDED.reactions;                       
            """

            cur.execute(upsert_query, (
                row['message_id'],
                row['date'],
                row['text'],
                row['views'],
                row['forwards'],
                hashtags,
                telegraph_link,
                reactions_json
            ))

            conn.commit()

        return {"message_count": len(df)}