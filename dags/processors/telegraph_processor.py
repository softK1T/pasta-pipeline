import asyncio
from datetime import datetime

import aiohttp
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bs4 import BeautifulSoup
from sqlalchemy.databases import postgres
from urllib.parse import unquote

def get_links_to_process(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')

    conn = hook.get_conn()
    with conn.cursor() as cur:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS telegraph_content (
            url TEXT PRIMARY KEY,
            title TEXT,
            content TEXT,
            date_published TIMESTAMP,
            processed_at TIMESTAMP
        );
        """
        cur.execute(create_table_query)
        conn.commit()

    select_query = """
    SELECT DISTINCT telegraph_link
    FROM telegram_messages
    WHERE telegraph_link IS NOT NULL 
    AND telegraph_link NOT IN (SELECT url FROM telegraph_content)
    """

    links = hook.get_records(select_query)
    clean_links = []
    for link_tuple in links:
        link = link_tuple[0]
        if not link or link == '{}' or link == '':
            continue
        # Remove curly braces if present
        if link.startswith('{') and link.endswith('}'):
            link = link[1:-1]
        if link.startswith('https://telegra.ph/'):
            clean_links.append([link])
    print("Links to process:", clean_links)
    return clean_links


async def process_telegraph_link(link, **context):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(link, timeout=30) as response:
                if response.status != 200:
                    return None

                html_content = await response.text()

        soup = BeautifulSoup(html_content, 'html.parser')

        title = None
        header = soup.find('header', class_='tl_article_header')
        if header and header.find('h1'):
            title = header.find('h1').text.strip()
        elif soup.title:
            title = soup.title.text.strip()
            title = title.replace(" – Telegraph", "")

        description = None
        meta_description = soup.find('meta', property='twitter:description')
        if meta_description:
            description = meta_description.get('content', '').strip()
            # Remove URL prefix if present
            if description.startswith('https://mrakopedia.net/wiki/'):
                pasta_name = unquote(description.split('/wiki/')[1].split()[0])
                description = description[len(f'https://mrakopedia.net/wiki/{pasta_name}'):].strip()

        date_published = soup.find('meta', property='article:published_time')
        if date_published:
            published_date = date_published.get('content', '').strip()


        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()

        with conn.cursor() as cur:
            insert_query = """
            INSERT INTO telegraph_content (url, title, content, date_published, processed_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (url) DO UPDATE SET
                title = EXCLUDED.title,
                content = EXCLUDED.content,
                date_published = EXCLUDED.date_published,
                processed_at = EXCLUDED.processed_at;
            """

            cur.execute(insert_query, (
                link,
                title,
                html_content,
                published_date,
                datetime.now()
            ))

            conn.commit()

        return {"processed:": True, "url": link}

    except Exception as e:
        print("error:", str(e))
        raise

def process_telegraph_link_sync(link, **context):
    print(link)
    return asyncio.get_event_loop().run_until_complete(process_telegraph_link(link, **context))
