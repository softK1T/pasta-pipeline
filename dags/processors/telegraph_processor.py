# dags/processors/telegraph_processor.py
import asyncio
import logging
from datetime import datetime
import aiohttp
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
from urllib.parse import unquote
import os

logger = logging.getLogger(__name__)

def get_sql_connection_string():
    return os.getenv('BUSINESS_SQL_CONN')

def get_links_to_process(**context):
    """Get Telegraph links to process from SQL Server"""
    logger.info("Getting Telegraph links to process")

    connection_string = get_sql_connection_string()
    engine = create_engine(connection_string)

    try:
        with engine.connect() as conn:
            select_query = text("""
            SELECT DISTINCT telegraph_link
            FROM telegram_messages
            WHERE telegraph_link IS NOT NULL
            AND telegraph_link NOT IN (SELECT url FROM telegraph_content)
            AND telegraph_link NOT LIKE '%telegra.ph/file/%'
            """)

            result = conn.execute(select_query)
            links = result.fetchall()

        clean_links = []
        for link_tuple in links:
            link = link_tuple[0]
            if not link or link == '{}' or link == '':
                continue

            if isinstance(link, str) and link.startswith('{') and link.endswith('}'):
                link = link[1:-1]

            if isinstance(link, str) and link.startswith('https://telegra.ph/'):
                clean_links.append([link])

        logger.info(f"Found {len(clean_links)} links to process")
        return clean_links

    except Exception as e:
        logger.error(f"Error getting links to process: {str(e)}")
        raise


async def process_telegraph_link(link, **context):
    """Process Telegraph link and save to SQL Server"""
    logger.info(f"Processing link: {link}")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(link, timeout=30) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch {link}: HTTP {response.status}")
                    return {"processed": False, "url": link, "error": f"HTTP {response.status}"}

                html_content = await response.text()

        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract title
        title = None
        header = soup.find('header', class_='tl_article_header')
        if header and header.find('h1'):
            title = header.find('h1').text.strip()
        elif soup.title:
            title = soup.title.text.strip()
            title = title.replace(" – Telegraph", "")

        # Extract description and source URL
        description = None
        source_url = None
        meta_description = soup.find('meta', property='og:description')
        if meta_description:
            description = meta_description.get('content', '').strip()
            if description and description.startswith('https://mrakopedia.net/wiki/'):
                source_url = description.split('\n')[0].strip()
                pasta_name = unquote(description.split('/wiki/')[1].split()[0])
                description = description[len(f'https://mrakopedia.net/wiki/{pasta_name}'):].strip()

        # Extract author
        author = None
        meta_author_list = soup.find_all('meta', property='article:author')
        if meta_author_list:
            meta_author = meta_author_list[0]
            author = meta_author.get('content', '').strip()

        # Extract image URL
        image_url = None
        meta_image = soup.find('meta', property='og:image')
        if meta_image:
            image_url = meta_image.get('content', '').strip()

        # Extract published date
        published_date = None
        date_meta = soup.find('meta', property='article:published_time')
        if date_meta:
            published_date = date_meta.get('content', '').strip()

        # Save to database
        connection_string = get_sql_connection_string()
        engine = create_engine(connection_string)

        with engine.connect() as conn:
            insert_query = text("""
            MERGE telegraph_content AS target
            USING (VALUES (:url, :title, :content, :description, :author, :image_url, :source_url, :date_published, :processed_at)) 
            AS source (url, title, content, description, author, image_url, source_url, date_published, processed_at)
            ON target.url = source.url
            WHEN MATCHED THEN
                UPDATE SET 
                    title = source.title,
                    content = source.content,
                    description = source.description,
                    author = source.author,
                    date_published = source.date_published,
                    processed_at = source.processed_at
            WHEN NOT MATCHED THEN
                INSERT (url, title, content, description, author, image_url, source_url, date_published, processed_at)
                VALUES (source.url, source.title, source.content, source.description, source.author, source.image_url, source.source_url, source.date_published, source.processed_at);
            """)

            conn.execute(insert_query, {
                'url': link,
                'title': title,
                'content': html_content,
                'description': description,
                'author': author,
                'image_url': image_url,
                'source_url': source_url,
                'date_published': published_date,
                'processed_at': datetime.now()
            })
            conn.commit()

        logger.info(f"Successfully processed: {link}")
        return {"processed": True, "url": link, "title": title}

    except Exception as e:
        logger.error(f"Error processing {link}: {str(e)}")
        return {"processed": False, "url": link, "error": str(e)}


def process_telegraph_link_sync(link, **context):
    """Synchronous wrapper for processing Telegraph links"""
    logger.info(f"Processing link: {link}")
    try:
        result = asyncio.get_event_loop().run_until_complete(process_telegraph_link(link, **context))
        return result
    except Exception as e:
        logger.error(f"Error in process_telegraph_link_sync: {str(e)}")
        return {"processed": False, "url": link, "error": str(e)}
