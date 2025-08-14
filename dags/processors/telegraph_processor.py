import asyncio
import logging
import hashlib
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bs4 import BeautifulSoup
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import unquote, urlparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration constants
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 1  # seconds between requests
MAX_CONTENT_LENGTH = 1000000  # 1MB limit for content


def create_telegraph_table_if_not_exists(conn) -> None:
    """Create telegraph_content table if it doesn't exist."""
    try:
        with conn.cursor() as cur:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS telegraph_content (
                url TEXT PRIMARY KEY,
                title TEXT,
                content TEXT,
                description TEXT,
                content_hash TEXT,
                description_hash TEXT,
                date_published TIMESTAMP,
                word_count INTEGER,
                status TEXT DEFAULT 'success',
                retry_count INTEGER DEFAULT 0,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cur.execute(create_table_query)
            
            # Create indexes for better performance
            create_indexes = [
                "CREATE INDEX IF NOT EXISTS idx_telegraph_content_hash ON telegraph_content(content_hash);",
                "CREATE INDEX IF NOT EXISTS idx_telegraph_status ON telegraph_content(status);",
                "CREATE INDEX IF NOT EXISTS idx_telegraph_date ON telegraph_content(date_published);"
            ]
            
            for index_query in create_indexes:
                cur.execute(index_query)
            
            conn.commit()
            logger.info("Telegraph content table created/verified successfully")
            
    except SQLAlchemyError as e:
        logger.error(f"Database error creating telegraph table: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating telegraph table: {str(e)}")
        raise


def get_links_to_process(**context) -> List[List[str]]:
    """Get unprocessed Telegraph links from the database."""
    logger.info("Fetching unprocessed Telegraph links")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        
        # Ensure table exists
        create_telegraph_table_if_not_exists(conn)
        
        # Query for unprocessed links
        select_query = """
        SELECT DISTINCT telegraph_link
        FROM telegram_messages
        WHERE telegraph_link IS NOT NULL 
        AND telegraph_link != ''
        AND telegraph_link NOT IN (
            SELECT url FROM telegraph_content 
            WHERE status = 'success'
        )
        LIMIT 1000;
        """
        
        links = hook.get_records(select_query)
        clean_links = []
        
        for link_tuple in links:
            link = link_tuple[0]
            if not link or link == '{}' or link == '':
                continue
            
            # Clean the link
            if link.startswith('{') and link.endswith('}'):
                link = link[1:-1]
            
            # Validate Telegraph URL
            if link.startswith('https://telegra.ph/'):
                clean_links.append([link])
        
        logger.info(f"Found {len(clean_links)} links to process")
        return clean_links
        
    except Exception as e:
        logger.error(f"Error fetching links to process: {str(e)}")
        raise


def validate_telegraph_url(url: str) -> bool:
    """Validate if URL is a proper Telegraph URL."""
    try:
        parsed = urlparse(url)
        return (parsed.scheme == 'https' and 
                parsed.netloc == 'telegra.ph' and 
                len(parsed.path) > 1)
    except Exception:
        return False


def extract_content_hash(content: str) -> str:
    """Generate hash for content to detect changes."""
    if not content:
        return ""
    return hashlib.md5(content.encode('utf-8')).hexdigest()


def extract_description_hash(description: str) -> str:
    """Generate hash for description to detect changes."""
    if not description:
        return ""
    return hashlib.md5(description.encode('utf-8')).hexdigest()


def count_words(text: str) -> int:
    """Count words in text."""
    if not text:
        return 0
    return len(text.split())


async def process_telegraph_link(link: str, **context) -> Optional[Dict]:
    """Process a single Telegraph link with improved error handling and retry logic."""
    if not validate_telegraph_url(link):
        logger.warning(f"Invalid Telegraph URL: {link}")
        return None
    
    logger.info(f"Processing link: {link}")
    
    for attempt in range(MAX_RETRIES):
        try:
            # Rate limiting
            if attempt > 0:
                await asyncio.sleep(RATE_LIMIT_DELAY * attempt)
            
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            ) as session:
                async with session.get(link) as response:
                    if response.status != 200:
                        logger.warning(f"HTTP {response.status} for {link}")
                        if response.status == 404:
                            await update_link_status(link, 'not_found')
                            return None
                        elif response.status >= 500:
                            if attempt < MAX_RETRIES - 1:
                                continue
                            else:
                                await update_link_status(link, 'server_error')
                                return None
                        else:
                            await update_link_status(link, 'http_error')
                            return None
                    
                    html_content = await response.text()
                    
                    # Check content length
                    if len(html_content) > MAX_CONTENT_LENGTH:
                        logger.warning(f"Content too large for {link}: {len(html_content)} bytes")
                        await update_link_status(link, 'content_too_large')
                        return None
                    
                    # Parse content
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # Extract title
                    title = None
                    header = soup.find('header', class_='tl_article_header')
                    if header and header.find('h1'):
                        title = header.find('h1').text.strip()
                    elif soup.title:
                        title = soup.title.text.strip()
                        title = title.replace(" – Telegraph", "")
                    
                    # Extract description
                    description = None
                    meta_description = soup.find('meta', property='twitter:description')
                    if meta_description:
                        description = meta_description.get('content', '').strip()
                        # Remove URL prefix if present
                        if description.startswith('https://mrakopedia.net/wiki/'):
                            try:
                                pasta_name = unquote(description.split('/wiki/')[1].split()[0])
                                description = description[len(f'https://mrakopedia.net/wiki/{pasta_name}'):].strip()
                            except (IndexError, Exception) as e:
                                logger.warning(f"Error processing description for {link}: {str(e)}")
                    
                    # Extract publication date
                    date_published = None
                    date_meta = soup.find('meta', property='article:published_time')
                    if date_meta:
                        try:
                            date_str = date_meta.get('content', '').strip()
                            if date_str:
                                date_published = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        except Exception as e:
                            logger.warning(f"Error parsing date for {link}: {str(e)}")
                    
                    # Generate hashes
                    content_hash = extract_content_hash(html_content)
                    description_hash = extract_description_hash(description) if description else ""
                    word_count = count_words(html_content)
                    
                    # Save to database
                    await save_telegraph_content(
                        link, title, html_content, description, 
                        content_hash, description_hash, date_published, word_count
                    )
                    
                    logger.info(f"Successfully processed {link}")
                    return {"processed": True, "url": link, "title": title}
                    
        except asyncio.TimeoutError:
            logger.warning(f"Timeout processing {link} (attempt {attempt + 1})")
            if attempt == MAX_RETRIES - 1:
                await update_link_status(link, 'timeout')
                return None
        except aiohttp.ClientError as e:
            logger.warning(f"Client error processing {link} (attempt {attempt + 1}): {str(e)}")
            if attempt == MAX_RETRIES - 1:
                await update_link_status(link, 'client_error')
                return None
        except Exception as e:
            logger.error(f"Unexpected error processing {link} (attempt {attempt + 1}): {str(e)}")
            if attempt == MAX_RETRIES - 1:
                await update_link_status(link, 'error')
                return None
    
    return None


async def save_telegraph_content(
    url: str, title: str, content: str, description: str,
    content_hash: str, description_hash: str, date_published: datetime, word_count: int
) -> None:
    """Save telegraph content to database."""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        
        with conn.cursor() as cur:
            insert_query = """
            INSERT INTO telegraph_content 
                (url, title, content, description, content_hash, description_hash, 
                 date_published, word_count, status, processed_at, last_checked)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO UPDATE SET
                title = EXCLUDED.title,
                content = EXCLUDED.content,
                description = EXCLUDED.description,
                content_hash = EXCLUDED.content_hash,
                description_hash = EXCLUDED.description_hash,
                date_published = EXCLUDED.date_published,
                word_count = EXCLUDED.word_count,
                status = 'success',
                retry_count = 0,
                processed_at = EXCLUDED.processed_at,
                last_checked = EXCLUDED.last_checked;
            """
            
            cur.execute(insert_query, (
                url, title, content, description, content_hash, description_hash,
                date_published, word_count, 'success', 
                datetime.now(timezone.utc), datetime.now(timezone.utc)
            ))
            
            conn.commit()
            
    except Exception as e:
        logger.error(f"Error saving telegraph content for {url}: {str(e)}")
        raise


async def update_link_status(url: str, status: str) -> None:
    """Update link status in database."""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        
        with conn.cursor() as cur:
            update_query = """
            INSERT INTO telegraph_content (url, status, retry_count, last_checked)
            VALUES (%s, %s, 1, %s)
            ON CONFLICT (url) DO UPDATE SET
                status = EXCLUDED.status,
                retry_count = telegraph_content.retry_count + 1,
                last_checked = EXCLUDED.last_checked;
            """
            
            cur.execute(update_query, (url, status, datetime.now(timezone.utc)))
            conn.commit()
            
    except Exception as e:
        logger.error(f"Error updating status for {url}: {str(e)}")


def process_telegraph_link_sync(link: str, **context) -> Optional[Dict]:
    """Synchronous wrapper for process_telegraph_link."""
    try:
        return asyncio.get_event_loop().run_until_complete(
            process_telegraph_link(link, **context)
        )
    except Exception as e:
        logger.error(f"Error in sync wrapper for {link}: {str(e)}")
        return None
