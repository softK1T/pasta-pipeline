from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from contextlib import contextmanager
import time

import aiohttp
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bs4 import BeautifulSoup
from urllib.parse import unquote

_LINK_OUTPUT_DIR = "/opt/airflow/data/links"
_ALLOWED_IMG_EXT = (".jpg", ".jpeg", ".png", ".gif")
_TELEGRAPH_PREFIX = "https://telegra.ph/"
_MRAKOPEDIA_PREFIX = "https://mrakopedia.net/wiki/"


# CONNECTION MANAGEMENT
@contextmanager
def get_postgres_hook_with_retry(max_retries: int = 3, retry_delay: int = 5):
    """Context manager with retry logic for database connections"""
    hook = None
    conn = None

    for attempt in range(max_retries):
        try:
            hook = PostgresHook(postgres_conn_id='postgres_default')
            conn = hook.get_conn()
            yield hook, conn
            break
        except Exception as e:
            print(f"Database connection attempt {attempt + 1} failed: {str(e)}")
            if conn:
                try:
                    conn.close()
                except:
                    pass

            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    # Cleanup in finally block
    try:
        if conn:
            conn.close()
    except Exception as e:
        print(f"Error closing connection: {str(e)}")


# PUBLIC TASK HELPERS
def get_links_to_process(**context) -> List[List[str]]:
    """Get links that need processing - returns list of lists for mapped task expansion"""
    params = context.get("params", {})
    mode: str = params.get("processing_mode", "incremental")
    days: int = params.get("reprocess_older_than_days", 30)
    limit: int = params.get("max_links_per_run", 0)  # Default 0 (no limit)

    with get_postgres_hook_with_retry() as (hook, conn):
        _ensure_content_table(hook, conn)
        query, q_params = _compose_selection_sql(mode, days, limit)
        rows = hook.get_records(query, parameters=q_params)

    links: List[str] = [
        _sanitize_link(row[0])
        for row in rows
        if _is_valid_link(row[0])
    ]

    _log_sample("Links to process", links)
    # Return as list of single-item lists for mapped task expansion
    return [[link] for link in links]


def process_telegraph_link_sync(link: str, **context) -> Dict[str, Any]:
    """Process a single telegraph link synchronously"""
    try:
        result = asyncio.get_event_loop().run_until_complete(
            _process_single_link(link)
        )
        return result if result else {}
    except Exception as e:
        print(f"Error processing link {link}: {str(e)}")
        return {
            "url": link,
            "processed": False,
            "error": str(e)
        }


# INTERNAL UTILITIES
def _ensure_content_table(hook: PostgresHook, conn) -> None:
    create_sql = """
        CREATE TABLE IF NOT EXISTS telegraph_content (
            url TEXT PRIMARY KEY,
            title TEXT,
            content TEXT,
            description TEXT,
            author TEXT,
            mrakopedia_link TEXT,
            date_published TIMESTAMP,
            processed_at TIMESTAMP,
            content_hash TEXT,
            description_hash TEXT,
            last_checked TIMESTAMP,
            word_count INTEGER,
            status VARCHAR(20) DEFAULT 'active',
            retry_count INTEGER DEFAULT 0,
            error_message TEXT
        );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise


def _compose_selection_sql(
    mode: str, days: int, limit: int
) -> tuple[str, List[Any]]:
    if mode == "incremental":
        sql = """
            SELECT telegraph_link
            FROM telegram_messages
            WHERE telegraph_link IS NOT NULL
              AND telegraph_link NOT IN (SELECT url FROM telegraph_content)
            ORDER BY telegraph_link
        """
        if limit and limit > 0:
            sql += " LIMIT %s"
            return sql, [limit]
        return sql, []

    if mode == "refresh_old":
        sql = """
            SELECT url
            FROM telegraph_content
            WHERE processed_at < NOW() - INTERVAL '%s days'
               OR status = 'error'
            ORDER BY processed_at ASC
        """
        if limit and limit > 0:
            sql += " LIMIT %s"
            return sql, [days, limit]
        return sql, [days]

    if mode == "full":
        sql = """
            SELECT telegraph_link
            FROM telegram_messages
            WHERE telegraph_link IS NOT NULL
            ORDER BY telegraph_link
        """
        return sql, []

    raise ValueError(f"Unknown processing mode: {mode}")


def _sanitize_link(link: str) -> str:
    return link[1:-1] if link.startswith("{") and link.endswith("}") else link


def _is_valid_link(link: str | None) -> bool:
    if not link:
        return False
    if any(link.lower().endswith(ext) for ext in _ALLOWED_IMG_EXT):
        return False
    return link.startswith(_TELEGRAPH_PREFIX) and "/file/" not in link


def _write_links_to_file(links: List[str]) -> str:
    os.makedirs(_LINK_OUTPUT_DIR, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = f"{_LINK_OUTPUT_DIR}/links_to_process_{ts}.json"
    with open(file_path, "w") as fp:
        json.dump(links, fp)
    return file_path


def _log_sample(label: str, items: List[str], n: int = 5) -> None:
    print(f"{label}: {len(items)}")
    print("Sample:", items[:n])


# ASYNC PAGE PROCESSOR
async def _process_single_link(link: str) -> Dict[str, Any]:
    start = datetime.utcnow()

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(link, timeout=120) as resp:
                if resp.status != 200:
                    return {"url": link, "processed": False, "error": f"HTTP {resp.status}"}
                if resp.headers.get("Content-Type", "").startswith("image/"):
                    return {"url": link, "processed": False, "reason": "image_file"}
                html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")

        title = _extract_title(soup)
        author = _extract_author(soup)
        description, mrakopedia_link = _extract_description_and_wiki(soup)
        published = _extract_published_date(soup)
        content_hash = hashlib.md5(html.encode()).hexdigest()
        description_hash = (
            hashlib.md5(description.encode()).hexdigest() if description else None
        )
        word_count = len(html.split())

        # Use context manager for database operations
        with get_postgres_hook_with_retry() as (hook, conn):
            _upsert_article(
                hook,
                conn,
                link,
                title,
                html,
                description,
                author,
                mrakopedia_link,
                published,
                content_hash,
                description_hash,
                word_count,
            )

        return {
            "url": link,
            "processed": True,
            "word_count": word_count,
            "elapsed": str(datetime.utcnow() - start),
        }

    except Exception as e:
        print(f"Error processing {link}: {str(e)}")
        return {
            "url": link,
            "processed": False,
            "error": str(e)
        }


# PARSING HELPERS
def _extract_title(soup: BeautifulSoup) -> Optional[str]:
    header = soup.find("header", class_="tl_article_header")
    if header and header.h1:
        return header.h1.text.strip()
    if soup.title:
        return soup.title.text.replace(" – Telegraph", "").strip()
    return None


def _extract_author(soup: BeautifulSoup) -> Optional[str]:
    """Extract author from telegraph page"""
    # Try to find author in meta tags
    meta_author = soup.find("meta", attrs={"name": "author"})
    if meta_author and meta_author.get("content"):
        return meta_author["content"].strip()
    
    # Try to find author in address tag (common in telegraph)
    address = soup.find("address")
    if address:
        return address.text.strip()
    
    # Try to find author link or span in header
    header = soup.find("header", class_="tl_article_header")
    if header:
        author_link = header.find("a")
        if author_link:
            return author_link.text.strip()
        author_span = header.find("span")
        if author_span:
            return author_span.text.strip()
    
    return None


def _extract_description_and_wiki(
        soup: BeautifulSoup,
) -> tuple[Optional[str], Optional[str]]:
    meta = (
            soup.find("meta", attrs={"name": "twitter:description"})
            or soup.find("meta", attrs={"name": "description"})
            or soup.find("meta", attrs={"property": "og:description"})
    )
    description = meta["content"].strip() if meta and meta.get("content") else None

    mrako_link: Optional[str] = None
    if description and description.startswith(_MRAKOPEDIA_PREFIX):
        mrako_link = description.split()[0]
        description = description[len(mrako_link):].lstrip()

    if not mrako_link:
        first_a = soup.find("article")
        if first_a:
            anchor = first_a.find("a", href=True)
            if anchor and anchor["href"].startswith(_MRAKOPEDIA_PREFIX):
                mrako_link = anchor["href"]

    return description, mrako_link


def _extract_published_date(soup: BeautifulSoup) -> Optional[str]:
    meta = soup.find("meta", attrs={"property": "article:published_time"})
    return meta["content"] if meta and meta.get("content") else None


# DATABASE UPSERT
def _upsert_article(
        hook: PostgresHook,
        conn,
        url: str,
        title: Optional[str],
        html: str,
        description: Optional[str],
        author: Optional[str],
        mrakopedia_link: Optional[str],
        date_pub: Optional[str],
        content_hash: str,
        description_hash: Optional[str],
        word_count: int,
) -> None:
    try:
        existing = hook.get_first(
            """
            SELECT content_hash, description_hash
            FROM telegraph_content
            WHERE url = %s
            """,
            parameters=[url],
        )

        hashes_identical = (
                existing
                and existing[0] == content_hash
                and existing[1] == description_hash
        )

        with conn.cursor() as cur:
            if hashes_identical:
                cur.execute(
                    "UPDATE telegraph_content SET last_checked = NOW() WHERE url = %s",
                    [url]
                )
            else:
                cur.execute(
                    """
                    INSERT INTO telegraph_content
                      (url, title, content, description, author, mrakopedia_link,
                       date_published, processed_at, content_hash,
                       description_hash, last_checked, word_count,
                       status, retry_count)
                    VALUES
                      (%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s,NOW(),%s,'active',0)
                    ON CONFLICT (url) DO UPDATE SET
                      title            = EXCLUDED.title,
                      content          = EXCLUDED.content,
                      description      = EXCLUDED.description,
                      author           = EXCLUDED.author,
                      mrakopedia_link  = EXCLUDED.mrakopedia_link,
                      date_published   = EXCLUDED.date_published,
                      processed_at     = EXCLUDED.processed_at,
                      content_hash     = EXCLUDED.content_hash,
                      description_hash = EXCLUDED.description_hash,
                      last_checked     = EXCLUDED.last_checked,
                      word_count       = EXCLUDED.word_count,
                      status           = 'active',
                      error_message    = NULL
                    """,
                    (
                        url,
                        title,
                        html,
                        description,
                        author,
                        mrakopedia_link,
                        date_pub,
                        content_hash,
                        description_hash,
                        word_count,
                    ),
                )
            conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Database error for {url}: {str(e)}")
        raise