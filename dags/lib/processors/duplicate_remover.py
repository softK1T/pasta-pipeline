"""
Duplicate removal processor for the Pasta Pipeline.
Handles cleanup of duplicate messages and orphaned links.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def remove_duplicate_messages(**context) -> Dict:
    """Remove duplicate Telegram messages, keeping the latest processed version."""
    logger.info("Starting duplicate message removal")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        
        with conn.cursor() as cur:
            # Find and remove duplicate messages
            duplicate_query = """
            WITH duplicates AS (
                SELECT message_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY message_id 
                           ORDER BY processed_at DESC
                       ) as rn
                FROM telegram_messages
            )
            DELETE FROM telegram_messages 
            WHERE message_id IN (
                SELECT message_id 
                FROM duplicates 
                WHERE rn > 1
            );
            """
            
            cur.execute(duplicate_query)
            deleted_messages = cur.rowcount
            conn.commit()
            
            logger.info(f"Removed {deleted_messages} duplicate messages")
            
            return {
                "deleted_messages": deleted_messages,
                "status": "completed"
            }
            
    except SQLAlchemyError as e:
        logger.error(f"Database error removing duplicate messages: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error removing duplicate messages: {str(e)}")
        raise


def remove_duplicate_telegraph_content(**context) -> Dict:
    """Remove duplicate Telegraph content, keeping the first URL per identical content hash."""
    logger.info("Starting duplicate Telegraph content removal")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        
        with conn.cursor() as cur:
            # Find and remove duplicate content
            duplicate_query = """
            WITH duplicates AS (
                SELECT url,
                       ROW_NUMBER() OVER (
                           PARTITION BY content_hash 
                           ORDER BY processed_at ASC
                       ) as rn
                FROM telegraph_content
                WHERE content_hash IS NOT NULL 
                AND content_hash != ''
            )
            DELETE FROM telegraph_content 
            WHERE url IN (
                SELECT url 
                FROM duplicates 
                WHERE rn > 1
            );
            """
            
            cur.execute(duplicate_query)
            deleted_content = cur.rowcount
            conn.commit()
            
            logger.info(f"Removed {deleted_content} duplicate Telegraph content entries")
            
            return {
                "deleted_content": deleted_content,
                "status": "completed"
            }
            
    except SQLAlchemyError as e:
        logger.error(f"Database error removing duplicate content: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error removing duplicate content: {str(e)}")
        raise


def clean_orphaned_links(**context) -> Dict:
    """Clean orphaned Telegraph links that don't exist in the content table."""
    logger.info("Starting orphaned link cleanup")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        
        with conn.cursor() as cur:
            # Find and clean orphaned links
            orphan_query = """
            UPDATE telegram_messages 
            SET telegraph_link = NULL 
            WHERE telegraph_link IS NOT NULL 
            AND telegraph_link NOT IN (
                SELECT url FROM telegraph_content
            );
            """
            
            cur.execute(orphan_query)
            cleaned_links = cur.rowcount
            conn.commit()
            
            logger.info(f"Cleaned {cleaned_links} orphaned Telegraph links")
            
            return {
                "cleaned_links": cleaned_links,
                "status": "completed"
            }
            
    except SQLAlchemyError as e:
        logger.error(f"Database error cleaning orphaned links: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error cleaning orphaned links: {str(e)}")
        raise


def cleanup_failed_telegraph_links(**context) -> Dict:
    """Clean up failed Telegraph links that have exceeded retry limits."""
    logger.info("Starting failed link cleanup")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        
        with conn.cursor() as cur:
            # Find and clean failed links
            failed_query = """
            DELETE FROM telegraph_content 
            WHERE status IN ('error', 'timeout', 'client_error', 'server_error')
            AND retry_count >= 3
            AND last_checked < NOW() - INTERVAL '7 days';
            """
            
            cur.execute(failed_query)
            deleted_failed = cur.rowcount
            conn.commit()
            
            logger.info(f"Cleaned {deleted_failed} failed Telegraph links")
            
            return {
                "deleted_failed": deleted_failed,
                "status": "completed"
            }
            
    except SQLAlchemyError as e:
        logger.error(f"Database error cleaning failed links: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error cleaning failed links: {str(e)}")
        raise


def cleanup_old_data(**context) -> Dict:
    """Clean up old data based on retention policy."""
    logger.info("Starting old data cleanup")
    
    try:
        # Get retention days from context or use default
        retention_days = context.get('params', {}).get('retention_days', 90)
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        
        with conn.cursor() as cur:
            # Clean old messages
            old_messages_query = """
            DELETE FROM telegram_messages 
            WHERE date < NOW() - INTERVAL '%s days';
            """ % retention_days
            
            cur.execute(old_messages_query)
            deleted_old_messages = cur.rowcount
            
            # Clean old content
            old_content_query = """
            DELETE FROM telegraph_content 
            WHERE processed_at < NOW() - INTERVAL '%s days'
            AND status != 'success';
            """ % retention_days
            
            cur.execute(old_content_query)
            deleted_old_content = cur.rowcount
            
            conn.commit()
            
            logger.info(f"Cleaned {deleted_old_messages} old messages and {deleted_old_content} old content")
            
            return {
                "deleted_old_messages": deleted_old_messages,
                "deleted_old_content": deleted_old_content,
                "retention_days": retention_days,
                "status": "completed"
            }
            
    except SQLAlchemyError as e:
        logger.error(f"Database error cleaning old data: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error cleaning old data: {str(e)}")
        raise


def get_cleanup_statistics(**context) -> Dict:
    """Get statistics about the current state of the database."""
    logger.info("Getting cleanup statistics")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        
        with conn.cursor() as cur:
            # Get message statistics
            message_stats_query = """
            SELECT 
                COUNT(*) as total_messages,
                COUNT(DISTINCT message_id) as unique_messages,
                COUNT(telegraph_link) as messages_with_links,
                COUNT(*) - COUNT(DISTINCT message_id) as duplicate_messages
            FROM telegram_messages;
            """
            
            cur.execute(message_stats_query)
            message_stats = cur.fetchone()
            
            # Get content statistics
            content_stats_query = """
            SELECT 
                COUNT(*) as total_content,
                COUNT(DISTINCT content_hash) as unique_content,
                COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_content,
                COUNT(CASE WHEN status != 'success' THEN 1 END) as failed_content,
                COUNT(*) - COUNT(DISTINCT content_hash) as duplicate_content
            FROM telegraph_content;
            """
            
            cur.execute(content_stats_query)
            content_stats = cur.fetchone()
            
            # Get orphaned links count
            orphaned_query = """
            SELECT COUNT(*) as orphaned_links
            FROM telegram_messages 
            WHERE telegraph_link IS NOT NULL 
            AND telegraph_link NOT IN (
                SELECT url FROM telegraph_content
            );
            """
            
            cur.execute(orphaned_query)
            orphaned_count = cur.fetchone()[0]
            
            stats = {
                "messages": {
                    "total": message_stats[0],
                    "unique": message_stats[1],
                    "with_links": message_stats[2],
                    "duplicates": message_stats[3]
                },
                "content": {
                    "total": content_stats[0],
                    "unique": content_stats[1],
                    "successful": content_stats[2],
                    "failed": content_stats[3],
                    "duplicates": content_stats[4]
                },
                "orphaned_links": orphaned_count,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            logger.info(f"Cleanup statistics: {stats}")
            return stats
            
    except SQLAlchemyError as e:
        logger.error(f"Database error getting statistics: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error getting statistics: {str(e)}")
        raise


def run_full_cleanup(**context) -> Dict:
    """Run a complete cleanup of the database."""
    logger.info("Starting full database cleanup")
    
    try:
        results = {}
        
        # Get initial statistics
        results["initial_stats"] = get_cleanup_statistics(**context)
        
        # Run cleanup tasks
        results["duplicate_messages"] = remove_duplicate_messages(**context)
        results["duplicate_content"] = remove_duplicate_telegraph_content(**context)
        results["orphaned_links"] = clean_orphaned_links(**context)
        results["failed_links"] = cleanup_failed_telegraph_links(**context)
        results["old_data"] = cleanup_old_data(**context)
        
        # Get final statistics
        results["final_stats"] = get_cleanup_statistics(**context)
        
        # Calculate summary
        total_cleaned = (
            results["duplicate_messages"]["deleted_messages"] +
            results["duplicate_content"]["deleted_content"] +
            results["orphaned_links"]["cleaned_links"] +
            results["failed_links"]["deleted_failed"] +
            results["old_data"]["deleted_old_messages"] +
            results["old_data"]["deleted_old_content"]
        )
        
        results["summary"] = {
            "total_cleaned": total_cleaned,
            "cleanup_completed": True,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Full cleanup completed. Total items cleaned: {total_cleaned}")
        return results
        
    except Exception as e:
        logger.error(f"Error during full cleanup: {str(e)}")
        raise 