import json
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from telethon import TelegramClient
import pandas as pd
import asyncio
from utils.utils import extract_telegraph_links, extract_hashtags
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TIME_LIMIT = 7
MAX_MESSAGES_PER_BATCH = 1000
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5  # seconds


def format_message(message) -> Dict:
    """Format a Telegram message into a standardized dictionary."""
    try:
        formatted = {
            'message_id': message.id,
            'date': str(message.date.isoformat()),
            'text': message.text if message.text else "",
            'views': getattr(message, 'views', 0),
            'forwards': getattr(message, 'forwards', 0),
            'hashtags': extract_hashtags(message.text),
            'telegraph_link': extract_telegraph_links(message.text),
            'scraped_at': datetime.now(timezone.utc).isoformat()
        }

        # Add reactions if available
        if hasattr(message, 'reactions') and message.reactions:
            reactions = {}
            for reaction in message.reactions.results:
                emoji = getattr(reaction.reaction, 'emoticon', None)
                if emoji:
                    reactions[emoji] = reaction.count
            formatted['reactions'] = reactions

        return formatted
    except Exception as e:
        logger.error(f"Error formatting message {getattr(message, 'id', 'unknown')}: {str(e)}")
        return None


async def scrape_messages(**context) -> Dict:
    """Scrape messages from Telegram channel with improved error handling."""
    logger.info("Starting to scrape messages")
    
    try:
        params = context["params"]
        channel_name = params["channel_name"]
        api_id = params["api_id"]
        api_hash = params["api_hash"]
        scrape_all_messages = params["all_messages"]
        session_string = params["session_string"]
        phone_number = params["phone_number"]

        # Validate required parameters
        if not all([api_id, api_hash, session_string, phone_number]):
            raise ValueError("Missing required Telegram API credentials")

        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        time_limit = datetime.now(timezone.utc) - timedelta(days=TIME_LIMIT)
        output_dir = '/opt/airflow/data/telegram'
        os.makedirs(output_dir, exist_ok=True)

        try:
            await client.start(phone=phone_number)
            logger.info("Successfully connected to Telegram")
            
            channel = await client.get_entity(channel_name)
            logger.info(f"Found channel: {channel.title}")

            scraped_messages = []
            message_count = 0
            
            if scrape_all_messages:
                logger.info("Scraping all messages from channel")
                async for message in client.iter_messages(channel, limit=MAX_MESSAGES_PER_BATCH):
                    formatted_message = format_message(message)
                    if formatted_message:
                        scraped_messages.append(formatted_message)
                        message_count += 1
                    
                    if message_count % 100 == 0:
                        logger.info(f"Downloaded {message_count} messages so far")
            else:
                logger.info(f"Scraping messages from last {TIME_LIMIT} days")
                async for message in client.iter_messages(channel, limit=MAX_MESSAGES_PER_BATCH):
                    if message.date < time_limit:
                        logger.info(f"Reached time limit, stopping at message from {message.date}")
                        break

                    formatted_message = format_message(message)
                    if formatted_message:
                        scraped_messages.append(formatted_message)
                        message_count += 1

                    if message_count % 100 == 0:
                        logger.info(f"Downloaded {message_count} messages so far")

            logger.info(f"Scraping completed. Total messages: {len(scraped_messages)}")

            # Save data with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            time_range = "all_messages" if scrape_all_messages else "past_week"
            channel_clean = channel_name.replace('@', '')
            
            csv_filename = f"{output_dir}/{channel_clean}_{time_range}_{timestamp}.csv"
            json_filename = f"{output_dir}/{channel_clean}_{time_range}_{timestamp}.json"

            # Save as CSV
            df = pd.DataFrame(scraped_messages)
            df.to_csv(csv_filename, index=False)
            logger.info(f"Saved CSV to: {csv_filename}")

            # Save as JSON
            with open(json_filename, "w", encoding='utf-8') as f:
                json.dump(scraped_messages, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved JSON to: {json_filename}")

            return {
                'csv_file': csv_filename,
                'json_file': json_filename,
                'message_count': len(scraped_messages),
                'channel_name': channel_name,
                'scraped_at': datetime.now(timezone.utc).isoformat()
            }

        except SessionPasswordNeededError:
            logger.error("Two-factor authentication required. Please provide session string.")
            raise
        except PhoneCodeInvalidError:
            logger.error("Invalid phone code provided.")
            raise
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            raise

    except Exception as e:
        logger.error(f"Critical error in scrape_messages: {str(e)}")
        raise

    finally:
        if 'client' in locals() and client.is_connected():
            await client.disconnect()
            logger.info("Disconnected from Telegram")


def run_scraper(**context) -> Dict:
    """Wrapper function to run scraper in Airflow context."""
    return asyncio.get_event_loop().run_until_complete(scrape_messages(**context))
