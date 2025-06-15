import json
import os
import logging
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient
import pandas as pd
import asyncio
from utils.utils import extract_telegraph_links, extract_hashtags
from telethon.sessions import StringSession

logger = logging.getLogger(__name__)

TIME_LIMIT = 7

def format_message(message):
    message_text = message.text if message.text else ""

    formatted = {
        'message_id': message.id,
        'date': str(message.date.isoformat()),
        'text': message_text,
        'views': getattr(message, 'views', 0),
        'forwards': getattr(message, 'forwards', 0),
        'hashtags': extract_hashtags(message_text),
        'telegraph_link': extract_telegraph_links(message_text)
    }

    if hasattr(message, 'reactions') and message.reactions:
        reactions = {}
        for reaction in message.reactions.results:
            emoji = getattr(reaction.reaction, 'emoticon', None)
            if emoji:
                reactions[emoji] = reaction.count
        formatted['reactions'] = reactions

    return formatted


async def scrape_messages(**context):
    logger.info("Starting to scrape messages")

    params = context["params"]
    channel_name = params["channel_name"]
    api_id = params["api_id"]
    api_hash = params["api_hash"]
    scrape_all_messages = params["all_messages"]
    session_string = params["session_string"]

    time_limit = datetime.now(timezone.utc) - timedelta(days=TIME_LIMIT)

    output_dir = '/opt/airflow/data/telegram'
    os.makedirs(output_dir, exist_ok=True)

    client = TelegramClient(StringSession(session_string), api_id, api_hash)

    try:
        await client.start(phone=params["phone_number"])
        channel = await client.get_entity(channel_name)

        scraped_messages = []

        if scrape_all_messages:
            logger.info("Scraping all messages")
            async for message in client.iter_messages(channel):
                scraped_messages.append(format_message(message))
                if len(scraped_messages) % 100 == 0:
                    logger.info(f"Downloaded {len(scraped_messages)} messages so far")
        else:
            logger.info(f"Scraping messages from the last {TIME_LIMIT} days")
            async for message in client.iter_messages(channel):
                if message.date < time_limit:
                    break
                scraped_messages.append(format_message(message))
                if len(scraped_messages) % 100 == 0:
                    logger.info(f"Downloaded {len(scraped_messages)} messages so far")

        logger.info(f"Scraped {len(scraped_messages)} messages")

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        time_range = "all_messages" if scrape_all_messages else f"past_{TIME_LIMIT}_days"
        csv_filename = f"{output_dir}/{channel_name.replace('@', '')}_{time_range}_{timestamp}.csv"
        json_filename = f"{output_dir}/{channel_name.replace('@', '')}_{time_range}_{timestamp}.json"

        df = pd.DataFrame(scraped_messages)
        df.to_csv(csv_filename, index=False)

        with open(json_filename, "w") as f:
            json.dump(scraped_messages, f)

        logger.info(f"Saved {len(scraped_messages)} messages to {csv_filename} and {json_filename}")

        return {
            'csv_file': csv_filename,
            'json_file': json_filename,
            'message_count': len(scraped_messages)
        }

    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")
        if client.is_connected():
            await client.disconnect()
        raise

    finally:
        await client.disconnect()


def run_scraper(**context):
    return asyncio.get_event_loop().run_until_complete(scrape_messages(**context))
