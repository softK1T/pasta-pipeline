import json
import os
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient
import pandas as pd
import asyncio
from utils.utils import extract_telegraph_links, extract_hashtags
from telethon.sessions import StringSession

TIME_LIMIT = 7


def format_message(message, scraped_timestamp=None):
    if scraped_timestamp is None:
        scraped_timestamp = datetime.now(timezone.utc)

    formatted = {
        'message_id': message.id,
        'date': str(message.date.isoformat()),
        'text': message.text if message.text else "",
        'views': getattr(message, 'views', 0),
        'forwards': getattr(message, 'forwards', 0),
        'hashtags': extract_hashtags(message.text),
        'telegraph_link': extract_telegraph_links(message.text),
        'scraped_at': scraped_timestamp.isoformat(),
        'processed_at': scraped_timestamp.isoformat(),
        'last_updated': scraped_timestamp.isoformat()
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
    print("Starting to scrape messages")
    params = context["params"]
    channel_name = params["channel_name"]
    api_id = params["api_id"]
    api_hash = params["api_hash"]
    scrape_all_messages = params["all_messages"]
    session_string = params["session_string"]

    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    time_limit = datetime.now(timezone.utc) - timedelta(days=TIME_LIMIT)
    output_dir = '/opt/airflow/data/telegram'
    os.makedirs(output_dir, exist_ok=True)

    scraping_started_at = datetime.now(timezone.utc)
    print(f"Scraping started at: {scraping_started_at.isoformat()}")

    try:
        await client.start(phone=params["phone_number"])
        channel = await client.get_entity(channel_name)

        scraped_messages = []
        if scrape_all_messages:
            async for message in client.iter_messages(channel):
                print(f"Processing message {message.id}")
                scraped_messages.append(format_message(message, scraping_started_at))
                if len(scraped_messages) % 100 == 0:
                    print(f"Downloaded {len(scraped_messages)} messages so far.")
        else:
            async for message in client.iter_messages(channel):
                if message.date < time_limit:
                    break

                print(f"Processing message {message.id}")
                scraped_messages.append(format_message(message, scraping_started_at))

                if len(scraped_messages) % 100 == 0:
                    print(f"Downloaded {len(scraped_messages)} messages so far.")

        scraping_completed_at = datetime.now(timezone.utc)
        print(f"Scraping completed at: {scraping_completed_at.isoformat()}")
        print(f"Total scraping time: {scraping_completed_at - scraping_started_at}")

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        time_range = "all_messages" if scrape_all_messages else "past_day"
        csv_filename = f"{output_dir}/{channel_name.replace('@', '')}_{time_range}_{timestamp}.csv"
        json_filename = f"{output_dir}/{channel_name.replace('@', '')}_{time_range}_{timestamp}.json"

        df = pd.DataFrame(scraped_messages)
        df.to_csv(csv_filename, index=False)

        with open(json_filename, "w") as f:
            json.dump(scraped_messages, f, indent=2)

        print(f"Scraping completed. {len(scraped_messages)} messages saved to {csv_filename} and {json_filename}")

        return {
            'csv_file': csv_filename,
            'json_file': json_filename,
            'message_count': len(scraped_messages),
            'scraping_started_at': scraping_started_at.isoformat(),
            'scraping_completed_at': scraping_completed_at.isoformat(),
            'total_scraping_time': str(scraping_completed_at - scraping_started_at)
        }

    except Exception as e:
        print(f"Error during scraping: {str(e)}")
        if client.is_connected():
            await client.disconnect()
        raise

    finally:
        await client.disconnect()


def run_scraper(**context):
    return asyncio.get_event_loop().run_until_complete(scrape_messages(**context))
