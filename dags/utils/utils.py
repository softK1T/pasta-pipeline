import re
import logging

logger = logging.getLogger(__name__)



def extract_telegraph_links(message_text: str) -> list:
    if not message_text or not isinstance(message_text, (str, bytes)):
        return []

    telegraph_pattern = r'https://telegra\.ph/[^\s\n\])>_*}]+'

    try:
        links = re.findall(telegraph_pattern, message_text)

        seen = set()
        unique_links = []

        for link in links:
            cleaned_link = re.sub(r'[.,"\'\*_]+$', '', link)

            if cleaned_link not in seen:
                seen.add(cleaned_link)
                unique_links.append(cleaned_link)

        return unique_links
    except Exception as e:
        logger.error(f"Error extracting Telegraph links: {str(e)}")
        return []


def extract_hashtags(message_text: str) -> list:
    if not message_text or not isinstance(message_text, (str, bytes)):
        return []

    hashtag_pattern = r'#([a-zA-Zа-яА-Я0-9_]+)'

    try:
        hashtags = re.findall(hashtag_pattern, message_text)

        seen = set()
        unique_hashtags = []

        for tag in hashtags:
            tag = tag.lower()
            if tag not in seen:
                seen.add(tag)
                unique_hashtags.append(tag)

        return unique_hashtags
    except Exception as e:
        logger.error(f"Error extracting hashtags: {str(e)}")
        return []
