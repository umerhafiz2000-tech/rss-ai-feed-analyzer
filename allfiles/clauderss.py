
import requests
from bs4 import BeautifulSoup
import feedparser
from pymongo import MongoClient, errors
import time
import logging
from urllib.parse import urljoin, urlparse

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ===== CONFIG =====
FEEDSPOT_URL = "https://rss.feedspot.com/cyber_security_rss_feeds/"
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "1cyber_rss_db"
COLLECTION_NAME = "1rss_articles"
SLEEP_INTERVAL = 600  # 10 minutes
REQUEST_TIMEOUT = 30  # seconds

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
collection.create_index("link", unique=True)  # prevent duplicates

def get_feed_urls():
    """Scrape Feedspot page to get all RSS feed URLs dynamically"""
    try:
        response = requests.get(FEEDSPOT_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        feeds = []

        # Expanded search patterns for RSS feeds
        rss_patterns = ['rss', 'feed', 'atom', 'xml', 'feedburner']
        
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            href_lower = href.lower()
            
            # Check if URL contains any RSS-related patterns
            if any(pattern in href_lower for pattern in rss_patterns):
                feed_name = a_tag.get_text(strip=True) or "Unknown Feed"
                feeds.append({"name": feed_name, "url": href})

        # Remove duplicates
        unique_feeds = []
        seen_urls = set()
        for f in feeds:
            if f["url"] not in seen_urls:
                seen_urls.add(f["url"])
                unique_feeds.append(f)

        logger.info(f"Found {len(unique_feeds)} unique feeds")
        return unique_feeds
        
    except requests.RequestException as e:
        logger.error(f"Failed to fetch Feedspot page: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error in get_feed_urls: {e}")
        return []


def is_valid_feed_entry(entry):
    """Check if feed entry has required fields"""
    return (
        entry.get("link") and 
        entry.get("link").strip() and 
        entry.get("title") and 
        entry.get("title").strip()
    )


def fetch_and_store(feeds):
    """Parse RSS feeds and store in MongoDB with error handling"""
    successful_feeds = 0
    failed_feeds = 0
    total_articles = 0
    
    for i, feed in enumerate(feeds, 1):
        url = feed["url"]
        feed_name = feed["name"]
        
        logger.info(f"Processing feed {i}/{len(feeds)}: {feed_name}")
        
        try:
            # Parse feed with error handling
            parsed_feed = feedparser.parse(url)
            
            # Check if feed parsing was successful
            if parsed_feed.bozo:
                logger.warning(f"Feed parsing issues for {feed_name}: {parsed_feed.bozo_exception}")
            
            if not parsed_feed.entries:
                logger.warning(f"No entries found in feed: {feed_name}")
                failed_feeds += 1
                continue
            
            feed_articles = 0
            for entry in parsed_feed.entries:
                # Skip entries that don't have required fields
                if not is_valid_feed_entry(entry):
                    logger.debug(f"Skipping invalid entry from {feed_name}: missing link or title")
                    continue
                
                article = {
                    "feed_name": feed_name,
                    "feed_url": url,
                    "title": entry.get("title", "").strip(),
                    "link": entry.get("link", "").strip(),
                    "author": entry.get("author", "Unknown"),
                    "pubDate": entry.get("published", ""),
                    "description": entry.get("summary", "")[:500],  # Limit description length
                    "scraped_at": time.time()
                }
                
                try:
                    result = collection.update_one(
                        {"link": article["link"]},  # filter by link
                        {"$set": article},          # insert or update
                        upsert=True
                    )
                    
                    if result.upserted_id:
                        logger.debug(f"New article saved: {article['title'][:50]}...")
                        feed_articles += 1
                    else:
                        logger.debug(f"Article updated: {article['title'][:50]}...")
                    
                except errors.DuplicateKeyError:
                    logger.debug(f"Duplicate skipped: {article['title'][:50]}...")
                except Exception as e:
                    logger.error(f"Database error for article '{article['title'][:50]}...': {e}")
            
            logger.info(f"Feed '{feed_name}': {feed_articles} new articles processed")
            total_articles += feed_articles
            successful_feeds += 1
            
        except Exception as e:
            logger.error(f"Failed to process feed '{feed_name}' ({url}): {e}")
            failed_feeds += 1
            continue
    
    logger.info(f"Batch complete: {successful_feeds} successful, {failed_feeds} failed, {total_articles} total new articles")


if __name__ == "__main__":
    logger.info("Starting RSS feed monitor...")
    
    while True:
        try:
            logger.info("Fetching latest feed URLs from Feedspot...")
            feeds = get_feed_urls()
            
            if not feeds:
                logger.error("No feeds found! Retrying in next cycle...")
            else:
                logger.info(f"Found {len(feeds)} feeds. Updating MongoDB...")
                fetch_and_store(feeds)
            
            logger.info(f"Sleeping for {SLEEP_INTERVAL//60} minutes before next update...\n")
            time.sleep(SLEEP_INTERVAL)
            
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            logger.info(f"Continuing after {SLEEP_INTERVAL//60} minute delay...")
            time.sleep(SLEEP_INTERVAL)