import requests
from bs4 import BeautifulSoup
import feedparser
from pymongo import MongoClient, errors
import time
import http.client

# ===== CONFIG =====
FEEDSPOT_URL = "https://rss.feedspot.com/cyber_security_rss_feeds/"
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "trycyber_rss_db"
COLLECTION_NAME = "tryrss_articles"
SLEEP_INTERVAL = 600  # 10 minutes
REQUEST_DELAY = 2      # seconds between feeds
RETRIES = 3            # retry attempts
RETRY_DELAY = 5        # seconds between retries

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
collection.create_index("link", unique=True)  # prevent duplicates


def get_feed_urls():
    """Scrape Feedspot page to get all valid RSS feed URLs dynamically"""
    response = requests.get(FEEDSPOT_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    feeds = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        feed_name = a_tag.get_text(strip=True)

        # Only accept real RSS/Atom feed URLs
        if any(ext in href.lower() for ext in [".rss", "/rss", "feedburner", ".xml", "/feed"]):
            feeds.append({"name": feed_name, "url": href})

    # Remove duplicates
    unique_feeds = []
    seen_urls = set()
    for f in feeds:
        if f["url"] not in seen_urls:
            seen_urls.add(f["url"])
            unique_feeds.append(f)

    return unique_feeds


def safe_parse(url, retries=RETRIES, delay=RETRY_DELAY):
    """Safely parse a feed with retries and custom User-Agent"""
    feedparser.USER_AGENT = "Mozilla/5.0 (compatible; CyberRSSBot/1.0)"
    for attempt in range(retries):
        try:
            parsed = feedparser.parse(url)
            if parsed.bozo:  # feedparser sets this if parsing failed
                raise Exception(parsed.bozo_exception)
            return parsed
        except (http.client.RemoteDisconnected, Exception) as e:
            print(f"[{attempt+1}/{retries}] Error fetching {url}: {e}")
            time.sleep(delay)
    return None


def fetch_and_store(feeds):
    """Parse RSS feeds and store in MongoDB"""
    for feed in feeds:
        url = feed["url"]
        feed_name = feed["name"]

        parsed_feed = safe_parse(url)
        if not parsed_feed:
            print(f"❌ Skipping {feed_name} ({url}) after repeated failures.\n")
            continue

        for entry in parsed_feed.entries:
            article = {
                "feed_name": feed_name,
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "author": entry.get("author", "Unknown"),
                "pubDate": entry.get("published", "")
            }
            try:
                collection.update_one(
                    {"link": article["link"]},  # filter by link
                    {"$set": article},          # insert or update
                    upsert=True
                )
                print(f"✅ Saved/Updated: {article['title']}")
            except errors.DuplicateKeyError:
                print(f"⚠️ Duplicate skipped: {article['title']}")

        # Small delay between feeds
        time.sleep(REQUEST_DELAY)


if __name__ == "__main__":
    while True:
        print("🔎 Fetching latest feed URLs from Feedspot...")
        feeds = get_feed_urls()
        print(f"📌 Found {len(feeds)} feeds. Updating MongoDB...\n")
        fetch_and_store(feeds)
        print(f"😴 Sleeping for {SLEEP_INTERVAL//60} minutes before next update...\n")
        time.sleep(SLEEP_INTERVAL)
