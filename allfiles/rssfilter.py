import requests
from bs4 import BeautifulSoup
import feedparser
from pymongo import MongoClient, errors
import time

# ===== CONFIG =====
FEEDSPOT_URL = "https://rss.feedspot.com/cyber_security_rss_feeds/"
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "filter_rss_db"
COLLECTION_NAME = "filterrss_articles"
SLEEP_INTERVAL = 600  # 10 minutes

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
collection.create_index("link", unique=True)  # prevent duplicates


def get_feed_urls():
    """Scrape Feedspot page to get ALL possible feed URLs dynamically"""
    response = requests.get(FEEDSPOT_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    feeds = []

    # Collect ALL hrefs (no filtering)
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        feed_name = a_tag.get_text(strip=True)
        feeds.append({"name": feed_name, "url": href})

    # Remove duplicates
    unique_feeds = []
    seen_urls = set()
    for f in feeds:
        if f["url"] not in seen_urls:
            seen_urls.add(f["url"])
            unique_feeds.append(f)

    return unique_feeds


def fetch_and_store(feeds):
    """Parse feeds and store in MongoDB"""
    for feed in feeds:
        url = feed["url"]
        feed_name = feed["name"]

        try:
            parsed_feed = feedparser.parse(url)
        except Exception as e:
            print(f"[ERROR] Failed to parse {url}: {e}")
            continue

        if not parsed_feed.entries:  # not a valid feed
            print(f"[SKIP] Not a valid feed: {feed_name} ({url})")
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
                print(f"Saved/Updated: {article['title']}")
            except errors.DuplicateKeyError:
                print(f"Duplicate skipped: {article['title']}")


if __name__ == "__main__":
    while True:
        print("Fetching latest feed URLs from Feedspot...")
        feeds = get_feed_urls()
        print(f"Found {len(feeds)} links. Checking for valid feeds and updating MongoDB...")
        fetch_and_store(feeds)
        print(f"Sleeping for {SLEEP_INTERVAL//60} minutes before next update...\n")
        time.sleep(SLEEP_INTERVAL)
