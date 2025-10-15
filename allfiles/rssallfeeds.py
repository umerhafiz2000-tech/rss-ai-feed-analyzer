import requests
from bs4 import BeautifulSoup
import feedparser
from pymongo import MongoClient, errors
import time
import datetime

# ===== CONFIG =====
FEEDSPOT_URL = "https://rss.feedspot.com/cyber_security_rss_feeds/"
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "all_rss_db"
COLLECTION_NAME = "all_rss_articles"
SLEEP_INTERVAL = 600  # 10 minutes

# ===== MONGO SETUP =====
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
collection.create_index("link", unique=True)  # prevent duplicates


def log(msg):
    """Helper function to log with timestamp"""
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def get_feed_urls():
    """Scrape Feedspot page to get all RSS/Atom feed URLs dynamically"""
    response = requests.get(FEEDSPOT_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    feeds = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if any(x in href.lower() for x in ["rss", "feed", "atom", "xml"]):
            feed_name = a_tag.get_text(strip=True) or "Unknown"
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
    """Parse RSS feeds and store/update in MongoDB"""
    for feed in feeds:
        url = feed["url"]
        feed_name = feed["name"]

        log(f"📡 Fetching feed: {feed_name} ({url})")
        parsed_feed = feedparser.parse(url)

        # Handle malformed/invalid feeds
        if parsed_feed.bozo:
            log(f"⚠️ Skipped invalid feed: {url} (Error: {parsed_feed.bozo_exception})")
            continue

        count_new, count_updated, count_skipped = 0, 0, 0

        for entry in parsed_feed.entries:
            article = {
                "feed_name": feed_name,
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "author": entry.get("author", "Unknown"),
                "pubDate": entry.get("published", "")
            }

            if not article["link"]:
                continue  # skip if no link (invalid entry)

            try:
                result = collection.update_one(
                    {"link": article["link"]},   # filter by link
                    {"$set": article},           # update if exists
                    upsert=True                  # insert if not exists
                )
                if result.matched_count > 0:
                    count_updated += 1
                elif result.upserted_id is not None:
                    count_new += 1
                else:
                    count_skipped += 1
            except errors.DuplicateKeyError:
                count_skipped += 1

        log(f"✅ Done: {count_new} new, {count_updated} updated, {count_skipped} skipped.")


if __name__ == "__main__":
    while True:
        try:
            log("🔎 Fetching latest feed URLs from Feedspot...")
            feeds = get_feed_urls()
            log(f"📂 Found {len(feeds)} feeds. Starting update cycle...")

            fetch_and_store(feeds)

        except Exception as e:
            log(f"❌ Error occurred: {e}")

        log(f"⏳ Sleeping for {SLEEP_INTERVAL//60} minutes before next update...\n")
        time.sleep(SLEEP_INTERVAL)
