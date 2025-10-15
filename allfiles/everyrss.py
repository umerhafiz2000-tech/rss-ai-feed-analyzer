import requests
from bs4 import BeautifulSoup
import feedparser
from pymongo import MongoClient, errors
import time
import datetime

# ===== CONFIG =====
FEEDSPOT_URL = "https://rss.feedspot.com/cyber_security_rss_feeds/"
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "everycyber_rss_db"
COLLECTION_NAME = "everyrss_articles"
SLEEP_INTERVAL = 600  # 10 minutes

# ===== MONGO SETUP =====
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
collection.create_index("link", unique=True)  # avoid duplicates


def log(msg):
    """Helper to log with timestamp"""
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def get_feed_urls():
    """Scrape Feedspot page to get all feed-like URLs"""
    response = requests.get(FEEDSPOT_URL, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(response.text, "html.parser")
    feeds = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if any(x in href.lower() for x in ["rss", "feed", "atom", "xml"]):
            feed_name = a_tag.get_text(strip=True) or "Unknown"
            feeds.append({"name": feed_name, "url": href})

    # Deduplicate
    unique_feeds, seen = [], set()
    for f in feeds:
        if f["url"] not in seen:
            seen.add(f["url"])
            unique_feeds.append(f)

    return unique_feeds


def fetch_feed_or_html(feed):
    """Try parsing as RSS/Atom, else fallback to HTML scraping"""
    url, feed_name = feed["url"], feed["name"]

    # --- Try RSS/Atom first ---
    parsed = feedparser.parse(url)
    if parsed.entries and not parsed.bozo:
        count_new, count_updated = 0, 0
        for entry in parsed.entries:
            article = {
                "feed_name": feed_name,
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "author": entry.get("author", "Unknown"),
                "pubDate": entry.get("published", ""),
                "source_url": url,
                "type": "rss"
            }
            if not article["link"]:
                continue
            result = collection.update_one(
                {"link": article["link"]},
                {"$set": article},
                upsert=True
            )
            if result.matched_count > 0:
                count_updated += 1
            else:
                count_new += 1
        log(f"✅ RSS feed: {feed_name} → {count_new} new, {count_updated} updated")
        return

    # --- Fallback to HTML ---
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.title.string.strip() if soup.title else url
        links = [a.get("href") for a in soup.find_all("a", href=True)]

        doc = {
            "feed_name": feed_name,
            "title": title,
            "link": url,
            "links_found": links[:20],  # limit to 20 links
            "source_url": url,
            "type": "html"
        }
        collection.update_one({"link": url}, {"$set": doc}, upsert=True)
        log(f"🌐 HTML page stored: {feed_name} ({url})")
    except Exception as e:
        log(f"❌ Failed to fetch {url}: {e}")


if __name__ == "__main__":
    while True:
        try:
            log("🔎 Fetching feed URLs from Feedspot...")
            feeds = get_feed_urls()
            log(f"📂 Found {len(feeds)} feeds. Processing...")

            for feed in feeds:
                fetch_feed_or_html(feed)

        except Exception as e:
            log(f"❌ Fatal error: {e}")

        log(f"⏳ Sleeping {SLEEP_INTERVAL//60} minutes before next cycle...\n")
        time.sleep(SLEEP_INTERVAL)
