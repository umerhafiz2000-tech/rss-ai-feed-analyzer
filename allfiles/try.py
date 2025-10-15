import requests
from bs4 import BeautifulSoup
import feedparser
from pymongo import MongoClient, errors
import time

# ===== CONFIG =====
FEEDSPOT_URL = "https://rss.feedspot.com/cyber_security_rss_feeds/"
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "fullfinalcyber_rss_db"
COLLECTION_NAME = "fullfinalrss_articles"
SLEEP_INTERVAL = 600  # 10 minutes

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
collection.create_index("link", unique=True)  # prevent duplicates

# def get_feed_urls():
#     """Scrape Feedspot page to get all RSS feed URLs dynamically"""
#     response = requests.get(FEEDSPOT_URL)
#     soup = BeautifulSoup(response.text, "html.parser")
#     feeds = []

#     # Look for all links containing "rss"
#     for a_tag in soup.find_all("a", href=True):
#         href = a_tag["href"]
#         if "rss" in href.lower() or "feedburner" in href.lower():
#             feed_name = a_tag.get_text(strip=True)
#             feeds.append({"name": feed_name, "url": href})

#     # Remove duplicates
#     unique_feeds = []
#     seen_urls = set()
#     for f in feeds:
#         if f["url"] not in seen_urls:
#             seen_urls.add(f["url"])
#             unique_feeds.append(f)

#     return unique_feeds


def get_feed_urls():
    """Scrape Feedspot page to get all possible feed URLs dynamically"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0 Safari/537.36"
    }

    try:
        response = requests.get(
            FEEDSPOT_URL,
            headers=headers,
            timeout=15  # prevent hanging forever
        )
        response.raise_for_status()  # raise exception for bad HTTP codes
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to fetch Feedspot page: {e}")
        return []  # return empty so script keeps running

    soup = BeautifulSoup(response.text, "html.parser")
    feeds = []

    # Broader set of keywords to detect RSS/Atom/Feed links
    keywords = ["rss", "feed", "xml", "atom"]

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if any(kw in href.lower() for kw in keywords):
            feed_name = a_tag.get_text(strip=True) or href
            feeds.append({"name": feed_name, "url": href})

    # Remove duplicates by URL
    unique_feeds = []
    seen_urls = set()
    for f in feeds:
        if f["url"] not in seen_urls:
            seen_urls.add(f["url"])
            unique_feeds.append(f)

    return unique_feeds



def fetch_and_store(feeds):
    """Parse RSS feeds and store in MongoDB"""
    for feed in feeds:
        url = feed["url"]
        feed_name = feed["name"]
        parsed_feed = feedparser.parse(url)
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
        print(f"Found {len(feeds)} feeds. Updating MongoDB...")
        fetch_and_store(feeds)
        print(f"Sleeping for {SLEEP_INTERVAL//60} minutes before next update...\n")
        time.sleep(SLEEP_INTERVAL)

