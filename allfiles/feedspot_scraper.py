#!/usr/bin/env python3
"""
FeedSpot RSS Scraper -> MongoDB

Features:
- Fetch RSS feed URLs from FeedSpot page (https://rss.feedspot.com/cyber_security_rss_feeds/)
- Count extracted URLs
- Parse feeds for author, link, pubDate, title (robust parsing with fallbacks)
- Insert/update MongoDB without duplication (unique keys + upserts)
- Periodic/dynamic refresh (configurable)
- Concurrent fetching for speed
"""

import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
import re

import requests
import feedparser
from bs4 import BeautifulSoup
from lxml import etree
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import schedule
from tqdm import tqdm

# ------------- CONFIG -------------
FEEDSPOT_PAGE = "https://rss.feedspot.com/cyber_security_rss_feeds/"
USER_AGENT = "Mozilla/5.0 (compatible; FeedScraper/1.0; +https://example.com)"
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "rss_scraper_db"
MONGO_COLLECTION = "feed_items"
RUN_INTERVAL_MINUTES = 15       # how often to refresh feeds
MAX_WORKERS = 10                # concurrency for feed fetching
REQUEST_TIMEOUT = 20
# ------------- END CONFIG -------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("feedspot_scraper")

# MongoDB setup
mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = mongo[MONGO_DB]
col = db[MONGO_COLLECTION]

# Ensure deduplication: unique index on link or guid; fallback to title+published
# We'll create a compound unique index: (feed_link, guid or link) if possible
# Simpler approach: create unique index on 'unique_id' field we generate deterministically.
col.create_index([("unique_id", ASCENDING)], unique=True)

# ---------- Helpers ----------
def fetch_feedspot_page(url: str) -> str:
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text

def extract_rss_urls_from_feedspot(html: str) -> List[str]:
    """
    From the FeedSpot page HTML, extract RSS URLs that appear in the 'RSS Feed' links.
    This targets hrefs that look like typical feed URLs and avoids website page links.
    """
    soup = BeautifulSoup(html, "lxml")
    urls = set()

    # Common pattern on FeedSpot: there are links with text "RSS Feed" and href points to the feed
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        # heuristics: accept links that look like feeds (contain 'feed', 'rss', '.xml', 'feeds.feedburner', 'rss', 'atom')
        if re.search(r"(feed|rss|\.xml|feedburner|/rss|/feeds/|/atom)", href, re.IGNORECASE):
            # normalize relative
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = "https://rss.feedspot.com" + href
            urls.add(href)

    # additional: sometimes they list RSS feed URLs plainly in text
    text_urls = set(re.findall(r"https?://[^\s'\"<>]+", soup.get_text()))
    for u in text_urls:
        if re.search(r"(feed|rss|\.xml|feedburner|/rss|/feeds/|/atom)", u, re.IGNORECASE):
            urls.add(u)

    # final cleanup: remove probable webpage links that are not feeds (if they don't contain typical feed tokens, they were filtered above)
    # convert to list
    return sorted(urls)

def _generate_unique_id(item: Dict, feed_url: str) -> str:
    """
    Deterministic unique id for deduplication. Prefer guid/id, then link, then title+published, then content hash.
    """
    guid = item.get("guid") or item.get("id") or ""
    link = item.get("link") or ""
    title = (item.get("title") or "").strip()
    published = (item.get("published") or item.get("pubDate") or "")
    base = guid or link or f"{title}|{published}|{feed_url}"
    # sanitize
    return re.sub(r"\s+", " ", base).strip()

def save_item_to_mongo(item: Dict, feed_url: str):
    """
    Insert or update an item into MongoDB using 'unique_id' to avoid duplicates.
    """
    uid = _generate_unique_id(item, feed_url)
    doc = {
        "unique_id": uid,
        "feed_url": feed_url,
        "title": item.get("title"),
        "link": item.get("link"),
        "author": item.get("author") or item.get("author_detail", {}).get("name"),
        "published": item.get("published") or item.get("pubDate"),
        "summary": item.get("summary") or item.get("description"),
        "raw": item,    # store raw feedparser item for debugging
        "last_seen": int(time.time()),
    }
    try:
        # Upsert by unique_id: insert if not exist, else update 'last_seen' and other fields (avoid duplicate entries)
        col.update_one(
            {"unique_id": uid},
            {"$set": doc, "$setOnInsert": {"inserted_at": int(time.time())}},
            upsert=True
        )
    except DuplicateKeyError:
        # Very rare race condition, ignore
        logger.debug("Duplicate detected race condition for uid %s", uid)

# ---------- Robust feed parsing ----------
def parse_feed_with_feedparser(url: str) -> Optional[feedparser.FeedParserDict]:
    """
    Primary parse attempt using feedparser (handles most feeds).
    """
    headers = {"User-Agent": USER_AGENT}
    try:
        d = feedparser.parse(url, request_headers=headers)
        if d.bozo:
            # bozo flag indicates there was a problem parsing; log but still return (some feeds still usable)
            logger.warning("Feed parsing warning for %s : %s", url, getattr(d, "bozo_exception", "bozo"))
        return d
    except Exception as e:
        logger.exception("feedparser failed for %s: %s", url, e)
        return None

def parse_feed_with_lxml_recover(url: str) -> Optional[List[Dict]]:
    """
    Secondary attempt: download content and parse with lxml XMLParser(recover=True),
    then try to extract items (item/entry).
    """
    try:
        headers = {"User-Agent": USER_AGENT}
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        content = r.content
        parser = etree.XMLParser(recover=True, ns_clean=True, huge_tree=True)
        try:
            root = etree.fromstring(content, parser=parser)
        except Exception:
            # final fallback: try HTML parser
            parser = etree.HTMLParser()
            root = etree.fromstring(content, parser=parser)

        items = []
        # try RSS item nodes
        for node in root.findall(".//item"):
            item = {}
            title = node.findtext("title")
            link = node.findtext("link")
            pubDate = node.findtext("pubDate")
            author = node.findtext("author") or node.findtext("{http://purl.org/dc/elements/1.1/}creator")
            guid = node.findtext("guid")
            description = node.findtext("description")
            item.update({
                "title": title,
                "link": link,
                "published": pubDate,
                "author": author,
                "guid": guid,
                "summary": description
            })
            items.append(item)
        # Atom entry nodes
        if not items:
            for node in root.findall(".//{http://www.w3.org/2005/Atom}entry") + root.findall(".//entry"):
                item = {}
                title = node.findtext("{http://www.w3.org/2005/Atom}title") or node.findtext("title")
                link_elem = node.find("{http://www.w3.org/2005/Atom}link") or node.find("link")
                link = link_elem.get("href") if link_elem is not None else None
                pubDate = node.findtext("{http://www.w3.org/2005/Atom}updated") or node.findtext("updated")
                author = node.findtext("{http://www.w3.org/2005/Atom}author/{http://www.w3.org/2005/Atom}name") or node.findtext("author")
                summary = node.findtext("{http://www.w3.org/2005/Atom}summary") or node.findtext("summary") or node.findtext("content")
                item.update({
                    "title": title,
                    "link": link,
                    "published": pubDate,
                    "author": author,
                    "summary": summary
                })
                items.append(item)
        return items
    except Exception as e:
        logger.exception("lxml recovery parse failed for %s: %s", url, e)
        return None

def parse_feed_with_bs4(url: str) -> Optional[List[Dict]]:
    """
    Final fallback: use BeautifulSoup to attempt to extract items heuristically.
    """
    try:
        headers = {"User-Agent": USER_AGENT}
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "lxml")
        items = []
        # try <item> tags
        for it in soup.find_all("item"):
            title = it.title.string if it.title else None
            link = it.link.string if it.link else None
            pubDate = it.pubdate.string if it.pubdate else (it.find("pubdate").string if it.find("pubdate") else None)
            author = it.find("author").string if it.find("author") else None
            summary = (it.description.string if it.description else None)
            items.append({
                "title": title,
                "link": link,
                "published": pubDate,
                "author": author,
                "summary": summary
            })
        # if none, try common article-like anchors
        if not items:
            articles = soup.find_all(["entry", "article", "item"])
            for a in articles:
                title = a.find(["title"])
                link_tag = a.find("link")
                link = None
                if link_tag:
                    link = link_tag.get("href") or (link_tag.string if link_tag.string else None)
                pub = a.find(["pubdate","updated","published"])
                author = a.find(["author", "dc:creator"])
                summary = a.find(["summary","description","content"])
                items.append({
                    "title": title.string if title else None,
                    "link": link,
                    "published": pub.string if pub else None,
                    "author": author.string if author else None,
                    "summary": summary.string if summary else None
                })
        return items or None
    except Exception as e:
        logger.exception("BeautifulSoup fallback failed for %s: %s", url, e)
        return None

# ---------- Feed processor ----------
def process_single_feed(feed_url: str):
    try:
        d = parse_feed_with_feedparser(feed_url)
        if d and getattr(d, "entries", None):
            entries = []
            for e in d.entries:
                # Normalize keys to simple dict
                item = {
                    "title": e.get("title"),
                    "link": e.get("link"),
                    "published": e.get("published") or e.get("updated") or e.get("pubDate"),
                    "author": e.get("author") or (e.get("author_detail") or {}).get("name"),
                    "guid": e.get("id") or e.get("guid"),
                    "summary": e.get("summary") or e.get("description")
                }
                save_item_to_mongo(item, feed_url)
                entries.append(item)
            logger.info("Parsed %d entries from %s", len(entries), feed_url)
            return len(entries)
        else:
            # try lxml recover
            items = parse_feed_with_lxml_recover(feed_url)
            if items:
                for it in items:
                    save_item_to_mongo(it, feed_url)
                logger.info("Parsed %d entries (lxml fallback) from %s", len(items), feed_url)
                return len(items)
            # try bs4 fallback
            items2 = parse_feed_with_bs4(feed_url)
            if items2:
                for it in items2:
                    save_item_to_mongo(it, feed_url)
                logger.info("Parsed %d entries (bs4 fallback) from %s", len(items2), feed_url)
                return len(items2)
            logger.warning("No entries found for %s", feed_url)
            return 0
    except Exception as e:
        logger.exception("Failed to process feed %s: %s", feed_url, e)
        return 0

def process_feeds_concurrently(feed_urls: List[str]):
    total = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process_single_feed, url): url for url in feed_urls}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Feeds"):
            try:
                count = fut.result()
                total += count or 0
            except Exception as e:
                logger.exception("Worker failed: %s", e)
    return total

# ---------- Main scraping job ----------
def scrape_job():
    logger.info("Starting scrape job: fetch FeedSpot page")
    try:
        html = fetch_feedspot_page(FEEDSPOT_PAGE)
    except Exception as e:
        logger.exception("Failed to fetch FeedSpot page: %s", e)
        return

    rss_urls = extract_rss_urls_from_feedspot(html)
    logger.info("Extracted %d RSS URLs from FeedSpot", len(rss_urls))
    # Print the number and first 10 for debugging
    logger.debug("Sample RSS URLs: %s", rss_urls[:10])

    # Process feeds concurrently and send into MongoDB
    parsed_items = process_feeds_concurrently(rss_urls)
    logger.info("Completed scrape job: total parsed items added/updated this run: %d", parsed_items)

# ---------- Runner ----------
def run_loop():
    # First immediate run
    scrape_job()
    # Then schedule
    schedule.every(RUN_INTERVAL_MINUTES).minutes.do(scrape_job)
    logger.info("Scheduler started: running every %d minutes", RUN_INTERVAL_MINUTES)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    try:
        logger.info("FeedSpot Scraper starting up")
        run_loop()
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting.")
    except Exception:
        logger.exception("Unhandled exception, exiting.")
