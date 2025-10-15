#!/usr/bin/env python3
"""
rss_aggregator_final.py

Features:
- Read FeedSpot resource pages from resources.txt (one URL per line).
- Extract feed URLs (rewrites feedspot infiniterss.php?q=site:... -> real feed URL).
- Report the number of URLs extracted.
- Parse each feed and save only Author, Link, PubDate, Title to MongoDB.
- Robust fallbacks for malformed feeds (feedparser -> lxml recover -> HTML heuristics).
- Continuous polling (dynamic updates). Upserts into MongoDB; avoids duplicates.
- Concurrent fetching/parsing for speed.

Requirements:
pip install requests beautifulsoup4 feedparser lxml python-dateutil pymongo

Configure at top of file.
"""

import os
import time
import logging
from urllib.parse import urljoin, urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
import feedparser
from lxml import etree
from dateutil import parser as dateparser
from pymongo import MongoClient, ASCENDING, errors
from typing import List, Dict, Any, Tuple, Optional

# ---------------- Config ----------------
RESOURCES_FILE = "resources.txt"    # each line: a FeedSpot resource page (e.g. https://rss.feedspot.com/cyber_security_rss_feeds/)
POLL_INTERVAL_SECONDS = 60 * 10    # 10 minutes
MAX_WORKERS = 12
REQUEST_TIMEOUT = 20
USER_AGENT = "rss-aggregator/1.0 (+https://example.local/)"

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "25-09-2025rssfeeds"
MONGO_COLL = "entries"

# Optional: domains to ignore entirely (e.g. known problematic sites you want to skip)
BLOCKLIST_DOMAINS = set([
    # "facebook.com",
])

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("rss_aggregator")

# ---------------- MongoDB ----------------
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB]
    coll = db[MONGO_COLL]
    # unique index on Link to avoid duplicates; if Link missing we will fallback to composite key
    coll.create_index([("Link", ASCENDING)], unique=True, background=True)
except Exception as e:
    logger.error("MongoDB connection/index creation failed: %s", e)
    raise

# ---------------- Helpers ----------------
def read_resources() -> List[str]:
    if not os.path.exists(RESOURCES_FILE):
        logger.error("%s not found. Create it and add FeedSpot resource URLs (one per line).", RESOURCES_FILE)
        return []
    with open(RESOURCES_FILE, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    logger.info("Loaded %d resource URLs from %s", len(lines), RESOURCES_FILE)
    return lines

def clean_feedspot_infiniterss(href: str) -> str:
    """
    If href is a Feedspot infiniterss.php link with q=site:..., extract the real site URL.
    Example:
    https://www.feedspot.com/infiniterss.php?...&q=site:https%3A%2F%2Fexample.com%2Frss
    -> https://example.com/rss
    """
    try:
        parsed = urlparse(href)
        if "infiniterss.php" in parsed.path and parsed.query:
            qs = parse_qs(parsed.query)
            q = qs.get("q") or qs.get("_q") or qs.get("url")
            if q:
                qval = q[0]
                if qval.startswith("site:"):
                    qval = qval[len("site:"):]
                return qval
    except Exception:
        pass
    return href

def fetch_url_text(url: str) -> Optional[str]:
    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as e:
        logger.warning("Failed to fetch %s : %s", url, e)
        return None

def extract_feed_links_from_resource(html_text: str, base_url: str) -> List[str]:
    """
    Extract candidate feed links from a FeedSpot resource page or similar HTML.
    Filters by feed-like substrings and rewrites FeedSpot proxies.
    """
    soup = BeautifulSoup(html_text, "html.parser")
    candidates = set()

    # <a href> heuristics
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        href = clean_feedspot_infiniterss(href)
        # make absolute
        try:
            full = urljoin(base_url, href)
        except Exception:
            full = href
        low = full.lower()
        # Accept if it clearly looks like a feed or xml or atom
        if any(tok in low for tok in (".xml", "rss", "feed", "atom", "feedburner")):
            candidates.add(full)

    # <link href> tags in head
    for link in soup.find_all("link", href=True):
        href = link["href"].strip()
        href = clean_feedspot_infiniterss(href)
        full = urljoin(base_url, href)
        if any(tok in full.lower() for tok in (".xml", "rss", "feed", "atom")):
            candidates.add(full)

    # Text-based heuristics (rare)
    text = soup.get_text(" ")
    for token in text.split():
        if token.startswith("http") and any(x in token.lower() for x in (".xml", "rss", "feed", "atom")):
            candidates.add(token)

    # final filtering: remove mailto and js and blocklist
    final = []
    for u in sorted(candidates):
        if u.startswith("javascript:") or u.startswith("mailto:"):
            continue
        domain = urlparse(u).netloc.lower()
        if any(b in domain for b in BLOCKLIST_DOMAINS):
            logger.debug("Skipping blocked domain: %s", domain)
            continue
        final.append(u)
    return final

def parse_date_iso(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    try:
        dt = dateparser.parse(val)
        if dt:
            return dt.isoformat()
    except Exception:
        pass
    return None

# ---------------- Feed parsing with fallbacks ----------------
def normalize_entry_from_feedparser(entry: Any) -> Dict[str, Any]:
    """Return dict with exactly Author, Link, PubDate, Title (keys capitalized as requested)."""
    title = entry.get("title") or (entry.get("title_detail") or {}).get("value")
    link = entry.get("link")
    # Atom alternate hrefs
    if not link:
        for l in entry.get("links", []):
            if l.get("rel") in (None, "alternate") and l.get("href"):
                link = l.get("href")
                break
    author = entry.get("author") or (entry.get("author_detail") or {}).get("name")
    pub = entry.get("published") or entry.get("updated") or entry.get("pubDate")
    return {
        "Author": author.strip() if isinstance(author, str) else author,
        "Link": link.strip() if isinstance(link, str) else link,
        "PubDate": parse_date_iso(pub),
        "Title": title.strip() if isinstance(title, str) else title
    }

def fallback_parse_lxml(content: bytes, base_url: str) -> List[Dict[str, Any]]:
    items = []
    try:
        parser = etree.XMLParser(recover=True, encoding="utf-8")
        root = etree.fromstring(content, parser=parser)
        # find <item> and <entry>
        nodes = root.findall(".//item") + root.findall(".//{http://www.w3.org/2005/Atom}entry") + root.findall(".//entry")
        for el in nodes:
            title_el = el.find("title")
            title = title_el.text.strip() if title_el is not None and title_el.text else None

            # link
            link = None
            link_el = el.find("link")
            if link_el is not None:
                href = link_el.get("href")
                if href:
                    link = href.strip()
                elif link_el.text:
                    link = link_el.text.strip()
            if not link:
                guid = el.find("guid")
                if guid is not None and guid.text:
                    link = guid.text.strip()

            # author
            author = None
            auth_el = el.find("author")
            if auth_el is not None:
                # atom author name
                name = auth_el.find("name")
                if name is not None and name.text:
                    author = name.text.strip()
                elif auth_el.text:
                    author = auth_el.text.strip()
            if not author:
                dc = el.find("{http://purl.org/dc/elements/1.1/}creator")
                if dc is not None and dc.text:
                    author = dc.text.strip()

            # pubDate
            pub = None
            pd = el.find("pubDate") or el.find("{http://www.w3.org/2005/Atom}updated") or el.find("updated")
            if pd is not None and pd.text:
                pub = parse_date_iso(pd.text.strip())

            if title or link:
                items.append({
                    "Author": author,
                    "Link": link,
                    "PubDate": pub,
                    "Title": title
                })
    except Exception as e:
        logger.debug("lxml fallback error: %s", e)
    return items

def fallback_parse_html(content: str, base_url: str) -> List[Dict[str, Any]]:
    """Last-resort: scan HTML for article-like blocks."""
    soup = BeautifulSoup(content, "html.parser")
    found = []
    candidates = soup.find_all(["article", "li", "div"], limit=200)
    for c in candidates:
        a = c.find("a", href=True)
        title = None
        link = None
        pub = None
        author = None
        if a:
            title = a.get_text(strip=True)
            link = urljoin(base_url, a["href"])
        h = c.find(["h1", "h2", "h3"])
        if not title and h:
            title = h.get_text(strip=True)
        time_tag = c.find("time")
        if time_tag and time_tag.get("datetime"):
            pub = parse_date_iso(time_tag.get("datetime"))
        elif time_tag and time_tag.text:
            pub = parse_date_iso(time_tag.text.strip())
        author_tag = c.find(class_="author") or c.find("span", {"rel": "author"})
        if author_tag and author_tag.text:
            author = author_tag.get_text(strip=True)
        if title or link:
            found.append({
                "Author": author,
                "Link": link,
                "PubDate": pub,
                "Title": title
            })
    return found

def parse_feed_with_fallbacks(feed_url: str) -> List[Dict[str, Any]]:
    """
    Try feedparser -> lxml recover -> HTML heuristic.
    Return list of normalized items for this feed URL.
    """
    logger.debug("Parsing feed: %s", feed_url)
    headers = {"User-Agent": USER_AGENT}
    try:
        # First try feedparser by URL (it handles redirects)
        parsed = feedparser.parse(feed_url)
        if parsed and getattr(parsed, "entries", None):
            if getattr(parsed, "bozo", False):
                logger.warning("Feed parsing warning for %s : %s", feed_url, getattr(parsed, "bozo_exception", None))
            items = []
            for e in parsed.entries:
                n = normalize_entry_from_feedparser(e)
                # we want only the four fields requested; ensure at least Title or Link exists
                if n.get("Link") or n.get("Title"):
                    items.append(n)
            if items:
                return items
        # If feedparser didn't produce entries, fetch raw and try fallback parsing
        raw = fetch_url_text(feed_url)
        if not raw:
            logger.debug("No content for %s", feed_url)
            return []
        # try feedparser on content as well
        parsed2 = feedparser.parse(raw)
        if parsed2 and getattr(parsed2, "entries", None):
            items = []
            for e in parsed2.entries:
                n = normalize_entry_from_feedparser(e)
                if n.get("Link") or n.get("Title"):
                    items.append(n)
            if items:
                return items
        # lxml fallback (recover)
        try:
            content_bytes = raw.encode("utf-8") if isinstance(raw, str) else raw
            items_lxml = fallback_parse_lxml(content_bytes, feed_url)
            if items_lxml:
                logger.info("lxml fallback succeeded for %s -> %d items", feed_url, len(items_lxml))
                return items_lxml
        except Exception as e:
            logger.debug("lxml fallback exception for %s : %s", feed_url, e)
        # HTML fallback
        items_html = fallback_parse_html(raw, feed_url)
        if items_html:
            logger.info("HTML fallback succeeded for %s -> %d items", feed_url, len(items_html))
            return items_html
    except Exception as e:
        logger.exception("Unexpected error parsing feed %s : %s", feed_url, e)
    return []

# ---------------- Mongo upsert ----------------
def upsert_items(items: List[Dict[str, Any]]) -> Tuple[int, int]:
    """
    Upsert items into MongoDB.
    Return (inserted_count, updated_count).
    """
    inserted = 0
    updated = 0
    for it in items:
        # ensure we only store requested fields plus FetchedAt
        doc = {
            "Author": it.get("Author"),
            "Link": it.get("Link"),
            "PubDate": it.get("PubDate"),
            "Title": it.get("Title"),
            "FetchedAt": time.time()
        }
        # Build key
        if doc.get("Link"):
            key = {"Link": doc["Link"]}
        else:
            key = {"Title": doc.get("Title"), "PubDate": doc.get("PubDate")}
        try:
            res = coll.update_one(key, {"$set": doc, "$setOnInsert": {"FirstSeen": time.time()}}, upsert=True)
            if getattr(res, "upserted_id", None):
                inserted += 1
            elif getattr(res, "modified_count", 0):
                updated += 1
        except errors.DuplicateKeyError:
            # race condition or index conflict; ignore
            logger.debug("DuplicateKeyError for key %s", key)
        except Exception as e:
            logger.warning("Mongo write failed for %s : %s", key, e)
    return inserted, updated

# ---------------- High-level workflow ----------------
def gather_all_feed_urls(resources: List[str]) -> List[str]:
    all_feeds = []
    for res in resources:
        html = fetch_url_text(res)
        if not html:
            logger.warning("Could not fetch resource page %s", res)
            continue
        feeds = extract_feed_links_from_resource(html, res)
        logger.info("Resource %s -> extracted %d candidate feed URLs", res, len(feeds))
        all_feeds.extend(feeds)
    # dedupe
    uniq = sorted(set(all_feeds))
    logger.info("Total unique feed URLs discovered: %d", len(uniq))
    return uniq

def process_feed_list(feed_urls: List[str]) -> Tuple[int,int,int]:
    """Process feeds concurrently. Returns (feeds_processed, inserted_total, updated_total)."""
    feeds_processed = 0
    inserted_total = 0
    updated_total = 0
    if not feed_urls:
        return 0,0,0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        future_to_url = {ex.submit(parse_feed_with_fallbacks, url): url for url in feed_urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                items = future.result()
                feeds_processed += 1
                if not items:
                    logger.info("No items extracted from %s", url)
                    continue
                ins, upd = upsert_items(items)
                inserted_total += ins
                updated_total += upd
                logger.info("Feed %s -> items: %d (inserted %d, updated %d)", url, len(items), ins, upd)
            except Exception as e:
                logger.exception("Error processing feed %s : %s", url, e)
    return feeds_processed, inserted_total, updated_total

# ---------------- Main loop ----------------
def run_once():
    resources = read_resources()
    if not resources:
        logger.error("No resources to process. Exiting run_once.")
        return
    feed_urls = gather_all_feed_urls(resources)
    logger.info("Will process %d feeds now.", len(feed_urls))
    processed, inserted, updated = process_feed_list(feed_urls)
    logger.info("Run complete: processed %d feeds, inserted %d new items, updated %d items", processed, inserted, updated)

def main_loop():
    logger.info("Starting RSS aggregator main loop. Poll interval: %s seconds", POLL_INTERVAL_SECONDS)
    while True:
        try:
            run_once()
        except Exception as e:
            logger.exception("Top-level error during run_once: %s", e)
        logger.info("Sleeping %s seconds before next cycle...", POLL_INTERVAL_SECONDS)
        try:
            time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Interrupted by user. Exiting.")
            break

if __name__ == "__main__":
    main_loop()
