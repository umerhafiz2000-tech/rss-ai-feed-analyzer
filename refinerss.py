
#!/usr/bin/env python3
"""
RSS Aggregator - Standalone Single File Version
A comprehensive RSS feed aggregator for cyber security feeds with database management capabilities.

This standalone version combines all functionality into a single file:
- RSS feed parsing and aggregation
- MongoDB database management
- Command-line interface for database operations
- Configuration management
- Logging and error handling

Usage:
    # Run the aggregator (continuous mode)
    python rss_aggregator_standalone.py

    # Database management commands
    python rss_aggregator_standalone.py stats
    python rss_aggregator_standalone.py search --query "malware"
    python rss_aggregator_standalone.py latest --limit 20
    python rss_aggregator_standalone.py author --author "John Doe"
    python rss_aggregator_standalone.py export --file export.json
    python rss_aggregator_standalone.py cleanup --days 30
    python rss_aggregator_standalone.py update
"""

import os
import sys
import feedparser
import requests
import pymongo
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta
import logging
import time
import schedule
import hashlib
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import re
import json
import argparse
from typing import List, Dict, Optional

# =============================================================================
# CONFIGURATION
# =============================================================================

# MongoDB Configuration
MONGODB_URI = 'mongodb://localhost:27017/'
DATABASE_NAME = 'refinecyber_security_feeds'
COLLECTION_NAME = 'refinefeed_entries'

# RSS Feed Configuration
FEED_UPDATE_INTERVAL_MINUTES = 1
MAX_RETRIES = 3
REQUEST_TIMEOUT = 60

# Logging Configuration
LOG_LEVEL = 'INFO'
LOG_FILE = 'logs/rss_aggregator.log'

# FeedSpot RSS Feeds URL
FEEDSPOT_URL = "https://rss.feedspot.com/cyber_security_rss_feeds/"

# =============================================================================
# LOGGING SETUP
# =============================================================================

# Create logs directory if it doesn't exist
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# RSS AGGREGATOR CLASS
# =============================================================================


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/140.0.0.0 Safari/537.36"
}

# HEADERS = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                   "AppleWebKit/537.36 (KHTML, like Gecko) "
#                   "Chrome/140.0.0.0 Safari/537.36",
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#     "Accept-Language": "en-US,en;q=0.5",
#     "Accept-Encoding": "gzip, deflate, br",
#     "Connection": "keep-alive",
#     "Upgrade-Insecure-Requests": "1",
# }


class RSSAggregator:
    def __init__(self):
        """Initialize the RSS Aggregator with MongoDB connection."""
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DATABASE_NAME]
        self.collection = self.db[COLLECTION_NAME]
        
        # Create indexes for better performance
        self.collection.create_index("link", unique=True)
        self.collection.create_index("pubDate")
        self.collection.create_index("author")
        
        logger.info("RSS Aggregator initialized successfully")
    
    def get_feed_urls_from_feedspot(self) -> List[str]:
        """
        Extract RSS feed URLs from the FeedSpot cyber security feeds page.
        """
        try:
            logger.info(f"Fetching feed URLs from {FEEDSPOT_URL}")
            response = requests.get(FEEDSPOT_URL,  headers=HEADERS ,timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            feed_urls = []
            
            # Look for RSS feed links in various formats
            # Common patterns for RSS feed links
            rss_patterns = [
                r'https?://[^\s<>"]+\.xml',
                r'https?://[^\s<>"]+/feed',
                r'https?://[^\s<>"]+/rss',
                r'https?://[^\s<>"]+/atom',
                r'https?://[^\s<>"]+/html'
            ]
            
            # Find all links that might be RSS feeds
            for link in soup.find_all('a', href=True):
                href = link['href']
                if any(re.search(pattern, href, re.IGNORECASE) for pattern in rss_patterns):
                    feed_urls.append(href)
            
            # Also look for direct RSS URLs in the page content
            page_text = soup.get_text()
            for pattern in rss_patterns:
                matches = re.findall(pattern, page_text, re.IGNORECASE)
                feed_urls.extend(matches)
            
            # Remove duplicates and filter valid URLs
            unique_urls = list(set(feed_urls))
            valid_urls = []
            
            for url in unique_urls:
                try:
                    parsed = urlparse(url)
                    if "feedspot.com" in parsed.netloc:
                        continue
                    if parsed.scheme in ['http', 'https'] and parsed.netloc:
                        valid_urls.append(url)
                except:
                    continue
            
            logger.info(f"Found {len(valid_urls)} valid RSS feed URLs")
            for url in valid_urls:
                print(f" - {url}")
            return valid_urls
            
        except Exception as e:
            logger.error(f"Error fetching feed URLs from FeedSpot: {e}")
            return []
    
    def parse_feed(self, feed_url: str) -> Optional[Dict]:
        """
        Parse a single RSS feed and return its entries.
        """
        try:
            logger.debug(f"Parsing feed: {feed_url}")
            response = requests.get(feed_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            feed = feedparser.parse(response.content)
            #feed = feedparser.parse(feed_url)
            
            if feed.bozo:
                logger.warning(f"Feed parsing warning for {feed_url}: {feed.bozo_exception}")
            
            if not feed.entries:
                logger.warning(f"No entries found in feed: {feed_url}")
                return None
            
            entries = []
            for entry in feed.entries:
                try:
                    # Extract required fields
                    title = entry.get('title', '').strip()
                    link = entry.get('link', '').strip()
                    author = entry.get('author', entry.get('author_detail', {}).get('name', 'Unknown')).strip()
                    
                    # Handle pubDate
                    pub_date = None
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        pub_date = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
                    else:
                        pub_date = datetime.now(timezone.utc)
                    
                    # Create unique hash for deduplication
                    content_hash = hashlib.md5(f"{title}{link}{author}".encode()).hexdigest()
                    
                    entry_data = {
                        'title': title,
                        'link': link,
                        'author': author,
                        'pubDate': pub_date
                    }
                    
                    entries.append(entry_data)
                    
                except Exception as e:
                    logger.error(f"Error processing entry in feed {feed_url}: {e}")
                    continue
            
            logger.info(f"Successfully parsed {len(entries)} entries from {feed_url}")
            return {
                'feed_url': feed_url,
                'entries': entries,
                'total_entries': len(entries)
            }
            
        except Exception as e:
            logger.error(f"Error parsing feed {feed_url}: {e}")
            return None
    
    def store_entries(self, feed_data: Dict) -> int:
        """
        Store feed entries in MongoDB with deduplication.
        Returns the number of new entries stored.
        """
        if not feed_data or not feed_data.get('entries'):
            return 0
        
        new_entries_count = 0
        feed_url = feed_data['feed_url']
        
        for entry in feed_data['entries']:
            try:
                # Check if entry already exists using link as unique identifier
                existing_entry = self.collection.find_one({'link': entry['link']})
                
                if existing_entry:
                    # Update existing entry if content has changed
                    if existing_entry.get('title') != entry['title'] or existing_entry.get('author') != entry['author']:
                        self.collection.update_one(
                            {'link': entry['link']},
                            {'$set': entry}
                        )
                        logger.debug(f"Updated existing entry: {entry['title']}")
                else:
                    # Insert new entry
                    self.collection.insert_one(entry)
                    new_entries_count += 1
                    logger.debug(f"Added new entry: {entry['title']}")
                    
            except pymongo.errors.DuplicateKeyError:
                logger.debug(f"Entry already exists (duplicate key): {entry['title']}")
            except Exception as e:
                logger.error(f"Error storing entry {entry.get('title', 'Unknown')}: {e}")
        
        logger.info(f"Stored {new_entries_count} new entries from {feed_url}")
        return new_entries_count
    
    def update_all_feeds(self) -> Dict[str, int]:
        """
        Update all RSS feeds and return statistics.
        """
        logger.info("Starting RSS feed update process")
        start_time = time.time()
        
        # Get all feed URLs
        feed_urls = self.get_feed_urls_from_feedspot()
        
        if not feed_urls:
            logger.warning("No feed URLs found")
            return {'total_feeds': 0, 'successful_feeds': 0, 'total_entries': 0, 'new_entries': 0}
        
        stats = {
            'total_feeds': len(feed_urls),
            'successful_feeds': 0,
            'total_entries': 0,
            'new_entries': 0
        }
        
        for feed_url in feed_urls:
            try:
                feed_data = self.parse_feed(feed_url)
                if feed_data:
                    stats['successful_feeds'] += 1
                    stats['total_entries'] += feed_data['total_entries']
                    new_entries = self.store_entries(feed_data)
                    stats['new_entries'] += new_entries
                
                # Add delay between requests to be respectful
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing feed {feed_url}: {e}")
                continue
        
        elapsed_time = time.time() - start_time
        logger.info(f"Feed update completed in {elapsed_time:.2f} seconds. "
                   f"Stats: {stats}")
        
        return stats
    
    def get_database_stats(self) -> Dict:
        """
        Get statistics about the database.
        """
        try:
            total_entries = self.collection.count_documents({})
            unique_authors = len(self.collection.distinct('author'))
            
            # Get recent entries (last 24 hours)
            yesterday = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            recent_entries = self.collection.count_documents({'pubDate': {'$gte': yesterday}})
            
            return {
                'total_entries': total_entries,
                'unique_authors': unique_authors,
                'recent_entries_24h': recent_entries
            }
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {}
    

# =============================================================================
# DATABASE MANAGER CLASS
# =============================================================================

class DatabaseManager:
    def __init__(self):
        """Initialize database manager."""
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DATABASE_NAME]
        self.collection = self.db[COLLECTION_NAME]
    
    def show_stats(self):
        """Display database statistics."""
        print("\n=== Database Statistics ===")
        
        try:
            total_entries = self.collection.count_documents({})
            unique_authors = len(self.collection.distinct('author'))
            
            # Recent entries (last 24 hours)
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            recent_entries = self.collection.count_documents({'pubDate': {'$gte': yesterday}})
            
            print(f"Total entries: {total_entries}")
            print(f"Unique authors: {unique_authors}")
            print(f"Recent entries (24h): {recent_entries}")
                
        except Exception as e:
            print(f"Error getting stats: {e}")
    
    def search_entries(self, query, limit=10):
        """Search entries by title, author, or content."""
        print(f"\n=== Search Results for '{query}' ===")
        
        try:
            # Create search query
            search_query = {
                '$or': [
                    {'title': {'$regex': query, '$options': 'i'}},
                    {'author': {'$regex': query, '$options': 'i'}},
                    {'link': {'$regex': query, '$options': 'i'}}
                ]
            }
            
            entries = list(self.collection.find(search_query).limit(limit))
            
            if not entries:
                print("No entries found.")
                return
            
            for i, entry in enumerate(entries, 1):
                print(f"\n{i}. {entry['title']}")
                print(f"   Author: {entry['author']}")
                print(f"   Date: {entry['pubDate']}")
                print(f"   Link: {entry['link']}")
                
        except Exception as e:
            print(f"Error searching entries: {e}")
    
    def get_latest_entries(self, limit=20):
        """Get the latest entries."""
        print(f"\n=== Latest {limit} Entries ===")
        
        try:
            entries = list(self.collection.find().sort('pubDate', -1).limit(limit))
            
            if not entries:
                print("No entries found.")
                return
            
            for i, entry in enumerate(entries, 1):
                print(f"\n{i}. {entry['title']}")
                print(f"   Author: {entry['author']}")
                print(f"   Date: {entry['pubDate']}")
                print(f"   Link: {entry['link']}")
                
        except Exception as e:
            print(f"Error getting latest entries: {e}")
    
    def get_entries_by_author(self, author, limit=10):
        """Get entries by specific author."""
        print(f"\n=== Entries by {author} ===")
        
        try:
            entries = list(self.collection.find(
                {'author': {'$regex': author, '$options': 'i'}}
            ).sort('pubDate', -1).limit(limit))
            
            if not entries:
                print("No entries found.")
                return
            
            for i, entry in enumerate(entries, 1):
                print(f"\n{i}. {entry['title']}")
                print(f"   Author: {entry['author']}")
                print(f"   Date: {entry['pubDate']}")
                print(f"   Link: {entry['link']}")
                
        except Exception as e:
            print(f"Error getting entries by author: {e}")
    
    def export_to_json(self, filename, limit=None):
        """Export entries to JSON file."""
        print(f"\n=== Exporting to {filename} ===")
        
        try:
            query = {} if limit is None else {}
            cursor = self.collection.find(query)
            
            if limit:
                cursor = cursor.limit(limit)
            
            entries = []
            for entry in cursor:
                # Convert datetime objects to strings for JSON serialization
                entry['pubDate'] = entry['pubDate'].isoformat()
                entries.append(entry)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(entries, f, indent=2, ensure_ascii=False)
            
            print(f"Exported {len(entries)} entries to {filename}")
            
        except Exception as e:
            print(f"Error exporting to JSON: {e}")
    
    def cleanup_old_entries(self, days=30):
        """Remove entries older than specified days."""
        print(f"\n=== Cleaning up entries older than {days} days ===")
        
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
            result = self.collection.delete_many({'pubDate': {'$lt': cutoff_date}})
            print(f"Removed {result.deleted_count} old entries")
            
        except Exception as e:
            print(f"Error cleaning up old entries: {e}")
    
    def run_manual_update(self):
        """Run a manual RSS feed update."""
        print("\n=== Running Manual Update ===")
        
        try:
            aggregator = RSSAggregator()
            stats = aggregator.update_all_feeds()
            print(f"Update completed: {stats}")
            
        except Exception as e:
            print(f"Error running manual update: {e}")

# =============================================================================
# MAIN FUNCTIONS
# =============================================================================

def run_aggregator():
    """Run the RSS aggregator in continuous mode."""
    aggregator = RSSAggregator()
    
    # Schedule regular updates
    schedule.every(FEED_UPDATE_INTERVAL_MINUTES).minutes.do(aggregator.update_all_feeds)
    
    # Run initial update
    logger.info("Running initial feed update...")
    stats = aggregator.update_all_feeds()
    logger.info(f"Initial update completed: {stats}")
    
    # Show database stats
    db_stats = aggregator.get_database_stats()
    logger.info(f"Database stats: {db_stats}")
    
    # Keep the script running
    logger.info(f"Starting scheduled updates every {FEED_UPDATE_INTERVAL_MINUTES} minutes...")
    try:
        while True:
            schedule.run_pending()
            logger.debug("Checked for scheduled jobs...")
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("RSS Aggregator stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

def main():
    """Main function for command-line interface."""
    parser = argparse.ArgumentParser(
        description='RSS Aggregator - Standalone Single File Version',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python rss_aggregator_standalone.py                    # Run aggregator (continuous mode)
  python rss_aggregator_standalone.py stats              # Show database statistics
  python rss_aggregator_standalone.py search --query "malware"  # Search entries
  python rss_aggregator_standalone.py latest --limit 20  # Show latest 20 entries
  python rss_aggregator_standalone.py author --author "John Doe"  # Show entries by author
  python rss_aggregator_standalone.py export --file export.json  # Export to JSON
  python rss_aggregator_standalone.py cleanup --days 30  # Clean up old entries
  python rss_aggregator_standalone.py update             # Run manual update
        """
    )
    
    parser.add_argument('command', nargs='?', choices=[
        'stats', 'search', 'latest', 'author', 'export', 'cleanup', 'update'
    ], help='Command to execute (omit to run aggregator in continuous mode)')
    parser.add_argument('--query', '-q', help='Search query')
    parser.add_argument('--limit', '-l', type=int, default=10, help='Limit results')
    parser.add_argument('--author', '-a', help='Author name for author search')
    parser.add_argument('--file', '-f', help='Output file for export')
    parser.add_argument('--days', '-d', type=int, default=30, help='Days for cleanup')
    
    args = parser.parse_args()
    
    # If no command specified, run the aggregator
    if args.command is None:
        run_aggregator()
        return
    
    # Otherwise, run database management commands
    db_manager = DatabaseManager()
    
    if args.command == 'stats':
        db_manager.show_stats()
    elif args.command == 'search':
        if not args.query:
            print("Error: --query is required for search command")
            return
        db_manager.search_entries(args.query, args.limit)
    elif args.command == 'latest':
        db_manager.get_latest_entries(args.limit)
    elif args.command == 'author':
        if not args.author:
            print("Error: --author is required for author command")
            return
        db_manager.get_entries_by_author(args.author, args.limit)
    elif args.command == 'export':
        filename = args.file or f"rss_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        db_manager.export_to_json(filename, args.limit)
    elif args.command == 'cleanup':
        db_manager.cleanup_old_entries(args.days)
    elif args.command == 'update':
        db_manager.run_manual_update()

if __name__ == "__main__":
    main()
