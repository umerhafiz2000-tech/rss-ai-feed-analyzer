from fastapi import FastAPI, Query , Body
import ollama
import re
import requests
from bs4 import BeautifulSoup
from typing import Optional
from refinerss import RSSAggregator, DatabaseManager

app = FastAPI(title="Cyber Security RSS Aggregator API")

# Initialize once
aggregator = RSSAggregator()
db_manager = DatabaseManager()

# -------------------------------
# Utility: Fetch full article text
# -------------------------------
def fetch_article_text(url: str) -> str:
    """Fetch and extract main text from an article link"""
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return f"Failed to fetch article: {resp.status_code}"

        soup = BeautifulSoup(resp.text, "html.parser")

        # Collect paragraphs
        paragraphs = [p.get_text() for p in soup.find_all("p")]
        article_text = "\n".join(paragraphs)

        return article_text.strip() if article_text else "No readable text found."
    except Exception as e:
        return f"Error fetching article: {str(e)}"


# -------------------------------
# API Endpoints
# -------------------------------

@app.get("/")
def root():
    return {"message": "Cyber Security RSS Aggregator API is running 🚀"}

@app.get("/update")
def update_feeds():
    """Trigger manual feed update"""
    stats = aggregator.update_all_feeds()
    return {"status": "success", "stats": stats}

@app.get("/stats")
def get_stats():
    """Get database statistics"""
    return aggregator.get_database_stats()

@app.get("/latest")
def get_latest(limit: int = Query(20, ge=1, le=100)):
    """Fetch latest entries"""
    entries = list(db_manager.collection.find().sort("pubDate", -1).limit(limit))
    for e in entries:
        e["_id"] = str(e["_id"])  # Convert ObjectId to string
        if e.get("pubDate"):
            e["pubDate"] = e["pubDate"].isoformat()
    return {"count": len(entries), "entries": entries}

@app.get("/search")
def search_entries(query: str, limit: int = Query(10, ge=1, le=100)):
    """Search entries by title, author, or link"""
    search_query = {
        "$or": [
            {"title": {"$regex": query, "$options": "i"}},
            {"author": {"$regex": query, "$options": "i"}},
            {"link": {"$regex": query, "$options": "i"}},
        ]
    }
    entries = list(db_manager.collection.find(search_query).limit(limit))
    for e in entries:
        e["_id"] = str(e["_id"])
        if e.get("pubDate"):
            e["pubDate"] = e["pubDate"].isoformat()
    return {"count": len(entries), "entries": entries}

@app.get("/author")
def get_by_author(author: str, limit: int = Query(10, ge=1, le=100)):
    """Fetch entries by author"""
    entries = list(
        db_manager.collection.find(
            {"author": {"$regex": author, "$options": "i"}}
        ).sort("pubDate", -1).limit(limit)
    )
    for e in entries:
        e["_id"] = str(e["_id"])
        if e.get("pubDate"):
            e["pubDate"] = e["pubDate"].isoformat()
    return {"count": len(entries), "entries": entries}

@app.get("/export")
def export(limit: Optional[int] = None):
    """Export entries to JSON (returns inline instead of file)"""
    cursor = db_manager.collection.find({})
    if limit:
        cursor = cursor.limit(limit)
    entries = []
    for e in cursor:
        e["_id"] = str(e["_id"])
        e["pubDate"] = e["pubDate"].isoformat() if e.get("pubDate") else None
        entries.append(e)
    return {"count": len(entries), "entries": entries}

@app.delete("/cleanup")
def cleanup(days: int = Query(30, ge=1, le=365)):
    """Remove entries older than given days"""
    from datetime import datetime, timezone, timedelta
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    result = db_manager.collection.delete_many({"pubDate": {"$lt": cutoff_date}})
    return {"status": "success", "deleted": result.deleted_count}


# -------------------------------
# NEW: Multi-keyword Search
# -------------------------------

@app.get("/multi-keyword-search")
def multi_keyword_search(keywords: str, limit: int = Query(50, ge=1, le=1000)):
    """
    Search all articles based on multiple comma-separated keywords.
    Example: /multi-keyword-search?keywords=malware,ransomware,AI
    """
    keyword_list = [kw.strip() for kw in keywords.split(",") if kw.strip()]

    query = {"$or": []}
    for kw in keyword_list:
        regex = fr"\b{kw}\b"
        query["$or"].extend([
            {"title": {"$regex": regex, "$options": "i"}},
            {"author": {"$regex": regex, "$options": "i"}},
            {"link": {"$regex": regex, "$options": "i"}},
        ])

    entries = list(db_manager.collection.find(query).limit(limit))
    for e in entries:
        e["_id"] = str(e["_id"])
        if e.get("pubDate"):
            e["pubDate"] = e["pubDate"].isoformat()

    return {"count": len(entries), "keywords": keyword_list, "entries": entries}


@app.get("/multi-keyword-summary")
def multi_keyword_summary(keywords: str, limit: int = Query(5, ge=1, le=50)):
    """
    Search articles by keywords, fetch full article content,
    generate summaries using local Ollama model,
    and store summaries in a separate MongoDB collection.
    """
    keyword_list = [kw.strip() for kw in keywords.split(",") if kw.strip()]

    query = {"$or": []}
    for kw in keyword_list:
        regex = fr"\b{kw}\b"
        query["$or"].extend([
            {"title": {"$regex": regex, "$options": "i"}},
            {"author": {"$regex": regex, "$options": "i"}},
            {"link": {"$regex": regex, "$options": "i"}},
        ])

    # Fetch matching articles
    entries = list(db_manager.collection.find(query).limit(limit))
    for e in entries:
        e["_id"] = str(e["_id"])
        if e.get("pubDate"):
            e["pubDate"] = e["pubDate"].isoformat()

    summaries = []
    for e in entries:
        article_text = fetch_article_text(e.get("link", ""))

        response = ollama.chat(
            model="gemma2:2b",
            messages=[
                {"role": "system", "content": "You are a cybersecurity expert. Summarize the article in plain text. "
                                               "Your summary must cover every aspect of the article in detail, expand all abbreviations once, "
                                               "and write as a continuous paragraph without line breaks, lists, asterisks, or markdown formatting."},
                {"role": "user", "content": article_text},
            ],
        )

        summary = response["message"]["content"]
        summary = re.sub(r"[\n\r]+", " ", summary)  
        summary = re.sub(r"\s+", " ", summary).strip()

        # ✅ Store in a separate "summaries" collection
        db_manager.db["summaries"].update_one(
            {"link": e.get("link")},   # use link as unique key
            {"$set": {
                "title": e.get("title"),
                "author": e.get("author"),
                "pubDate": e.get("pubDate"),
                "link": e.get("link"),
                "summary": summary,
                "keywords": keyword_list
            }},
            upsert=True
        )

        summaries.append({
            "original": e,
            "summary": summary
        })

    return {"count": len(summaries), "keywords": keyword_list, "summaries": summaries}


@app.post("/add-user-article")
def add_user_article(
    link: str = Body(..., embed=True),
    title: Optional[str] = Body(None),
    author: Optional[str] = Body(None),
    pubDate: Optional[str] = Body(None),
    content: Optional[str] = Body(None),
):
    """
    Add a new article manually into a separate 'user_articles' collection.
    - Only 'link' is required
    - Other fields are optional
    - Prevents duplicates based on link
    """

    user_articles = db_manager.db["user_articles"]

    # ✅ Prevent duplication
    existing = user_articles.find_one({"link": link})
    if existing:
        return {
            "status": "duplicate",
            "message": "Article already exists in user_articles",
            "article": existing,
        }

    # Build document
    new_article = {
        "link": link,
        "title": title,
        "author": author,
        "pubDate": pubDate,
        "content": content,
        "source": "user"   # mark clearly that it came from user input
    }

    # Insert into user_articles collection
    result = user_articles.insert_one(new_article)
    new_article["_id"] = str(result.inserted_id)

    return {"status": "success", "article": new_article}


# @app.get("/multi-keyword-summary")
# def multi_keyword_summary(keywords: str, limit: int = Query(5, ge=1, le=50)):
#     """
#     Search articles by keywords, fetch full article content,
#     and generate summaries using local Ollama model.
#     """
#     keyword_list = [kw.strip() for kw in keywords.split(",") if kw.strip()]

#     query = {"$or": []}
#     for kw in keyword_list:
#         regex = fr"\b{kw}\b"
#         query["$or"].extend([
#             {"title": {"$regex": regex, "$options": "i"}},
#             {"author": {"$regex": regex, "$options": "i"}},
#             {"link": {"$regex": regex, "$options": "i"}},
#         ])

#     # Fetch matching articles
#     entries = list(db_manager.collection.find(query).limit(limit))
#     for e in entries:
#         e["_id"] = str(e["_id"])
#         if e.get("pubDate"):
#             e["pubDate"] = e["pubDate"].isoformat()

#     summaries = []
#     for e in entries:
#         article_text = fetch_article_text(e.get("link", ""))

#         response = ollama.chat(
#             model="gemma2:2b",  # or "mistral", "gemma"
#             messages=[
#                 {"role": "system", "content": "You are a cybersecurity expert. Summarize the article in plain text. "
#                                                "Your summary must cover every aspect of the article in detail, expand all abbreviations once, "
#                                                "and write as a continuous paragraph without line breaks, lists, asterisks, or markdown formatting."},
#                 {"role": "user", "content": article_text},
#             ],
#         )

#         summary = response["message"]["content"]
#         # Remove newlines, multiple spaces, asterisks, markdown artifacts
#         summary = re.sub(r"[\n\r]+", " ", summary)  
#         summary = re.sub(r"\s+", " ", summary).strip()
#         summaries.append({
#             "original": e,
#             "summary": summary
#         })

#     return {"count": len(summaries), "keywords": keyword_list, "summaries": summaries}


# -------------------------------
# Predefined keywords (backend-controlled)
# -------------------------------
# PREDEFINED_KEYWORDS = [
#     "ransomware",
#     "APT",
#     "Cisco",
#     "zero-day",
#     "CVE",
#     "phishing",
#     "supply chain attack"
# ]


# @app.get("/keyword-hits")
# def keyword_hits(limit: int = Query(20, ge=1, le=200)):
#     """
#     Fetch articles that contain any of the predefined keywords.
#     No summarization is done here.
#     """
#     query = {"$or": []}
#     for kw in PREDEFINED_KEYWORDS:
#         regex = fr"\b{kw}\b"
#         query["$or"].extend([
#             {"title": {"$regex": regex, "$options": "i"}},
#             {"author": {"$regex": regex, "$options": "i"}},
#             {"link": {"$regex": regex, "$options": "i"}},
#         ])

#     entries = list(db_manager.collection.find(query).sort("pubDate", -1).limit(limit))
#     for e in entries:
#         e["_id"] = str(e["_id"])
#         if e.get("pubDate"):
#             e["pubDate"] = e["pubDate"].isoformat()

#     return {
#         "count": len(entries),
#         "keywords": PREDEFINED_KEYWORDS,
#         "entries": entries
#     }
