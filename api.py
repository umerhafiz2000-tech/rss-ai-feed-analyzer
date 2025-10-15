from fastapi import FastAPI, Query
from typing import Optional
from refinerss import RSSAggregator, DatabaseManager

app = FastAPI(title="Cyber Security RSS Aggregator API")

# Initialize once
aggregator = RSSAggregator()
db_manager = DatabaseManager()

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


# @app.get("/multi-keyword-search-harcode")
# def multi_keyword_search_hardcode(limit: int = Query(50, ge=1, le=1000)):
#     """
#     Search all articles based on predefined keywords.
#     """
#     # 🔹 Hardcoded keywords
#     keyword_list = ["AI", "cybersecurity", "ransomware","malware"]

#     query = {"$or": []}
#     for kw in keyword_list:
#         regex = fr"\b{kw}\b"
#         query["$or"].extend([
#             {"title": {"$regex": regex, "$options": "i"}},
#             {"author": {"$regex": regex, "$options": "i"}},
#             {"link": {"$regex": regex, "$options": "i"}},
#             {"description": {"$regex": regex, "$options": "i"}},  # optionally search body
#         ])

#     entries = list(db_manager.collection.find(query).limit(limit))
#     for e in entries:
#         e["_id"] = str(e["_id"])
#         if e.get("pubDate"):
#             e["pubDate"] = e["pubDate"].isoformat()

#     return {"count": len(entries), "keywords": keyword_list, "entries": entries}



# from fastapi import FastAPI, Query
# import ollama
# from typing import Optional
# from refinerss import RSSAggregator, DatabaseManager

# app = FastAPI(title="Cyber Security RSS Aggregator API")

# # Initialize once
# aggregator = RSSAggregator()
# db_manager = DatabaseManager()

# # -------------------------------
# # API Endpoints
# # -------------------------------

# @app.get("/")
# def root():
#     return {"message": "Cyber Security RSS Aggregator API is running 🚀"}

# @app.get("/update")
# def update_feeds():
#     """Trigger manual feed update"""
#     stats = aggregator.update_all_feeds()
#     return {"status": "success", "stats": stats}

# @app.get("/stats")
# def get_stats():
#     """Get database statistics"""
#     return aggregator.get_database_stats()

# @app.get("/latest")
# def get_latest(limit: int = Query(20, ge=1, le=100)):
#     """Fetch latest entries"""
#     entries = list(db_manager.collection.find().sort("pubDate", -1).limit(limit))
#     for e in entries:
#         e["_id"] = str(e["_id"])  # Convert ObjectId to string
#         if e.get("pubDate"):
#             e["pubDate"] = e["pubDate"].isoformat()
#     return {"count": len(entries), "entries": entries}

# @app.get("/search")
# def search_entries(query: str, limit: int = Query(10, ge=1, le=100)):
#     """Search entries by title, author, or link"""
#     search_query = {
#         "$or": [
#             {"title": {"$regex": query, "$options": "i"}},
#             {"author": {"$regex": query, "$options": "i"}},
#             {"link": {"$regex": query, "$options": "i"}},
#         ]
#     }
#     entries = list(db_manager.collection.find(search_query).limit(limit))
#     for e in entries:
#         e["_id"] = str(e["_id"])
#         if e.get("pubDate"):
#             e["pubDate"] = e["pubDate"].isoformat()
#     return {"count": len(entries), "entries": entries}

# @app.get("/author")
# def get_by_author(author: str, limit: int = Query(10, ge=1, le=100)):
#     """Fetch entries by author"""
#     entries = list(
#         db_manager.collection.find(
#             {"author": {"$regex": author, "$options": "i"}}
#         ).sort("pubDate", -1).limit(limit)
#     )
#     for e in entries:
#         e["_id"] = str(e["_id"])
#         if e.get("pubDate"):
#             e["pubDate"] = e["pubDate"].isoformat()
#     return {"count": len(entries), "entries": entries}

# @app.get("/export")
# def export(limit: Optional[int] = None):
#     """Export entries to JSON (returns inline instead of file)"""
#     cursor = db_manager.collection.find({})
#     if limit:
#         cursor = cursor.limit(limit)
#     entries = []
#     for e in cursor:
#         e["_id"] = str(e["_id"])
#         e["pubDate"] = e["pubDate"].isoformat() if e.get("pubDate") else None
#         entries.append(e)
#     return {"count": len(entries), "entries": entries}

# @app.delete("/cleanup")
# def cleanup(days: int = Query(30, ge=1, le=365)):
#     """Remove entries older than given days"""
#     from datetime import datetime, timezone, timedelta
#     cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
#     result = db_manager.collection.delete_many({"pubDate": {"$lt": cutoff_date}})
#     return {"status": "success", "deleted": result.deleted_count}

# # -------------------------------
# # NEW: Multi-keyword Search
# # -------------------------------

# @app.get("/multi-keyword-search")
# def multi_keyword_search(keywords: str, limit: int = Query(100, ge=1, le=1000)):
#     """
#     Search all articles based on multiple comma-separated keywords.
#     Example: /multi-keyword-search?keywords=malware,ransomware,AI
#     """
#     keyword_list = [kw.strip() for kw in keywords.split(",") if kw.strip()]

#     query = {"$or": []}
#     for kw in keyword_list:
#         # Use word boundary regex for exact keyword match
#         regex = fr"\b{kw}\b"
#         query["$or"].extend([
#             {"title": {"$regex": regex, "$options": "i"}},
#             {"author": {"$regex": regex, "$options": "i"}},
#             {"link": {"$regex": regex, "$options": "i"}},
#         ])

#     entries = list(db_manager.collection.find(query).limit(limit))
#     for e in entries:
#         e["_id"] = str(e["_id"])
#         if e.get("pubDate"):
#             e["pubDate"] = e["pubDate"].isoformat()

#     return {"count": len(entries), "keywords": keyword_list, "entries": entries}
