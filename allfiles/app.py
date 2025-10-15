from flask import Flask, render_template, jsonify
from pymongo import MongoClient

app = Flask(__name__)

# ===== MongoDB Config =====
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "cyber_rss_db"
COLLECTION_NAME = "rss_articles"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# ===== Routes =====
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/feeds")
def get_feeds():
    articles = list(collection.find({}, {"_id": 0}).sort("pubDate", -1))
    return jsonify(articles)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
