# 🧠 RSS AI Feed Analyzer

An intelligent **Cybersecurity RSS Aggregator and Analyzer** built with **FastAPI**, **MongoDB**, and **Ollama AI**.  
This project automatically collects, analyzes, and summarizes cybersecurity news and threat intelligence feeds using local AI language models.

---

## 🌍 Overview

The **RSS AI Feed Analyzer** helps cybersecurity researchers, SOC teams, and threat analysts stay informed about emerging cyber threats and vulnerabilities.  
It aggregates multiple RSS feeds, extracts their full content, stores them in MongoDB, and uses a **local LLM (Large Language Model)** to generate detailed AI-based summaries for quick insights.

This means you can stay updated on new **ransomware**, **CVE disclosures**, **APT campaigns**, or **zero-day exploits** — all summarized automatically by AI.

---

## ✨ Key Features

- 🔄 **RSS Feed Aggregation** — Collects cybersecurity articles from trusted RSS sources.  
- 🗄️ **MongoDB Integration** — Stores raw feeds, AI summaries, and user-submitted entries.  
- 🤖 **AI Summarization** — Uses local **Ollama (`gemma2:2b`)** model for natural-language summaries.  
- 🔍 **Advanced Search** — Search articles by title, author, or keyword.  
- 🧠 **Multi-Keyword Analysis** — Fetch multiple articles based on comma-separated keywords (e.g., `malware, ransomware, CVE`).  
- 🧰 **User Article Submission** — Add your own articles to the database for personal tracking.  
- 🧼 **Data Cleanup** — Remove old data automatically using a single API call.  
- 📤 **Export to JSON** — Easily export results for reporting or research.  

---

## 🧠 AI Integration Explained

The AI feature is what makes this project unique.  
It uses **Ollama’s local model** (`gemma2:2b`) to analyze full-length cybersecurity articles and generate comprehensive summaries.

**Workflow:**
1. Collects cybersecurity articles from RSS feeds.  
2. Extracts full article text using BeautifulSoup.  
3. Sends the text to the local LLM via Ollama.  
4. Receives a concise, human-readable summary written in natural language.  
5. Stores the summarized result in a dedicated MongoDB collection called `summaries`.  

The AI is prompted as a *cybersecurity expert*, ensuring technical accuracy and context-aware writing.

---

## ⚙️ Tech Stack

| Component | Description |
|------------|-------------|
| **Framework** | FastAPI |
| **Database** | MongoDB |
| **AI Model** | Ollama (`gemma2:2b`) |
| **Language** | Python 3.x |
| **Libraries** | Requests, BeautifulSoup, Feedparser, Pymongo, Uvicorn |
| **Environment** | Local / Server Deployment Supported |

---

## Create and activate a virtual environment
python -m venv venv
# Activate it
venv\Scripts\activate        # For Windows
source venv/bin/activate     # For macOS / Linux

## Install all dependencies
pip install -r requirements.txt

## Run MongoDB locally or remotely

If using local MongoDB:

sudo systemctl start mongod


Or connect to your cloud MongoDB Atlas instance by updating the connection URI in your code.

## Install and set up Ollama

Ollama allows you to run open-source LLMs locally.
Download and install from https://ollama.ai

Pull the required model:

ollama pull gemma2:2b

## Start the FastAPI server
uvicorn main:app --reload


Once running, open your browser and visit:

http://127.0.0.1:8000

## Access API docs (Swagger UI)

FastAPI automatically generates interactive documentation:

http://127.0.0.1:8000/docs

## 📡 API Endpoints
Endpoint	Method	Description
/	GET	Health check – verifies the API is running
/update	GET	Trigger a manual RSS feed update
/stats	GET	Get database statistics
/latest	GET	Retrieve the latest feed entries
/search?query=keyword	GET	Search articles by title, author, or link
/multi-keyword-search?keywords=malware,ransomware	GET	Search articles using multiple keywords
/multi-keyword-summary?keywords=malware,CVE	GET	Fetch and summarize multiple articles using AI
/add-user-article	POST	Manually add your own article
/cleanup?days=30	DELETE	Delete old entries older than given days
/export	GET	Export stored data in JSON format

📘 Example API Usage
## Fetch and summarize articles
GET /multi-keyword-summary?keywords=ransomware,CVE,APT


## Response Example:

{
  "count": 2,
  "keywords": ["ransomware", "CVE", "APT"],
  "summaries": [
    {
      "original": {
        "title": "New APT Group Exploits Zero-Day Vulnerability",
        "link": "https://example.com/article"
      },
      "summary": "A newly discovered Advanced Persistent Threat (APT) group has exploited a zero-day vulnerability in enterprise systems..."
    }
  ]
}

## Add your own article
POST /add-user-article


## Request Body:

{
  "link": "https://example.com/my-article",
  "title": "Custom Cybersecurity Research",
  "author": "Umer Hafiz",
  "content": "This is a manually added entry about phishing detection techniques."
}


## Response:

{
  "status": "success",
  "article": { "title": "Custom Cybersecurity Research", "source": "user" }
}

## 🧩 Example Workflow

Run /update to fetch latest RSS feeds.

Use /multi-keyword-search to identify relevant articles.

Run /multi-keyword-summary to get AI-generated summaries.

View stored results directly in MongoDB or export via /export.

## 🧰 Requirements (requirements.txt)
fastapi
uvicorn
pymongo
feedparser
beautifulsoup4
requests
schedule
ollama


## 🧱 Example MongoDB Collections
Collection Name	Purpose
feeds	Stores raw RSS articles
summaries	Stores AI-generated summaries
user_articles	Stores manually added entries

## 👨‍💻 Author
Hafiz Umer Mehmood
Python Developer | Cybersecurity & AI Enthusiast

🐍 Skilled in Python, FastAPI, MongoDB, and AI-Integrations for automation

🧠 Focused on building intelligent automation tools for security and data analysis

🌐 GitHub: umerhafiz2000-tech

## 🪪 License

This project is open source and available under the MIT License.
Feel free to use, modify, and contribute — just give credit.

## 💡 Summary

RSS AI Feed Analyzer showcases a complete end-to-end backend project combining data collection, AI summarization, and cybersecurity intelligence automation.
It’s a perfect example of integrating Python backend engineering with real-world AI applications.