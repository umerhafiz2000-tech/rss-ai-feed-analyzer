import requests

try:
    url = "https://news.aliasrobotics.com/rss/"
    response = requests.get(url)
    response.raise_for_status()
    print("✅ SSL connection successful!")
    print(f"Status code: {response.status_code}")
    print(f"Content length: {len(response.text)} characters")
except requests.exceptions.SSLError as ssl_err:
    print("❌ SSL error:", ssl_err)
except Exception as e:
    print("❌ Error:", e)
