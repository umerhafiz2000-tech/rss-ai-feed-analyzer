import requests

url = "https://heimdalsecurity.com/blog/feed/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/140.0.0.0 Safari/537.36"
}

resp = requests.get(url, headers=headers, timeout=10)
print(resp.status_code)
print(resp.text[:200])
