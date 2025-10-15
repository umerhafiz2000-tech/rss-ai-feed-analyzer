from curl_cffi import requests

url = "https://www.sdmmag.com/rss/topic/6802-cybersecurity-chronicles"
r = requests.get(url, impersonate="chrome")
print(r.status_code, len(r.text))
print(r.text[:100000])
