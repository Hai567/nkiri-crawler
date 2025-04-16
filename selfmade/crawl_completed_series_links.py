import requests
from bs4 import BeautifulSoup
import time

url = "https://nkiri.com/page/{page_num}/?s=complete&post_type=post"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://nkiri.com/',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
}

# Load existing URLs to avoid duplicates
existing_urls = set()
try:
    with open("completed_series_links.txt", "r") as file:
        existing_urls = set(line.strip() for line in file)
    print(f"Loaded {len(existing_urls)} existing URLs")
except FileNotFoundError:
    print("No existing file found, creating new one")

new_urls = set()
page_num = 0
max_failures = 3
failures = 0

while failures < max_failures:
    page_num += 1
    try:
        print(f"Crawling page {page_num}...")
        response = requests.get(url.format(page_num=page_num), headers=headers)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            aTags = soup.select("div > div.search-entry-content.clr > header > h2 > a")
            
            if not aTags:
                print(f"No links found on page {page_num}, might be the end")
                failures += 1
                continue
                
            page_urls = set()
            for aTag in aTags:
                link_url = aTag.get("href")
                if link_url and link_url not in existing_urls:
                    page_urls.add(link_url)
            
            if page_urls:
                new_urls.update(page_urls)
                print(f"Found {len(page_urls)} new links on page {page_num}")
            else:
                print(f"No new links on page {page_num}")
                
            # Sleep to avoid overloading the server
            time.sleep(1)
        else:
            print(f"Failed to fetch page {page_num}: status code {response.status_code}")
            failures += 1
    except Exception as e:
        print(f"Error on page {page_num}: {str(e)}")
        failures += 1
        time.sleep(5)  # Wait longer on error

# Write new URLs to file
if new_urls:
    with open("completed_series_links.txt", "a+") as file:
        for url in new_urls:
            file.write(url + "\n")
    print(f"Added {len(new_urls)} new URLs to the file")
else:
    print("No new URLs found")