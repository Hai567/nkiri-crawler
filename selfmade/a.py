import requests
import os
from bs4 import BeautifulSoup
from urllib.parse import urlparse, unquote
import subprocess

def extract_filename(response, fallback_filename) -> str:
    '''
    Extracts the filename from the response headers
    '''
    if 'Content-Disposition' in response.headers:
        content_disposition = response.headers['Content-Disposition']
        if 'filename=' in content_disposition:
            filename = content_disposition.split('filename=')[1].strip('"\'')
            return filename
    return fallback_filename

if os.path.exists("./need_to_download.txt"):
    with open("./need_to_download.txt", "r") as f:
        urls = f.readlines()
else:
    print("No url file provided")

for url in urls:
    url = url.strip()
    if url:
        ''' Extracts all episode links from the main page URL '''
        res = requests.get(url, verify=False)
        series_name = urlparse(url).path.split("/")[1]
        download_dir = f"./{series_name}"
        os.makedirs(download_dir, exist_ok=True)
        
        soup = BeautifulSoup(res.text, "html.parser")
        episode_elements = soup.select("div > div.elementor > section.elementor-section.elementor-top-section.elementor-element.elementor-section-boxed.elementor-section-height-default.elementor-section-height-default > div > div.elementor-column.elementor-col-50.elementor-top-column.elementor-element > div > div > div > div > a")
        
        episode_urls = [element.get('href') for element in episode_elements if element.get('href')]

        for i, episode_url in enumerate(episode_urls):
            ''' Downloads an episode from Nkiri episode url '''
            if "downloadwella" in episode_url:
                # Handle downloadwella URLs
                request_headers = {
                    "Content-Type": "application/x-www-form-urlencoded",
                }
                request_data = {}
                
                episode_res = requests.get(episode_url, verify=False)
                
                episode_soup = BeautifulSoup(episode_res.text, "html.parser")
                episode_form = episode_soup.select_one("form")
                
                inputs = episode_form.select("input")
                for input_tag in inputs:
                    if input_tag.get("name") and input_tag.get("value"):
                        request_data[input_tag.get("name")] = input_tag.get("value")
                
                response = requests.post(headers=request_headers, data=request_data, stream=True, verify=False)
                file_name = extract_filename(response, f"{series_name} E{0 if i < 9 else ''}{i+1}")
                file_path = os.path.join(download_dir, file_name)
                
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Downloaded: {file_name} to {file_path}")
                subprocess.run(["rclone", "move", file_path, f"onedrive:nkiri/{series_name}", 
                                "--progress", "--stats-one-line", "--stats=15s", "--retries", "3", 
                                "--low-level-retries", "10", "--check", "--checksum", "--log-file=rclone-log.txt"
                                ])
            else:
                # Direct download URL
                    file_response = requests.get(episode_url, stream=True, verify=False)
                    # Safely extract filename
                    file_name = extract_filename(file_response, f"{series_name} E{0 if i < 9 else ''}{i+1}")
                    file_path = os.path.join(download_dir, file_name)
                    
                    with open(file_path, "wb") as f:
                        for chunk in file_response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print(f"Downloaded: {file_name} to {file_path}")
                    subprocess.run(["rclone", "move", file_path, f"onedrive:nkiri/{series_name}", 
                                    "--progress", "--stats-one-line", "--stats=15s", "--retries", "3", 
                                    "--low-level-retries", "10", "--check", "--checksum", "--log-file=rclone-log.txt"
                                    ])

        print(f"Downloaded all episodes for {series_name}")
