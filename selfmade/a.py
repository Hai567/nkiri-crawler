import requests
import os
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import subprocess
import warnings
import logging

warnings.filterwarnings("ignore")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Tracking files
DOWNLOADED_SERIES_FILE = "./downloaded_series.txt"
DOWNLOADED_EPISODES_FILE = "./downloaded_episodes.txt"

def rclone_upload_file(file_path, target_dir):
    output = subprocess.run(["rclone", "move", file_path, target_dir, 
                            "--progress", "--stats-one-line", "--stats=15s", "--retries", "3", 
                            "--low-level-retries", "10", "--checksum", "--log-file=rclone-log.txt"
                            ], shell=True, capture_output=True, text=True)
    if output.stdout.strip() == "" and output.stderr.strip() == "":
        logger.info(f'Uploaded {file_path} to {target_dir}')
        return True
    else:
        if output.stdout.strip():
            logger.info(f'Output: {output.stdout.strip()}')
            return False
        if output.stderr.strip():
            logger.error(f'Error: {output.stderr.strip()}')
            return False

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

def load_downloaded_urls(file_path) -> set[str]:
    '''
    Loads already downloaded URLs from the tracking file
    '''
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return set(line.strip() for line in f.readlines())
    else:
        # Create the file if it doesn't exist
        with open(file_path, "w") as f:
            pass
        return set()

def add_to_downloaded_urls(file_path, url):
    '''
    Adds a URL to the tracking file
    '''
    with open(file_path, "a") as f:
        f.write(f"{url}\n")

# Load already downloaded URLs
downloaded_series = load_downloaded_urls(DOWNLOADED_SERIES_FILE)
downloaded_episodes = load_downloaded_urls(DOWNLOADED_EPISODES_FILE)

if os.path.exists("./need_to_download.txt"):
    with open("./need_to_download.txt", "r") as f:
        urls = f.readlines()
else:
    logger.warning("No url file provided")
    urls = []

for url in urls:
    url = url.strip()
    if not url:
        continue
        
    # Skip if this series has already been downloaded
    if url in downloaded_series:
        logger.info(f"Skipping already downloaded series: {url}")
        continue
    
    ''' Extracts all episode links from the main page URL '''
    res = requests.get(url, verify=False)
    series_name = urlparse(url).path.split("/")[1]
    download_dir = f"./{series_name}"
    os.makedirs(download_dir, exist_ok=True)
    
    soup = BeautifulSoup(res.text, "html.parser")
    episode_elements = soup.select("div > div.elementor > section.elementor-section.elementor-top-section.elementor-element.elementor-section-boxed.elementor-section-height-default.elementor-section-height-default > div > div.elementor-column.elementor-col-33.elementor-top-column.elementor-element > div > div > div > div > a")
    episode_urls = [element.get('href') for element in episode_elements if element.get('href')]
    
    all_episodes_downloaded = True
    
    for i, episode_url in enumerate(episode_urls):
        isDownloaded = False
        while not isDownloaded:
            # Skip if this episode has already been downloaded
            if episode_url in downloaded_episodes:
                logger.info(f"Skipping already downloaded episode: {episode_url}")
                continue
                
            all_episodes_downloaded = False
            
            ''' Downloads an episode from Nkiri episode url '''
            if "downloadwella" in episode_url:
                # Handle downloadwella URLs
                request_headers = {
                    "Content-Type": "application/x-www-form-urlencoded",
                }
                request_body = {}
                
                episode_res = requests.get(episode_url, verify=False)
                
                episode_soup = BeautifulSoup(episode_res.text, "html.parser")
                episode_form = episode_soup.select_one("form")
                
                inputs = episode_form.select("input")
                for input_tag in inputs:
                    if input_tag.get("name") and input_tag.get("value"):
                        request_body[input_tag.get("name")] = input_tag.get("value")
                
                response = requests.post(episode_url, headers=request_headers, data=request_body, stream=True, verify=False)
                file_name = extract_filename(response, f"{series_name} E{0 if i < 9 else ''}{i+1}.mkv")
                file_path = os.path.join(download_dir, file_name)
                
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"Downloaded: {file_name} to {file_path}")
                is_uploaded = rclone_upload_file(file_path, f"onedrive:nkiri/{series_name}")
                if is_uploaded:
                    isDownloaded = True
                
                # Track this episode as downloaded
                add_to_downloaded_urls(DOWNLOADED_EPISODES_FILE, episode_url)
                
            else:
                # Direct download URL
                file_response = requests.get(episode_url, stream=True, verify=False)
                # Safely extract filename
                file_name = extract_filename(file_response, f"{series_name} E{0 if i < 9 else ''}{i+1}.mkv")
                file_path = os.path.join(download_dir, file_name)
                
                with open(file_path, "wb") as f:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"Downloaded: {file_name} to {file_path}")
                is_uploaded = rclone_upload_file(file_path, f"onedrive:nkiri/{series_name}")
                if is_uploaded:
                    isDownloaded = True
                # Track this episode as downloaded
                add_to_downloaded_urls(DOWNLOADED_EPISODES_FILE, episode_url)
    
    logger.info(f"Downloaded all episodes for {series_name}")
    
    # Only mark the series as complete if all episodes were already downloaded or newly downloaded
    if all_episodes_downloaded:
        logger.info(f"All episodes for {series_name} were already downloaded")
    
    os.remove(f"./{series_name}")
    
    # Track this series as processed
    add_to_downloaded_urls(DOWNLOADED_SERIES_FILE, url)
