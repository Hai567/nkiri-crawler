import requests
import os
import shutil
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

# Tracking files
DOWNLOADED_SERIES_FILE = "./downloaded_series.txt"
DOWNLOADED_EPISODES_FILE = "./downloaded_episodes.txt"

def rclone_upload_file(file_path, target_dir):
    try:
        output = subprocess.run(["rclone", "move", file_path, target_dir, 
                                "--progress", "--stats-one-line", "--stats=15s", "--retries", "3", 
                                "--low-level-retries", "10", "--checksum", "--log-file", "rclone-log.txt"
                                ], shell=False, capture_output=True, text=True)
        if output.returncode == 0:
            logger.info(f'Uploaded {file_path} to {target_dir}')
            return True
        else:
            if output.stdout.strip():
                logger.info(f'Output: {output.stdout.strip()}')
            if output.stderr.strip():
                logger.error(f'Error: {output.stderr.strip()}')
            return False
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
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
    
    try:
        ''' Extracts all episode links from the main page URL '''
        res = requests.get(url, headers=headers, verify=False)
        res.raise_for_status()  # Raise exception for bad status codes
        
        series_name = urlparse(url).path.split("/")[1]
        download_dir = f"./{series_name}"
        os.makedirs(download_dir, exist_ok=True)
        
        soup = BeautifulSoup(res.text, "html.parser")
        episode_elements = soup.select("div > div.elementor > section.elementor-section.elementor-top-section.elementor-element.elementor-section-boxed.elementor-section-height-default.elementor-section-height-default > div > div.elementor-column.elementor-col-33.elementor-top-column.elementor-element > div > div > div > div > a")
        episode_urls = [element.get('href') for element in episode_elements if element.get('href')]
        
        all_episodes_downloaded = True
        
        for i, episode_url in enumerate(episode_urls):
            # Skip if this episode has already been downloaded
            if episode_url in downloaded_episodes:
                logger.info(f"Skipping already downloaded episode: {episode_url}")
                continue
                
            all_episodes_downloaded = False
            isDownloaded = False
            retry_count = 0
            
            while not isDownloaded and retry_count < 3:
                try:
                    ''' Downloads an episode from Nkiri episode url '''
                    if "downloadwella" in episode_url:
                        # Handle downloadwella URLs
                        request_headers = {
                            "Content-Type": "application/x-www-form-urlencoded",
                        }
                        request_body = {}
                        
                        episode_res = requests.get(episode_url, headers=headers, verify=False)
                        episode_res.raise_for_status()
                        
                        episode_soup = BeautifulSoup(episode_res.text, "html.parser")
                        episode_form = episode_soup.select_one("form")
                        
                        if not episode_form:
                            logger.error(f"No form found on page: {episode_url}")
                            break
                            
                        inputs = episode_form.select("input")
                        for input_tag in inputs:
                            if input_tag.get("name") and input_tag.get("value"):
                                request_body[input_tag.get("name")] = input_tag.get("value")
                        
                        response = requests.post(episode_url, headers=request_headers, data=request_body, stream=True, verify=False)
                        response.raise_for_status()
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
                        file_response = requests.get(episode_url, headers=headers, stream=True, verify=False)
                        file_response.raise_for_status()
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
                
                except requests.exceptions.RequestException as e:
                    logger.error(f"Network error downloading {episode_url}: {str(e)}")
                    retry_count += 1
                except Exception as e:
                    logger.error(f"Error downloading {episode_url}: {str(e)}")
                    retry_count += 1
        
        logger.info(f"Downloaded all episodes for {series_name}")
        
        # Only mark the series as complete if all episodes were already downloaded or newly downloaded
        if all_episodes_downloaded:
            logger.info(f"All episodes for {series_name} were already downloaded")
        
        # Clean up directory after processing all episodes
        try:
            shutil.rmtree(download_dir)
            logger.info(f"Removed directory: {download_dir}")
        except Exception as e:
            logger.error(f"Failed to remove directory {download_dir}: {str(e)}")
        
        # Track this series as processed
        add_to_downloaded_urls(DOWNLOADED_SERIES_FILE, url)
    
    except Exception as e:
        logger.error(f"Failed to process series {url}: {str(e)}")
