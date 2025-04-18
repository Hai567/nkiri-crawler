import requests
import os
import shutil
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import subprocess
import warnings
import logging
import threading
import concurrent.futures
import queue
import time
from typing import List, Set, Dict, Tuple

MAX_WORKERS = 10
warnings.filterwarnings("ignore")
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler('download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Thread-safe locks for file operations
downloaded_series_lock = threading.Lock()
downloaded_episodes_lock = threading.Lock()

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

# Thread-safe global sets
downloaded_series = set()
downloaded_episodes = set()

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

def load_downloaded_urls(file_path) -> set:
    '''
    Loads already downloaded URLs from the tracking file
    '''
    result = set()
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            result = set(line.strip() for line in f.readlines())
    else:
        # Create the file if it doesn't exist
        with open(file_path, "w") as f:
            pass
    return result

def add_to_downloaded_urls(file_path, url, lock):
    '''
    Thread-safe function to add a URL to the tracking file
    '''
    with lock:
        with open(file_path, "a") as f:
            f.write(f"{url}\n")

def is_url_downloaded(url, downloaded_set, lock):
    '''
    Thread-safe function to check if a URL is in the downloaded set
    '''
    with lock:
        return url in downloaded_set

def add_to_downloaded_set(url, downloaded_set, lock):
    '''
    Thread-safe function to add a URL to the downloaded set
    '''
    with lock:
        downloaded_set.add(url)

def download_episode(episode_url, series_name, index, download_dir):
    '''
    Downloads a single episode
    '''
    # Skip if this episode has already been downloaded
    if is_url_downloaded(episode_url, downloaded_episodes, downloaded_episodes_lock):
        logger.info(f"Skipping already downloaded episode: {episode_url}")
        return True
    
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
                file_name = extract_filename(response, f"{series_name} E{0 if index < 9 else ''}{index+1}.mkv")
                file_path = os.path.join(download_dir, file_name)
                
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"Downloaded: {file_name} to {file_path}")
                is_uploaded = rclone_upload_file(file_path, f"onedrive:nkiri/{series_name}")
                if is_uploaded:
                    isDownloaded = True
                    # Track this episode as downloaded
                    add_to_downloaded_urls(DOWNLOADED_EPISODES_FILE, episode_url, downloaded_episodes_lock)
                    add_to_downloaded_set(episode_url, downloaded_episodes, downloaded_episodes_lock)

            else:
                # Direct download URL
                file_response = requests.get(episode_url, headers=headers, stream=True, verify=False)
                file_response.raise_for_status()
                # Safely extract filename
                file_name = extract_filename(file_response, f"{series_name} E{0 if index < 9 else ''}{index+1}.mkv")
                file_path = os.path.join(download_dir, file_name)
                
                with open(file_path, "wb") as f:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"Downloaded: {file_name} to {file_path}")
                is_uploaded = rclone_upload_file(file_path, f"onedrive:nkiri/{series_name}")
                if is_uploaded:
                    isDownloaded = True
                    # Track this episode as downloaded
                    add_to_downloaded_urls(DOWNLOADED_EPISODES_FILE, episode_url, downloaded_episodes_lock)
                    add_to_downloaded_set(episode_url, downloaded_episodes, downloaded_episodes_lock)

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error downloading {episode_url}: {str(e)}")
            retry_count += 1
        except Exception as e:
            logger.error(f"Error downloading {episode_url}: {str(e)}")
            retry_count += 1
            
    return isDownloaded

def download_series(url):
    '''
    Downloads all episodes from a series URL
    '''
    url = url.strip()
    if not url:
        return
        
    # Skip if this series has already been downloaded
    if is_url_downloaded(url, downloaded_series, downloaded_series_lock):
        logger.info(f"Skipping already downloaded series: {url}")
        return
    
    try:
        ''' Extracts all episode links from the main page URL '''
        res = requests.get(url, headers=headers, verify=False)
        res.raise_for_status()  # Raise exception for bad status codes
        
        series_name = urlparse(url).path.split("/")[1]
        download_dir = f"./{series_name}"
        
        # Thread-safe directory creation
        if not os.path.exists(download_dir):
            try:
                os.makedirs(download_dir, exist_ok=True)
            except FileExistsError:
                # If another thread created the directory at the same time
                pass
        
        soup = BeautifulSoup(res.text, "html.parser")
        episode_elements = soup.select("div > div.elementor > section.elementor-section.elementor-top-section.elementor-element.elementor-section-boxed.elementor-section-height-default.elementor-section-height-default > div > div.elementor-column.elementor-col-33.elementor-top-column.elementor-element > div > div > div > div > a")
        episode_urls = [element.get('href') for element in episode_elements if element.get('href')]
        number_of_episodes = len(episode_urls)
        
        logger.info(f"Found {number_of_episodes} episodes for {series_name}")
        
        # Download episodes in parallel
        successful_downloads = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # Map episode download tasks
            future_to_episode = {
                executor.submit(download_episode, episode_url, series_name, i, download_dir): (i, episode_url)
                for i, episode_url in enumerate(episode_urls)
            }
            
            # Process completed tasks
            for future in concurrent.futures.as_completed(future_to_episode):
                i, episode_url = future_to_episode[future]
                try:
                    if future.result():
                        successful_downloads += 1
                except Exception as e:
                    logger.error(f"Exception downloading episode {i+1} from {series_name}: {str(e)}")
        
        logger.info(f"Downloaded {successful_downloads}/{number_of_episodes} episodes for {series_name}")
        
        # Clean up directory after processing all episodes
        try:
            shutil.rmtree(download_dir)
            logger.info(f"Removed directory: {download_dir}")
        except Exception as e:
            logger.error(f"Failed to remove directory {download_dir}: {str(e)}")
        
        # Track this series as processed if all episodes were downloaded
        if successful_downloads == number_of_episodes:
            add_to_downloaded_urls(DOWNLOADED_SERIES_FILE, url, downloaded_series_lock)
            add_to_downloaded_set(url, downloaded_series, downloaded_series_lock)
            logger.info(f"Marked series {series_name} as completely downloaded")
    
    except Exception as e:
        logger.error(f"Failed to process series {url}: {str(e)}")

def main():
    # Load already downloaded URLs
    global downloaded_series, downloaded_episodes
    
    logger.info("Starting multithreaded Nkiri downloader")
    
    # Load tracking data with thread safety
    with downloaded_series_lock:
        downloaded_series = load_downloaded_urls(DOWNLOADED_SERIES_FILE)
        
    with downloaded_episodes_lock:
        downloaded_episodes = load_downloaded_urls(DOWNLOADED_EPISODES_FILE)
    
    logger.info(f"Loaded {len(downloaded_series)} downloaded series and {len(downloaded_episodes)} downloaded episodes")
    
    # Load URL list
    if os.path.exists("./need_to_download.txt"):
        with open("./need_to_download.txt", "r") as f:
            urls = f.readlines()
    else:
        logger.warning("No url file provided")
        urls = []
    
    logger.info(f"Found {len(urls)} series URLs to process")
    
    # Process series in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Map series download tasks
        executor.map(download_series, urls)
    
    logger.info("All downloads completed")

if __name__ == "__main__":
    main() 