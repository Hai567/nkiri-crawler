#!/usr/bin/env python3
"""
Nkiri Bulk Downloader and Uploader
This script reads URLs from a file, downloads videos, and uploads them to cloud storage using rclone.

Features:
- Reads download URLs from a file
- Downloads videos using functions from funcs.py
- Uploads downloaded content to cloud storage via rclone
- Verifies uploads to ensure integrity
- Tracks processed URLs to avoid duplications
- Automatically deletes local files after successful upload
- Implements retry mechanism for failed downloads and uploads
"""

import os
import sys
import time
import logging
import json
import argparse
import subprocess
import traceback
import shutil
from datetime import datetime
from functools import wraps
from typing import Dict, List, Optional, Tuple, Any, Callable, Union
from urllib.parse import urlparse

# Import functions from funcs.py
from funcs import download_episode, extract_episodes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bulk_downloader.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Decorator for retry logic
def retry(max_tries: int = 3, delay_seconds: int = 5, 
          backoff_factor: int = 2, exceptions: tuple = (Exception,)):
    """
    Retry decorator with exponential backoff for functions
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            mtries, mdelay = max_tries, delay_seconds
            last_exception = None
            
            while mtries > 0:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    mtries -= 1
                    if mtries == 0:
                        logger.error(f"All {max_tries} retries failed for {func.__name__}. Last error: {str(e)}")
                        last_exception = e
                        break
                        
                    logger.warning(f"Retry {max_tries - mtries} for {func.__name__} failed with {str(e)}. "
                                  f"Retrying in {mdelay} seconds...")
                    time.sleep(mdelay)
                    mdelay *= backoff_factor
            
            if last_exception:
                raise last_exception
        return wrapper
    return decorator

class RcloneUploader:
    """Handles uploads to cloud storage using rclone"""
    
    def __init__(self, remote_name: str = "onedrive", remote_path: str = "Videos", verification_config: Dict = None):
        self.remote_name = remote_name
        self.remote_path = remote_path
        self.rclone_path = self._find_rclone()
        self.last_error = None
        self.verification_config = verification_config or {
            "verify_uploads": True,
            "use_full_hash": False,
            "verification_timeout": 300
        }
        
    def _find_rclone(self) -> Optional[str]:
        """Find rclone executable in PATH"""
        if os.name == "nt":  # Windows
            rclone_cmd = "rclone.exe"
        else:  # Linux, macOS
            rclone_cmd = "rclone"
            
        # Check if rclone is in PATH
        rclone_path = shutil.which(rclone_cmd)
        if rclone_path:
            logger.info(f"Found rclone in PATH: {rclone_path}")
            return rclone_path
            
        # Check common installation locations
        common_paths = [
            r"C:\Program Files\rclone\rclone.exe",
            r"C:\rclone\rclone.exe",
            os.path.expanduser("~/.local/bin/rclone"),
            "/usr/local/bin/rclone",
            "/usr/bin/rclone"
        ]
        
        for path in common_paths:
            if os.path.isfile(path):
                logger.info(f"Found rclone at: {path}")
                return path
                
        error_msg = "rclone executable not found. Please install rclone or ensure it's in your PATH"
        self.last_error = error_msg
        logger.error(error_msg)
        return None
    
    @retry(max_tries=2, delay_seconds=2, exceptions=(subprocess.SubprocessError, OSError))
    def check_rclone_config(self) -> bool:
        """Check if rclone is configured properly"""
        if not self.rclone_path:
            error_msg = "rclone not found"
            self.last_error = error_msg
            logger.error(error_msg)
            return False
            
        try:
            result = subprocess.run(
                [self.rclone_path, "listremotes"],
                capture_output=True, text=True, check=True,
                timeout=30
            )
            remotes = result.stdout.strip().split('\n')
            
            if f"{self.remote_name}:" in remotes:
                logger.info(f"Found {self.remote_name} remote in rclone configuration")
                self.last_error = None
                return True
            else:
                error_msg = f"{self.remote_name} remote not found in rclone configuration"
                self.last_error = error_msg
                logger.error(error_msg)
                return False
        except subprocess.SubprocessError as e:
            error_msg = f"Error checking rclone config: {e}"
            self.last_error = error_msg
            logger.error(error_msg)
            raise
    
    @retry(max_tries=2, delay_seconds=10, exceptions=(subprocess.SubprocessError, OSError, IOError))
    def upload_file(self, local_path: str, remote_subpath: str = "") -> bool:
        """Upload a file to cloud storage via rclone with retry"""
        if not self.rclone_path:
            error_msg = "rclone not found, cannot upload"
            self.last_error = error_msg
            logger.error(error_msg)
            return False
            
        if not os.path.exists(local_path):
            error_msg = f"Local path does not exist: {local_path}"
            self.last_error = error_msg
            logger.error(error_msg)
            return False
            
        # Validate the local path to ensure it's accessible
        try:
            if os.path.isdir(local_path):
                # Check if directory is readable
                os.listdir(local_path)
            else:
                # Check if file is readable
                with open(local_path, 'rb') as f:
                    f.read(1)  # Just read 1 byte to test access
        except (PermissionError, IOError) as e:
            error_msg = f"Cannot access local path {local_path}: {e}"
            self.last_error = error_msg
            logger.error(error_msg)
            return False
            
        # Construct the remote path
        remote_full_path = f"{self.remote_name}:{self.remote_path}"
        if remote_subpath:
            remote_full_path = os.path.join(remote_full_path, remote_subpath)
        else:
            # Only append local basename if remote_subpath is not provided
            local_basename = os.path.basename(os.path.normpath(local_path))
            if local_basename:
                remote_full_path = os.path.join(remote_full_path, local_basename)
        
        # Run rclone copy command
        try:
            logger.info(f"Starting upload: {local_path} -> {remote_full_path}")
            
            # Get file/directory size before upload
            try:
                if os.path.isfile(local_path):
                    size_mb = os.path.getsize(local_path) / (1024 * 1024)
                    item_type = "file"
                else:
                    size_mb = sum(os.path.getsize(os.path.join(dirpath, filename)) 
                               for dirpath, _, filenames in os.walk(local_path) 
                               for filename in filenames) / (1024 * 1024)
                    item_type = "directory"
                    
                logger.info(f"Uploading {item_type} of size {size_mb:.2f} MB")
            except (PermissionError, OSError) as e:
                logger.warning(f"Could not calculate size of {local_path}: {e}")
                # Continue with upload despite size calculation failure
            
            # Execute the rclone command with progress monitoring
            process = subprocess.Popen(
                [
                    self.rclone_path, "copy", local_path, remote_full_path, "--log-file=rclone-log.txt",
                    "--progress", "--stats-one-line", "--stats=15s",
                    "--retries", "3",
                    "--low-level-retries", "10",
                ],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1
            )
            
            # Monitor and log the progress
            last_log_time = time.time()
            for line in process.stdout:
                # Limit logging frequency to avoid flooding logs
                current_time = time.time()
                if "Transferred:" in line and (current_time - last_log_time) >= 60:
                    logger.info(line.strip())
                    last_log_time = current_time
                
            process.wait()
            
            if process.returncode == 0:
                logger.info(f"Successfully uploaded to {remote_full_path}")
                self.last_error = None
                return True
            else:
                error_msg = f"Failed to upload to {remote_full_path}"
                self.last_error = error_msg
                logger.error(error_msg)
                return False
                
        except Exception as e:
            error_msg = f"Error during upload: {str(e)}"
            self.last_error = error_msg
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise

    @retry(max_tries=2, delay_seconds=5, exceptions=(subprocess.SubprocessError, OSError))
    def verify_upload(self, local_path: str, remote_subpath: str = "") -> bool:
        """Verify that files/folders were uploaded correctly using rclone check"""
        # Skip verification if disabled in config
        if not self.verification_config.get("verify_uploads", True):
            logger.info("Upload verification skipped (disabled in config)")
            return True
            
        if not self.rclone_path:
            error_msg = "rclone not found, cannot verify"
            self.last_error = error_msg
            logger.error(error_msg)
            return False
            
        if not os.path.exists(local_path):
            error_msg = f"Local path does not exist: {local_path}"
            self.last_error = error_msg
            logger.error(error_msg)
            return False
            
        # Construct the remote path
        remote_full_path = f"{self.remote_name}:{self.remote_path}"
        if remote_subpath:
            remote_full_path = os.path.join(remote_full_path, remote_subpath)
        else:
            # Only append local basename if remote_subpath is not provided
            local_basename = os.path.basename(os.path.normpath(local_path))
            if local_basename:
                remote_full_path = os.path.join(remote_full_path, local_basename)
        
        # Run rclone check command to verify the upload
        try:
            logger.info(f"Verifying upload: {local_path} -> {remote_full_path}")
            
            # Build command with parameters based on config
            check_cmd = [
                self.rclone_path, "check", local_path, remote_full_path,
                "--one-way"  # Only check that source files exist in destination
            ]
            
            # Decide between size-only or full hash verification
            if not self.verification_config.get("use_full_hash", False):
                check_cmd.append("--size-only")  # Faster check based on sizes only
            
            # Set verification timeout from config
            timeout = self.verification_config.get("verification_timeout", 300)  # Default 5 minutes
            
            result = subprocess.run(
                check_cmd,
                capture_output=True, text=True, timeout=timeout
            )
            
            # Check if verification was successful
            if result.returncode == 0:
                logger.info(f"Upload verification successful for {local_path}")
                self.last_error = None
                return True
            else:
                # If the check failed, log the errors
                error_msg = f"Upload verification failed: {result.stderr}"
                self.last_error = error_msg
                logger.error(error_msg)
                
                # Log specific file differences if available
                if result.stdout:
                    logger.error(f"Differences detected: {result.stdout}")
                
                return False
                
        except subprocess.TimeoutExpired:
            error_msg = f"Verification timed out after {timeout} seconds"
            self.last_error = error_msg
            logger.error(error_msg)
            return False
        except Exception as e:
            error_msg = f"Error during verification: {str(e)}"
            self.last_error = error_msg
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            raise

class BulkDownloadManager:
    """Main class to manage bulk downloads and uploads"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.rclone = RcloneUploader(
            remote_name=config.get("rclone", {}).get("remote_name", "onedrive"),
            remote_path=config.get("rclone", {}).get("remote_path", "Videos"),
            verification_config=config.get("verification", {})
        )
        self.processed_urls = self._load_processed_urls()
        self.failed_downloads = self._load_failed_downloads()
        self.max_failures = config.get("max_download_failures", 3)
        self.auto_delete = config.get("auto_delete", True)
        self.download_dir = config.get("download_dir", "./downloads")
        self.urls_file = config.get("urls_file", "need_to_download_url.txt")
        
    def _load_processed_urls(self) -> Dict:
        """Load list of already processed URLs"""
        return self._load_json_file("processed_urls.json")
            
    def _load_failed_downloads(self) -> Dict:
        """Load list of failed downloads to manage retries"""
        return self._load_json_file("failed_downloads.json")
    
    def _load_json_file(self, filename: str) -> Dict:
        """Generic JSON file loader with error handling"""
        try:
            if os.path.exists(filename):
                with open(filename, "r") as f:
                    return json.load(f)
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing {filename}: {e}")
            # Create backup of corrupted file
            if os.path.exists(filename):
                backup_name = f"{filename}.{int(time.time())}.bak"
                try:
                    shutil.copy2(filename, backup_name)
                    logger.info(f"Created backup of corrupted file: {backup_name}")
                except Exception as backup_err:
                    logger.error(f"Failed to create backup of corrupted file: {backup_err}")
            return {}
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
            return {}
            
    def _save_processed_urls(self) -> bool:
        """Save list of processed URLs to avoid re-downloading"""
        return self._save_json_file("processed_urls.json", self.processed_urls)
            
    def _save_failed_downloads(self) -> bool:
        """Save list of failed downloads for retry tracking"""
        return self._save_json_file("failed_downloads.json", self.failed_downloads)
    
    def _save_json_file(self, filename: str, data: Dict) -> bool:
        """Generic JSON file saver with error handling"""
        try:
            # First write to a temporary file, then rename for atomicity
            temp_filename = f"{filename}.tmp"
            with open(temp_filename, "w") as f:
                json.dump(data, f, indent=2)
            
            # Replace the original file with the temp file
            if os.path.exists(filename):
                os.replace(temp_filename, filename)
            else:
                os.rename(temp_filename, filename)
            return True
        except Exception as e:
            logger.error(f"Error saving {filename}: {e}")
            logger.error(traceback.format_exc())
            return False
            
    def _delete_content(self, content_path: str) -> bool:
        """Delete content folder/file from the filesystem after successful upload"""
        if not content_path or not os.path.exists(content_path):
            logger.warning(f"Cannot delete nonexistent path: {content_path}")
            return False
            
        try:
            logger.info(f"Deleting content: {content_path}")
            
            if os.path.isdir(content_path):
                shutil.rmtree(content_path)
                logger.info(f"Successfully deleted directory: {content_path}")
            else:
                os.remove(content_path)
                logger.info(f"Successfully deleted file: {content_path}")
                
            return True
        except (PermissionError, OSError) as e:
            logger.error(f"Error deleting content {content_path}: {e}")
            logger.error(traceback.format_exc())
            return False

    def _load_urls_from_file(self) -> List[str]:
        """Load URLs from the input file"""
        urls = []
        try:
            if not os.path.exists(self.urls_file):
                logger.error(f"URLs file not found: {self.urls_file}")
                return []
                
            with open(self.urls_file, "r") as f:
                for line in f:
                    url = line.strip()
                    if url and not url.startswith("#"):  # Skip empty lines and comments
                        urls.append(url)
                        
            logger.info(f"Loaded {len(urls)} URLs from {self.urls_file}")
            return urls
        except Exception as e:
            logger.error(f"Error loading URLs from file: {e}")
            return []
    
    def _record_download_failure(self, url: str, error_message: Optional[str]) -> None:
        """Record a failed download attempt for retry later"""
        url_hash = str(hash(url))
        
        if url_hash not in self.failed_downloads:
            self.failed_downloads[url_hash] = {
                "url": url,
                "first_failure": datetime.now().isoformat(),
                "last_failure": datetime.now().isoformat(),
                "failures": 1,
                "last_error": error_message or "Unknown error"
            }
        else:
            self.failed_downloads[url_hash]["failures"] += 1
            self.failed_downloads[url_hash]["last_failure"] = datetime.now().isoformat()
            self.failed_downloads[url_hash]["last_error"] = error_message or "Unknown error"
            
        self._save_failed_downloads()
    
    def _extract_folder_name(self, url: str) -> str:
        """Extract a folder name from the URL for organizing uploads"""
        try:
            parsed_url = urlparse(url)
            path_parts = parsed_url.path.split("/")
            
            # Try to get a meaningful folder name from the URL path
            if len(path_parts) > 1 and path_parts[1]:
                return path_parts[1].replace("-", " ").title()
            
            # Fallback to domain name if path doesn't have useful segments
            domain = parsed_url.netloc
            if domain:
                return domain.split(".")[0].title()
                
            # Last resort fallback
            return "Unsorted"
        except Exception as e:
            logger.warning(f"Error extracting folder name from URL {url}: {e}")
            return "Unsorted"
    
    def download_and_upload(self, url: str) -> bool:
        """Download from a URL and upload to cloud storage"""
        url_hash = str(hash(url))
        
        # Skip if already processed successfully
        if url_hash in self.processed_urls:
            logger.info(f"Skipping already processed URL: {url}")
            return True
            
        # Check if this URL has failed too many times
        if (url_hash in self.failed_downloads and 
                self.failed_downloads[url_hash].get("failures", 0) >= self.max_failures):
            logger.warning(f"Skipping URL that failed {self.max_failures} times: {url}")
            return False
            
        # Extract a folder name for organizing uploads
        folder_name = self._extract_folder_name(url)
        download_output_dir = os.path.join(self.download_dir, folder_name)
            
        # Ensure download directory exists
        os.makedirs(download_output_dir, exist_ok=True)
            
        try:
            # Download the episode
            logger.info(f"Downloading from URL: {url}")
            success, message, downloaded_path = download_episode(url, download_output_dir)
            
            if not success or not downloaded_path:
                logger.error(f"Download failed: {message}")
                self._record_download_failure(url, message)
                return False
                
            logger.info(f"Download successful: {downloaded_path}")
                
            # Upload the downloaded content
            if os.path.exists(downloaded_path):
                upload_success = self.rclone.upload_file(downloaded_path, folder_name)
                
                if upload_success:
                    # Verify the upload was successful
                    logger.info(f"Verifying upload for: {downloaded_path}")
                    verify_success = self.rclone.verify_upload(downloaded_path, folder_name)
                    
                    if verify_success:
                        # Mark as processed
                        self.processed_urls[url_hash] = {
                            "url": url,
                            "downloaded_at": datetime.now().isoformat(),
                            "path": downloaded_path,
                            "folder": folder_name
                        }
                        self._save_processed_urls()
                        
                        # Remove from failed downloads if it was there
                        if url_hash in self.failed_downloads:
                            del self.failed_downloads[url_hash]
                            self._save_failed_downloads()
                        
                        # Delete the local file if configured to do so
                        if self.auto_delete:
                            logger.info(f"Deleting local file after successful upload: {downloaded_path}")
                            self._delete_content(downloaded_path)
                        
                        return True
                    else:
                        # Verification failed
                        error_msg = "Upload verification failed"
                        self._record_download_failure(url, error_msg)
                        logger.error(error_msg)
                        return False
                else:
                    # Upload failed
                    error_msg = self.rclone.last_error or "Upload failed"
                    self._record_download_failure(url, error_msg)
                    logger.error(f"Upload failed for {downloaded_path}: {error_msg}")
                    return False
            else:
                error_msg = f"Downloaded path not found: {downloaded_path}"
                self._record_download_failure(url, error_msg)
                logger.error(error_msg)
                return False
                
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            logger.error(traceback.format_exc())
            self._record_download_failure(url, str(e))
            return False
    
    def _retry_failed_downloads(self) -> None:
        """Retry previously failed downloads"""
        if not self.failed_downloads:
            return
            
        logger.info(f"Checking {len(self.failed_downloads)} failed downloads for retry")
        
        # Create a copy of the keys since we might modify the dictionary
        failed_keys = list(self.failed_downloads.keys())
        
        for url_hash in failed_keys:
            failed_info = self.failed_downloads[url_hash]
            
            # Skip if too many failures
            if failed_info.get("failures", 0) >= self.max_failures:
                logger.debug(f"Skipping retry for {failed_info['url']} - too many failures")
                continue
                
            # Attempt to download and upload
            url = failed_info.get("url", "")
            if not url:
                logger.warning(f"No URL found in failed download record, removing: {url_hash}")
                del self.failed_downloads[url_hash]
                self._save_failed_downloads()
                continue
                
            logger.info(f"Retrying download for: {url}")
            success = self.download_and_upload(url)
            
            if success:
                logger.info(f"Successfully processed previously failed URL: {url}")
                # The URL has been added to processed_urls and removed from failed_downloads
                # in the download_and_upload method
            else:
                logger.warning(f"Retry failed for URL: {url}")
                # The failure count has been updated in the download_and_upload method
    
    def process_urls(self) -> None:
        """Process all URLs from the input file"""
        # First check for any failed downloads to retry
        self._retry_failed_downloads()
        
        # Load and process new URLs
        urls = self._load_urls_from_file()
        
        if not urls:
            logger.warning("No URLs to process")
            return
            
        for url in urls:
            # Skip if already processed
            url_hash = str(hash(url))
            if url_hash in self.processed_urls:
                logger.debug(f"Skipping already processed URL: {url}")
                continue
                
            logger.info(f"Processing URL: {url}")
            success = self.download_and_upload(url)
            
            if success:
                logger.info(f"Successfully processed URL: {url}")
            else:
                logger.warning(f"Failed to process URL: {url}")
    
    def run(self) -> bool:
        """Run the main manager loop"""
        # Health checks
        health_check_success = True
        
        # Check if rclone is configured
        if not self.rclone.check_rclone_config():
            logger.error("rclone is not properly configured. Please set up the remote first.")
            health_check_success = False
            
        # Check if URLs file exists
        if not os.path.exists(self.urls_file):
            logger.error(f"URLs file not found: {self.urls_file}")
            health_check_success = False
            
        # Decide whether to continue based on config
        if not health_check_success and not self.config.get("continue_on_errors", False):
            logger.error("Health checks failed. Set 'continue_on_errors' to true in config to run anyway.")
            return False
            
        logger.info("Starting bulk download and upload service")
        
        # Ensure download directory exists
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Main loop
        try:
            while True:
                try:
                    self.process_urls()
                except Exception as e:
                    logger.error(f"Error in process_urls cycle: {e}")
                    logger.error(traceback.format_exc())
                    # Continue despite errors
                    
                interval = self.config.get("check_interval", 3600)  # Default: 1 hour
                logger.info(f"Sleeping for {interval} seconds")
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Service stopped by user")
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}")
            logger.error(traceback.format_exc())
            return False
            
        return True


def create_default_config() -> Dict:
    """Create a default configuration file"""
    config = {
        "rclone": {
            "remote_name": "onedrive",
            "remote_path": "Videos"
        },
        "download_dir": "./downloads",
        "urls_file": "need_to_download_url.txt",
        "check_interval": 3600,  # 1 hour
        "max_download_failures": 3,
        "continue_on_errors": False,
        "auto_delete": True,  # Delete local files after successful upload
        "verification": {
            "verify_uploads": True,      # Verify uploads before deletion
            "use_full_hash": False,      # Use full hash checking (slower but more accurate) instead of size-only
            "verification_timeout": 300  # Timeout for verification in seconds
        }
    }
    
    try:
        # First write to temp file, then move (atomic operation)
        temp_file = "config.json.tmp"
        with open(temp_file, "w") as f:
            json.dump(config, f, indent=4)
        
        # Move temp file to actual config file
        if os.path.exists("config.json"):
            os.replace(temp_file, "config.json")
        else:
            os.rename(temp_file, "config.json")
            
        logger.info("Created default configuration file: config.json")
        return config
    except Exception as e:
        logger.error(f"Error creating default configuration: {e}")
        logger.error(traceback.format_exc())
        return config


def load_config() -> Dict:
    """Load configuration from file or create default"""
    try:
        if os.path.exists("config.json"):
            with open("config.json", "r") as f:
                config = json.load(f)
            logger.info("Loaded configuration from config.json")
            return config
        else:
            logger.info("Configuration file not found, creating default")
            return create_default_config()
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config.json: {e}")
        # Create backup of invalid config
        try:
            backup_name = f"config.json.{int(time.time())}.bak"
            shutil.copy2("config.json", backup_name)
            logger.info(f"Created backup of invalid config as {backup_name}")
        except Exception as backup_err:
            logger.error(f"Failed to backup invalid config: {backup_err}")
        # Create a fresh config
        return create_default_config()
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        logger.error(traceback.format_exc())
        return create_default_config()


def main() -> None:
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Nkiri Bulk Downloader and Uploader")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument("--setup", action="store_true", help="Create default configuration file and exit")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                       default="INFO", help="Set the logging level")
    parser.add_argument("--once", action="store_true", help="Run once and exit (do not loop)")
    args = parser.parse_args()
    
    # Set log level based on argument
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Create default config and exit if --setup is provided
    if args.setup:
        create_default_config()
        print("Created default configuration file: config.json")
        print("Please edit this file with your rclone settings")
        return
    
    # Load configuration
    config_file = args.config if args.config else "config.json"
    if args.config and os.path.exists(args.config):
        try:
            with open(args.config, "r") as f:
                config = json.load(f)
        except Exception as e:
            logger.error(f"Error reading config file {args.config}: {e}")
            sys.exit(1)
    else:
        config = load_config()
    
    # Create and run manager
    manager = BulkDownloadManager(config)
    
    if args.once:
        # Run once and exit
        try:
            manager.process_urls()
            sys.exit(0)
        except Exception as e:
            logger.critical(f"Error during single run: {e}")
            logger.critical(traceback.format_exc())
            sys.exit(1)
    else:
        # Run in continuous loop
        try:
            success = manager.run()
            sys.exit(0 if success else 1)
        except KeyboardInterrupt:
            print("\nService stopped by user")
        except Exception as e:
            logger.critical(f"Unhandled exception: {e}")
            logger.critical(traceback.format_exc())
            sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Fatal unhandled exception: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)
