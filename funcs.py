import requests
import os
import logging
from bs4 import BeautifulSoup
from urllib.parse import urlparse, unquote
from typing import Optional, List, Tuple, Dict, Any, Callable
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_filename(response, fallback_filename) -> str:
    '''
    Extracts the filename from the response headers
    
    Args:
        response: HTTP response object
        fallback_filename: Filename to use if not found in headers
        
    Returns:
        str: Extracted filename
    '''
    try:
        if 'Content-Disposition' in response.headers:
            content_disposition = response.headers['Content-Disposition']
            if 'filename=' in content_disposition:
                filename = content_disposition.split('filename=')[1].strip('"\'')
                return filename
        return fallback_filename
    except Exception as e:
        logger.error(f"Error extracting filename: {str(e)}")
        return fallback_filename

def download_file(url: str, output_path: str, headers: Optional[Dict[str, str]] = None) -> Tuple[bool, str]:
    '''
    Generic file download function
    
    Args:
        url: URL to download
        output_path: Path to save the file
        headers: Optional HTTP headers
        
    Returns:
        Tuple[bool, str]: (Success status, Message or error)
    '''
    try:
        response = requests.get(url, headers=headers, stream=True, verify=False)
        response.raise_for_status()
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return True, f"Downloaded to {output_path}"
    except requests.exceptions.RequestException as e:
        logger.error(f"Download request error: {str(e)}")
        return False, f"Download failed: {str(e)}"
    except IOError as e:
        logger.error(f"File I/O error: {str(e)}")
        return False, f"File operation failed: {str(e)}"
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return False, f"Unexpected error: {str(e)}"

def download_with_progress(url: str, output_path: str, 
                          progress_callback: Optional[Callable[[int], None]] = None, 
                          headers: Optional[Dict[str, str]] = None) -> Tuple[bool, str]:
    '''
    Downloads a file with progress tracking
    
    Args:
        url: URL to download
        output_path: Path to save the file
        progress_callback: Function to call with progress percentage (0-100)
        headers: Optional HTTP headers
        
    Returns:
        Tuple[bool, str]: (Success status, Message or error)
    '''
    try:
        response = requests.get(url, headers=headers, stream=True, verify=False)
        response.raise_for_status()
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Get total file size if available
        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0
        
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    
                    # Calculate and report progress
                    if progress_callback and total_size > 0:
                        progress = int((downloaded_size / total_size) * 100)
                        progress_callback(progress)
        
        # Ensure 100% progress is reported
        if progress_callback:
            progress_callback(100)
            
        return True, f"Downloaded to {output_path}"
    except requests.exceptions.RequestException as e:
        logger.error(f"Download request error: {str(e)}")
        return False, f"Download failed: {str(e)}"
    except IOError as e:
        logger.error(f"File I/O error: {str(e)}")
        return False, f"File operation failed: {str(e)}"
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return False, f"Unexpected error: {str(e)}"

def download_episode(episode_url: str, output_dir: Optional[str] = None) -> Tuple[bool, str, Optional[str]]:
    '''
    Downloads an episode from Nkiri episode url
    
    Args:
        episode_url: URL of the episode to download
        output_dir: Custom output directory (optional)
        
    Returns:
        Tuple[bool, str, Optional[str]]: (Success status, Message, Downloaded file path if successful)
    '''
    try:
        # Extract series name from URL safely
        try:
            path_parts = urlparse(episode_url).path.split("/")
            series_name = path_parts[1] if len(path_parts) > 1 else "unknown"
        except (IndexError, AttributeError):
            series_name = "unknown"
        
        # Set output directory
        if output_dir:
            download_dir = output_dir
        else:
            download_dir = f"./{series_name}"
        
        # Ensure directory exists
        os.makedirs(download_dir, exist_ok=True)
        
        if "downloadwella" in episode_url:
            try:
                # Handle downloadwella URLs
                request_headers = {
                    "Content-Type": "application/x-www-form-urlencoded",
                }
                request_data = {}
                
                episode_res = requests.get(episode_url, verify=False)
                episode_res.raise_for_status()
                
                episode_soup = BeautifulSoup(episode_res.text, "html.parser")
                episode_form = episode_soup.select_one("form")
                
                if not episode_form:
                    return False, "Form not found on the page", None
                
                inputs = episode_form.select("input")
                for input_tag in inputs:
                    if input_tag.get("name") and input_tag.get("value"):
                        request_data[input_tag.get("name")] = input_tag.get("value")
                
                action_url = episode_form.get("action")
                if not action_url:
                    return False, "Form action URL not found", None
                
                response = requests.post(action_url, headers=request_headers, data=request_data, stream=True, verify=False)
                response.raise_for_status()
                
                # Safely extract filename
                try:
                    file_name = extract_filename(response, os.path.basename(unquote(urlparse(episode_url).path)))
                    if not file_name or file_name == "":
                        file_name = f"download_{int(time.time())}"
                except Exception:
                    file_name = f"download_{int(time.time())}"
                
                file_path = os.path.join(download_dir, file_name)
                
                # Create parent directory if needed
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                logger.info(f"Downloaded: {file_name} to {file_path}")
                return True, f"Downloaded: {file_name}", file_path
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error for downloadwella URL: {str(e)}")
                return False, f"Request failed: {str(e)}", None
            except Exception as e:
                logger.error(f"Error processing downloadwella URL: {str(e)}")
                return False, f"Error: {str(e)}", None
        else:
            # Direct download URL
            try:
                file_response = requests.get(episode_url, stream=True, verify=False)
                file_response.raise_for_status()
                
                # Safely extract filename
                try:
                    file_name = extract_filename(file_response, os.path.basename(unquote(urlparse(episode_url).path)))
                    if not file_name or file_name == "":
                        file_name = f"download_{int(time.time())}"
                except Exception:
                    file_name = f"download_{int(time.time())}"
                
                file_path = os.path.join(download_dir, file_name)
                
                # Create parent directory if needed
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                with open(file_path, "wb") as f:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                logger.info(f"Downloaded: {file_name} to {file_path}")
                return True, f"Downloaded: {file_name}", file_path
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {str(e)}")
                return False, f"Request failed: {str(e)}", None
            except Exception as e:
                logger.error(f"Error downloading direct URL: {str(e)}")
                return False, f"Error: {str(e)}", None
    
    except Exception as e:
        logger.error(f"Unexpected error in download_episode: {str(e)}")
        return False, f"Unexpected error: {str(e)}", None
    
def download_episode_with_progress(episode_url: str, output_dir: Optional[str] = None, 
                                  progress_callback: Optional[Callable[[int], None]] = None) -> Tuple[bool, str, Optional[str]]:
    '''
    Downloads an episode from Nkiri episode url with progress tracking
    
    Args:
        episode_url: URL of the episode to download
        output_dir: Custom output directory (optional)
        progress_callback: Function to call with progress percentage (0-100)
        
    Returns:
        Tuple[bool, str, Optional[str]]: (Success status, Message, Downloaded file path if successful)
    '''
    try:
        # Extract series name from URL safely
        try:
            path_parts = urlparse(episode_url).path.split("/")
            series_name = path_parts[1] if len(path_parts) > 1 else "unknown"
        except (IndexError, AttributeError):
            series_name = "unknown"
        
        # Set output directory
        if output_dir:
            download_dir = output_dir
        else:
            download_dir = f"./{series_name}"
        
        # Ensure the directory exists
        os.makedirs(download_dir, exist_ok=True)
        
        # Report initial progress
        if progress_callback:
            progress_callback(5)
        
        if "downloadwella" in episode_url:
            try:
                # Handle downloadwella URLs
                request_headers = {
                    "Content-Type": "application/x-www-form-urlencoded",
                }
                request_data = {}
                
                episode_res = requests.get(episode_url, verify=False)
                episode_res.raise_for_status()
                
                if progress_callback:
                    progress_callback(10)
                
                episode_soup = BeautifulSoup(episode_res.text, "html.parser")
                episode_form = episode_soup.select_one("form")
                
                if not episode_form:
                    return False, "Form not found on the page", None
                
                inputs = episode_form.select("input")
                for input_tag in inputs:
                    if input_tag.get("name") and input_tag.get("value"):
                        request_data[input_tag.get("name")] = input_tag.get("value")
                
                action_url = episode_form.get("action")
                if not action_url:
                    return False, "Form action URL not found", None
                
                if progress_callback:
                    progress_callback(15)
                
                response = requests.post(action_url, headers=request_headers, data=request_data, stream=True, verify=False)
                response.raise_for_status()
                
                file_name = extract_filename(response, os.path.basename(unquote(urlparse(episode_url).path)))
                file_path = os.path.join(download_dir, file_name)
                
                # Get total file size if available
                total_size = int(response.headers.get('content-length', 0))
                downloaded_size = 0
                
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            
                            # Calculate and report progress from 20% to 100%
                            if progress_callback and total_size > 0:
                                progress = 15 + int((downloaded_size / total_size) * 85)
                                progress_callback(progress)
                
                # Ensure 100% progress is reported
                if progress_callback:
                    progress_callback(100)
                
                logger.info(f"Downloaded: {file_name} to {file_path}")
                return True, f"Downloaded: {file_name}", file_path
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error for downloadwella URL: {str(e)}")
                return False, f"Request failed: {str(e)}", None
            except Exception as e:
                logger.error(f"Error processing downloadwella URL: {str(e)}")
                return False, f"Error: {str(e)}", None
        else:
            # Direct download URL
            try:
                if progress_callback:
                    progress_callback(5)
                
                file_response = requests.get(episode_url, stream=True, verify=False)
                file_response.raise_for_status()
                
                # Safely extract filename
                try:
                    file_name = extract_filename(file_response, os.path.basename(unquote(urlparse(episode_url).path)))
                    if not file_name or file_name == "":
                        file_name = f"download_{int(time.time())}"
                except Exception:
                    file_name = f"download_{int(time.time())}"
                
                file_path = os.path.join(download_dir, file_name)
                
                # Get total file size if available
                total_size = int(file_response.headers.get('content-length', 0))
                downloaded_size = 0
                
                # Create parent directory if needed
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                with open(file_path, "wb") as f:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            
                            # Calculate and report progress
                            if progress_callback and total_size > 0:
                                progress = 5 + int((downloaded_size / total_size) * 95)
                                progress_callback(progress)
                
                # Ensure 100% progress is reported
                if progress_callback:
                    progress_callback(100)
                
                logger.info(f"Downloaded: {file_name} to {file_path}")
                return True, f"Downloaded: {file_name}", file_path
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {str(e)}")
                return False, f"Request failed: {str(e)}", None
            except Exception as e:
                logger.error(f"Error downloading direct URL: {str(e)}")
                return False, f"Error: {str(e)}", None
    
    except Exception as e:
        logger.error(f"Unexpected error in download_episode_with_progress: {str(e)}")
        return False, f"Unexpected error: {str(e)}", None
    
def extract_episodes(url: str) -> Tuple[bool, List[str], str]:
    '''
    Extracts all episode links from the main page URL
    
    Args:
        url: Main page URL
        
    Returns:
        Tuple[bool, List[str], str]: (Success status, List of episode URLs, Error message if any)
    '''
    try:
        res = requests.get(url, verify=False)
        res.raise_for_status()
        
        soup = BeautifulSoup(res.text, "html.parser")
        episode_elements = soup.select("div > div.elementor > section.elementor-section.elementor-top-section.elementor-element.elementor-section-boxed.elementor-section-height-default.elementor-section-height-default > div > div.elementor-column.elementor-col-50.elementor-top-column.elementor-element > div > div > div > div > a")
        
        if not episode_elements:
            logger.warning(f"No episodes found at {url}")
            return False, [], "No episodes found"
        
        episode_urls = [element.get('href') for element in episode_elements if element.get('href')]
        logger.info(f"Found {len(episode_urls)} episodes at {url}")
        
        return True, episode_urls, ""
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {str(e)}")
        return False, [], f"Request failed: {str(e)}"
    except Exception as e:
        logger.error(f"Error extracting episodes: {str(e)}")
        return False, [], f"Error: {str(e)}"