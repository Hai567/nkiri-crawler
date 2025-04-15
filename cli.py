#!/usr/bin/env python3
import argparse
import sys
import os
import logging
from funcs import download_episode, extract_episodes

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Download episodes from Nkiri')
    parser.add_argument('url', help='URL to download from (either a single episode or main page with multiple episodes)')
    parser.add_argument('-o', '--output-dir', help='Output directory for downloaded files')
    parser.add_argument('-a', '--all', action='store_true', help='Download all episodes from the main page')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Configure logging based on verbosity
    log_level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Process the URL
    if args.all or '/series/' in args.url:
        # Extract and download all episodes
        print(f"Extracting episodes from {args.url}...")
        success, episode_urls, error = extract_episodes(args.url)
        
        if not success:
            print(f"Error: {error}")
            return 1
        
        if not episode_urls:
            print("No episodes found.")
            return 1
        
        print(f"Found {len(episode_urls)} episodes.")
        for i, episode_url in enumerate(episode_urls, 1):
            print(f"Downloading episode {i}/{len(episode_urls)}: {episode_url}")
            success, message, file_path = download_episode(episode_url, args.output_dir)
            if success:
                print(f"✓ {message}")
            else:
                print(f"✗ {message}")
        
        print("Download complete!")
    else:
        # Download a single episode
        print(f"Downloading episode from {args.url}...")
        success, message, file_path = download_episode(args.url, args.output_dir)
        
        if success:
            print(f"✓ {message}")
            print(f"Downloaded to: {file_path}")
        else:
            print(f"✗ {message}")
            return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
