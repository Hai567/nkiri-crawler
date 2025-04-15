# Nkiri Downloader

A GUI application for downloading content from Nkiri with progress tracking and multiple download management.

## Features

-   Download single episodes or entire series
-   Track download progress
-   Manage multiple downloads simultaneously
-   Queue, start, and cancel downloads
-   Customize download location

## Installation

1. Clone this repository:

```
git clone https://github.com/yourusername/nkiri-downloader.git
cd nkiri-downloader
```

2. Install the required dependencies:

```
pip install -r requirements.txt
```

## Usage

1. Run the GUI application:

```
python gui.py
```

2. Using the application:
    - Enter a Nkiri URL in the input field
    - Click "Add URL" to add a single episode to the download queue
    - Click "Extract Episodes" if you entered a series page URL to extract all episodes
    - Use "Set Output Directory" to change where files are saved
    - Click "Start" on any queued download to begin downloading
    - Monitor progress in the progress bar
    - Cancel downloads with the "Cancel" button

## Command Line Version

A command line version is also available:

```
python cli.py [URL] [options]
```

Options:

-   `-o, --output-dir`: Specify output directory
-   `-a, --all`: Download all episodes from a series page
-   `-v, --verbose`: Enable verbose output

## License

This project is for educational purposes only. Please respect copyright laws and use responsibly.
