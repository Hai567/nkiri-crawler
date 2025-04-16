#!/usr/bin/env python3
import sys
import os

# Set environment variable to suppress sipPyTypeDict() deprecation warnings
os.environ["QT_ENABLE_DEPRECATED_WARNINGS"] = "0"

import threading
import time
import re
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                            QPushButton, QTableWidget, QTableWidgetItem, QHeaderView,
                            QProgressBar, QFileDialog, QLabel, QLineEdit, QMessageBox,
                            QDialog, QTextEdit, QCheckBox, QProgressDialog, QSpinBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, pyqtSlot, QEvent, QMetaObject, Q_ARG
from funcs import extract_episodes, download_episode_with_progress
from urllib.parse import urlparse

# Custom event for thread-safe addition of URLs
class AddUrlEvent(QEvent):
    EVENT_TYPE = QEvent.Type(QEvent.registerEventType())
    
    def __init__(self, url):
        super().__init__(AddUrlEvent.EVENT_TYPE)
        self.url = url

# Dialog for adding multiple URLs
class MultiUrlDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add Multiple URLs")
        self.setGeometry(300, 300, 500, 300)
        self.initUI()
        
    def initUI(self):
        layout = QVBoxLayout()
        
        # Instructions
        instructions = QLabel("Enter one URL per line:")
        layout.addWidget(instructions)
        
        # Text field for URLs
        self.url_text = QTextEdit()
        self.url_text.setPlaceholderText("https://nkiri.com/url1\nhttps://nkiri.com/url2\n...")
        layout.addWidget(self.url_text)
        
        # Auto-start checkbox
        self.auto_start_checkbox = QCheckBox("Start downloads automatically")
        layout.addWidget(self.auto_start_checkbox)
        
        # Auto-extract checkbox
        self.auto_extract_checkbox = QCheckBox("Auto-extract episodes from series URLs")
        self.auto_extract_checkbox.setChecked(True)
        layout.addWidget(self.auto_extract_checkbox)
        
        # Buttons
        button_layout = QHBoxLayout()
        self.add_button = QPushButton("Add URLs")
        self.add_button.clicked.connect(self.accept)
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.reject)
        
        button_layout.addWidget(self.add_button)
        button_layout.addWidget(self.cancel_button)
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
    
    def getUrls(self):
        text = self.url_text.toPlainText()
        urls = [url.strip() for url in text.split('\n') if url.strip()]
        return urls
        
    def shouldAutoStart(self):
        return self.auto_start_checkbox.isChecked()
        
    def shouldAutoExtract(self):
        return self.auto_extract_checkbox.isChecked()

class DownloadWorker(QThread):
    # Signals
    progress_updated = pyqtSignal(int, int)  # row_id, progress_value
    status_updated = pyqtSignal(int, str)  # row_id, status
    download_complete = pyqtSignal(int, bool, str)  # row_id, success, message

    def __init__(self, row_id, url, output_dir):
        super().__init__()
        self.row_id = row_id
        self.url = url
        self.output_dir = output_dir
        self.is_running = True
        
    def run(self):
        # Update status
        self.status_updated.emit(self.row_id, "Downloading")
        
        def progress_callback(progress):
            self.progress_updated.emit(self.row_id, progress)
        
        # Start download with progress monitoring
        success, message, file_path = download_episode_with_progress(
            self.url, 
            self.output_dir, 
            progress_callback
        )
        
        if success:
            self.status_updated.emit(self.row_id, "Completed")
        else:
            self.status_updated.emit(self.row_id, "Failed")
        
        self.download_complete.emit(self.row_id, success, message)
        
    def stop(self):
        self.is_running = False
        self.terminate()


class NkiriDownloaderGUI(QMainWindow):
    # Signal for showing error messages from threads
    error_signal = pyqtSignal(str, str)
    episodes_extracted_signal = pyqtSignal(list)  # List of extracted episode URLs
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Nkiri Downloader")
        self.setGeometry(100, 100, 900, 650)
        
        self.download_threads = {}
        self.download_count = 0
        self.url_mapping = {}  # Maps row positions to URLs
        self.download_queue = []  # Queue for pending downloads
        self.max_concurrent_downloads = 3  # Default maximum concurrent downloads
        self.auto_extract = True  # Auto-extract episodes from series URLs
        
        # Set default output directory and ensure it exists
        self.output_dir = os.path.join(os.getcwd(), "downloads")
        try:
            os.makedirs(self.output_dir, exist_ok=True)
        except Exception as e:
            # If we can't create the default directory, use the current directory
            print(f"Error creating downloads directory: {str(e)}")
            self.output_dir = os.getcwd()
            
        # Connect signals
        self.error_signal.connect(self.show_error_message)
        self.episodes_extracted_signal.connect(self.process_extracted_episodes)
        
        self.initUI()
        
    def initUI(self):
        # Main widget and layout
        main_widget = QWidget()
        main_layout = QVBoxLayout()
        
        # URL input section
        url_layout = QHBoxLayout()
        url_label = QLabel("URL:")
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("Enter Nkiri URL (episode or series page)")
        url_layout.addWidget(url_label)
        url_layout.addWidget(self.url_input)
        
        # Buttons section
        button_layout = QHBoxLayout()
        
        self.add_button = QPushButton("Add URL")
        self.add_button.clicked.connect(self.add_download)
        
        self.multi_url_button = QPushButton("Add Multiple URLs")
        self.multi_url_button.clicked.connect(self.show_multi_url_dialog)
        
        self.output_button = QPushButton("Set Output Directory")
        self.output_button.clicked.connect(self.set_output_directory)
        
        button_layout.addWidget(self.add_button)
        button_layout.addWidget(self.multi_url_button)
        button_layout.addWidget(self.output_button)
        
        # Concurrent downloads setting
        concurrent_layout = QHBoxLayout()
        concurrent_label = QLabel("Max Concurrent Downloads:")
        self.concurrent_spinner = QSpinBox()
        self.concurrent_spinner.setMinimum(1)
        self.concurrent_spinner.setMaximum(10)
        self.concurrent_spinner.setValue(self.max_concurrent_downloads)
        self.concurrent_spinner.valueChanged.connect(self.update_max_concurrent)
        
        # Auto-extract checkbox
        self.auto_extract_checkbox = QCheckBox("Auto-extract episodes from series URLs")
        self.auto_extract_checkbox.setChecked(self.auto_extract)
        self.auto_extract_checkbox.stateChanged.connect(self.toggle_auto_extract)
        
        concurrent_layout.addWidget(concurrent_label)
        concurrent_layout.addWidget(self.concurrent_spinner)
        concurrent_layout.addWidget(self.auto_extract_checkbox)
        concurrent_layout.addStretch()
        
        # Output directory display - remove the directory creation here since we do it in __init__
        self.directory_label = QLabel(f"Output Directory: {self.output_dir}")
        
        # Downloads table
        self.downloads_table = QTableWidget(0, 4)
        self.downloads_table.setHorizontalHeaderLabels(["Name", "Status", "Progress", "Actions"])
        self.downloads_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.downloads_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.downloads_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.downloads_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        
        # Batch action buttons
        batch_button_layout = QHBoxLayout()
        
        self.start_all_button = QPushButton("Start All")
        self.start_all_button.clicked.connect(self.start_all_downloads)
        
        self.cancel_all_button = QPushButton("Cancel All")
        self.cancel_all_button.clicked.connect(self.cancel_all_downloads)
        
        batch_button_layout.addWidget(self.start_all_button)
        batch_button_layout.addWidget(self.cancel_all_button)
        
        # Add all widgets to main layout
        main_layout.addLayout(url_layout)
        main_layout.addLayout(button_layout)
        main_layout.addLayout(concurrent_layout)
        main_layout.addWidget(self.directory_label)
        main_layout.addWidget(self.downloads_table)
        main_layout.addLayout(batch_button_layout)
        
        # Set the main widget
        main_widget.setLayout(main_layout)
        self.setCentralWidget(main_widget)
    
    def update_max_concurrent(self, value):
        self.max_concurrent_downloads = value
        # Check if we can start new downloads from the queue
        self.process_download_queue()
    
    def toggle_auto_extract(self, state):
        self.auto_extract = (state == Qt.Checked)
        
    def event(self, event):
        # Handle our custom event
        if event.type() == AddUrlEvent.EVENT_TYPE:
            # Add the URL to the queue
            self.add_url_to_queue(event.url)
            return True
        return super().event(event)
    
    @pyqtSlot(str, str)
    def show_error_message(self, title, message):
        QMessageBox.warning(self, title, message)
    
    @pyqtSlot(list)
    def process_extracted_episodes(self, episode_urls):
        if not episode_urls:
            return
            
        # Show progress dialog for large URL lists
        progress_dialog = None
        if len(episode_urls) > 10:
            progress_dialog = QProgressDialog("Adding episodes...", "Cancel", 0, len(episode_urls), self)
            progress_dialog.setWindowTitle("Processing Episodes")
            progress_dialog.setWindowModality(Qt.WindowModal)
            progress_dialog.show()
        
        # Add URLs to queue
        added_urls = []
        for i, url in enumerate(episode_urls):
            if progress_dialog and progress_dialog.wasCanceled():
                break
            
            self.add_url_to_queue(url)
            added_urls.append(url)
            
            if progress_dialog:
                progress_dialog.setValue(i + 1)
        
        # Close progress dialog if it exists
        if progress_dialog:
            progress_dialog.close()
        
        QMessageBox.information(self, "Episodes Added", f"Added {len(added_urls)} episodes to the queue")
    
    def should_auto_extract(self, url):
        """Check if a URL is likely a series page that should be auto-extracted"""
        if not self.auto_extract:
            return False
            
        # Various checks for series page URLs
        if '/series/' in url:
            return True
            
        # Check for common patterns in Nkiri URLs
        if 'nkiri.com/' in url and not url.endswith(('.mp4', '.mkv', '.avi', '.mov')):
            # Inspect URL structure - if it doesn't look like a direct download, likely a series
            path_parts = urlparse(url).path.split('/')
            # If URL path has few parts and doesn't end with a file extension, likely a series page
            if len(path_parts) <= 3 and '.' not in path_parts[-1]:
                return True
                
        return False
    
    def show_multi_url_dialog(self):
        dialog = MultiUrlDialog(self)
        if dialog.exec_():
            # Get URLs from dialog
            urls = dialog.getUrls()
            if urls:
                # Show progress dialog for large URL lists
                progress_dialog = None
                if len(urls) > 10:
                    progress_dialog = QProgressDialog("Adding URLs...", "Cancel", 0, len(urls), self)
                    progress_dialog.setWindowTitle("Processing URLs")
                    progress_dialog.setWindowModality(Qt.WindowModal)
                    progress_dialog.show()
                
                # Add URLs to queue
                added_urls = []
                auto_extract = dialog.shouldAutoExtract()
                
                for i, url in enumerate(urls):
                    if progress_dialog and progress_dialog.wasCanceled():
                        break
                    
                    # Check if should auto-extract
                    if auto_extract and self.should_auto_extract(url):
                        self.extract_episodes_from_url(url)
                    else:
                        self.add_url_to_queue(url)
                        added_urls.append(url)
                    
                    if progress_dialog:
                        progress_dialog.setValue(i + 1)
                
                # Close progress dialog if it exists
                if progress_dialog:
                    progress_dialog.close()
                
                if added_urls:
                    QMessageBox.information(self, "URLs Added", f"Added {len(added_urls)} URLs to the queue")
                
                # Auto-start downloads if requested
                if dialog.shouldAutoStart() and (added_urls or self.download_queue):
                    self.start_all_downloads()
    
    def add_url_to_queue(self, url):
        if not url:
            return
            
        # Add to table
        row_position = self.downloads_table.rowCount()
        self.downloads_table.insertRow(row_position)
        
        # Extract filename from URL
        filename = os.path.basename(url.split('?')[0])
        
        # Set table items
        self.downloads_table.setItem(row_position, 0, QTableWidgetItem(filename))
        self.downloads_table.setItem(row_position, 1, QTableWidgetItem("Queued"))
        
        # Store URL in mapping
        self.url_mapping[row_position] = url
        
        # Progress bar
        progress_bar = QProgressBar()
        progress_bar.setRange(0, 100)
        progress_bar.setValue(0)
        self.downloads_table.setCellWidget(row_position, 2, progress_bar)
        
        # Action buttons
        action_widget = QWidget()
        action_layout = QHBoxLayout()
        action_layout.setContentsMargins(0, 0, 0, 0)
        
        start_button = QPushButton("Start")
        start_button.setObjectName(f"start_button_{row_position}")
        cancel_button = QPushButton("Cancel")
        cancel_button.setObjectName(f"cancel_button_{row_position}")
        
        # Use row_id to track this download
        row_id = self.download_count
        self.download_count += 1
        
        start_button.clicked.connect(lambda: self.queue_download(row_id, row_position, url))
        cancel_button.clicked.connect(lambda: self.cancel_download(row_id, row_position))
        
        action_layout.addWidget(start_button)
        action_layout.addWidget(cancel_button)
        action_widget.setLayout(action_layout)
        
        self.downloads_table.setCellWidget(row_position, 3, action_widget)
        
    def start_all_downloads(self):
        for row in range(self.downloads_table.rowCount()):
            status = self.downloads_table.item(row, 1).text()
            if status == "Queued":
                # Get the URL from our mapping
                if row in self.url_mapping:
                    url = self.url_mapping[row]
                    # Create a new row_id
                    row_id = self.download_count
                    self.download_count += 1
                    # Queue the download
                    self.queue_download(row_id, row, url)
    
    def cancel_all_downloads(self):
        # Create a list of row_ids to cancel (can't modify during iteration)
        to_cancel = []
        for row_id, data in self.download_threads.items():
            to_cancel.append((row_id, data["row"]))
        
        for row_id, row_position in to_cancel:
            self.cancel_download(row_id, row_position)
        
        # Clear the download queue
        self.download_queue.clear()
        
        # Update status for queued items
        for row in range(self.downloads_table.rowCount()):
            status = self.downloads_table.item(row, 1).text()
            if status == "Queued":
                self.downloads_table.item(row, 1).setText("Cancelled")
        
    def set_output_directory(self):
        dir_path = QFileDialog.getExistingDirectory(self, "Select Output Directory")
        if dir_path:
            try:
                # Test write access by creating the directory
                os.makedirs(dir_path, exist_ok=True)
                # Try to create a test file
                test_file = os.path.join(dir_path, '.test_write_access')
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)  # Clean up
                
                # If we get here, we have write access
                self.output_dir = dir_path
                self.directory_label.setText(f"Output Directory: {self.output_dir}")
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Cannot use this directory: {str(e)}")
    
    def add_download(self):
        url = self.url_input.text().strip()
        if not url:
            QMessageBox.warning(self, "Error", "Please enter a URL")
            return
        
        # Check if this is a series URL that should be auto-extracted
        if self.should_auto_extract(url):
            # Clear the URL input first, since extraction takes time
            self.url_input.clear()
            # Extract episodes from the URL
            self.extract_episodes_from_url(url)
        else:
            self.add_url_to_queue(url)
            # Clear the URL input
            self.url_input.clear()
    
    def extract_episodes_from_url(self, url):
        """Extract episodes from a series URL in the background"""
        parent = self  # Reference to the main window
        
        # Show a progress indicator
        QMessageBox.information(self, "Extracting Episodes", "Starting episode extraction. This may take a moment.")
        
        # Run in a separate thread to avoid freezing UI
        def extract_thread():
            try:
                success, episode_urls, error = extract_episodes(url)
                
                if not success or not episode_urls:
                    # Use signal to show error message safely
                    parent.error_signal.emit("Error", f"Failed to extract episodes: {error}")
                    return
                
                # Use signal to process the extracted episodes
                parent.episodes_extracted_signal.emit(episode_urls)
            except Exception as e:
                # Handle any unexpected errors
                parent.error_signal.emit("Error", f"Extraction failed: {str(e)}")
        
        # Start the extraction thread
        threading.Thread(target=extract_thread, daemon=True).start()
    
    def queue_download(self, row_id, row_position, url):
        """Add a download to the queue and process if possible"""
        # Update UI
        self.downloads_table.item(row_position, 1).setText("Queued")
        
        # Add to download queue
        queue_item = {
            "row_id": row_id,
            "row": row_position,
            "url": url
        }
        self.download_queue.append(queue_item)
        
        # Process the queue
        self.process_download_queue()
    
    def process_download_queue(self):
        """Process the download queue based on max concurrent downloads limit"""
        # Check how many downloads are active
        active_downloads = len(self.download_threads)
        
        # Start new downloads if under the limit
        while active_downloads < self.max_concurrent_downloads and self.download_queue:
            # Get the next download from the queue
            item = self.download_queue.pop(0)
            
            # Start the download
            self.start_download(item["row_id"], item["row"], item["url"])
            
            # Update active downloads count
            active_downloads += 1
        
    def start_download(self, row_id, row_position, url):
        # Ensure the output directory exists
        try:
            os.makedirs(self.output_dir, exist_ok=True)
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Cannot create output directory: {str(e)}")
            self.downloads_table.item(row_position, 1).setText("Failed")
            return
        
        # Create and start download worker
        worker = DownloadWorker(row_id, url, self.output_dir)
        
        # Connect signals
        worker.progress_updated.connect(self.update_progress)
        worker.status_updated.connect(self.update_status)
        worker.download_complete.connect(self.download_finished)
        
        # Store the worker and start
        self.download_threads[row_id] = {
            "thread": worker,
            "row": row_position
        }
        
        worker.start()
        
        # Update UI
        self.downloads_table.item(row_position, 1).setText("Starting")
        
        # Disable the start button
        action_widget = self.downloads_table.cellWidget(row_position, 3)
        start_button = action_widget.findChild(QPushButton, f"start_button_{row_position}")
        if start_button:
            start_button.setEnabled(False)
    
    def cancel_download(self, row_id, row_position):
        # Check if in active downloads
        if row_id in self.download_threads and self.download_threads[row_id]["thread"].isRunning():
            # Stop the download thread
            self.download_threads[row_id]["thread"].stop()
            
            # Update UI
            self.downloads_table.item(row_position, 1).setText("Cancelled")
            
            # Remove from active downloads
            del self.download_threads[row_id]
            
            # Process the queue to start next download
            self.process_download_queue()
        else:
            # Check if in queue and remove
            for i, item in enumerate(self.download_queue):
                if item["row"] == row_position:
                    del self.download_queue[i]
                    # Update UI
                    self.downloads_table.item(row_position, 1).setText("Cancelled")
                    break
            
        # Keep the URL in the mapping in case the user wants to restart it
    
    @pyqtSlot(int, int)
    def update_progress(self, row_id, progress_value):
        if row_id in self.download_threads:
            row = self.download_threads[row_id]["row"]
            progress_bar = self.downloads_table.cellWidget(row, 2)
            if progress_bar:
                progress_bar.setValue(progress_value)
    
    @pyqtSlot(int, str)
    def update_status(self, row_id, status):
        if row_id in self.download_threads:
            row = self.download_threads[row_id]["row"]
            self.downloads_table.item(row, 1).setText(status)
    
    @pyqtSlot(int, bool, str)
    def download_finished(self, row_id, success, message):
        if row_id in self.download_threads:
            row = self.download_threads[row_id]["row"]
            
            # Update UI with result
            if success:
                self.downloads_table.item(row, 1).setText("Completed")
                
                # No need to keep completed URLs in the mapping, but we could
                # if we want to implement a "restart" feature
                # if row in self.url_mapping:
                #     del self.url_mapping[row]
            else:
                self.downloads_table.item(row, 1).setText("Failed")
                QMessageBox.warning(self, "Download Failed", message)
            
            # Enable the start button (for retry)
            action_widget = self.downloads_table.cellWidget(row, 3)
            start_button = action_widget.findChild(QPushButton, f"start_button_{row}")
            if start_button:
                start_button.setEnabled(True)
            
            # Remove from active downloads
            if row_id in self.download_threads:
                del self.download_threads[row_id]
            
            # Process the queue to start next download
            self.process_download_queue()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = NkiriDownloaderGUI()
    window.show()
    sys.exit(app.exec_()) 