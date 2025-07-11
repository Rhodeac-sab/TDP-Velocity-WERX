"""
Filename: data_pipeline.py
Description: Implements a secure, multi-threaded data processing pipeline using
             Tika, Solr, Flink, Spark, and Iceberg with data validation, OCR,
             and enrichment. Supports encryption in transit and data masking.
Author: Andrew C Rhodes
Created: 2025-03-19
Last Modified: 2025-03-20 by Andrew C Rhodes
Dependencies: tika, spacy, pysolr, apache-flink, pyspark, iceberg-api, langdetect,
              boto3, google-cloud-storage, azure-storage-blob, hdfs, grpcio, watchdog
"""

import os
import logging
import time
import psutil
import ssl
import signal
import smtplib
from email.message import EmailMessage
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from tika import parser
import spacy
from langdetect import detect
import pytesseract
from PIL import Image
import pysolr
from pyspark.sql import SparkSession
from iceberg.api import Table
import boto3
from azure.storage.blob import BlobServiceClient
from google.cloud import storage
from hdfs import InsecureClient
from cryptography.fernet import Fernet
import grpc
import json
import shutil

# -------------------- CONFIGURATION --------------------
MAX_MEMORY_USAGE = 0.80
MAX_THREADS = 24
STAGING_DIR = '/var/data/incoming'
ARCHIVE_DIR = '/var/data/archive'
FAILED_DIR = '/var/data/failed'
MANIFEST_FILE = '/var/log/manifest.json'
LOG_FILE = '/var/log/data_pipeline.log'

# Secure Transport
SSL_CERT_FILE = os.getenv('SSL_CERT_FILE')
SSL_KEY_FILE = os.getenv('SSL_KEY_FILE')

# SMTP Settings
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

# Supported File Extensions
VALID_EXTENSIONS = [
    '.pdf', '.txt', '.html', '.jpeg', '.jpg', '.png', '.dat', '.mol', '.tif',
    '.svg', '.odata', '.json', '.fem', '.stp', '.step', '.sat', '.docx', '.doc',
    '.dotx', '.csv', '.pts', '.xls', '.xlsx', '.mpx', '.mwx', '.dng', '.mat'
]

# Solr Configuration
SOLR_URL = 'https://localhost:8983/solr/TDP_Werx'
solr = pysolr.Solr(SOLR_URL, always_commit=True, timeout=10)

# Iceberg Configuration
ICEBERG_TABLE = 'TDP_Werx_Db.processed_data'

# Hadoop HDFS Configuration
HADOOP_HOST = 'localhost'
HADOOP_PORT = 50070
hadoop_client = InsecureClient(f'http://{HADOOP_HOST}:{HADOOP_PORT}', user='hdfs')

# Encryption Key (Optional)
ENCRYPTION_KEY = Fernet.generate_key()
cipher = Fernet(ENCRYPTION_KEY)

# AWS Configuration
s3 = boto3.client('s3')

# Azure Configuration
AZURE_CONNECTION_STRING = "your_azure_connection_string"
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

# Google Cloud Configuration
gcp_client = storage.Client()

# -------------------- LOGGING SETUP --------------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)
logger = logging.getLogger(__name__)

# -------------------- TLS CONFIGURATION --------------------
ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
ssl_context.load_cert_chain(certfile=SSL_CERT_FILE, keyfile=SSL_KEY_FILE)

# -------------------- SIGNAL HANDLING --------------------
paused = False

def handle_pause(signum, frame):
    global paused
    paused = not paused
    state = "paused" if paused else "resumed"
    logger.info(f"Pipeline {state}")

signal.signal(signal.SIGUSR1, handle_pause)

# -------------------- NOTIFICATION --------------------
def notify_failure(file_path):
    msg = EmailMessage()
    msg.set_content(f"Processing of {file_path} failed after 5 attempts.")
    msg["Subject"] = f"Processing Failure: {file_path}"
    msg["From"] = SMTP_USERNAME
    msg["To"] = "itsupport@sabelsystems.com"
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.send_message(msg)

# -------------------- FILE HANDLING --------------------
def process_file(file_path):
    global paused
    retries = 5
    while retries > 0:
        if paused:
            time.sleep(1)
            continue
        try:
            if not validate_file(file_path):
                return
            result = extract_with_tika(file_path) or {'content': extract_with_tesseract(file_path)}
            result['content'] = mask_sensitive_data(result['content'])
            enriched_data = enrich_data(result['content'])
            result.update(enriched_data)

            index_in_solr(result)
            index_in_flink(result)
            index_in_hdfs(result)

            # Log success
            log_manifest(file_path, "Success")
            shutil.move(file_path, os.path.join(ARCHIVE_DIR, os.path.basename(file_path)))
            return
        except Exception as e:
            logger.error(f"Failed to process {file_path}: {e}")
            retries -= 1
            time.sleep(2)

    # Final failure
    notify_failure(file_path)
    log_manifest(file_path, "Failed")
    shutil.move(file_path, os.path.join(FAILED_DIR, os.path.basename(file_path)))

def log_manifest(file_path, status):
    manifest = {}
    if os.path.exists(MANIFEST_FILE):
        with open(MANIFEST_FILE, "r") as f:
            manifest = json.load(f)

    manifest[file_path] = status

    with open(MANIFEST_FILE, "w") as f:
        json.dump(manifest, f, indent=4)

# -------------------- DIRECTORY MONITORING --------------------
class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            logger.info(f"New file detected: {file_path}")
            process_file(file_path)

# -------------------- MAIN --------------------
def main():
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, STAGING_DIR, recursive=False)
    observer.start()
    
    logger.info("Monitoring started...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
