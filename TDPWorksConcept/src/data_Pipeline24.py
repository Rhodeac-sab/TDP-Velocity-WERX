# Filename: data_pipeline.py
# Description: Secure, multi-threaded data processing pipeline with OCR, NLP, indexing, Iceberg, Hive, and embarkation to PLM, ALM, GitLab, and ERP.
# Author: Andrew C Rhodes
# Created: 2025-03-19
# Last Modified: 2025-03-26

import os
import time
import json
import logging
import signal
import shutil
import smtplib
import ssl
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from email.message import EmailMessage
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from functools import lru_cache

# Third-party Imports
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
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler

# Load environment variables
load_dotenv()

# -------------------- CONFIGURATION --------------------
from config import CONFIG
from logging_setup import setup_logger

# Setup logger for data pipeline
logger = setup_logger()

# -------------------- SECURITY SETUP --------------------
ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
ssl_context.load_cert_chain(certfile=CONFIG['SSL_CERT_FILE'], keyfile=CONFIG['SSL_KEY_FILE'])

# Encryption Setup
cipher = Fernet(CONFIG['ENCRYPTION_KEY'])

# -------------------- SIGNAL HANDLING --------------------
paused = False

def handle_pause(signum, frame):
    global paused
    paused = not paused
    state = "paused" if paused else "resumed"
    logger.info(f"Pipeline {state}")

signal.signal(signal.SIGUSR1, handle_pause)

# -------------------- APACHE ICEBERG & HIVE INTEGRATION --------------------
spark = SparkSession.builder \
    .appName("DataPipeline") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

def write_to_iceberg(data, table_name):
    df = spark.createDataFrame([data])
    df.write.format("iceberg").mode("append").save(f"my_catalog.default.{table_name}")

def query_hive_table(table_name):
    return spark.sql(f"SELECT * FROM {table_name}").show()

# -------------------- DIRECTORY CREATION --------------------
def ensure_directories():
    """Ensure required directories exist."""
    directories = [CONFIG['FAILED_DIR'], CONFIG['EMBARKATION_DIR'], CONFIG['ARCHIVE_DIR']]
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created missing directory: {directory}")

# Call the function at startup
ensure_directories()

# -------------------- DIRECTORY MONITORING --------------------
class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            logger.info(f"New file detected: {file_path}")
            process_file(file_path)

# -------------------- FILE VALIDATION --------------------
def validate_file(file_path):
    """Checks if the file is valid for processing."""
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} does not exist.")
        return False
    if os.path.getsize(file_path) == 0:
        logger.error(f"File {file_path} is empty.")
        return False
    return True

# -------------------- CLEANUP FAILED FILES --------------------
def cleanup_failed_files():
    failed_dir = CONFIG['FAILED_DIR']
    retention_period = timedelta(days=30)
    now = datetime.now()
    
    for file_name in os.listdir(failed_dir):
        file_path = os.path.join(failed_dir, file_name)
        if os.path.isfile(file_path):
            file_modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if now - file_modified_time > retention_period:
                os.remove(file_path)
                logger.info(f"Deleted old failed file: {file_path}")

# Scheduler for cleanup
scheduler = BackgroundScheduler()
scheduler.add_job(cleanup_failed_files, 'interval', days=1)
scheduler.start()

# -------------------- EMAIL NOTIFICATIONS --------------------
def send_email(subject, body, to_email):
    """Sends an email notification."""
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = CONFIG['EMAIL_SENDER']
    msg['To'] = to_email

    with smtplib.SMTP_SSL(CONFIG['SMTP_SERVER'], CONFIG['SMTP_PORT'], context=ssl.create_default_context()) as server:
        server.login(CONFIG['EMAIL_SENDER'], CONFIG['EMAIL_PASSWORD'])
        server.send_message(msg)
        logger.info(f"Email sent to {to_email}: {subject}")

# -------------------- FILE PROCESSING --------------------
def process_file(file_path):
    """Processes a file and sends it to the embarkation directory."""
    retries = CONFIG['MAX_RETRIES']
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
            index_in_hdfs(result)
            write_to_iceberg(result, "processed_data")

            send_to_embarkation(file_path)
            log_manifest(file_path, "Success")
            shutil.move(file_path, os.path.join(CONFIG['ARCHIVE_DIR'], os.path.basename(file_path)))
            logger.info(f"Successfully processed file: {file_path}")
            return
        except Exception as e:
            logger.error(f"Failed to process {file_path}: {e}")
        retries -= 1
        time.sleep(2 ** (CONFIG['MAX_RETRIES'] - retries))
        if retries == 0:
            send_email("File Processing Failed", f"The file {file_path} failed processing.", CONFIG['ADMIN_EMAIL'])
            log_manifest(file_path, "Failed")
            shutil.move(file_path, os.path.join(CONFIG['FAILED_DIR'], os.path.basename(file_path)))

# -------------------- MAIN --------------------
def main():
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, CONFIG['STAGING_DIR'], recursive=False)
    observer.start()
    
    logger.info("Monitoring started...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        scheduler.shutdown()
    observer.join()

if __name__ == "__main__":
    main()
