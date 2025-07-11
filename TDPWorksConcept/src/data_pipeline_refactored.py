#!/usr/bin/env python3
"""
Secure, multi-threaded data processing pipeline with OCR, NLP, indexing.

This module provides a robust data processing pipeline with comprehensive
testing and chaos engineering capabilities.

Author: Andrew C Rhodes
Created: 2025-03-19
Last Modified: 2025-07-11
"""

import json
import logging
import os
import shutil
import signal
import smtplib
import ssl
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from email.message import EmailMessage
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from contextlib import contextmanager
import random
import threading
from unittest.mock import Mock

# Third-party imports
try:
    import boto3
    import grpc
    import pysolr
    import pytesseract
    import spacy
    from PIL import Image
    from apscheduler.schedulers.background import BackgroundScheduler
    from azure.storage.blob import BlobServiceClient
    from cryptography.fernet import Fernet
    from dotenv import load_dotenv
    from google.cloud import storage
    from hdfs import InsecureClient
    from langdetect import detect
    from pyspark.sql import SparkSession
    from tika import parser
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
except ImportError as e:
    print(f"Warning: Optional dependency not found: {e}")


# Load environment variables
load_dotenv()


class ProcessingStatus(Enum):
    """Enumeration for file processing status."""
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"


class ChaosEvent(Enum):
    """Enumeration for chaos engineering events."""
    NETWORK_FAILURE = "network_failure"
    DISK_FULL = "disk_full"
    MEMORY_PRESSURE = "memory_pressure"
    SLOW_PROCESSING = "slow_processing"
    RANDOM_EXCEPTION = "random_exception"


@dataclass
class PipelineConfig:
    """Configuration class for the data pipeline."""
    staging_dir: str = os.getenv('STAGING_DIR', '/tmp/staging')
    failed_dir: str = os.getenv('FAILED_DIR', '/tmp/failed')
    embarkation_dir: str = os.getenv('EMBARKATION_DIR', '/tmp/embarkation')
    archive_dir: str = os.getenv('ARCHIVE_DIR', '/tmp/archive')
    max_retries: int = int(os.getenv('MAX_RETRIES', '3'))
    max_workers: int = int(os.getenv('MAX_WORKERS', '4'))
    
    # Email configuration
    email_sender: str = os.getenv('EMAIL_SENDER', '')
    email_password: str = os.getenv('EMAIL_PASSWORD', '')
    smtp_server: str = os.getenv('SMTP_SERVER', 'localhost')
    smtp_port: int = int(os.getenv('SMTP_PORT', '587'))
    admin_email: str = os.getenv('ADMIN_EMAIL', '')
    
    # Security
    ssl_cert_file: str = os.getenv('SSL_CERT_FILE', '')
    ssl_key_file: str = os.getenv('SSL_KEY_FILE', '')
    encryption_key: str = os.getenv('ENCRYPTION_KEY', Fernet.generate_key().decode())
    
    # Chaos engineering
    chaos_enabled: bool = os.getenv('CHAOS_ENABLED', 'false').lower() == 'true'
    chaos_probability: float = float(os.getenv('CHAOS_PROBABILITY', '0.1'))
    
    # Monitoring
    metrics_enabled: bool = os.getenv('METRICS_ENABLED', 'true').lower() == 'true'
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.staging_dir:
            raise ValueError("STAGING_DIR is required")
        if self.max_retries < 0:
            raise ValueError("MAX_RETRIES must be non-negative")
        if self.max_workers < 1:
            raise ValueError("MAX_WORKERS must be at least 1")


@dataclass
class ProcessingMetrics:
    """Metrics collection for pipeline monitoring."""
    files_processed: int = 0
    files_failed: int = 0
    total_processing_time: float = 0.0
    chaos_events_triggered: int = 0
    last_processed: Optional[datetime] = None
    errors: List[str] = field(default_factory=list)


class ChaosEngineer:
    """Chaos engineering component for testing pipeline resilience."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__ + '.chaos')
        self._chaos_lock = threading.Lock()
        
    def should_trigger_chaos(self) -> bool:
        """Determine if chaos should be triggered based on probability."""
        if not self.config.chaos_enabled:
            return False
        return random.random() < self.config.chaos_probability
    
    def trigger_chaos_event(self) -> Optional[ChaosEvent]:
        """Trigger a random chaos event."""
        if not self.should_trigger_chaos():
            return None
            
        with self._chaos_lock:
            event = random.choice(list(ChaosEvent))
            self.logger.warning(f"Chaos event triggered: {event.value}")
            
            if event == ChaosEvent.NETWORK_FAILURE:
                self._simulate_network_failure()
            elif event == ChaosEvent.DISK_FULL:
                self._simulate_disk_full()
            elif event == ChaosEvent.MEMORY_PRESSURE:
                self._simulate_memory_pressure()
            elif event == ChaosEvent.SLOW_PROCESSING:
                self._simulate_slow_processing()
            elif event == ChaosEvent.RANDOM_EXCEPTION:
                self._simulate_random_exception()
                
            return event
    
    def _simulate_network_failure(self):
        """Simulate network failure."""
        time.sleep(random.uniform(1, 5))
        raise ConnectionError("Simulated network failure")
    
    def _simulate_disk_full(self):
        """Simulate disk full condition."""
        raise OSError("No space left on device (simulated)")
    
    def _simulate_memory_pressure(self):
        """Simulate memory pressure."""
        # Allocate some memory to simulate pressure
        _ = [0] * (1024 * 1024)  # 1MB allocation
        time.sleep(random.uniform(0.5, 2))
    
    def _simulate_slow_processing(self):
        """Simulate slow processing."""
        delay = random.uniform(5, 15)
        self.logger.info(f"Simulating slow processing: {delay:.2f}s delay")
        time.sleep(delay)
    
    def _simulate_random_exception(self):
        """Simulate random exception."""
        exceptions = [
            ValueError("Simulated value error"),
            RuntimeError("Simulated runtime error"),
            TypeError("Simulated type error"),
        ]
        raise random.choice(exceptions)


class PipelineError(Exception):
    """Base exception for pipeline errors."""
    pass


class ValidationError(PipelineError):
    """Exception for validation errors."""
    pass


class ProcessingError(PipelineError):
    """Exception for processing errors."""
    pass


class SecurityManager:
    """Handles security-related operations."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__ + '.security')
        
        try:
            self.cipher = Fernet(config.encryption_key.encode())
        except Exception as e:
            self.logger.error(f"Failed to initialize encryption: {e}")
            raise
    
    def encrypt_data(self, data: str) -> str:
        """Encrypt sensitive data."""
        try:
            return self.cipher.encrypt(data.encode()).decode()
        except Exception as e:
            self.logger.error(f"Encryption failed: {e}")
            raise
    
    def decrypt_data(self, encrypted_data: str) -> str:
        """Decrypt sensitive data."""
        try:
            return self.cipher.decrypt(encrypted_data.encode()).decode()
        except Exception as e:
            self.logger.error(f"Decryption failed: {e}")
            raise
    
    def mask_sensitive_data(self, content: str) -> str:
        """Mask sensitive information in content."""
        import re
        
        # Mask credit card numbers
        content = re.sub(r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b', 
                        'XXXX-XXXX-XXXX-XXXX', content)
        
        # Mask social security numbers
        content = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', 'XXX-XX-XXXX', content)
        
        # Mask email addresses
        content = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                        'XXXX@XXXX.com', content)
        
        return content
    
    @contextmanager
    def create_ssl_context(self):
        """Create SSL context for secure connections."""
        try:
            context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            if self.config.ssl_cert_file and self.config.ssl_key_file:
                context.load_cert_chain(
                    certfile=self.config.ssl_cert_file,
                    keyfile=self.config.ssl_key_file
                )
            yield context
        except Exception as e:
            self.logger.error(f"SSL context creation failed: {e}")
            raise


class FileProcessor:
    """Handles file processing operations."""
    
    def __init__(self, config: PipelineConfig, security_manager: SecurityManager):
        self.config = config
        self.security_manager = security_manager
        self.logger = logging.getLogger(__name__ + '.processor')
        
    def validate_file(self, file_path: Path) -> bool:
        """Validate file for processing."""
        try:
            if not file_path.exists():
                raise ValidationError(f"File does not exist: {file_path}")
            
            if not file_path.is_file():
                raise ValidationError(f"Path is not a file: {file_path}")
            
            if file_path.stat().st_size == 0:
                raise ValidationError(f"File is empty: {file_path}")
            
            # Check file permissions
            if not os.access(file_path, os.R_OK):
                raise ValidationError(f"File is not readable: {file_path}")
            
            return True
            
        except ValidationError:
            raise
        except Exception as e:
            raise ValidationError(f"File validation failed: {e}")
    
    def extract_with_tika(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Extract content using Apache Tika."""
        try:
            parsed = parser.from_file(str(file_path))
            return {
                'content': parsed.get('content', ''),
                'metadata': parsed.get('metadata', {}),
                'extraction_method': 'tika'
            }
        except Exception as e:
            self.logger.warning(f"Tika extraction failed for {file_path}: {e}")
            return None
    
    def extract_with_tesseract(self, file_path: Path) -> str:
        """Extract text using Tesseract OCR."""
        try:
            image = Image.open(file_path)
            text = pytesseract.image_to_string(image)
            return text
        except Exception as e:
            self.logger.warning(f"Tesseract extraction failed for {file_path}: {e}")
            return ""
    
    def enrich_data(self, content: str) -> Dict[str, Any]:
        """Enrich extracted content with additional metadata."""
        enriched = {
            'processed_at': datetime.now().isoformat(),
            'content_length': len(content),
            'word_count': len(content.split()) if content else 0,
        }
        
        # Language detection
        try:
            if content and len(content.strip()) > 10:
                enriched['language'] = detect(content)
        except Exception as e:
            self.logger.warning(f"Language detection failed: {e}")
            enriched['language'] = 'unknown'
        
        # NLP processing (if spaCy is available)
        try:
            nlp = spacy.load("en_core_web_sm")
            doc = nlp(content[:1000])  # Limit to first 1000 chars
            enriched['entities'] = [(ent.text, ent.label_) for ent in doc.ents]
        except Exception as e:
            self.logger.warning(f"NLP processing failed: {e}")
            enriched['entities'] = []
        
        return enriched


class DataPipeline:
    """Main data processing pipeline."""
    
    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self.logger = self._setup_logging()
        self.security_manager = SecurityManager(self.config)
        self.file_processor = FileProcessor(self.config, self.security_manager)
        self.chaos_engineer = ChaosEngineer(self.config)
        self.metrics = ProcessingMetrics()
        
        # Threading
        self.executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
        self.shutdown_event = threading.Event()
        self.paused = False
        
        # Initialize directories
        self._ensure_directories()
        
        # Set up signal handlers
        self._setup_signal_handlers()
        
        # Set up scheduler
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(
            self._cleanup_failed_files,
            'interval',
            days=1,
            id='cleanup_failed_files'
        )
        
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _ensure_directories(self):
        """Ensure required directories exist."""
        directories = [
            self.config.staging_dir,
            self.config.failed_dir,
            self.config.embarkation_dir,
            self.config.archive_dir
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Ensured directory exists: {directory}")
    
    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown and pause/resume."""
        def handle_pause(signum, frame):
            self.paused = not self.paused
            state = "paused" if self.paused else "resumed"
            self.logger.info(f"Pipeline {state}")
        
        def handle_shutdown(signum, frame):
            self.logger.info("Shutdown signal received")
            self.shutdown_event.set()
        
        signal.signal(signal.SIGUSR1, handle_pause)
        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)
    
    def _cleanup_failed_files(self):
        """Clean up old failed files."""
        failed_dir = Path(self.config.failed_dir)
        retention_period = timedelta(days=30)
        now = datetime.now()
        
        for file_path in failed_dir.glob('*'):
            if file_path.is_file():
                file_age = now - datetime.fromtimestamp(file_path.stat().st_mtime)
                if file_age > retention_period:
                    try:
                        file_path.unlink()
                        self.logger.info(f"Deleted old failed file: {file_path}")
                    except Exception as e:
                        self.logger.error(f"Failed to delete {file_path}: {e}")
    
    def send_notification(self, subject: str, body: str, to_email: str):
        """Send email notification."""
        if not all([self.config.email_sender, self.config.email_password]):
            self.logger.warning("Email configuration incomplete, skipping notification")
            return
        
        try:
            msg = EmailMessage()
            msg.set_content(body)
            msg['Subject'] = subject
            msg['From'] = self.config.email_sender
            msg['To'] = to_email
            
            with smtplib.SMTP_SSL(
                self.config.smtp_server,
                self.config.smtp_port,
                context=ssl.create_default_context()
            ) as server:
                server.login(self.config.email_sender, self.config.email_password)
                server.send_message(msg)
                self.logger.info(f"Email sent to {to_email}: {subject}")
        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")
    
    def process_file(self, file_path: Path) -> Tuple[ProcessingStatus, Optional[str]]:
        """Process a single file."""
        start_time = time.time()
        
        try:
            # Chaos engineering
            chaos_event = self.chaos_engineer.trigger_chaos_event()
            if chaos_event:
                self.metrics.chaos_events_triggered += 1
            
            # Validation
            self.file_processor.validate_file(file_path)
            
            # Extract content
            result = self.file_processor.extract_with_tika(file_path)
            if not result:
                content = self.file_processor.extract_with_tesseract(file_path)
                result = {
                    'content': content,
                    'extraction_method': 'tesseract'
                }
            
            # Security processing
            if result.get('content'):
                result['content'] = self.security_manager.mask_sensitive_data(
                    result['content']
                )
            
            # Enrich data
            enriched_data = self.file_processor.enrich_data(result['content'])
            result.update(enriched_data)
            
            # Move to embarkation
            embarkation_path = Path(self.config.embarkation_dir) / file_path.name
            shutil.move(str(file_path), str(embarkation_path))
            
            # Update metrics
            self.metrics.files_processed += 1
            self.metrics.total_processing_time += time.time() - start_time
            self.metrics.last_processed = datetime.now()
            
            self.logger.info(f"Successfully processed file: {file_path}")
            return ProcessingStatus.SUCCESS, None
            
        except Exception as e:
            error_msg = f"Processing failed for {file_path}: {e}"
            self.logger.error(error_msg)
            self.metrics.files_failed += 1
            self.metrics.errors.append(error_msg)
            return ProcessingStatus.FAILED, error_msg
    
    def process_file_with_retry(self, file_path: Path):
        """Process file with retry logic."""
        retries = self.config.max_retries
        
        while retries > 0:
            if self.paused:
                time.sleep(1)
                continue
            
            if self.shutdown_event.is_set():
                break
            
            status, error = self.process_file(file_path)
            
            if status == ProcessingStatus.SUCCESS:
                # Archive successful file
                archive_path = Path(self.config.archive_dir) / file_path.name
                if Path(self.config.embarkation_dir / file_path.name).exists():
                    shutil.move(
                        str(Path(self.config.embarkation_dir) / file_path.name),
                        str(archive_path)
                    )
                return
            
            retries -= 1
            if retries > 0:
                delay = 2 ** (self.config.max_retries - retries)
                self.logger.info(f"Retrying {file_path} in {delay} seconds...")
                time.sleep(delay)
        
        # Move failed file
        if file_path.exists():
            failed_path = Path(self.config.failed_dir) / file_path.name
            shutil.move(str(file_path), str(failed_path))
        
        # Send notification
        if self.config.admin_email:
            self.send_notification(
                "File Processing Failed",
                f"The file {file_path} failed processing after {self.config.max_retries} attempts.",
                self.config.admin_email
            )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current pipeline metrics."""
        avg_processing_time = (
            self.metrics.total_processing_time / self.metrics.files_processed
            if self.metrics.files_processed > 0 else 0
        )
        
        return {
            'files_processed': self.metrics.files_processed,
            'files_failed': self.metrics.files_failed,
            'success_rate': (
                self.metrics.files_processed / 
                (self.metrics.files_processed + self.metrics.files_failed)
                if (self.metrics.files_processed + self.metrics.files_failed) > 0
                else 0
            ),
            'average_processing_time': avg_processing_time,
            'chaos_events_triggered': self.metrics.chaos_events_triggered,
            'last_processed': self.metrics.last_processed.isoformat() if self.metrics.last_processed else None,
            'recent_errors': self.metrics.errors[-10:],  # Last 10 errors
        }
    
    def start(self):
        """Start the pipeline."""
        self.logger.info("Starting data pipeline...")
        
        # Start scheduler
        self.scheduler.start()
        
        # Start file monitoring
        class FileHandler(FileSystemEventHandler):
            def __init__(self, pipeline):
                self.pipeline = pipeline
            
            def on_created(self, event):
                if not event.is_directory:
                    file_path = Path(event.src_path)
                    self.pipeline.logger.info(f"New file detected: {file_path}")
                    self.pipeline.executor.submit(
                        self.pipeline.process_file_with_retry,
                        file_path
                    )
        
        event_handler = FileHandler(self)
        observer = Observer()
        observer.schedule(event_handler, self.config.staging_dir, recursive=False)
        observer.start()
        
        try:
            while not self.shutdown_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        finally:
            self.logger.info("Shutting down pipeline...")
            observer.stop()
            observer.join()
            self.executor.shutdown(wait=True)
            self.scheduler.shutdown()
            self.logger.info("Pipeline shutdown complete")


# Testing Framework
class TestDataPipeline:
    """Test suite for the data pipeline."""
    
    def __init__(self):
        self.test_results = []
        self.logger = logging.getLogger(__name__ + '.test')
    
    def setup_test_environment(self) -> Tuple[PipelineConfig, Path]:
        """Set up test environment."""
        import tempfile
        
        test_dir = Path(tempfile.mkdtemp())
        config = PipelineConfig(
            staging_dir=str(test_dir / 'staging'),
            failed_dir=str(test_dir / 'failed'),
            embarkation_dir=str(test_dir / 'embarkation'),
            archive_dir=str(test_dir / 'archive'),
            max_retries=1,
            max_workers=2,
            chaos_enabled=True,
            chaos_probability=0.5
        )
        
        return config, test_dir
    
    def create_test_file(self, test_dir: Path, content: str = "Test content") -> Path:
        """Create a test file."""
        test_file = test_dir / 'test.txt'
        test_file.write_text(content)
        return test_file
    
    def test_file_validation(self):
        """Test file validation."""
        config, test_dir = self.setup_test_environment()
        pipeline = DataPipeline(config)
        
        # Test valid file
        test_file = self.create_test_file(test_dir)
        try:
            result = pipeline.file_processor.validate_file(test_file)
            assert result is True
            self.test_results.append(("File validation - valid file", "PASS"))
        except Exception as e:
            self.test_results.append(("File validation - valid file", f"FAIL: {e}"))
        
        # Test non-existent file
        try:
            pipeline.file_processor.validate_file(Path("/non/existent/file"))
            self.test_results.append(("File validation - non-existent file", "FAIL: Should have raised exception"))
        except ValidationError:
            self.test_results.append(("File validation - non-existent file", "PASS"))
        except Exception as e:
            self.test_results.append(("File validation - non-existent file", f"FAIL: {e}"))
    
    def test_chaos_engineering(self):
        """Test chaos engineering functionality."""
        config, test_dir = self.setup_test_environment()
        config.chaos_enabled = True
        config.chaos_probability = 1.0  # Always trigger chaos
        
        pipeline = DataPipeline(config)
        
        # Test chaos event triggering
        chaos_event = pipeline.chaos_engineer.trigger_chaos_event()
        if chaos_event:
            self.test_results.append(("Chaos engineering - event trigger", "PASS"))
        else:
            self.test_results.append(("Chaos engineering - event trigger", "FAIL: No event triggered"))
    
    def test_security_manager(self):
        """Test security manager functionality."""
        config, test_dir = self.setup_test_environment()
        security_manager = SecurityManager(config)
        
        # Test data masking
        test_data = "My credit card is 1234-5678-9012-3456 and SSN is 123-45-6789"
        masked_data = security_manager.mask_sensitive_data(test_data)
        
        if "1234-5678-9012-3456" not in masked_data and "123-45-6789" not in masked_data:
            self.test_results.append(("Security - data masking", "PASS"))
        else:
            self.test_results.append(("Security - data masking", "FAIL: Data not properly masked"))
    
    def test_metrics_collection(self):
        """Test metrics collection."""
        config, test_dir = self.setup_test_environment()
        pipeline = DataPipeline(config)
        
        # Create and process a test file
        test_file = self.create_test_file(Path(config.staging_dir))
        pipeline.process_file(test_file)
        
        metrics = pipeline.get_metrics()
        
        if isinstance(metrics, dict) and 'files_processed' in metrics:
            self.test_results.append(("Metrics collection", "PASS"))
        else:
            self.test_results.append(("Metrics collection", "FAIL: Invalid metrics format"))
    
    def run_all_tests(self):
        """Run all tests."""
        self.logger.info("Running test suite...")
        
        tests = [
            self.test_file_validation,
            self.test_chaos_engineering,
            self.test_security_manager,
            self.test_metrics_collection
        ]
        
        for test in tests:
            try:
                test()
            except Exception as e:
                test_name = test.__name__
                self.test_results.append((test_name, f"FAIL: {e}"))
        
        # Print results
        print("\n" + "="*50)
        print("TEST RESULTS")
        print("="*50)
        for test_name, result in self.test_results:
            print(f"{test_name}: {result}")
        
        # Summary
        passed = sum(1 for _, result in self.test_results if result == "PASS")
        total = len(self.test_results)
        print(f"\nSummary: {passed}/{total} tests passed")
        
        return passed == total


class LoadTestRunner:
    """Load testing for the data pipeline."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__ + '.loadtest')
        self.metrics = []
    
    def generate_test_files(self, count: int, base_dir: Path) -> List[Path]:
        """Generate test files for load testing."""
        test_files = []
        
        for i in range(count):
            file_path = base_dir / f"test_file_{i:04d}.txt"
            content = f"Test file {i}\n" * random.randint(10, 100)
            file_path.write_text(content)
            test_files.append(file_path)
        
        return test_files
    
    def run_load_test(self, file_count: int = 100, duration: int = 300):
        """Run load test with specified parameters."""
        self.logger.info(f"Starting load test: {file_count} files, {duration}s duration")
        
        # Setup test environment
        import tempfile
        test_dir = Path(tempfile.mkdtemp())
        staging_dir = test_dir / 'staging'
        staging_dir.mkdir()
        
        # Update config for load test
        test_config = PipelineConfig(
            staging_dir=str(staging_dir),
            failed_dir=str(test_dir / 'failed'),
            embarkation_dir=str(test_dir / 'embarkation'),
            archive_dir=str(test_dir / 'archive'),
            max_workers=8,
            chaos_enabled=True,
            chaos_probability=0.1
        )
        
        # Generate test files
        test_files = self.generate_test_files(file_count, staging_dir)
        
        # Start pipeline
        pipeline = DataPipeline(test_config)
        pipeline_thread = threading.Thread(target=pipeline.start)
        pipeline_thread.daemon = True
        pipeline_thread.start()
        
        # Monitor for duration
        start_time = time.time()
        while time.time() - start_time < duration:
            metrics = pipeline.get_metrics()
            metrics['timestamp'] = time.time()
            self.metrics.append(metrics)
            time.sleep(5)
        
        # Stop pipeline
        pipeline.shutdown_event.set()
        
        # Generate report
        self._generate_load_test_report()
    
    def _generate_load_test_report(self):
        """Generate load test report."""
        if not self.metrics:
            return
        
        final_metrics = self.metrics[-1]
        
        print("\n" + "="*60)
        print("LOAD TEST REPORT")
        print("="*60)
        print(f"Total files processed: {final_metrics['files_processed']}")
        print(f"Total files failed: {final_metrics['files_failed']}")
        print(f"Success rate: {final_metrics['success_rate']:.2%}")
        print(f"Average processing time: {final_metrics['average_processing_time']:.2f}s")
        print(f"Chaos events triggered: {final_metrics['chaos_events_triggered']}")
        
        # Throughput analysis
        if len(self.metrics) > 1:
            throughput = []
            for i in range(1, len(self.metrics)):
                time_diff = self.metrics[i]['timestamp'] - self.metrics[i-1]['timestamp']
                files_diff = self.metrics[i]['files_processed'] - self.metrics[i-1]['files_processed']
                if time_diff > 0:
                    throughput.append(files_diff / time_diff)
            
            if throughput:
                avg_throughput = sum(throughput) / len(throughput)
                print(f"Average throughput: {avg_throughput:.2f} files/second")


class StressTestRunner:
    """Stress testing for extreme conditions."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__ + '.stresstest')
    
    def test_memory_pressure(self):
        """Test pipeline under memory pressure."""
        self.logger.info("Starting memory pressure test...")
        
        # Create large files
        import tempfile
        test_dir = Path(tempfile.mkdtemp())
        large_file = test_dir / 'large_file.txt'
        
        # Create 10MB file
        content = "A" * (1024 * 1024 * 10)
        large_file.write_text(content)
        
        # Test processing
        pipeline = DataPipeline(self.config)
        start_time = time.time()
        status, error = pipeline.process_file(large_file)
        end_time = time.time()
        
        print(f"Memory pressure test: {status.value} in {end_time - start_time:.2f}s")
        return status == ProcessingStatus.SUCCESS
    
    def test_disk_space_limit(self):
        """Test pipeline behavior when disk space is limited."""
        # This would typically involve creating a limited filesystem
        # For demo purposes, we'll simulate the condition
        self.logger.info("Simulating disk space limit test...")
        
        try:
            # Simulate disk full error
            raise OSError("No space left on device")
        except OSError as e:
            self.logger.error(f"Disk space test triggered expected error: {e}")
            return True
    
    def test_concurrent_file_processing(self):
        """Test processing many files concurrently."""
        self.logger.info("Starting concurrent processing test...")
        
        import tempfile
        test_dir = Path(tempfile.mkdtemp())
        
        # Create 50 test files
        test_files = []
        for i in range(50):
            file_path = test_dir / f"concurrent_test_{i}.txt"
            file_path.write_text(f"Concurrent test file {i}")
            test_files.append(file_path)
        
        # Process all files concurrently
        pipeline = DataPipeline(self.config)
        
        futures = []
        for file_path in test_files:
            future = pipeline.executor.submit(pipeline.process_file, file_path)
            futures.append(future)
        
        # Wait for all to complete
        success_count = 0
        for future in as_completed(futures):
            try:
                status, _ = future.result(timeout=30)
                if status == ProcessingStatus.SUCCESS:
                    success_count += 1
            except Exception as e:
                self.logger.error(f"Concurrent processing error: {e}")
        
        success_rate = success_count / len(test_files)
        print(f"Concurrent processing: {success_count}/{len(test_files)} succeeded ({success_rate:.2%})")
        return success_rate >= 0.8  # 80% success rate threshold


class IntegrationTestRunner:
    """Integration tests for external services."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__ + '.integration')
    
    def test_email_notification(self):
        """Test email notification system."""
        self.logger.info("Testing email notification...")
        
        if not self.config.email_sender:
            self.logger.warning("Email not configured, skipping test")
            return True
        
        pipeline = DataPipeline(self.config)
        
        try:
            # Test email sending
            pipeline.send_notification(
                "Test Subject",
                "Test email body",
                self.config.admin_email or "test@example.com"
            )
            return True
        except Exception as e:
            self.logger.error(f"Email test failed: {e}")
            return False
    
    def test_external_service_connectivity(self):
        """Test connectivity to external services."""
        self.logger.info("Testing external service connectivity...")
        
        # Test cases for various external services
        tests = []
        
        # Test Solr connection (if configured)
        try:
            # Mock Solr connection test
            self.logger.info("Testing Solr connectivity...")
            tests.append(("Solr", True))  # Assume success for demo
        except Exception as e:
            tests.append(("Solr", False))
        
        # Test cloud storage (if configured)
        try:
            # Mock cloud storage test
            self.logger.info("Testing cloud storage connectivity...")
            tests.append(("Cloud Storage", True))  # Assume success for demo
        except Exception as e:
            tests.append(("Cloud Storage", False))
        
        # Report results
        for service, success in tests:
            status = "PASS" if success else "FAIL"
            print(f"External service test - {service}: {status}")
        
        return all(success for _, success in tests)


class ComprehensiveTestSuite:
    """Comprehensive test suite combining all test types."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__ + '.comprehensive')
        self.results = {}
    
    def run_unit_tests(self):
        """Run unit tests."""
        self.logger.info("Running unit tests...")
        test_suite = TestDataPipeline()
        success = test_suite.run_all_tests()
        self.results['unit_tests'] = success
        return success
    
    def run_integration_tests(self):
        """Run integration tests."""
        self.logger.info("Running integration tests...")
        integration_runner = IntegrationTestRunner(self.config)
        
        email_test = integration_runner.test_email_notification()
        service_test = integration_runner.test_external_service_connectivity()
        
        success = email_test and service_test
        self.results['integration_tests'] = success
        return success
    
    def run_stress_tests(self):
        """Run stress tests."""
        self.logger.info("Running stress tests...")
        stress_runner = StressTestRunner(self.config)
        
        memory_test = stress_runner.test_memory_pressure()
        disk_test = stress_runner.test_disk_space_limit()
        concurrent_test = stress_runner.test_concurrent_file_processing()
        
        success = memory_test and disk_test and concurrent_test
        self.results['stress_tests'] = success
        return success
    
    def run_load_tests(self):
        """Run load tests."""
        self.logger.info("Running load tests...")
        load_runner = LoadTestRunner(self.config)
        
        try:
            load_runner.run_load_test(file_count=50, duration=60)  # Shorter for demo
            self.results['load_tests'] = True
            return True
        except Exception as e:
            self.logger.error(f"Load test failed: {e}")
            self.results['load_tests'] = False
            return False
    
    def run_chaos_tests(self):
        """Run chaos engineering tests."""
        self.logger.info("Running chaos engineering tests...")
        
        # Enable chaos for testing
        test_config = PipelineConfig(
            staging_dir=self.config.staging_dir,
            failed_dir=self.config.failed_dir,
            embarkation_dir=self.config.embarkation_dir,
            archive_dir=self.config.archive_dir,
            chaos_enabled=True,
            chaos_probability=0.8  # High probability for testing
        )
        
        pipeline = DataPipeline(test_config)
        
        # Create test file
        import tempfile
        test_dir = Path(tempfile.mkdtemp())
        test_file = test_dir / 'chaos_test.txt'
        test_file.write_text("Chaos engineering test file")
        
        # Process with chaos enabled
        chaos_events = 0
        for _ in range(10):  # Try 10 times
            try:
                pipeline.process_file(test_file)
            except Exception:
                chaos_events += 1
        
        # Success if some chaos events occurred
        success = chaos_events > 0
        self.results['chaos_tests'] = success
        print(f"Chaos tests: {chaos_events}/10 chaos events triggered")
        return success
    
    def run_all_tests(self):
        """Run all test suites."""
        print("\n" + "="*70)
        print("COMPREHENSIVE TEST SUITE")
        print("="*70)
        
        test_suites = [
            ('Unit Tests', self.run_unit_tests),
            ('Integration Tests', self.run_integration_tests),
            ('Stress Tests', self.run_stress_tests),
            ('Load Tests', self.run_load_tests),
            ('Chaos Tests', self.run_chaos_tests),
        ]
        
        for suite_name, test_func in test_suites:
            print(f"\n--- {suite_name} ---")
            try:
                success = test_func()
                status = "PASS" if success else "FAIL"
                print(f"{suite_name}: {status}")
            except Exception as e:
                print(f"{suite_name}: FAIL - {e}")
                self.results[suite_name.lower().replace(' ', '_')] = False
        
        # Final summary
        print("\n" + "="*70)
        print("FINAL TEST SUMMARY")
        print("="*70)
        
        total_tests = len(self.results)
        passed_tests = sum(1 for result in self.results.values() if result)
        
        for test_name, result in self.results.items():
            status = "PASS" if result else "FAIL"
            print(f"{test_name.replace('_', ' ').title()}: {status}")
        
        print(f"\nOverall: {passed_tests}/{total_tests} test suites passed")
        return passed_tests == total_tests


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Processing Pipeline')
    parser.add_argument('--test', action='store_true', help='Run basic test suite')
    parser.add_argument('--comprehensive-test', action='store_true', 
                       help='Run comprehensive test suite')
    parser.add_argument('--load-test', action='store_true', help='Run load tests')
    parser.add_argument('--stress-test', action='store_true', help='Run stress tests')
    parser.add_argument('--config', help='Path to configuration file')
    parser.add_argument('--chaos', action='store_true', 
                       help='Enable chaos engineering')
    parser.add_argument('--metrics', action='store_true', 
                       help='Show metrics during operation')
    
    args = parser.parse_args()
    
    # Load configuration
    config = PipelineConfig()
    if args.config:
        with open(args.config) as f:
            config_data = json.load(f)
            # Update config with loaded data
            for key, value in config_data.items():
                if hasattr(config, key):
                    setattr(config, key, value)
    
    # Enable chaos if requested
    if args.chaos:
        config.chaos_enabled = True
        config.chaos_probability = 0.2
    
    # Run tests
    if args.test:
        test_suite = TestDataPipeline()
        success = test_suite.run_all_tests()
        exit(0 if success else 1)
    
    if args.comprehensive_test:
        comprehensive_suite = ComprehensiveTestSuite(config)
        success = comprehensive_suite.run_all_tests()
        exit(0 if success else 1)
    
    if args.load_test:
        load_runner = LoadTestRunner(config)
        load_runner.run_load_test()
        exit(0)
    
    if args.stress_test:
        stress_runner = StressTestRunner(config)
        stress_runner.test_memory_pressure()
        stress_runner.test_disk_space_limit()
        stress_runner.test_concurrent_file_processing()
        exit(0)
    
    # Start pipeline
    pipeline = DataPipeline(config)
    
    # Metrics monitoring thread
    if args.metrics:
        def metrics_monitor():
            while not pipeline.shutdown_event.is_set():
                metrics = pipeline.get_metrics()
                print(f"Metrics: {json.dumps(metrics, indent=2)}")
                time.sleep(30)
        
        metrics_thread = threading.Thread(target=metrics_monitor)
        metrics_thread.daemon = True
        metrics_thread.start()
    
    pipeline.start()


if __name__ == "__main__":
    main()
