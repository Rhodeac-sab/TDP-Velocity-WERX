import os
import time
import requests
import smtplib
import mysql.connector
from email.mime.text import MIMEText
from typing import Optional, Callable  # Import Optional and Callable
import logging  # Import the logging module
from secrets import token_urlsafe  # Import secure token generator
import asyncio  # Import asyncio
import aiohttp  # Import aiohttp
import pytest  # Import pytest
import unittest
from unittest.mock import patch
from unittest.mock import MagicMock


# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)  # Get a logger for this module


# Database connection
DB_HOST = "localhost"
DB_USER = "TBP_admin"
DB_PASSWORD = "your_password"
DB_NAME = "your_database"


def connect_to_database() -> mysql.connector.MySQLConnection:
    """Establish a connection to the MySQL database.

    Returns:
        mysql.connector.MySQLConnection: A database connection object.

    Raises:
        mysql.connector.Error: If a database connection error occurs.
    """
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
        )
        return connection
    except mysql.connector.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise  # Re-raise the exception for the caller to handle



# Slack webhook URL
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"  #  Use environment variable

# Max retries before failure
MAX_RETRIES = 3


def update_metrics(db_connection: mysql.connector.MySQLConnection) -> None:
    """Update metrics table after each process.

    Args:
        db_connection (mysql.connector.MySQLConnection): The database connection.
    """
    try:
        cursor = db_connection.cursor()
        cursor.execute(
            """
            INSERT INTO FileProcessingMetrics (TotalProcessed, TotalFailed, TotalRetried, AvgProcessingTime)
            SELECT
                COUNT(*) AS TotalProcessed,
                SUM(CASE WHEN Status = 'Failed' THEN 1 ELSE 0 END) AS TotalFailed,
                SUM(CASE WHEN Status = 'Retried' THEN 1 ELSE 0 END) AS TotalRetried,
                AVG(DurationSeconds) AS AvgProcessingTime
            FROM FileProcessingMonitor
            """
        )
        db_connection.commit()
        logger.info("Metrics table updated successfully.")
    except mysql.connector.Error as e:
        logger.error(f"Error updating metrics: {e}")
        raise  # Re-raise to allow for handling at a higher level
    finally:
        cursor.close()  # Ensure the cursor is closed


def log_message(
    db_connection: mysql.connector.MySQLConnection,
    file_id: int,
    level: str,
    message: str,
) -> None:
    """Log messages to the database.

    Args:
        db_connection (mysql.connector.MySQLConnection): The database connection.
        file_id (int): The ID of the file being processed.
        level (str): The log level (e.g., 'INFO', 'ERROR').
        message (str): The log message.
    """
    try:
        cursor = db_connection.cursor()
        query = "INSERT INTO FileProcessingLogs (FileID, LogMessage, LogLevel) VALUES (%s, %s, %s)"
        cursor.execute(query, (file_id, message, level))
        db_connection.commit()
        logger.info(f"Message logged to database: {message}")
    except mysql.connector.Error as e:
        logger.error(f"Error logging message: {e}")
        raise
    finally:
        cursor.close()


def update_status(
    db_connection: mysql.connector.MySQLConnection,
    file_id: int,
    status: str,
    error_log: Optional[str] = None,
    duration: Optional[float] = None,
    retries: Optional[int] = None,
) -> None:
    """Update processing status in the database.

    Args:
        db_connection (mysql.connector.MySQLConnection): The database connection.
        file_id (int): The ID of the file.
        status (str): The processing status (e.g., 'Processing', 'Completed').
        error_log (Optional[str]): An error message, if any.
        duration (Optional[float]): The processing duration in seconds.
        retries (Optional[int]): The number of retries.
    """
    try:
        cursor = db_connection.cursor()
        query = """
            UPDATE FileProcessingMonitor
            SET Status = %s, ErrorLog = %s, DurationSeconds = %s, LastRetryAt = NOW(), Retries = %s, ProcessedAt = NOW()
            WHERE FileID = %s
        """
        cursor.execute(query, (status, error_log, duration, retries, file_id))
        db_connection.commit()
        logger.info(f"Status updated for file {file_id} to {status}.")
    except mysql.connector.Error as e:
        logger.error(f"Error updating status: {e}")
        raise
    finally:
        cursor.close()



async def send_slack_notification(message: str) -> None:
    """Send Slack notification asynchronously.

    Args:
        message (str): The message to send.
    """
    payload = {"text": message}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(SLACK_WEBHOOK_URL, json=payload) as response:
                response.raise_for_status()  # Raise for bad status codes.
                logger.info("Slack notification sent successfully.")
    except aiohttp.ClientError as e:
        logger.error(f"Failed to send Slack notification: {e}")
        raise  # Re-raise the exception after logging


def send_email_alert(email: str, subject: str, message: str) -> None:
    """Send email alert.

    Args:
        email (str): The recipient's email address.
        subject (str): The email subject.
        message (str): The email message.
    """
    smtp_server = "smtp.your-email.com"  #  Use environment variable
    sender_email = "alert@yourdomain.com"  # Use environment variable
    password = "yourpassword"  #  Use environment variable

    msg = MIMEText(message)
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = email

    try:
        server = smtplib.SMTP(smtp_server, 587)
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, email, msg.as_string())
        server.quit()
        logger.info("Email alert sent successfully.")
    except (smtplib.SMTPException, Exception) as e:
        logger.error(f"Email error: {e}")
        raise



async def process_file(
    db_connection: mysql.connector.MySQLConnection, file_id: int, file_path: str, retries: int = 0
) -> None:
    """Process a file, with retry logic, using asyncio for I/O-bound operations.

    Args:
        db_connection (mysql.connector.MySQLConnection): The database connection.
        file_id (int): The ID of the file to process.
        file_path (str): The path to the file.
        retries (int, optional): The number of retries. Defaults to 0.
    """
    start_time = time.time()

    log_message(db_connection, file_id, "INFO", f"Processing file: {file_path}")
    try:
        # Simulate an I/O-bound operation using asyncio.sleep
        await asyncio.sleep(2)

        if retries > 0:
            log_message(
                db_connection, file_id, "WARNING", f"Retry attempt {retries} for file: {file_path}"
            )

        duration = round(time.time() - start_time, 2)
        update_status(db_connection, file_id, "Completed", duration=duration, retries=retries)
        update_metrics(db_connection)
        log_message(
            db_connection, file_id, "INFO", f"File processed successfully in {duration} seconds."
        )

    except Exception as e:
        error_msg = str(e)
        log_message(db_connection, file_id, "ERROR", f"Error processing file: {error_msg}")

        if retries < MAX_RETRIES:
            retries += 1
            update_status(db_connection, file_id, "Retried", error_log=error_msg, retries=retries)
            await asyncio.sleep(2**retries)  # Exponential backoff using asyncio.sleep
            await process_file(db_connection, file_id, file_path, retries)  # Retry
        else:
            update_status(db_connection, file_id, "Failed", error_log=error_msg, retries=retries)
            update_metrics(db_connection)
            log_message(
                db_connection, file_id, "ERROR", "File processing failed after max retries."
            )

            alert_message = (
                f"🚨 File Processing Failed 🚨\n"
                f"File: {file_path}\n"
                f"Retries: {MAX_RETRIES}\n"
                f"Error: {error_msg}"
            )
            await send_slack_notification(alert_message)  # Await the async function
            send_email_alert(
                "admin@yourdomain.com", "File Processing Failure", alert_message
            )



async def main() -> None:
    """Main function to run the file processing."""
    db_connection = connect_to_database()  # Establish database connection
    try:
        file_id = 1  # Should be retrieved dynamically, see comment below
        file_path = "/path/to/your/file.txt"
        update_status(db_connection, file_id, "Processing")
        await process_file(db_connection, file_id, file_path)  # Await the async process_file
    except Exception as e:
        logger.error(f"An error occurred in main: {e}")
    finally:
        db_connection.close()  # Ensure the database connection is closed



if __name__ == "__main__":
    asyncio.run(main())  # Run the main function using asyncio


class TestFileProcessing(unittest.IsolatedAsyncioTestCase):  # Use IsolatedAsyncioTestCase

    @patch("mysql.connector.connect")
    @patch("logging.Logger.info")
    async def test_update_metrics(self, mock_logger, mock_db_connect):
        mock_db_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_connection.cursor.return_value = mock_cursor
        mock_db_connect.return_value = mock_db_connection

        update_metrics(mock_db_connection)

        mock_cursor.execute.assert_called_once()
        mock_db_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_logger.assert_called_with("Metrics table updated successfully.")

    @patch("mysql.connector.connect")
    @patch("logging.Logger.error")
    async def test_update_metrics_db_error(self, mock_logger, mock_db_connect):
        mock_db_connection = MagicMock()
        mock_db_connect.return_value = mock_db_connection
        mock_db_connection.cursor.side_effect = mysql.connector.Error("DB Error")

        with self.assertRaises(mysql.connector.Error):
            update_metrics(mock_db_connection)
        mock_logger.assert_called_once()

    @patch("mysql.connector.connect")
    @patch("logging.Logger.info")
    async def test_log_message(self, mock_logger, mock_db_connect):
        mock_db_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_connection.cursor.return_value = mock_cursor
        mock_db_connect.return_value = mock_db_connection

        test_file_id = 123
        test_level = "INFO"
        test_message = "Test log message"
        log_message(mock_db_connection, test_file_id, test_level, test_message)

        mock_cursor.execute.assert_called_once_with(
            "INSERT INTO FileProcessingLogs (FileID, LogMessage, LogLevel) VALUES (%s, %s, %s)",
            (test_file_id, test_message, test_level),
        )
        mock_db_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_logger.assert_called_with(f"Message logged to database: {test_message}")

    @patch("mysql.connector.connect")
    @patch("logging.Logger.error")
    async def test_log_message_db_error(self, mock_logger, mock_db_connect):
        mock_db_connection = MagicMock()
        mock_db_connect.return_value = mock_db_connection
        mock_db_connection.cursor.side_effect = mysql.connector.Error("DB Error")
        test_file_id = 123
        test_level = "INFO"
        test_message = "Test log message"

        with self.assertRaises(mysql.connector.Error):
            log_message(mock_db_connection, test_file_id, test_level, test_message)
        mock_logger.assert_called_once()

    @patch("mysql.connector.connect")
    @patch("logging.Logger.info")
    async def test_update_status(self, mock_logger, mock_db_connect):
        mock_db_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_connection.cursor.return_value = mock_cursor
        mock_db_connect.return_value = mock_db_connection

        test_file_id = 456
        test_status = "Completed"
        test_error_log = "Some error"
        test_duration = 10.5
        test_retries = 2
        update_status(
            mock_db_connection,
            test_file_id,
            test_status,
            test_error_log,
            test_duration,
            test_retries,
        )

        mock_cursor.execute.assert_called_once_with(
            """
            UPDATE FileProcessingMonitor
            SET Status = %s, ErrorLog = %s, DurationSeconds = %s, LastRetryAt = NOW(), Retries = %s, ProcessedAt = NOW()
            WHERE FileID = %s
            """,
            (
                test_status,
                test_error_log,
                test_duration,
                test_retries,
                test_file_id,
            ),
        )
        mock_db_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_logger.assert_called_with(f"Status updated for file {test_file_id} to {test_status}.")

    @patch("mysql.connector.connect")
    @patch("logging.Logger.error")
    async def test_update_status_db_error(self, mock_logger, mock_db_connect):
        mock_db_connection = MagicMock()
        mock_db_connect.return_value = mock_db_connection
        mock_db_connection.cursor.side_effect = mysql.connector.Error("DB Error")
        test_file_id = 456
        test_status = "Completed"
        test_error_log = "Some error"
        test_duration = 10.5
        test_retries = 2

        with self.assertRaises(mysql.connector.Error):
            update_status(
                mock_db_connection,
                test_file_id,
                test_status,
                test_error_log,
                test_duration,
                test_retries,
            )
        mock_logger.assert_called_once()

    @patch("aiohttp.ClientSession.post")
    @patch("logging.Logger.info")
    async def test_send_slack_notification(self, mock_logger, mock_post):
        mock_response = MagicMock()
        mock_response.status = 200
        mock_post.return_value.__aenter__.return_value = mock_response

        test_message = "Test Slack message"
        await send_slack_notification(test_message)

        mock_post.assert_called_once_with(
            SLACK_WEBHOOK_URL, json={"text": test_message}
        )
        mock_response.raise_for_status.assert_called_once()
        mock_logger.assert_called_with("Slack notification sent successfully.")

    @patch("aiohttp.ClientSession.post")
    @patch("logging.Logger.error")
    async def test_send_slack_notification_error(self, mock_logger, mock_post):
        mock_post.side_effect = aiohttp.ClientError("Slack error")
        test_message = "Test Slack message"

        with self.assertRaises(aiohttp.ClientError):
            await send_slack_notification(test_message)
        mock_logger.assert_called_once()

    @patch("smtplib.SMTP")
    @patch("logging.Logger.info")
    def test_send_email_alert(self, mock_logger, mock_smtp):
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        mock_server.starttls.return_value = None
        mock_server.login.return_value = None
        mock_server.sendmail.return_value = None
        mock_server.quit.return_value = None

        test_email = "test@example.com"
        test_subject = "Test Subject"
        test_message = "Test email message"
        send_email_alert(test_email, test_subject, test_message)

        mock_smtp.assert_called_once_with("smtp.your-email.com", 587)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("alert@yourdomain.com", "yourpassword")
        mock_server.sendmail.assert_called_once()
        mock_server.quit.assert_called_once()
        mock_logger.assert_called_with("Email alert sent successfully.")

    @patch("smtplib.SMTP")
    @patch("logging.Logger.error")
    def test_send_email_alert_error(self, mock_logger, mock_smtp):
        mock_smtp.side_effect = smtplib.SMTPException("SMTP error")
        test_email = "test@example.com"
        test_subject = "Test Subject"
        test_message = "Test email message"

        with self.assertRaises(smtplib.SMTPException):
            send_email_alert(test_email, test_subject, test_message)
        mock_logger.assert_called_once()

    @patch("time.time")
    @patch("asyncio.sleep")
    @patch("logging.Logger.info")
    @patch("mysql.connector.connect")
    async def test_process_file(self, mock_db_connect, mock_logger, mock_async_sleep, mock_time):
        mock_db_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_connection.cursor.return_value = mock_cursor
        mock_db_connect.return_value = mock_db_connection

        mock_time.return_value = 100.0  # Set initial time
        mock_async_sleep.return_value = None

        test_file_id = 789
        test_file_path = "/path/to/test_file.txt"

        await process_file(mock_db_connection, test_file_id, test_file_path)

        mock_logger.assert_has_calls(
            [
                call("Processing file: /path/to/test_file.txt"),
                call("File processed successfully in 0.0 seconds."),
            ]
        )
        mock_async_sleep.assert_called_once_with(2)
        mock_cursor.execute.assert_called()  # Check for database interactions
        mock_db_connection.commit.assert_called()

    @patch("time.time")
    @patch("asyncio.sleep")
    @patch("logging.Logger.error")
    @patch("mysql.connector.connect")
    async def test_process_file_with_retry(self, mock_db_connect, mock_logger, mock_async_sleep, mock_time):
        mock_db_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_connection.cursor.return_value = mock_cursor
        mock_db_connect.return_value = mock_db_connection

        mock_time.return_value = 100.0
        mock_async_sleep.return_value = None
        # Simulate an exception
        mock_cursor.execute.side_effect = Exception("Test Error")

        test_file_id = 789
        test_file_path = "/path/to/test_file.txt"

        # Run process_file and assert that it doesn't raise an exception
        await process_file(mock_db_connection, test_file_id, test_file_path)

        # Assert that error was logged
        mock_logger.assert_called_with("Error processing file: Test Error")
        # Assert that asyncio.sleep was called with exponential backoff
        mock_async_sleep.assert_called_with(2)
        mock_cursor.execute.call_count == 2
        mock_db_connection.commit.call_count == 2

    @patch("time.time")
    @patch("asyncio.sleep")
    @patch("logging.Logger.error")
    @patch("mysql.connector.connect")
    async def test_process_file_max_retries(self, mock_db_connect, mock_logger, mock_async_sleep, mock_time):
        mock_db_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_connection.cursor.return_value = mock_cursor
        mock_db_connect.return_value = mock_db_connection

        mock_time.return_value = 100.0
        mock_async_sleep.return_value = None
        # Simulate an exception
        mock_cursor.execute.side_effect = Exception("Test Error")

        test_file_id = 789
        test_file_path = "/path/to/test_file.txt"

        # Run process_file and assert that it doesn't raise an exception
        await process_file(mock_db_connection, test_file_id, test_file_path, retries=MAX_RETRIES)

        # Assert that error was logged
        mock_logger.assert_called_with("Error processing file: Test Error")
        mock_async_sleep.call_count == MAX_RETRIES
        mock_cursor.execute.call_count == MAX_RETRIES + 1 #one initial call and then MAX_RETRIES retries
        mock_db_connection.commit.call_count == MAX_RETRIES + 1

    @patch("mysql.connector.connect")
    @patch("logging.Logger.error")
    async def test_main_db_connection_error(self, mock_logger, mock_db_connect):
        mock_db_connect.side_effect = mysql.connector.Error("Failed to connect")

        with self.assertRaises(mysql.connector.Error):
            await main()  # Call the async main function
        mock_logger.assert_called_once()

