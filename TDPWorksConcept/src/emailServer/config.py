import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# SMTP Configuration
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", 25))
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "sender@yourdomain.com")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL", "receiver@example.com")
SMTP_USER = os.getenv("SMTP_USER", None)  # Optional, for authentication
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", None)  # Optional, for authentication
