import logging
import asyncio
from email_sender import send_email
from config import SMTP_HOST, SMTP_PORT, SENDER_EMAIL, RECEIVER_EMAIL

# Set up logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/app.log"),
        logging.StreamHandler()
    ]
)

# Asynchronous task to send email
async def main():
    try:
        logging.info("Starting email sending process")
        await send_email(SMTP_HOST, SMTP_PORT, SENDER_EMAIL, RECEIVER_EMAIL)
        logging.info("Email sent successfully")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
