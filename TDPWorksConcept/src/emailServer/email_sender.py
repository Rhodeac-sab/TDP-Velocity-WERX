import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging

# Set up logging
logger = logging.getLogger(__name__)

async def send_email(smtp_host, smtp_port, sender_email, receiver_email):
    # Prepare the message
    subject = "Test Email from Docker SMTP"
    body = "This is a test email sent from your Docker SMTP server."

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        # Asynchronous send using SMTP
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            logger.info("Connecting to SMTP server")
            server.sendmail(sender_email, receiver_email, msg.as_string())
            logger.info(f"Email sent from {sender_email} to {receiver_email}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        raise
