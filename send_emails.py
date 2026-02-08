#!/usr/bin/env python3
"""
Email Sender - Sends emails to leads via SendGrid
Reads a CSV with business_name + email columns and sends each one.
"""

import csv
import os
import sys
import time
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email

# Load environment variables from .env
load_dotenv()

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
FROM_EMAIL = os.getenv('FROM_EMAIL', 'jai@askvora.com')


def send_email(to_email, business_name, subject=None, html_content=None):
    """Send a single email via SendGrid"""

    if subject is None:
        subject = "Hello from Vora"

    if html_content is None:
        html_content = f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2>Hi {business_name},</h2>
            <p>This is a test email from Vora Business Finder.</p>
            <p>If you're receiving this, the email system is working correctly!</p>
            <br>
            <p>Best,<br>Jai</p>
        </div>
        """

    message = Mail(
        from_email=Email(FROM_EMAIL, 'Jai Ashar'),
        to_emails=to_email,
        subject=subject,
        html_content=html_content
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        return response.status_code, None
    except Exception as e:
        return None, str(e)


def send_from_csv(csv_path, subject=None, html_content=None, delay=1.0):
    """Read a CSV and send emails to all recipients"""

    if not SENDGRID_API_KEY:
        print("ERROR: SENDGRID_API_KEY not found in .env file")
        sys.exit(1)

    print(f"From: {FROM_EMAIL}")
    print(f"CSV:  {csv_path}")
    print(f"{'='*50}\n")

    # Read CSV
    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total = len(rows)
    sent = 0
    failed = 0

    print(f"Found {total} recipient(s)\n")

    for i, row in enumerate(rows):
        business_name = row.get('business_name', 'there')
        email = row.get('email', '').strip()

        if not email:
            print(f"  [{i+1}/{total}] SKIP - no email for {business_name}")
            continue

        print(f"  [{i+1}/{total}] Sending to {email}...", end=' ', flush=True)

        status_code, error = send_email(email, business_name, subject, html_content)

        if status_code and status_code in (200, 201, 202):
            sent += 1
            print(f"OK (status {status_code})")
        else:
            failed += 1
            print(f"FAILED - {error}")

        # Rate limit delay between sends
        if i < total - 1:
            time.sleep(delay)

    # Summary
    print(f"\n{'='*50}")
    print(f"DONE!")
    print(f"Total:  {total}")
    print(f"Sent:   {sent}")
    print(f"Failed: {failed}")
    print(f"{'='*50}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python send_emails.py <recipients.csv> [subject]")
        print("\nCSV must have 'business_name' and 'email' columns")
        print("Example: python send_emails.py results/test_emails.csv")
        sys.exit(1)

    csv_path = sys.argv[1]
    subject = sys.argv[2] if len(sys.argv) > 2 else None

    send_from_csv(csv_path, subject=subject)
