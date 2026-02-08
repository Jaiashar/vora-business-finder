#!/usr/bin/env python3
"""
Email Extractor - Extracts emails from business websites
Takes a CSV with website URLs and outputs business_name + email
"""

import csv
import re
import sys
import time
import urllib.request
import urllib.error
from urllib.parse import urlparse
import ssl

# Ignore SSL certificate errors (some small business sites have bad certs)
ssl._create_default_https_context = ssl._create_unverified_context

# Fake browser headers to avoid blocks
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

# Patterns for fake/placeholder emails to exclude
FAKE_PATTERNS = [
    r'first\.last@', r'example@', r'test@', r'your\.?email@',
    r'email@example', r'name@company', r'user@domain', r'info@example',
    r'sample@', r'demo@', r'placeholder@', r'noreply@', r'no-reply@',
    r'donotreply@', r'@sentry', r'@wixpress', r'@mailchimp',
]

# Common email patterns to find
EMAIL_REGEX = re.compile(
    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)


def is_valid_email(email):
    """Check if email looks real (not a placeholder or system email)"""
    if not email or '@' not in email:
        return False
    
    email_lower = email.lower().strip()
    
    # Check against fake patterns
    for pattern in FAKE_PATTERNS:
        if re.search(pattern, email_lower):
            return False
    
    # Skip image files that look like emails
    if email_lower.endswith(('.png', '.jpg', '.gif', '.svg', '.webp')):
        return False
    
    # Skip if domain is too short or looks fake
    domain = email_lower.split('@')[1]
    if len(domain) < 5:
        return False
    
    return True


def fetch_page(url, timeout=10):
    """Fetch a webpage and return its content"""
    try:
        # Ensure URL has scheme
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        request = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            # Read and decode content
            content = response.read()
            
            # Try to decode as UTF-8, fall back to latin-1
            try:
                return content.decode('utf-8')
            except UnicodeDecodeError:
                return content.decode('latin-1', errors='ignore')
                
    except urllib.error.HTTPError as e:
        if e.code == 403:
            return None  # Blocked
        return None
    except Exception:
        return None


def extract_emails_from_text(text):
    """Extract all valid emails from text"""
    if not text:
        return []
    
    # Find all email-like patterns
    candidates = EMAIL_REGEX.findall(text)
    
    # Filter to valid emails only
    valid = []
    seen = set()
    for email in candidates:
        email_lower = email.lower()
        if email_lower not in seen and is_valid_email(email):
            valid.append(email)
            seen.add(email_lower)
    
    return valid


def extract_emails_from_website(url):
    """Extract emails from a website (tries main page and contact page)"""
    emails = set()
    
    # Try main page
    content = fetch_page(url)
    if content:
        for email in extract_emails_from_text(content):
            emails.add(email.lower())
    
    # Try common contact page URLs
    parsed = urlparse(url if url.startswith('http') else 'https://' + url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    
    contact_paths = ['/contact', '/contact-us', '/about', '/about-us']
    for path in contact_paths:
        if len(emails) >= 3:  # Don't need too many
            break
        contact_url = base + path
        content = fetch_page(contact_url, timeout=5)
        if content:
            for email in extract_emails_from_text(content):
                emails.add(email.lower())
    
    return list(emails)


def process_csv(input_file, output_file):
    """Process input CSV and extract emails from websites"""
    
    results = []
    processed = 0
    found = 0
    
    print(f"Reading {input_file}...")
    
    with open(input_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    total = len(rows)
    print(f"Found {total} businesses to process\n")
    
    for i, row in enumerate(rows):
        business_name = row.get('title', row.get('business_name', ''))
        website = row.get('website', '')
        
        if not website or website.strip() == '':
            continue
        
        processed += 1
        print(f"[{processed}/{total}] {business_name[:40]}...", end=' ', flush=True)
        
        emails = extract_emails_from_website(website)
        
        if emails:
            found += 1
            print(f"✓ Found: {emails[0]}")
            for email in emails:
                results.append({
                    'business_name': business_name,
                    'email': email
                })
        else:
            print("✗ No email")
        
        # Small delay to be nice to servers
        time.sleep(0.5)
    
    # Write results
    print(f"\nWriting {len(results)} results to {output_file}...")
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['business_name', 'email'])
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\n{'='*50}")
    print(f"DONE!")
    print(f"Businesses processed: {processed}")
    print(f"Businesses with emails: {found}")
    print(f"Total emails found: {len(results)}")
    print(f"Success rate: {found/processed*100:.1f}%" if processed > 0 else "N/A")
    print(f"{'='*50}")
    
    return results


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python extract_emails.py <input.csv> [output.csv]")
        print("\nInput CSV must have 'title' and 'website' columns")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'emails_found.csv'
    
    process_csv(input_file, output_file)
