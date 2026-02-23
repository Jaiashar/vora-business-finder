#!/usr/bin/env python3
"""
UCLA Playwright Scraper — JS-rendered department pages
=====================================================
Uses headless Chromium to scrape department pages that 
return empty HTML to raw HTTP requests.

Pushes new emails directly to Supabase with dedup.
"""

import os
import re
import csv
import json
import time
import urllib.request
import urllib.error
import ssl
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

SKIP_EMAILS = {
    'webmaster@ucla.edu', 'communications@ucla.edu', 'info@ucla.edu',
    'registrar@ucla.edu', 'accessibility@ucla.edu', 'noreply@ucla.edu',
    'its@ucla.edu', 'postmaster@ucla.edu', 'support@google.com',
}

SKIP_DOMAINS = {'sentry.io', 'example.com', 'w3.org', 'schema.org', 'google.com',
                'facebook.com', 'twitter.com', 'instagram.com', 'youtube.com',
                'linkedin.com', 'tiktok.com', 'apple.com', 'microsoft.com',
                'amazon.com', 'cloudflare.com', 'jquery.com', 'wordpress.org',
                'gravatar.com', 'wp.com', 'gstatic.com', 'googleapis.com'}

# ── Target URLs: JS-rendered pages that failed with raw HTTP ──
TARGETS = [
    # Social Sciences
    ("https://polisci.ucla.edu/people/graduate-students/", "Political Science"),
    ("https://www.econ.ucla.edu/gradstudents/", "Economics"),
    ("https://www.psych.ucla.edu/graduate/current-students/", "Psychology"),
    ("https://www.philosophy.ucla.edu/people/graduate-students/", "Philosophy"),
    ("https://www.english.ucla.edu/people/graduate-students/", "English"),
    ("https://geog.ucla.edu/people/graduate-students/", "Geography"),
    # STEM
    ("https://www.math.ucla.edu/people/grad", "Mathematics"),
    ("https://www.physics.ucla.edu/people/graduate-students/", "Physics"),
    ("https://www.stat.ucla.edu/people/graduate-students/", "Statistics"),
    ("https://www.cs.ucla.edu/people/graduate-students/", "Computer Science"),
    ("https://www.bioeng.ucla.edu/people/graduate-students/", "Bioengineering"),
    ("https://www.ee.ucla.edu/people/graduate-students/", "Electrical Engineering"),
    ("https://biolchem.ucla.edu/people/graduate-students", "Biological Chemistry"),
    ("https://www.mcdb.ucla.edu/people/graduate-students", "MCDB"),
    ("https://www.mse.ucla.edu/people/graduate-students/", "Materials Science"),
    ("https://www.mae.ucla.edu/people/graduate-students/", "Mechanical & Aerospace"),
    # Arts & Humanities
    ("https://www.art.ucla.edu/graduate-students/", "Art"),
    ("https://www.tft.ucla.edu/students/student-profiles/", "Theater Film TV"),
    ("https://www.music.ucla.edu/people/graduate-students", "Music"),
    ("https://schoolofmusic.ucla.edu/graduate-students/", "School of Music"),
    # Professional Schools
    ("https://luskin.ucla.edu/phd-students", "Luskin Public Affairs"),
    ("https://ph.ucla.edu/students/current-students", "Public Health"),
    ("https://gseis.ucla.edu/directory/students/", "Info Studies"),
    ("https://www.anderson.ucla.edu/programs-and-admissions/phd/current-students", "Anderson PhD"),
    ("https://socialwelfare.ucla.edu/people/students/", "Social Welfare"),
    ("https://www.nursing.ucla.edu/students/", "Nursing"),
    # Additional dept/lab pages
    ("https://www.ioes.ucla.edu/people/", "Environment & Sustainability"),
    ("https://compmed.ucla.edu/people", "Computational Medicine"),
    ("https://ccn.ucla.edu/people/", "Computation & Cognition"),
    ("https://www.neuroscience.ucla.edu/people/", "Neuroscience IDP"),
]


def extract_emails_from_text(text):
    """Extract and filter emails."""
    found = set()
    for m in EMAIL_RE.findall(text):
        e = m.lower().strip()
        domain = e.split('@')[-1]
        if domain in SKIP_DOMAINS:
            continue
        if e in SKIP_EMAILS:
            continue
        if any(kw in e for kw in ['noreply', 'no-reply', 'webmaster', 'postmaster']):
            continue
        if len(e) < 6 or len(e) > 80:
            continue
        # Only keep UCLA-related emails
        if 'ucla.edu' in domain or '@gmail.com' in e:
            found.add(e)
    return list(found)


def supabase_upsert(rows):
    """Push rows to Supabase, skipping duplicates."""
    if not rows:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/college_contacts"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates",
    }
    data = json.dumps(rows).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method='POST')
    try:
        resp = urllib.request.urlopen(req, context=SSL_CTX)
        return len(rows) if resp.getcode() in (200, 201) else 0
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        if '409' in str(e.code) or 'duplicate' in body.lower():
            return 0  # All dupes, that's fine
        print(f"    ✗ Supabase error: {e.code} — {body[:150]}")
        return 0


def get_existing_emails():
    """Get all emails already in Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/college_contacts?select=email&university=eq.UCLA"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        resp = urllib.request.urlopen(req, context=SSL_CTX)
        data = json.loads(resp.read().decode())
        return {r['email'].lower() for r in data}
    except Exception:
        return set()


def main():
    print("╔════════════════════════════════════════════════════════════╗")
    print("║  UCLA PLAYWRIGHT SCRAPER — JS-RENDERED DEPT PAGES        ║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<55}║")
    print("╚════════════════════════════════════════════════════════════╝")

    # Get existing emails for dedup
    existing = get_existing_emails()
    print(f"\n  Existing emails in Supabase: {len(existing)}")

    all_new = []
    total_found = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        )

        for url, dept in TARGETS:
            try:
                page = context.new_page()
                page.goto(url, timeout=20000, wait_until="networkidle")
                time.sleep(1)

                # Scroll to load lazy content
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1)

                text = page.content()
                page.close()

                emails = extract_emails_from_text(text)
                new_emails = [e for e in emails if e not in existing]

                rows = []
                for e in new_emails:
                    is_student = '@g.ucla.edu' in e
                    rows.append({
                        "email": e,
                        "name": None,
                        "title": None,
                        "department": dept,
                        "role": "student" if is_student else "staff",
                        "university": "UCLA",
                        "source_url": url,
                        "segment": "grad_student" if is_student else "staff",
                    })
                    existing.add(e)  # Track for dedup across pages

                if rows:
                    pushed = supabase_upsert(rows)
                    all_new.extend(rows)

                total_found += len(emails)
                print(f"  {'✓' if new_emails else '·'} {dept:<30} → {len(emails)} emails, {len(new_emails)} NEW")

            except Exception as e:
                print(f"  ✗ {dept:<30} → Error: {str(e)[:60]}")

        browser.close()

    # Summary
    students = [r for r in all_new if r['role'] == 'student']
    print(f"\n{'='*60}")
    print(f"  PLAYWRIGHT RESULTS")
    print(f"{'='*60}")
    print(f"  Total emails found: {total_found}")
    print(f"  NEW emails (not in Supabase): {len(all_new)}")
    print(f"  New students: {len(students)}")
    print(f"  New staff/faculty: {len(all_new) - len(students)}")

    if students:
        print(f"\n  New student emails:")
        for s in students[:20]:
            print(f"    {s['email']:<40} {s['department']}")
        if len(students) > 20:
            print(f"    ... and {len(students)-20} more")


if __name__ == "__main__":
    main()
