#!/usr/bin/env python3
"""
UCLA Student & Staff Email Scraper
====================================
Scrapes publicly available emails from UCLA websites:
  1. Recreation department staff directory (70+ contacts)
  2. Athletics department staff
  3. Student org pages (fitness, health, pre-med, kinesiology clubs)
  4. Department directories (kinesiology, public health, etc.)
  5. Student Wellness Commission

NO emailing — just data collection to CSV.
"""

import os
import re
import csv
import time
import json
import random
import urllib.request
import urllib.error
import ssl
from datetime import datetime
from urllib.parse import urlparse, urljoin

# ─── Setup ───────────────────────────────────────────────────────
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.ucla\.edu', re.IGNORECASE)

# Also catch general emails on UCLA pages
GENERAL_EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

SKIP_EMAILS = {
    'webmaster@ucla.edu', 'communications@ucla.edu', 'info@ucla.edu',
    'registrar@ucla.edu', 'accessibility@ucla.edu',
}

def fetch_page(url, timeout=12):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        return resp.read().decode('utf-8', errors='ignore')
    except Exception as e:
        return None


def extract_ucla_emails(text):
    """Extract @ucla.edu emails from text."""
    if not text:
        return []
    found = EMAIL_RE.findall(text)
    cleaned = []
    seen = set()
    for e in found:
        e = e.lower().strip()
        if e in seen or e in SKIP_EMAILS:
            continue
        if any(kw in e for kw in ['noreply', 'no-reply', 'donotreply', 'webmaster']):
            continue
        seen.add(e)
        cleaned.append(e)
    return cleaned


def extract_name_email_pairs(html):
    """Try to extract (name, email, title) tuples from structured HTML."""
    pairs = []

    # Pattern 1: <h3>Name</h3> followed by title and email
    # Common on UCLA staff directory pages
    blocks = re.split(r'<h[23][^>]*>', html)
    for block in blocks[1:]:  # Skip first (before any h2/h3)
        # Get the name (text before closing tag)
        name_match = re.match(r'([^<]+)</h[23]>', block)
        if not name_match:
            continue
        name = name_match.group(1).strip()
        if not name or len(name) > 60 or len(name) < 3:
            continue

        # Get email from this block
        emails = EMAIL_RE.findall(block[:500])
        if not emails:
            continue

        # Try to get title (usually the line right after the name)
        title = ""
        title_match = re.search(r'</h[23]>\s*\n?\s*([^<\n]{5,80})', block)
        if title_match:
            title = title_match.group(1).strip()
            # Clean up common artifacts
            title = re.sub(r'\d{3}[\.\-]\d{3}[\.\-]\d{4}', '', title).strip()
            title = title.rstrip(',').strip()

        for email in emails[:1]:  # Take first email per person
            pairs.append({
                "name": name,
                "email": email.lower(),
                "title": title,
            })

    return pairs


# ═══════════════════════════════════════════════════════════════════
# SOURCE 1: UCLA Recreation Staff Directory
# ═══════════════════════════════════════════════════════════════════
def scrape_recreation_staff():
    """Scrape the UCLA Recreation contact/staff directory page."""
    print("\n  SOURCE 1: UCLA Recreation Staff Directory")
    print("  " + "-"*50)

    url = "https://recreation.ucla.edu/about/contact"
    html = fetch_page(url)
    if not html:
        print("    ✗ Could not fetch page")
        return []

    pairs = extract_name_email_pairs(html)

    # Also get department-level emails
    dept_emails = extract_ucla_emails(html)

    results = []
    seen_emails = set()

    for p in pairs:
        if p["email"] not in seen_emails:
            results.append({
                "name": p["name"],
                "email": p["email"],
                "title": p["title"],
                "department": "UCLA Recreation",
                "source_url": url,
                "role": classify_role(p["title"], p["name"]),
            })
            seen_emails.add(p["email"])

    # Add department emails not captured in pairs
    for e in dept_emails:
        if e not in seen_emails:
            results.append({
                "name": "",
                "email": e,
                "title": "",
                "department": "UCLA Recreation",
                "source_url": url,
                "role": "department",
            })
            seen_emails.add(e)

    print(f"    ✓ Found {len(results)} contacts")
    for r in results[:5]:
        print(f"      {r['name']:<30} {r['email']:<40} {r['title'][:30]}")
    if len(results) > 5:
        print(f"      ... and {len(results)-5} more")

    return results


# ═══════════════════════════════════════════════════════════════════
# SOURCE 2: UCLA Athletics Staff
# ═══════════════════════════════════════════════════════════════════
def scrape_athletics():
    """Scrape UCLA Athletics staff directory."""
    print("\n  SOURCE 2: UCLA Athletics Staff")
    print("  " + "-"*50)

    URLS = [
        "https://uclabruins.com/staff-directory",
        "https://uclabruins.com/sports/strength-and-conditioning",
        "https://uclabruins.com/sports/sports-medicine",
    ]

    results = []
    seen = set()

    for url in URLS:
        html = fetch_page(url)
        if not html:
            print(f"    ✗ {url[:60]}")
            continue

        emails = extract_ucla_emails(html)
        pairs = extract_name_email_pairs(html)

        for p in pairs:
            if p["email"] not in seen:
                results.append({
                    "name": p["name"],
                    "email": p["email"],
                    "title": p["title"],
                    "department": "UCLA Athletics",
                    "source_url": url,
                    "role": classify_role(p["title"], p["name"]),
                })
                seen.add(p["email"])

        for e in emails:
            if e not in seen:
                results.append({
                    "name": "", "email": e, "title": "",
                    "department": "UCLA Athletics", "source_url": url,
                    "role": "staff",
                })
                seen.add(e)

        print(f"    ✓ {url.split('/')[-1]}: {len(emails)} emails")
        time.sleep(1)

    print(f"    Total: {len(results)} contacts")
    return results


# ═══════════════════════════════════════════════════════════════════
# SOURCE 3: UCLA Student Orgs (fitness, health, pre-PT, kinesiology)
# ═══════════════════════════════════════════════════════════════════
def scrape_student_orgs():
    """Scrape UCLA student organization pages for contact emails."""
    print("\n  SOURCE 3: UCLA Student Organizations")
    print("  " + "-"*50)

    # Direct URLs for known student org pages
    URLS = [
        ("https://swcucla.org/", "Student Wellness Commission"),
        ("https://www.studentgroups.ucla.edu/", "Student Groups Hub"),
        ("https://community.ucla.edu/studentorgs/pre-professional", "Pre-Professional Orgs"),
        ("https://community.ucla.edu/studentorgs/medical", "Medical Orgs"),
        ("https://community.ucla.edu/about-us", "SOLE Staff"),
        ("https://bewellbruin.ucla.edu/", "Be Well Bruin"),
        ("https://bewellbruin.ucla.edu/resource/student-wellness-commission", "SWC Details"),
    ]

    results = []
    seen = set()

    for url, label in URLS:
        html = fetch_page(url)
        if not html:
            print(f"    ✗ {label}: no response")
            continue

        # Get all UCLA emails
        emails = extract_ucla_emails(html)
        # Also get non-UCLA emails (student orgs sometimes use gmail)
        all_emails = GENERAL_EMAIL_RE.findall(html.lower())
        for e in all_emails:
            if 'ucla.edu' in e or '@gmail.com' in e or '@g.ucla.edu' in e:
                if e not in seen and e not in SKIP_EMAILS:
                    if not any(kw in e for kw in ['noreply', 'webmaster', 'info@']):
                        emails.append(e)

        for e in emails:
            e = e.lower()
            if e not in seen:
                results.append({
                    "name": "",
                    "email": e,
                    "title": "",
                    "department": label,
                    "source_url": url,
                    "role": "student_org" if 'gmail' in e or '@g.ucla.edu' in e else "staff",
                })
                seen.add(e)

        print(f"    ✓ {label}: {len(emails)} emails")
        time.sleep(1)

    print(f"    Total: {len(results)} contacts")
    return results


# ═══════════════════════════════════════════════════════════════════
# SOURCE 4: UCLA Department Pages (Kinesiology, Public Health, etc.)
# ═══════════════════════════════════════════════════════════════════
def scrape_departments():
    """Scrape UCLA academic department pages."""
    print("\n  SOURCE 4: UCLA Academic Departments")
    print("  " + "-"*50)

    URLS = [
        ("https://www.physci.ucla.edu/people", "Physiological Science"),
        ("https://ph.ucla.edu/about/faculty-staff-directory", "Public Health"),
        ("https://www.psych.ucla.edu/people/", "Psychology"),
        ("https://www.bioeng.ucla.edu/people/", "Bioengineering"),
        ("https://www.nursing.ucla.edu/about/faculty-directory", "Nursing"),
        ("https://www.physed.ucla.edu/people", "Physical Education"),
        ("https://lifesciences.ucla.edu/people/", "Life Sciences"),
        ("https://www.ibp.ucla.edu/faculty/", "Integrative Biology & Physiology"),
        ("https://medschool.ucla.edu/departments/basic-science/physiology/people/faculty", "Med School Physiology"),
    ]

    results = []
    seen = set()

    for url, dept in URLS:
        html = fetch_page(url)
        if not html:
            print(f"    ✗ {dept}: no response")
            continue

        emails = extract_ucla_emails(html)
        pairs = extract_name_email_pairs(html)

        for p in pairs:
            if p["email"] not in seen:
                results.append({
                    "name": p["name"],
                    "email": p["email"],
                    "title": p["title"],
                    "department": dept,
                    "source_url": url,
                    "role": "faculty",
                })
                seen.add(p["email"])

        for e in emails:
            if e not in seen:
                results.append({
                    "name": "", "email": e, "title": "",
                    "department": dept, "source_url": url,
                    "role": "faculty",
                })
                seen.add(e)

        print(f"    ✓ {dept}: {len(emails)} emails")
        time.sleep(1)

    print(f"    Total: {len(results)} contacts")
    return results


# ═══════════════════════════════════════════════════════════════════
# SOURCE 5: Google search for UCLA student emails in the wild
# ═══════════════════════════════════════════════════════════════════
def scrape_google_ucla_students():
    """Search for UCLA student emails via various public pages."""
    print("\n  SOURCE 5: UCLA Student Emails from Public Pages")
    print("  " + "-"*50)

    # Pages that often list student emails publicly
    URLS = [
        ("https://usac.ucla.edu/", "USAC Student Government"),
        ("https://www.bruinlife.com/", "Bruin Life"),
        ("https://dailybruin.com/staff", "Daily Bruin Staff"),
        ("https://dailybruin.com/about", "Daily Bruin About"),
        ("https://bruinwalk.com/", "BruinWalk"),
    ]

    results = []
    seen = set()

    for url, label in URLS:
        html = fetch_page(url)
        if not html:
            print(f"    ✗ {label}: no response")
            continue

        emails = extract_ucla_emails(html)
        # Also catch @g.ucla.edu (student gmail-style addresses)
        student_emails = re.findall(r'[a-zA-Z0-9._%+\-]+@g\.ucla\.edu', html, re.IGNORECASE)
        emails.extend([e.lower() for e in student_emails])

        for e in emails:
            e = e.lower()
            if e not in seen and e not in SKIP_EMAILS:
                is_student = '@g.ucla.edu' in e
                results.append({
                    "name": "",
                    "email": e,
                    "title": "",
                    "department": label,
                    "source_url": url,
                    "role": "student" if is_student else "staff",
                })
                seen.add(e)

        print(f"    ✓ {label}: {len(emails)} emails")
        time.sleep(1)

    print(f"    Total: {len(results)} contacts")
    return results


# ═══════════════════════════════════════════════════════════════════
# HELPER: Classify role
# ═══════════════════════════════════════════════════════════════════
def classify_role(title, name):
    title_lower = (title or "").lower()
    name_lower = (name or "").lower()

    if any(k in title_lower for k in ['director', 'executive', 'associate director']):
        return "director"
    if any(k in title_lower for k in ['coach', 'trainer', 'instructor']):
        return "coach"
    if any(k in title_lower for k in ['professor', 'faculty', 'phd', 'dr.']):
        return "faculty"
    if any(k in title_lower for k in ['coordinator', 'manager', 'supervisor']):
        return "coordinator"
    if any(k in title_lower for k in ['student', 'intern', 'assistant']):
        return "student"
    if any(k in title_lower for k in ['analyst', 'specialist']):
        return "staff"
    return "staff"


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
def main():
    print("╔════════════════════════════════════════════════════════════╗")
    print("║       UCLA EMAIL SCRAPER — STUDENTS & STAFF              ║")
    print(f"║       {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<53}║")
    print("╚════════════════════════════════════════════════════════════╝")

    all_contacts = []

    # Run all sources
    all_contacts.extend(scrape_recreation_staff())
    all_contacts.extend(scrape_athletics())
    all_contacts.extend(scrape_student_orgs())
    all_contacts.extend(scrape_departments())
    all_contacts.extend(scrape_google_ucla_students())

    # Deduplicate by email
    seen = set()
    unique = []
    for c in all_contacts:
        if c["email"] not in seen:
            unique.append(c)
            seen.add(c["email"])

    # ── Results ──
    print("\n" + "="*60)
    print("  RESULTS")
    print("="*60)

    # Breakdown by role
    roles = {}
    for c in unique:
        r = c["role"]
        roles[r] = roles.get(r, 0) + 1

    # Breakdown by department
    depts = {}
    for c in unique:
        d = c["department"]
        depts[d] = depts.get(d, 0) + 1

    print(f"\n  Total unique emails: {len(unique)}")
    print(f"\n  By role:")
    for r, count in sorted(roles.items(), key=lambda x: -x[1]):
        print(f"    {r:<20} {count}")
    print(f"\n  By source:")
    for d, count in sorted(depts.items(), key=lambda x: -x[1]):
        print(f"    {d:<35} {count}")

    # Count student vs staff
    student_count = sum(1 for c in unique if c["role"] == "student" or '@g.ucla.edu' in c["email"])
    staff_count = len(unique) - student_count
    print(f"\n  Students: {student_count}")
    print(f"  Staff/Faculty: {staff_count}")

    # ── Save CSV ──
    base = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base, "ucla_contacts.csv")
    fieldnames = ["name", "email", "title", "department", "role", "source_url"]

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unique)

    json_path = os.path.join(base, "ucla_contacts.json")
    with open(json_path, 'w') as f:
        json.dump(unique, f, indent=2)

    print(f"\n  CSV: {csv_path}")
    print(f"  JSON: {json_path}")

    # Show sample of student emails if any
    students = [c for c in unique if c["role"] == "student" or '@g.ucla.edu' in c["email"]]
    if students:
        print(f"\n  Sample student emails:")
        for s in students[:10]:
            print(f"    {s['email']:<40} {s['department']}")


if __name__ == "__main__":
    main()
