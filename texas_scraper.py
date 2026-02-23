#!/usr/bin/env python3
"""
University of Texas at Austin — Graduate Student / Staff Email Scraper
======================================================================
Scrapes @utexas.edu emails from:
  - Liberal Arts graduate student directories
  - College of Natural Sciences departments
  - Cockrell School of Engineering departments
  - Professional schools (McCombs, Law, Education, LBJ, etc.)
  - Athletics (Longhorns)
  - Student organizations (Student Government, Daily Texan)
"""

import os
import re
import csv
import sys
import json
import time
import html
import signal
import urllib.request
import urllib.error
import ssl
from datetime import datetime
from urllib.parse import urlparse, urljoin

sys.stdout.reconfigure(line_buffering=True)

# ─── Setup ───────────────────────────────────────────────────────
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Regex for UT Austin emails
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@(?:[\w\-]+\.)*utexas\.edu', re.IGNORECASE)
MAILTO_RE = re.compile(r'mailto:\s*([a-zA-Z0-9._%+\-]+@(?:[\w\-]+\.)*utexas\.edu)', re.IGNORECASE)

# Generic / admin emails to skip
SKIP_PREFIXES = [
    'info@', 'admin@', 'office@', 'dept@', 'webmaster@', 'help@',
    'support@', 'contact@', 'registrar@', 'grad@', 'gradoffice@',
    'department@', 'chair@', 'advising@', 'undergrad@', 'dean@',
    'reception@', 'main@', 'general@', 'staff@', 'gradadmit@',
    'calendar@', 'events@', 'news@', 'newsletter@', 'web@',
    'marketing@', 'media@', 'communications@', 'hr@', 'hiring@',
    'jobs@', 'career@', 'alumni@', 'development@', 'giving@',
    'feedback@', 'safety@', 'security@', 'facilities@', 'it@',
    'tech@', 'helpdesk@', 'library@', 'gradapp@', 'apply@',
    'noreply@', 'no-reply@', 'donotreply@', 'postmaster@',
    'abuse@', 'root@', 'privacy@', 'records@', 'copyright@',
    'accessibility@', 'editor@', 'op-ed@', 'letters@',
    'studentaffairs@', 'financial-aid@', 'finaid@',
    'admissions@', 'enroll@', 'commencement@', 'provost@',
    'president@', 'chancellor@', 'its@', 'ehs@', 'police@',
    'parking@', 'housing@', 'dining@', 'rec@', 'ugs@',
]

SKIP_EMAILS = {
    'webmaster@utexas.edu', 'info@utexas.edu', 'help@utexas.edu',
    'registrar@utexas.edu', 'president@utexas.edu',
    'general@utexas.edu', 'provost@utexas.edu',
    'comments@utexas.edu', 'privacy@utexas.edu',
    'askus@lib.utexas.edu', 'ehs@utexas.edu',
}


def is_admin_email(email):
    """Filter out department/admin/generic emails."""
    e = email.lower()
    if e in SKIP_EMAILS:
        return True
    return any(e.startswith(p) for p in SKIP_PREFIXES)


def fetch_page(url, timeout=20):
    """Fetch a URL with strict timeout."""
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Fetch timed out for {url}")

    try:
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout + 5)
        req = urllib.request.Request(url, headers=HEADERS)
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        data = resp.read().decode('utf-8', errors='ignore')
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)
        return data
    except Exception as e:
        signal.alarm(0)
        try:
            signal.signal(signal.SIGALRM, old_handler)
        except:
            pass
        print(f"      [WARN] {url[:80]} — {str(e)[:60]}")
        return None


def decode_html_entities(text):
    """Decode HTML entities like &#97; &#64; etc."""
    if not text:
        return text
    decoded = html.unescape(text)
    decoded = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), decoded)
    decoded = re.sub(r'&#x([0-9a-fA-F]+);', lambda m: chr(int(m.group(1), 16)), decoded)
    return decoded


def extract_emails_from_html(raw_html):
    """Extract UT Austin emails from HTML, handling obfuscation."""
    if not raw_html:
        return []

    decoded = decode_html_entities(raw_html)
    emails = set()

    # Standard regex
    for m in EMAIL_RE.finditer(decoded):
        emails.add(m.group(0).lower().strip().rstrip('.'))

    # mailto: links
    for m in MAILTO_RE.finditer(decoded):
        emails.add(m.group(1).lower().strip().rstrip('.'))

    # Encoded mailto in raw HTML
    mailto_pattern = re.compile(r'mailto:((?:&#?\w+;|[a-zA-Z0-9._%+\-@])+)')
    for m in mailto_pattern.finditer(raw_html):
        raw_email = decode_html_entities(m.group(1))
        if re.match(r'^[a-zA-Z0-9._%+\-]+@(?:[\w\-]+\.)*utexas\.edu$', raw_email, re.IGNORECASE):
            emails.add(raw_email.lower().strip())

    # [at] obfuscation
    obfuscated = re.findall(
        r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*utexas\.edu)',
        decoded, re.IGNORECASE
    )
    for prefix, domain in obfuscated:
        emails.add(f"{prefix}@{domain}".lower())

    # JavaScript embedded emails
    js_blocks = re.findall(r'<script[^>]*>(.*?)</script>', raw_html, re.DOTALL | re.IGNORECASE)
    for js in js_blocks:
        for m in EMAIL_RE.finditer(js):
            emails.add(m.group(0).lower().strip().rstrip('.'))

    # data-* attributes
    data_attrs = re.findall(r'data-\w+="([^"]*@[^"]*utexas\.edu[^"]*)"', raw_html, re.IGNORECASE)
    for val in data_attrs:
        for m in EMAIL_RE.finditer(val):
            emails.add(m.group(0).lower().strip().rstrip('.'))

    return [e for e in emails if not is_admin_email(e)]


def extract_name_near_email(text, email, window=500):
    """Find a person's name near their email in text."""
    idx = text.lower().find(email.lower())
    if idx < 0:
        return derive_name_from_email(email)

    start = max(0, idx - window)
    end = min(len(text), idx + window)
    context = text[start:end]

    patterns = [
        r'<h[1-5][^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']+?)(?:\s*</a>)?\s*</h[1-5]>',
        r'<strong[^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']+?)(?:\s*</a>)?\s*</strong>',
        r'class="[^"]*name[^"]*"[^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']{3,50})',
        r'<a[^>]*>\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']{3,50})\s*</a>\s*(?:<[^>]+>)*\s*(?:Graduate|Doctoral|PhD|Student|Researcher|Fellow)',
        r'class="[^"]*title[^"]*"[^>]*>\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']{3,50})',
        r'<span[^>]*class="[^"]*name[^"]*"[^>]*>\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']{3,50})',
        r'<td[^>]*>\s*(?:<[^>]+>)*\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']{3,50})\s*(?:</[^>]+>)*\s*</td>',
        r'<div[^>]*class="[^"]*field-name[^"]*"[^>]*>\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']{3,50})',
        r'class="[^"]*views-field-title[^"]*"[^>]*>\s*(?:<[^>]+>)*\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']{3,50})',
    ]

    for pat in patterns:
        m = re.search(pat, context)
        if m:
            name = m.group(1).strip()
            name = re.sub(r'\s+', ' ', name)
            if 2 < len(name) < 60 and not re.search(r'[<>@{}\[\]]', name):
                return name

    return derive_name_from_email(email)


def derive_name_from_email(email):
    """Derive a likely name from email address."""
    local = email.split('@')[0]
    for sep in ['.', '_']:
        if sep in local:
            parts = local.split(sep)
            if all(len(p) > 1 for p in parts[:2]):
                cleaned = [re.sub(r'\d+', '', p) for p in parts[:2]]
                if all(len(p) > 1 for p in cleaned):
                    return ' '.join(p.capitalize() for p in cleaned)
    return ""


def scrape_url(url, department, follow_subpages=False):
    """Fetch URL and extract all UT Austin emails."""
    raw_html = fetch_page(url)
    if not raw_html:
        return []

    emails = extract_emails_from_html(raw_html)
    decoded = decode_html_entities(raw_html)

    results = []
    seen = set()
    for email in emails:
        if email not in seen:
            name = extract_name_near_email(decoded, email)
            results.append({
                "email": email,
                "name": name,
                "department": department,
                "source_url": url,
            })
            seen.add(email)

    # Follow pagination (?page=N)
    page_links = set()
    for m in re.finditer(r'href="([^"]*[?&]page=\d+[^"]*)"', raw_html):
        page_url = urljoin(url, m.group(1))
        if page_url != url:
            page_links.add(page_url)

    for m in re.finditer(r'href="([^"]*/page/\d+[^"]*)"', raw_html):
        page_url = urljoin(url, m.group(1))
        if page_url != url:
            page_links.add(page_url)

    for pg_url in sorted(page_links):
        time.sleep(0.4)
        pg_html = fetch_page(pg_url)
        if not pg_html:
            continue
        pg_emails = extract_emails_from_html(pg_html)
        pg_decoded = decode_html_entities(pg_html)
        for email in pg_emails:
            if email not in seen:
                name = extract_name_near_email(pg_decoded, email)
                results.append({
                    "email": email,
                    "name": name,
                    "department": department,
                    "source_url": pg_url,
                })
                seen.add(email)

    # Optionally follow profile sub-links
    if follow_subpages and len(results) < 5:
        profile_urls = set()
        for m in re.finditer(r'href="(/people/[a-z][\w\-]+)"', raw_html, re.IGNORECASE):
            full = urljoin(url, m.group(1))
            profile_urls.add(full)
        for m in re.finditer(r'href="(/person/[a-z][\w\-]+)"', raw_html, re.IGNORECASE):
            full = urljoin(url, m.group(1))
            profile_urls.add(full)
        for m in re.finditer(r'href="(/directory/[a-z][\w\-]+)"', raw_html, re.IGNORECASE):
            full = urljoin(url, m.group(1))
            profile_urls.add(full)
        # UT Austin often uses /users/ paths
        for m in re.finditer(r'href="(/users/[a-z][\w\-]+)"', raw_html, re.IGNORECASE):
            full = urljoin(url, m.group(1))
            profile_urls.add(full)

        for prof_url in list(profile_urls)[:50]:
            time.sleep(0.4)
            pr_html = fetch_page(prof_url)
            if not pr_html:
                continue
            pr_emails = extract_emails_from_html(pr_html)
            pr_decoded = decode_html_entities(pr_html)
            for email in pr_emails:
                if email not in seen:
                    name = extract_name_near_email(pr_decoded, email)
                    results.append({
                        "email": email,
                        "name": name,
                        "department": department,
                        "source_url": prof_url,
                    })
                    seen.add(email)

    return results


def scrape_with_fallbacks(url_list, department, follow_subpages=False):
    """Try multiple URLs for same department, accumulate results."""
    all_results = []
    seen = set()
    for url in url_list:
        results = scrape_url(url, department, follow_subpages=follow_subpages)
        new = 0
        for r in results:
            if r["email"] not in seen:
                all_results.append(r)
                seen.add(r["email"])
                new += 1
        if new > 0:
            print(f"    [{new:>3} new] {url[:80]}")
        time.sleep(0.5)
    return all_results


# ═══════════════════════════════════════════════════════════════════
# ALL TARGETED SOURCES
# ═══════════════════════════════════════════════════════════════════

LIBERAL_ARTS = [
    {
        "department": "Economics (Liberal Arts)",
        "urls": [
            "https://liberalarts.utexas.edu/economics/graduate-students/",
            "https://liberalarts.utexas.edu/economics/people/graduate-students",
            "https://liberalarts.utexas.edu/economics/graduate/current-students.html",
        ],
    },
    {
        "department": "Government / Political Science (Liberal Arts)",
        "urls": [
            "https://liberalarts.utexas.edu/government/graduate-students/",
            "https://liberalarts.utexas.edu/government/people/graduate-students",
            "https://liberalarts.utexas.edu/government/graduate/current-students.html",
        ],
    },
    {
        "department": "Sociology (Liberal Arts)",
        "urls": [
            "https://liberalarts.utexas.edu/sociology/graduate-students/",
            "https://liberalarts.utexas.edu/sociology/people/graduate-students",
            "https://liberalarts.utexas.edu/sociology/graduate/current-students.html",
        ],
    },
    {
        "department": "Psychology (Liberal Arts)",
        "urls": [
            "https://liberalarts.utexas.edu/psychology/graduate-students/",
            "https://liberalarts.utexas.edu/psychology/people/graduate-students",
            "https://liberalarts.utexas.edu/psychology/graduate/current-students.html",
        ],
    },
    {
        "department": "History (Liberal Arts)",
        "urls": [
            "https://liberalarts.utexas.edu/history/graduate-students/",
            "https://liberalarts.utexas.edu/history/people/graduate-students",
            "https://liberalarts.utexas.edu/history/graduate/current-students.html",
        ],
    },
    {
        "department": "English (Liberal Arts)",
        "urls": [
            "https://liberalarts.utexas.edu/english/graduate-students/",
            "https://liberalarts.utexas.edu/english/people/graduate-students",
            "https://liberalarts.utexas.edu/english/graduate/current-students.html",
        ],
    },
    {
        "department": "Philosophy (Liberal Arts)",
        "urls": [
            "https://liberalarts.utexas.edu/philosophy/graduate-students/",
            "https://liberalarts.utexas.edu/philosophy/people/graduate-students",
            "https://liberalarts.utexas.edu/philosophy/graduate/current-students.html",
        ],
    },
    {
        "department": "Linguistics (Liberal Arts)",
        "urls": [
            "https://liberalarts.utexas.edu/linguistics/graduate-students/",
            "https://liberalarts.utexas.edu/linguistics/people/graduate-students",
            "https://liberalarts.utexas.edu/linguistics/graduate/current-students.html",
        ],
    },
    {
        "department": "Anthropology (Liberal Arts)",
        "urls": [
            "https://liberalarts.utexas.edu/anthropology/graduate-students/",
            "https://liberalarts.utexas.edu/anthropology/people/graduate-students",
        ],
    },
    {
        "department": "Classics (Liberal Arts)",
        "urls": [
            "https://liberalarts.utexas.edu/classics/graduate-students/",
            "https://liberalarts.utexas.edu/classics/people/graduate-students",
        ],
    },
]

NATURAL_SCIENCES = [
    {
        "department": "Mathematics (CNS)",
        "urls": [
            "https://www.ma.utexas.edu/people/graduate-students",
            "https://www.ma.utexas.edu/directory/graduate-students",
            "https://www.ma.utexas.edu/users",
        ],
    },
    {
        "department": "Statistics (CNS)",
        "urls": [
            "https://stat.utexas.edu/people/graduate-students",
            "https://stat.utexas.edu/directory/graduate-students",
            "https://stat.utexas.edu/people",
        ],
    },
    {
        "department": "Physics (CNS)",
        "urls": [
            "https://ph.utexas.edu/people/graduate-students",
            "https://ph.utexas.edu/directory/graduate-students",
            "https://ph.utexas.edu/people",
        ],
    },
    {
        "department": "Chemistry (CNS)",
        "urls": [
            "https://cm.utexas.edu/people/graduate-students",
            "https://cm.utexas.edu/directory/graduate-students",
            "https://cm.utexas.edu/people",
        ],
    },
    {
        "department": "Geosciences (CNS)",
        "urls": [
            "https://geo.utexas.edu/people/graduate-students",
            "https://www.jsg.utexas.edu/people/graduate-students/",
            "https://geo.utexas.edu/people",
        ],
    },
    {
        "department": "Integrative Biology (CNS)",
        "urls": [
            "https://integrativebio.utexas.edu/people/graduate-students",
            "https://integrativebio.utexas.edu/people",
            "https://integrativebio.utexas.edu/directory/graduate-students",
        ],
    },
    {
        "department": "College of Natural Sciences",
        "urls": [
            "https://cns.utexas.edu/",
            "https://cns.utexas.edu/directory",
        ],
    },
]

ENGINEERING = [
    {
        "department": "Computer Science (Cockrell)",
        "urls": [
            "https://www.cs.utexas.edu/people/graduate-students",
            "https://www.cs.utexas.edu/people/phd-students",
            "https://www.cs.utexas.edu/people",
            "https://www.cs.utexas.edu/graduate-students",
        ],
    },
    {
        "department": "Electrical & Computer Engineering (Cockrell)",
        "urls": [
            "https://www.ece.utexas.edu/people/graduate-students",
            "https://www.ece.utexas.edu/people/phd-students",
            "https://www.ece.utexas.edu/people",
        ],
    },
    {
        "department": "Mechanical Engineering (Cockrell)",
        "urls": [
            "https://www.me.utexas.edu/people/graduate-students",
            "https://www.me.utexas.edu/people/phd-students",
            "https://www.me.utexas.edu/people",
        ],
    },
    {
        "department": "Civil Engineering (Cockrell)",
        "urls": [
            "https://www.ce.utexas.edu/people/graduate-students",
            "https://www.ce.utexas.edu/people/phd-students",
            "https://www.ce.utexas.edu/people",
        ],
    },
    {
        "department": "Biomedical Engineering (Cockrell)",
        "urls": [
            "https://www.bme.utexas.edu/people/graduate-students",
            "https://www.bme.utexas.edu/people/phd-students",
            "https://www.bme.utexas.edu/people",
        ],
    },
    {
        "department": "Aerospace Engineering (Cockrell)",
        "urls": [
            "https://www.ae.utexas.edu/people/graduate-students",
            "https://www.ae.utexas.edu/people/phd-students",
            "https://www.ae.utexas.edu/people",
        ],
    },
    {
        "department": "Chemical Engineering (Cockrell)",
        "urls": [
            "https://che.utexas.edu/people/graduate-students",
            "https://che.utexas.edu/people/phd-students",
            "https://che.utexas.edu/people",
        ],
    },
    {
        "department": "Petroleum Engineering (Cockrell)",
        "urls": [
            "https://petroleum.utexas.edu/people/graduate-students",
            "https://petroleum.utexas.edu/people/phd-students",
            "https://petroleum.utexas.edu/people",
        ],
    },
]

PROFESSIONAL_SCHOOLS = [
    {
        "department": "McCombs Business School (PhD)",
        "urls": [
            "https://www.mccombs.utexas.edu/phd/",
            "https://www.mccombs.utexas.edu/phd/students/",
            "https://www.mccombs.utexas.edu/phd/current-students/",
            "https://www.mccombs.utexas.edu/directory/",
        ],
    },
    {
        "department": "School of Law",
        "urls": [
            "https://law.utexas.edu/",
            "https://law.utexas.edu/directory/",
            "https://law.utexas.edu/student-organizations/",
            "https://law.utexas.edu/students/",
            "https://law.utexas.edu/faculty/",
        ],
    },
    {
        "department": "College of Education",
        "urls": [
            "https://education.utexas.edu/people/students/",
            "https://education.utexas.edu/people/",
            "https://education.utexas.edu/directory/",
            "https://education.utexas.edu/people/graduate-students/",
        ],
    },
    {
        "department": "LBJ School of Public Affairs",
        "urls": [
            "https://lbj.utexas.edu/people/students",
            "https://lbj.utexas.edu/people/",
            "https://lbj.utexas.edu/directory/",
            "https://lbj.utexas.edu/people/graduate-students",
        ],
    },
    {
        "department": "Moody College of Communication",
        "urls": [
            "https://moody.utexas.edu/people/graduate-students",
            "https://moody.utexas.edu/people/students",
            "https://moody.utexas.edu/people/",
            "https://moody.utexas.edu/directory/",
        ],
    },
    {
        "department": "School of Journalism",
        "urls": [
            "https://journalism.utexas.edu/people/students/",
            "https://journalism.utexas.edu/people/",
            "https://journalism.utexas.edu/people/graduate-students/",
            "https://journalism.utexas.edu/directory/",
        ],
    },
    {
        "department": "School of Social Work",
        "urls": [
            "https://socialwork.utexas.edu/people/students/",
            "https://socialwork.utexas.edu/people/",
            "https://socialwork.utexas.edu/directory/",
            "https://socialwork.utexas.edu/people/graduate-students/",
        ],
    },
    {
        "department": "College of Pharmacy",
        "urls": [
            "https://pharmacy.utexas.edu/people/students/",
            "https://pharmacy.utexas.edu/people/",
            "https://pharmacy.utexas.edu/directory/",
            "https://pharmacy.utexas.edu/people/graduate-students/",
        ],
    },
    {
        "department": "School of Architecture",
        "urls": [
            "https://soa.utexas.edu/people/students/",
            "https://soa.utexas.edu/people/",
            "https://soa.utexas.edu/directory/",
            "https://soa.utexas.edu/people/graduate-students/",
        ],
    },
    {
        "department": "School of Nursing",
        "urls": [
            "https://nursing.utexas.edu/people/students/",
            "https://nursing.utexas.edu/people/",
            "https://nursing.utexas.edu/directory/",
            "https://nursing.utexas.edu/people/graduate-students/",
        ],
    },
]

ATHLETICS = [
    {
        "department": "Longhorns Athletics",
        "urls": [
            "https://texassports.com/staff-directory",
            "https://texassports.com/staff-directory/",
            "https://texassports.com/sports/2024/1/1/staff-directory.aspx",
        ],
    },
    {
        "department": "Longhorns Athletics - Football",
        "urls": [
            "https://texassports.com/sports/football/coaches",
            "https://texassports.com/sports/football/roster",
        ],
    },
    {
        "department": "Longhorns Athletics - Basketball",
        "urls": [
            "https://texassports.com/sports/mens-basketball/coaches",
            "https://texassports.com/sports/womens-basketball/coaches",
        ],
    },
    {
        "department": "Longhorns Athletics - Baseball",
        "urls": [
            "https://texassports.com/sports/baseball/coaches",
        ],
    },
]

STUDENT_ORGS = [
    {
        "department": "Student Government",
        "urls": [
            "https://sg.utexas.edu/",
            "https://sg.utexas.edu/about/",
            "https://sg.utexas.edu/leadership/",
            "https://sg.utexas.edu/officers/",
            "https://sg.utexas.edu/executive/",
            "https://sg.utexas.edu/assembly/",
        ],
    },
    {
        "department": "The Daily Texan (Student Newspaper)",
        "urls": [
            "https://thedailytexan.com/",
            "https://thedailytexan.com/staff/",
            "https://thedailytexan.com/about/",
            "https://thedailytexan.com/contact/",
        ],
    },
    {
        "department": "Student Orgs (HornsLink)",
        "urls": [
            "https://utexas.campuslabs.com/engage/",
            "https://hornslink.utexas.edu/",
        ],
    },
    {
        "department": "Graduate Student Assembly",
        "urls": [
            "https://gsa.utexas.edu/",
            "https://gsa.utexas.edu/about/",
            "https://gsa.utexas.edu/officers/",
            "https://gsa.utexas.edu/leadership/",
        ],
    },
    {
        "department": "Texas Exes (Alumni Association)",
        "urls": [
            "https://www.texasexes.org/",
            "https://www.texasexes.org/about/",
        ],
    },
]

# Extra pages to search
EXTRA_PAGES = [
    ("https://www.cs.utexas.edu/research/", "CS Research Labs"),
    ("https://www.ece.utexas.edu/research", "ECE Research Labs"),
    ("https://www.me.utexas.edu/research", "ME Research Labs"),
    ("https://www.bme.utexas.edu/research", "BME Research Labs"),
    ("https://www.ae.utexas.edu/research", "Aerospace Research Labs"),
    ("https://che.utexas.edu/research", "Chemical Eng Research Labs"),
    ("https://www.cs.utexas.edu/research/artificial-intelligence", "CS AI Research"),
    ("https://www.cs.utexas.edu/research/machine-learning", "CS ML Research"),
    ("https://www.cs.utexas.edu/research/natural-language-processing", "CS NLP Research"),
    ("https://www.cs.utexas.edu/research/robotics", "CS Robotics Research"),
    ("https://www.cs.utexas.edu/research/computer-vision", "CS Computer Vision Research"),
    ("https://cockrell.utexas.edu/", "Cockrell School of Engineering"),
    ("https://cockrell.utexas.edu/research", "Cockrell Research"),
    ("https://liberalarts.utexas.edu/", "College of Liberal Arts"),
    ("https://www.utexas.edu/directory", "UT Austin Directory"),
]


def main():
    print("=" * 70)
    print("  UT AUSTIN — GRADUATE STUDENT / STAFF EMAIL SCRAPER")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    all_contacts = []
    all_seen = set()

    def add_results(results):
        new = 0
        for r in results:
            if r["email"] not in all_seen:
                all_contacts.append(r)
                all_seen.add(r["email"])
                new += 1
        return new

    # ── Phase 1: Liberal Arts ──
    print(f"\n  Phase 1: Scraping {len(LIBERAL_ARTS)} Liberal Arts departments...")
    print("  " + "-" * 60)

    for config in LIBERAL_ARTS:
        dept = config["department"]
        print(f"\n  >> {dept}")
        results = scrape_with_fallbacks(config["urls"], dept, follow_subpages=True)
        new = add_results(results)
        print(f"    -- Total: {len(results)} found, {new} new unique")
        for r in results[:3]:
            nm = r['name'][:30] if r['name'] else '-'
            print(f"       {nm:<32} {r['email']}")

    # ── Phase 2: Natural Sciences ──
    print(f"\n\n  Phase 2: Scraping {len(NATURAL_SCIENCES)} Natural Sciences departments...")
    print("  " + "-" * 60)

    for config in NATURAL_SCIENCES:
        dept = config["department"]
        print(f"\n  >> {dept}")
        results = scrape_with_fallbacks(config["urls"], dept, follow_subpages=True)
        new = add_results(results)
        print(f"    -- Total: {len(results)} found, {new} new unique")
        for r in results[:3]:
            nm = r['name'][:30] if r['name'] else '-'
            print(f"       {nm:<32} {r['email']}")

    # ── Phase 3: Engineering ──
    print(f"\n\n  Phase 3: Scraping {len(ENGINEERING)} Engineering departments...")
    print("  " + "-" * 60)

    for config in ENGINEERING:
        dept = config["department"]
        print(f"\n  >> {dept}")
        results = scrape_with_fallbacks(config["urls"], dept, follow_subpages=True)
        new = add_results(results)
        print(f"    -- Total: {len(results)} found, {new} new unique")
        for r in results[:3]:
            nm = r['name'][:30] if r['name'] else '-'
            print(f"       {nm:<32} {r['email']}")

    # ── Phase 4: Professional Schools ──
    print(f"\n\n  Phase 4: Scraping {len(PROFESSIONAL_SCHOOLS)} Professional Schools...")
    print("  " + "-" * 60)

    for config in PROFESSIONAL_SCHOOLS:
        dept = config["department"]
        print(f"\n  >> {dept}")
        results = scrape_with_fallbacks(config["urls"], dept, follow_subpages=True)
        new = add_results(results)
        print(f"    -- Total: {len(results)} found, {new} new unique")
        for r in results[:3]:
            nm = r['name'][:30] if r['name'] else '-'
            print(f"       {nm:<32} {r['email']}")

    # ── Phase 5: Athletics ──
    print(f"\n\n  Phase 5: Scraping {len(ATHLETICS)} Athletics sources...")
    print("  " + "-" * 60)

    for config in ATHLETICS:
        dept = config["department"]
        print(f"\n  >> {dept}")
        results = scrape_with_fallbacks(config["urls"], dept, follow_subpages=False)
        new = add_results(results)
        print(f"    -- Total: {len(results)} found, {new} new unique")
        for r in results[:3]:
            nm = r['name'][:30] if r['name'] else '-'
            print(f"       {nm:<32} {r['email']}")

    # ── Phase 6: Student Organizations ──
    print(f"\n\n  Phase 6: Scraping {len(STUDENT_ORGS)} Student Org sources...")
    print("  " + "-" * 60)

    for config in STUDENT_ORGS:
        dept = config["department"]
        print(f"\n  >> {dept}")
        results = scrape_with_fallbacks(config["urls"], dept, follow_subpages=True)
        new = add_results(results)
        print(f"    -- Total: {len(results)} found, {new} new unique")
        for r in results[:3]:
            nm = r['name'][:30] if r['name'] else '-'
            print(f"       {nm:<32} {r['email']}")

    # ── Phase 7: Extra research/directory pages ──
    print(f"\n\n  Phase 7: Scraping {len(EXTRA_PAGES)} extra pages...")
    print("  " + "-" * 60)

    for url, dept in EXTRA_PAGES:
        results = scrape_url(url, dept, follow_subpages=True)
        new = add_results(results)
        if new > 0:
            print(f"    [{new:>3} new] {dept}")
        time.sleep(0.4)

    # ── Results Summary ──
    print("\n\n" + "=" * 70)
    print("  RESULTS SUMMARY")
    print("=" * 70)

    print(f"\n  Total unique emails: {len(all_contacts)}")

    utexas_emails = [c for c in all_contacts if '@utexas.edu' in c['email']]
    sub_emails = [c for c in all_contacts
                  if 'utexas.edu' in c['email'] and '@utexas.edu' not in c['email']]
    other_emails = [c for c in all_contacts if 'utexas.edu' not in c['email']]
    print(f"  @utexas.edu: {len(utexas_emails)}")
    if sub_emails:
        print(f"  @*.utexas.edu subdomains: {len(sub_emails)}")
    if other_emails:
        print(f"  Other domains: {len(other_emails)}")

    # By department
    depts = {}
    for c in all_contacts:
        d = c["department"]
        depts[d] = depts.get(d, 0) + 1

    print(f"\n  By department/source:")
    for d, count in sorted(depts.items(), key=lambda x: -x[1]):
        print(f"    {d:<55} {count}")

    # ── Save CSV ──
    base = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base, "texas_dept_emails.csv")
    fieldnames = ["email", "name", "department", "source_url"]

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_contacts)

    json_path = os.path.join(base, "texas_dept_emails.json")
    with open(json_path, 'w') as f:
        json.dump(all_contacts, f, indent=2)

    print(f"\n  CSV saved:  {csv_path}")
    print(f"  JSON saved: {json_path}")

    # ── Print ALL emails grouped by department ──
    print(f"\n  {'=' * 70}")
    print(f"  ALL {len(all_contacts)} EMAILS")
    print(f"  {'=' * 70}")

    by_dept = {}
    for c in all_contacts:
        d = c["department"]
        if d not in by_dept:
            by_dept[d] = []
        by_dept[d].append(c)

    for dept in sorted(by_dept.keys()):
        contacts = by_dept[dept]
        print(f"\n  [{dept}] ({len(contacts)} contacts)")
        for c in contacts:
            nm = c['name'][:35] if c['name'] else '-'
            print(f"    {nm:<37} {c['email']}")

    print(f"\n{'=' * 70}")
    print(f"  COMPLETE. {len(all_contacts)} unique emails scraped.")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
