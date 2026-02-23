#!/usr/bin/env python3
"""
UC Berkeley Professional Schools, Research Labs, Student Orgs & Athletics
Email Scraper
=========================================================================
Scrapes @berkeley.edu emails from:
  - Professional schools (Haas, Law, GSPP, Journalism, Education, etc.)
  - Research labs (BAIR, RISE, Robot Learning, Autolab, etc.)
  - Student organizations (ASUC, Daily Cal, Greek life, etc.)
  - Athletics (Cal Bears staff/coaching)
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

# Regex for Berkeley emails
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@(?:[\w\-]+\.)*berkeley\.edu', re.IGNORECASE)
MAILTO_RE = re.compile(r'mailto:\s*([a-zA-Z0-9._%+\-]+@(?:[\w\-]+\.)*berkeley\.edu)', re.IGNORECASE)

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
]

SKIP_EMAILS = {
    'webmaster@berkeley.edu', 'info@berkeley.edu', 'help@berkeley.edu',
    'registrar@berkeley.edu', 'chancellor@berkeley.edu',
    'vcresearch@berkeley.edu', 'general@berkeley.edu',
    'asucemail@berkeley.edu', 'editor@dailycal.org',
}


def is_admin_email(email):
    """Filter out department/admin/generic emails."""
    e = email.lower()
    if e in SKIP_EMAILS:
        return True
    return any(e.startswith(p) for p in SKIP_PREFIXES)


def fetch_page(url, timeout=15):
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
        print(f"      [WARN] {url[:70]} — {str(e)[:60]}")
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
    """Extract Berkeley emails from HTML, handling obfuscation."""
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
        if re.match(r'^[a-zA-Z0-9._%+\-]+@(?:[\w\-]+\.)*berkeley\.edu$', raw_email, re.IGNORECASE):
            emails.add(raw_email.lower().strip())

    # [at] obfuscation
    obfuscated = re.findall(
        r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*berkeley\.edu)',
        decoded, re.IGNORECASE
    )
    for prefix, domain in obfuscated:
        emails.add(f"{prefix}@{domain}".lower())

    # JavaScript embedded emails
    js_blocks = re.findall(r'<script[^>]*>(.*?)</script>', raw_html, re.DOTALL | re.IGNORECASE)
    for js in js_blocks:
        for m in EMAIL_RE.finditer(js):
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
        r'<div[^>]*class="[^"]*title[^"]*"[^>]*>\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']{3,50})',
        r'<span[^>]*class="[^"]*name[^"]*"[^>]*>\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']{3,50})',
        r'<td[^>]*>\s*(?:<[^>]+>)*\s*([A-Z][a-zA-ZÀ-ÿ\s\.\-\']{3,50})\s*(?:</[^>]+>)*\s*</td>',
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
    """Fetch URL and extract all Berkeley emails."""
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
    for m in re.finditer(r'href="([^"]*\?page=\d+[^"]*)"', raw_html):
        page_url = urljoin(url, m.group(1))
        if page_url != url:
            page_links.add(page_url)

    for m in re.finditer(r'href="([^"]*/page/\d+[^"]*)"', raw_html):
        page_url = urljoin(url, m.group(1))
        if page_url != url:
            page_links.add(page_url)

    for pg_url in sorted(page_links):
        time.sleep(0.3)
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

        for prof_url in list(profile_urls)[:40]:
            time.sleep(0.3)
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
            print(f"    [{new:>3} new] {url[:75]}")
        time.sleep(0.4)
    return all_results


# ═══════════════════════════════════════════════════════════════════
# ALL TARGETED SOURCES
# ═══════════════════════════════════════════════════════════════════

PROFESSIONAL_SCHOOLS = [
    # 1. Haas Business School
    {
        "department": "Haas Business School",
        "urls": [
            "https://haas.berkeley.edu/phd/students/",
            "https://haas.berkeley.edu/people/students",
            "https://haas.berkeley.edu/phd/student-profiles/",
            "https://haas.berkeley.edu/faculty/phd-students/",
            "https://haas.berkeley.edu/phd/",
        ],
    },
    # 2. Berkeley Law
    {
        "department": "Berkeley Law",
        "urls": [
            "https://www.law.berkeley.edu/students/",
            "https://www.law.berkeley.edu/our-faculty/student-directory",
            "https://www.law.berkeley.edu/research/",
            "https://www.law.berkeley.edu/our-faculty/",
            "https://www.law.berkeley.edu/academics/jd-program/",
            "https://www.law.berkeley.edu/students/student-organizations/",
            "https://www.law.berkeley.edu/research/law-review/",
            "https://www.law.berkeley.edu/research/berkeley-technology-law-journal/",
        ],
    },
    # 3. Goldman School of Public Policy
    {
        "department": "Goldman School of Public Policy",
        "urls": [
            "https://gspp.berkeley.edu/people/students",
            "https://gspp.berkeley.edu/people/",
            "https://gspp.berkeley.edu/people/phd-students",
            "https://gspp.berkeley.edu/people/students?page=0",
            "https://gspp.berkeley.edu/people/students?page=1",
            "https://gspp.berkeley.edu/people/students?page=2",
        ],
    },
    # 4. Graduate School of Journalism
    {
        "department": "Graduate School of Journalism",
        "urls": [
            "https://journalism.berkeley.edu/people/students/",
            "https://journalism.berkeley.edu/people/",
            "https://journalism.berkeley.edu/students/",
        ],
    },
    # 5. Graduate School of Education
    {
        "department": "Graduate School of Education",
        "urls": [
            "https://education.berkeley.edu/people/students/",
            "https://education.berkeley.edu/people/",
            "https://gse.berkeley.edu/people/students",
            "https://gse.berkeley.edu/people/",
        ],
    },
    # 6. School of Public Health
    {
        "department": "School of Public Health",
        "urls": [
            "https://publichealth.berkeley.edu/people/students",
            "https://publichealth.berkeley.edu/people/",
            "https://publichealth.berkeley.edu/people/students?page=0",
            "https://publichealth.berkeley.edu/people/students?page=1",
            "https://publichealth.berkeley.edu/people/students?page=2",
            "https://publichealth.berkeley.edu/people/phd-students",
        ],
    },
    # 7. School of Information
    {
        "department": "School of Information",
        "urls": [
            "https://www.ischool.berkeley.edu/people/phd-students",
            "https://www.ischool.berkeley.edu/people/",
            "https://www.ischool.berkeley.edu/people/students",
            "https://www.ischool.berkeley.edu/people/phd",
            "https://www.ischool.berkeley.edu/people/mids",
            "https://www.ischool.berkeley.edu/people/mims",
        ],
    },
    # 8. School of Social Welfare
    {
        "department": "School of Social Welfare",
        "urls": [
            "https://socialwelfare.berkeley.edu/people/students",
            "https://socialwelfare.berkeley.edu/people/",
            "https://socialwelfare.berkeley.edu/people/doctoral-students",
            "https://socialwelfare.berkeley.edu/people/phd-students",
        ],
    },
    # 9. College of Natural Resources
    {
        "department": "College of Natural Resources",
        "urls": [
            "https://nature.berkeley.edu/people/students/",
            "https://nature.berkeley.edu/people/",
            "https://nature.berkeley.edu/people/graduate-students",
            "https://nature.berkeley.edu/people/phd-students",
        ],
    },
    # 10. School of Optometry
    {
        "department": "School of Optometry",
        "urls": [
            "https://optometry.berkeley.edu/people/",
            "https://optometry.berkeley.edu/people/students",
            "https://optometry.berkeley.edu/people/graduate-students",
            "https://optometry.berkeley.edu/research/",
        ],
    },
]

RESEARCH_LABS = [
    # 11. BAIR
    {
        "department": "BAIR (Berkeley AI Research)",
        "urls": [
            "https://bair.berkeley.edu/students.html",
            "https://bair.berkeley.edu/faculty.html",
            "https://bair.berkeley.edu/",
        ],
    },
    # 12. RISE Lab
    {
        "department": "RISE Lab",
        "urls": [
            "https://rise.cs.berkeley.edu/people/",
            "https://rise.cs.berkeley.edu/",
        ],
    },
    # 13. Robot Learning Lab
    {
        "department": "Robot Learning Lab",
        "urls": [
            "https://rll.berkeley.edu/people/",
            "https://rll.berkeley.edu/",
        ],
    },
    # 14. Automation Sciences Lab (Autolab)
    {
        "department": "Automation Sciences Lab (Autolab)",
        "urls": [
            "https://autolab.berkeley.edu/people",
            "https://autolab.berkeley.edu/",
            "https://autolab.berkeley.edu/members",
        ],
    },
    # 15. RAL (Robotic Automation Lab) - note: may redirect
    {
        "department": "Robotic Automation Lab",
        "urls": [
            "https://ral.berkeley.edu/people",
            "https://ral.berkeley.edu/",
        ],
    },
    # Additional high-value research labs
    {
        "department": "Berkeley DeepDrive (BDD)",
        "urls": [
            "https://bdd-data.berkeley.edu/people.html",
            "https://deepdrive.berkeley.edu/people",
        ],
    },
    {
        "department": "EECS Research Labs",
        "urls": [
            "https://www2.eecs.berkeley.edu/Research/Areas/",
        ],
    },
    {
        "department": "Berkeley Lab for Information & System Sciences (BLISS)",
        "urls": [
            "https://bliss.eecs.berkeley.edu/people.html",
            "https://bliss.eecs.berkeley.edu/",
        ],
    },
    {
        "department": "Statistical AI Lab (SAIL)",
        "urls": [
            "https://sail.berkeley.edu/members/",
            "https://sail.berkeley.edu/",
        ],
    },
    {
        "department": "Berkeley NLP Group",
        "urls": [
            "https://nlp.cs.berkeley.edu/",
        ],
    },
    {
        "department": "Sky Computing Lab",
        "urls": [
            "https://sky.cs.berkeley.edu/people/",
            "https://sky.cs.berkeley.edu/",
        ],
    },
    {
        "department": "AUTOLab (Ken Goldberg)",
        "urls": [
            "https://goldberg.berkeley.edu/people/",
            "https://goldberg.berkeley.edu/",
        ],
    },
    {
        "department": "CITRIS",
        "urls": [
            "https://citris-uc.org/people/",
            "https://citris-uc.org/about/people/",
        ],
    },
    {
        "department": "Pieter Abbeel Lab",
        "urls": [
            "https://people.eecs.berkeley.edu/~pabbeel/",
        ],
    },
    {
        "department": "Berkeley Vision & Learning Center (BVLC)",
        "urls": [
            "https://bvlc.eecs.berkeley.edu/",
        ],
    },
    {
        "department": "Energy & Resources Group (ERG)",
        "urls": [
            "https://erg.berkeley.edu/people/students/",
            "https://erg.berkeley.edu/people/",
        ],
    },
    {
        "department": "D-Lab",
        "urls": [
            "https://dlab.berkeley.edu/people",
            "https://dlab.berkeley.edu/people/staff",
        ],
    },
]

STUDENT_ORGS = [
    # 20. ASUC
    {
        "department": "ASUC (Student Government)",
        "urls": [
            "https://asuc.org/",
            "https://asuc.org/about/",
            "https://asuc.org/officers/",
            "https://asuc.org/senate/",
            "https://asuc.org/executives/",
            "https://asuc.berkeley.edu/",
            "https://lead.berkeley.edu/asuc/",
            "https://lead.berkeley.edu/asuc/senators/",
        ],
    },
    # 21. Daily Californian
    {
        "department": "Daily Californian (Student Newspaper)",
        "urls": [
            "https://www.dailycal.org/staff",
            "https://www.dailycal.org/about",
            "https://www.dailycal.org/about/contact",
            "https://www.dailycal.org/staff/",
        ],
    },
    # Student org directories
    {
        "department": "Student Organizations (CalLink)",
        "urls": [
            "https://callink.berkeley.edu/",
        ],
    },
    # Greek life
    {
        "department": "Greek Life - IFC",
        "urls": [
            "https://ifc.berkeley.edu/",
            "https://greeks.berkeley.edu/",
            "https://lead.berkeley.edu/greek-community/",
            "https://lead.berkeley.edu/greek-community/councils/",
        ],
    },
    {
        "department": "Greek Life - Panhellenic",
        "urls": [
            "https://panhellenic.berkeley.edu/",
            "https://berkeleypanhellenic.com/",
        ],
    },
    # Engineering student orgs
    {
        "department": "Engineering Student Orgs",
        "urls": [
            "https://engineering.berkeley.edu/students/student-organizations/",
            "https://hkn.eecs.berkeley.edu/about/officers",
            "https://hkn.eecs.berkeley.edu/",
            "https://IEEE.berkeley.edu/",
            "https://swe.berkeley.edu/",
        ],
    },
    # Debate / Model UN / other orgs
    {
        "department": "Berkeley Forum",
        "urls": [
            "https://forum.berkeley.edu/",
            "https://forum.berkeley.edu/about/",
        ],
    },
]

ATHLETICS = [
    # 25. Cal Bears Athletics
    {
        "department": "Cal Bears Athletics",
        "urls": [
            "https://calbears.com/staff-directory",
            "https://calbears.com/staff-directory/",
            "https://calbears.com/sports/2024/1/1/staff-directory.aspx",
        ],
    },
    # Coaching staff pages
    {
        "department": "Cal Bears - Football",
        "urls": [
            "https://calbears.com/sports/football/coaches",
        ],
    },
    {
        "department": "Cal Bears - Basketball",
        "urls": [
            "https://calbears.com/sports/mens-basketball/coaches",
            "https://calbears.com/sports/womens-basketball/coaches",
        ],
    },
    {
        "department": "Cal Bears - Swimming",
        "urls": [
            "https://calbears.com/sports/mens-swimming-and-diving/coaches",
            "https://calbears.com/sports/womens-swimming-and-diving/coaches",
        ],
    },
    {
        "department": "Cal Bears - Track & Field",
        "urls": [
            "https://calbears.com/sports/track-and-field/coaches",
        ],
    },
]

# Additional broad pages that tend to have emails
EXTRA_PAGES = [
    ("https://eecs.berkeley.edu/people/", "EECS - People"),
    ("https://cs.berkeley.edu/people/graduate-students/", "CS - Grad Students"),
    ("https://me.berkeley.edu/people/graduate-students/", "ME - Grad Students"),
    ("https://ce.berkeley.edu/people/graduate-students/", "CEE - Grad Students"),
    ("https://chemistry.berkeley.edu/people/graduate-students", "Chemistry - Grad Students"),
    ("https://bioeng.berkeley.edu/people/graduate-students/", "Bioengineering - Grad Students"),
    ("https://ieor.berkeley.edu/people/graduate-students/", "IEOR - Grad Students"),
    ("https://statistics.berkeley.edu/people/graduate-students", "Statistics - Grad Students"),
    ("https://math.berkeley.edu/people/grad-students", "Math - Grad Students"),
    ("https://physics.berkeley.edu/people/graduate-students", "Physics - Grad Students"),
    ("https://eps.berkeley.edu/people/graduate-students", "Earth & Planetary Science - Grad Students"),
]


def main():
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║  UC BERKELEY PRO SCHOOLS / LABS / ORGS / ATHLETICS SCRAPER   ║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<62}║")
    print("╚════════════════════════════════════════════════════════════════╝")

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

    # ── Phase 1: Professional Schools ──
    print(f"\n  Phase 1: Scraping {len(PROFESSIONAL_SCHOOLS)} Professional Schools...")
    print("  " + "─" * 60)

    for config in PROFESSIONAL_SCHOOLS:
        dept = config["department"]
        print(f"\n  📚 {dept}")
        results = scrape_with_fallbacks(config["urls"], dept, follow_subpages=True)
        new = add_results(results)
        print(f"    ── Total: {len(results)} found, {new} new unique")
        for r in results[:3]:
            nm = r['name'][:30] if r['name'] else '—'
            print(f"       {nm:<32} {r['email']}")

    # ── Phase 2: Research Labs ──
    print(f"\n\n  Phase 2: Scraping {len(RESEARCH_LABS)} Research Labs...")
    print("  " + "─" * 60)

    for config in RESEARCH_LABS:
        dept = config["department"]
        print(f"\n  🔬 {dept}")
        results = scrape_with_fallbacks(config["urls"], dept, follow_subpages=True)
        new = add_results(results)
        print(f"    ── Total: {len(results)} found, {new} new unique")
        for r in results[:3]:
            nm = r['name'][:30] if r['name'] else '—'
            print(f"       {nm:<32} {r['email']}")

    # ── Phase 3: Student Organizations ──
    print(f"\n\n  Phase 3: Scraping {len(STUDENT_ORGS)} Student Org sources...")
    print("  " + "─" * 60)

    for config in STUDENT_ORGS:
        dept = config["department"]
        print(f"\n  🎓 {dept}")
        results = scrape_with_fallbacks(config["urls"], dept, follow_subpages=True)
        new = add_results(results)
        print(f"    ── Total: {len(results)} found, {new} new unique")
        for r in results[:3]:
            nm = r['name'][:30] if r['name'] else '—'
            print(f"       {nm:<32} {r['email']}")

    # ── Phase 4: Athletics ──
    print(f"\n\n  Phase 4: Scraping {len(ATHLETICS)} Athletics sources...")
    print("  " + "─" * 60)

    for config in ATHLETICS:
        dept = config["department"]
        print(f"\n  🏈 {dept}")
        results = scrape_with_fallbacks(config["urls"], dept, follow_subpages=False)
        new = add_results(results)
        print(f"    ── Total: {len(results)} found, {new} new unique")
        for r in results[:3]:
            nm = r['name'][:30] if r['name'] else '—'
            print(f"       {nm:<32} {r['email']}")

    # ── Phase 5: Extra department pages ──
    print(f"\n\n  Phase 5: Scraping {len(EXTRA_PAGES)} extra department pages...")
    print("  " + "─" * 60)

    for url, dept in EXTRA_PAGES:
        results = scrape_url(url, dept, follow_subpages=True)
        new = add_results(results)
        if new > 0:
            print(f"    [{new:>3} new] {dept}")
        time.sleep(0.3)

    # ── Results Summary ──
    print("\n\n" + "═" * 70)
    print("  RESULTS SUMMARY")
    print("═" * 70)

    print(f"\n  Total unique emails: {len(all_contacts)}")

    berkeley_emails = [c for c in all_contacts if '@berkeley.edu' in c['email']]
    other_emails = [c for c in all_contacts if '@berkeley.edu' not in c['email']]
    print(f"  @berkeley.edu: {len(berkeley_emails)}")
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
    csv_path = os.path.join(base, "berkeley_pro_emails.csv")
    fieldnames = ["email", "name", "department", "source_url"]

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_contacts)

    json_path = os.path.join(base, "berkeley_pro_emails.json")
    with open(json_path, 'w') as f:
        json.dump(all_contacts, f, indent=2)

    print(f"\n  CSV saved:  {csv_path}")
    print(f"  JSON saved: {json_path}")

    # ── Print ALL emails grouped by department ──
    print(f"\n  {'═' * 70}")
    print(f"  ALL {len(all_contacts)} EMAILS")
    print(f"  {'═' * 70}")

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
            nm = c['name'][:35] if c['name'] else '—'
            print(f"    {nm:<37} {c['email']}")

    print(f"\n{'═' * 70}")
    print(f"  COMPLETE. {len(all_contacts)} unique emails scraped.")
    print(f"{'═' * 70}")


if __name__ == "__main__":
    main()
