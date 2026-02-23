#!/usr/bin/env python3
"""
Stanford University Email Scraper
==================================
Scrapes @stanford.edu emails from:
  - Professional schools (GSB, Law, Med, Education, Sustainability)
  - Research centers & institutes (FSI, SIEPR, HAI, Precourt, Woods, Cyber, King)
  - Student organizations (ASSU, Stanford Daily, Greek life)
  - Athletics (GoStanford coaching staff)

Outputs: stanford_pro_emails.csv
Columns: email, name, department, source_url
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import json
import time
import sys
import html as html_mod
from urllib.parse import urljoin, urlparse
from datetime import datetime

sys.stdout.reconfigure(line_buffering=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

session = requests.Session()
session.headers.update(HEADERS)

# ─── Email patterns ──────────────────────────────────────────────
EMAIL_RE = re.compile(
    r'[a-zA-Z0-9._%+\-]+@(?:[a-zA-Z0-9\-]+\.)*stanford\.edu',
    re.IGNORECASE
)

# Generic / admin emails to skip
SKIP_EMAILS = {
    'webmaster@stanford.edu', 'info@stanford.edu', 'help@stanford.edu',
    'admissions@stanford.edu', 'registrar@stanford.edu', 'noreply@stanford.edu',
    'donotreply@stanford.edu', 'feedback@stanford.edu', 'abuse@stanford.edu',
    'postmaster@stanford.edu', 'root@stanford.edu', 'security@stanford.edu',
    'communications@stanford.edu', 'contact@stanford.edu', 'support@stanford.edu',
    'news@stanford.edu', 'media@stanford.edu', 'giving@stanford.edu',
    'alumni@stanford.edu', 'development@stanford.edu', 'hr@stanford.edu',
    'privacy@stanford.edu', 'copyright@stanford.edu', 'dmca@stanford.edu',
    'accessibility@stanford.edu', 'itservices@stanford.edu',
    'library@stanford.edu', 'chancellor@stanford.edu',
    'president@stanford.edu', 'provost@stanford.edu',
    'dean@stanford.edu', 'events@stanford.edu',
    'web-admin@stanford.edu', 'web@stanford.edu',
    'press@stanford.edu', 'publicaffairs@stanford.edu',
    'its@stanford.edu', 'helpsu@stanford.edu',
    'admission@stanford.edu', 'finaid@stanford.edu',
    'cardinalcard@stanford.edu', 'housing@stanford.edu',
    'residential@stanford.edu', 'dining@stanford.edu',
    'titleix@stanford.edu', 'harrassment@stanford.edu',
    'gse-communications@stanford.edu', 'gse-admissions@stanford.edu',
    'gsb_phd@stanford.edu', 'gsb_mba@stanford.edu',
    'lawadmissions@stanford.edu', 'slsadmissions@stanford.edu',
    'medadmissions@stanford.edu', 'mdadmissions@stanford.edu',
    'som_admissions@stanford.edu',
    'editorial@stanforddaily.com', 'editor@stanforddaily.com',
    'marketing@stanforddaily.com', 'business@stanforddaily.com',
}

SKIP_PREFIXES = [
    'noreply', 'no-reply', 'donotreply', 'do-not-reply',
    'webmaster', 'wordpress', 'info@', 'admin@', 'office@',
    'help@', 'support@', 'contact@', 'registrar@',
    'admissions@', 'events@', 'media@', 'news@',
    'communications@', 'web-', 'web@', 'marketing@',
    'giving@', 'development@', 'advancement@',
]


def decode_html_entities(text):
    """Decode HTML entities."""
    if not text:
        return text
    decoded = html_mod.unescape(text)
    decoded = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), decoded)
    decoded = re.sub(r'&#x([0-9a-fA-F]+);', lambda m: chr(int(m.group(1), 16)), decoded)
    return decoded


def decode_cf_email(encoded_string):
    """Decode CloudFlare email protection."""
    try:
        r = int(encoded_string[:2], 16)
        return ''.join(chr(int(encoded_string[i:i+2], 16) ^ r)
                       for i in range(2, len(encoded_string), 2))
    except Exception:
        return ''


def is_skip_email(email):
    """Check if email should be skipped."""
    e = email.lower().strip()
    if e in SKIP_EMAILS:
        return True
    if any(e.startswith(p) for p in SKIP_PREFIXES):
        return True
    # Skip image/file-like emails
    if any(ext in e for ext in ['.jpg', '.png', '.gif', '.pdf', '.css', '.js']):
        return True
    return False


def extract_stanford_emails(text):
    """Extract @stanford.edu emails from raw text."""
    if not text:
        return []
    decoded = decode_html_entities(text)
    emails = set()

    for m in EMAIL_RE.finditer(decoded):
        e = m.group(0).lower().strip().rstrip('.')
        if not is_skip_email(e):
            emails.add(e)

    # mailto: links
    for m in re.finditer(r'mailto:([a-zA-Z0-9._%+\-]+@(?:[a-zA-Z0-9\-]+\.)*stanford\.edu)', decoded, re.IGNORECASE):
        e = m.group(1).lower().strip().rstrip('.')
        if not is_skip_email(e):
            emails.add(e)

    return list(emails)


def get_page(url, timeout=20):
    """Fetch page, return (soup, text) or (None, None)."""
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True, verify=True)
        if r.status_code != 200:
            return None, None
        soup = BeautifulSoup(r.text, 'html.parser')
        return soup, r.text
    except Exception as e:
        print(f"      [WARN] {url[:80]} — {str(e)[:60]}")
        return None, None


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


def extract_name_near_email(soup, raw_html, email, window=600):
    """Try to find a person name near their email in the HTML."""
    idx = raw_html.lower().find(email.lower())
    if idx < 0:
        return derive_name_from_email(email)

    start = max(0, idx - window)
    end = min(len(raw_html), idx + window)
    context = raw_html[start:end]

    patterns = [
        r'<h[1-4][^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-ZÀ-ÿ\s.\-\']+?)(?:\s*</a>)?\s*</h[1-4]>',
        r'<strong[^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-ZÀ-ÿ\s.\-\']+?)(?:\s*</a>)?\s*</strong>',
        r'class="[^"]*name[^"]*"[^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-ZÀ-ÿ\s.\-\']+)',
        r'<a[^>]*>\s*([A-Z][a-zA-ZÀ-ÿ\s.\-\']{3,40})\s*</a>\s*(?:<[^>]+>)*\s*(?:Graduate|Doctoral|PhD|Student)',
        r'(?:Dr\.\s+|Prof\.\s+)?([A-Z][a-zA-ZÀ-ÿ]+(?:\s+[A-Z][a-zA-ZÀ-ÿ]+){1,3})\s*(?:<[^>]*>)*\s*' + re.escape(email),
    ]

    for pat in patterns:
        m = re.search(pat, context, re.DOTALL)
        if m:
            name = m.group(1).strip()
            name = re.sub(r'<[^>]+>', '', name).strip()
            if 2 < len(name) < 60 and not re.search(r'[<>@{}\[\]]', name):
                return name

    return derive_name_from_email(email)


def scrape_page(url, department):
    """Scrape a single URL for stanford.edu emails."""
    print(f"    Fetching: {url[:90]}...")
    soup, raw_html = get_page(url)
    if not soup:
        return []

    decoded = decode_html_entities(raw_html)
    emails = extract_stanford_emails(raw_html)

    # Also check CF-protected emails
    for el in soup.find_all(attrs={'data-cfemail': True}):
        enc = el.get('data-cfemail', '')
        if enc:
            cf_decoded = decode_cf_email(enc)
            if cf_decoded and 'stanford.edu' in cf_decoded.lower():
                e = cf_decoded.lower().strip()
                if not is_skip_email(e):
                    emails.append(e)

    emails = list(set(emails))
    results = []
    for email in emails:
        name = extract_name_near_email(soup, decoded, email)
        results.append({
            'email': email,
            'name': name,
            'department': department,
            'source_url': url,
        })

    return results


def scrape_with_subpages(url, department, link_pattern=None, max_subpages=150, max_depth=1):
    """Scrape a page, then follow links matching pattern for more emails."""
    all_results = []
    all_seen_emails = set()
    visited_urls = set()

    def _scrape(current_url, dept, depth):
        if current_url in visited_urls or depth > max_depth:
            return
        visited_urls.add(current_url)

        results = scrape_page(current_url, dept)
        for r in results:
            if r['email'] not in all_seen_emails:
                all_seen_emails.add(r['email'])
                all_results.append(r)

        if link_pattern and depth < max_depth:
            soup, raw_html = get_page(current_url)
            if soup:
                links = set()
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if re.search(link_pattern, href):
                        full_url = urljoin(current_url, href)
                        if full_url not in visited_urls:
                            links.add(full_url)

                for sub_url in sorted(links)[:max_subpages]:
                    sub_results = scrape_page(sub_url, dept)
                    for r in sub_results:
                        if r['email'] not in all_seen_emails:
                            all_seen_emails.add(r['email'])
                            all_results.append(r)
                    time.sleep(0.3)

    _scrape(url, department, 0)
    return all_results


def scrape_paginated(base_url, department, max_pages=20, page_param='page'):
    """Scrape paginated directory pages."""
    all_results = []
    all_seen = set()

    for page_num in range(0, max_pages):
        sep = '&' if '?' in base_url else '?'
        url = f"{base_url}{sep}{page_param}={page_num}" if page_num > 0 else base_url
        results = scrape_page(url, department)

        new_count = 0
        for r in results:
            if r['email'] not in all_seen:
                all_seen.add(r['email'])
                all_results.append(r)
                new_count += 1

        if new_count == 0 and page_num > 0:
            break
        time.sleep(0.3)

    return all_results


# ═══════════════════════════════════════════════════════════════════
# SOURCE DEFINITIONS
# ═══════════════════════════════════════════════════════════════════

PROFESSIONAL_SCHOOLS = [
    # ── Stanford GSB (Business) ──
    ("https://www.gsb.stanford.edu/faculty-research/phd/students", "Stanford GSB - PhD Students"),
    ("https://www.gsb.stanford.edu/programs/phd/academic-experience/students", "Stanford GSB - PhD Students v2"),
    ("https://www.gsb.stanford.edu/programs/phd/students", "Stanford GSB - PhD Students v3"),
    ("https://www.gsb.stanford.edu/faculty-research/phd", "Stanford GSB - PhD Program"),
    ("https://www.gsb.stanford.edu/programs/phd/academic-experience", "Stanford GSB - Academic Experience"),
    ("https://www.gsb.stanford.edu/faculty-research/faculty", "Stanford GSB - Faculty"),
    ("https://www.gsb.stanford.edu/experience/phd/students", "Stanford GSB - PhD Experience"),
    ("https://www.gsb.stanford.edu/programs/phd", "Stanford GSB - PhD Main"),

    # ── Stanford Law School ──
    ("https://law.stanford.edu/directory/students/", "Stanford Law - Students"),
    ("https://law.stanford.edu/directory/", "Stanford Law - Directory"),
    ("https://law.stanford.edu/education/jsd/current-jsd-candidates/", "Stanford Law - JSD Candidates"),
    ("https://law.stanford.edu/education/phd-program/", "Stanford Law - PhD Program"),
    ("https://law.stanford.edu/student-life/student-organizations/", "Stanford Law - Student Orgs"),
    ("https://law.stanford.edu/directory/faculty/", "Stanford Law - Faculty"),
    ("https://law.stanford.edu/directory/staff/", "Stanford Law - Staff"),

    # ── Stanford Medical School ──
    ("https://med.stanford.edu/profiles.html", "Stanford Med - Profiles"),
    ("https://med.stanford.edu/education/phd-programs.html", "Stanford Med - PhD Programs"),
    ("https://med.stanford.edu/education/current-students.html", "Stanford Med - Current Students"),
    ("https://med.stanford.edu/profiles/students.html", "Stanford Med - Student Profiles"),
    ("https://med.stanford.edu/mstp/students.html", "Stanford Med - MSTP Students"),
    ("https://med.stanford.edu/biophysics/people/students.html", "Stanford Med - Biophysics Students"),
    ("https://med.stanford.edu/dbds/people/students.html", "Stanford Med - DBDS Students"),
    ("https://med.stanford.edu/cancer/people.html", "Stanford Med - Cancer Center People"),
    ("https://med.stanford.edu/bmi/people/students.html", "Stanford Med - BMI Students"),
    ("https://med.stanford.edu/immunol/people/graduate-students.html", "Stanford Med - Immunology Grad Students"),
    ("https://med.stanford.edu/genetics/people.html", "Stanford Med - Genetics People"),
    ("https://med.stanford.edu/neurosurgery/people.html", "Stanford Med - Neurosurgery People"),
    ("https://med.stanford.edu/biochemistry/people.html", "Stanford Med - Biochemistry People"),
    ("https://med.stanford.edu/microbiology-immunology/people.html", "Stanford Med - Microbio People"),
    ("https://med.stanford.edu/neurosciences-institute/people.html", "Stanford Med - Neurosciences People"),
    ("https://med.stanford.edu/pathology/people.html", "Stanford Med - Pathology People"),

    # ── Stanford School of Education (GSE) ──
    ("https://ed.stanford.edu/academics/doctoral/students", "Stanford Education - Doctoral Students"),
    ("https://ed.stanford.edu/people/students", "Stanford Education - Students"),
    ("https://ed.stanford.edu/academics/doctoral", "Stanford Education - Doctoral Program"),
    ("https://ed.stanford.edu/people", "Stanford Education - People"),
    ("https://ed.stanford.edu/faculty", "Stanford Education - Faculty"),
    ("https://ed.stanford.edu/academics/doctoral/current-students", "Stanford Education - Current Doctoral"),

    # ── Stanford School of Sustainability (Doerr School) ──
    ("https://sustainability.stanford.edu/people/students", "Stanford Sustainability - Students"),
    ("https://sustainability.stanford.edu/people", "Stanford Sustainability - People"),
    ("https://sustainability.stanford.edu/about/people", "Stanford Sustainability - About People"),
    ("https://doerr.stanford.edu/people", "Stanford Doerr School - People"),
    ("https://doerr.stanford.edu/people/students", "Stanford Doerr School - Students"),
    ("https://earth.stanford.edu/people", "Stanford Earth - People"),
    ("https://earth.stanford.edu/people/students", "Stanford Earth - Students"),

    # ── School of Engineering ──
    ("https://engineering.stanford.edu/people", "Stanford Engineering - People"),
    ("https://cs.stanford.edu/people", "Stanford CS - People"),
    ("https://cs.stanford.edu/people/phd-students", "Stanford CS - PhD Students"),
    ("https://ee.stanford.edu/people/graduate-students", "Stanford EE - Grad Students"),
    ("https://me.stanford.edu/people/phd-students", "Stanford ME - PhD Students"),
    ("https://cee.stanford.edu/people/students", "Stanford CEE - Students"),
    ("https://msande.stanford.edu/people/students", "Stanford MS&E - Students"),
    ("https://chemeng.stanford.edu/people/phd-students", "Stanford ChemEng - PhD Students"),
    ("https://matsci.stanford.edu/people/graduate-students", "Stanford MatSci - Grad Students"),
    ("https://bioengineering.stanford.edu/people/students", "Stanford BioE - Students"),
    ("https://aero-astro.stanford.edu/people/students", "Stanford Aero/Astro - Students"),

    # ── School of Humanities & Sciences ──
    ("https://economics.stanford.edu/people/phd-students", "Stanford Economics - PhD Students"),
    ("https://politicalscience.stanford.edu/people/graduate-students", "Stanford PoliSci - Grad Students"),
    ("https://sociology.stanford.edu/people/graduate-students", "Stanford Sociology - Grad Students"),
    ("https://psychology.stanford.edu/people/graduate-students", "Stanford Psychology - Grad Students"),
    ("https://history.stanford.edu/people/graduate-students", "Stanford History - Grad Students"),
    ("https://philosophy.stanford.edu/people/graduate-students", "Stanford Philosophy - Grad Students"),
    ("https://english.stanford.edu/people/graduate-students", "Stanford English - Grad Students"),
    ("https://statistics.stanford.edu/people/phd-students", "Stanford Statistics - PhD Students"),
    ("https://mathematics.stanford.edu/people/graduate-students", "Stanford Math - Grad Students"),
    ("https://physics.stanford.edu/people/students", "Stanford Physics - Students"),
    ("https://chemistry.stanford.edu/people/graduate-students", "Stanford Chemistry - Grad Students"),
    ("https://biology.stanford.edu/people/graduate-students", "Stanford Biology - Grad Students"),
]

RESEARCH_CENTERS = [
    # ── Freeman Spogli Institute (FSI) ──
    ("https://fsi.stanford.edu/people", "FSI - Freeman Spogli Institute"),
    ("https://fsi.stanford.edu/people/fellows", "FSI - Fellows"),
    ("https://fsi.stanford.edu/people/students", "FSI - Students"),
    ("https://fsi.stanford.edu/people?type=fellow", "FSI - Fellows v2"),
    ("https://fsi.stanford.edu/people?type=student", "FSI - Students v2"),

    # ── SIEPR ──
    ("https://siepr.stanford.edu/people", "SIEPR - People"),
    ("https://siepr.stanford.edu/people/researchers", "SIEPR - Researchers"),
    ("https://siepr.stanford.edu/people/fellows", "SIEPR - Fellows"),

    # ── HAI (Human-Centered AI) ──
    ("https://hai.stanford.edu/people", "HAI - Human-Centered AI"),
    ("https://hai.stanford.edu/people/researchers", "HAI - Researchers"),
    ("https://hai.stanford.edu/people/fellows", "HAI - Fellows"),
    ("https://hai.stanford.edu/people/students", "HAI - Students"),

    # ── Precourt Institute for Energy ──
    ("https://energy.stanford.edu/people", "Precourt Energy - People"),
    ("https://energy.stanford.edu/people/students", "Precourt Energy - Students"),
    ("https://energy.stanford.edu/people/researchers", "Precourt Energy - Researchers"),

    # ── Woods Institute for Environment ──
    ("https://woods.stanford.edu/people", "Woods Institute - People"),
    ("https://woods.stanford.edu/people/fellows", "Woods Institute - Fellows"),
    ("https://woods.stanford.edu/people/students", "Woods Institute - Students"),

    # ── Stanford Internet Observatory / Cyber Policy ──
    ("https://cyber.stanford.edu/people", "Stanford Cyber Policy - People"),
    ("https://cyber.stanford.edu/people/researchers", "Stanford Cyber - Researchers"),
    ("https://io.stanford.edu/people", "Stanford Internet Observatory - People"),
    ("https://io.stanford.edu/about/people", "Stanford Internet Observatory - About"),

    # ── King Center on Global Development ──
    ("https://kingcenter.stanford.edu/people", "King Center - Global Development"),
    ("https://kingcenter.stanford.edu/people/fellows", "King Center - Fellows"),
    ("https://kingcenter.stanford.edu/people/students", "King Center - Students"),

    # ── Additional research centers ──
    ("https://cisac.fsi.stanford.edu/people", "CISAC - People"),
    ("https://aparc.fsi.stanford.edu/people", "APARC - People"),
    ("https://cddrl.fsi.stanford.edu/people", "CDDRL - People"),
    ("https://sccei.fsi.stanford.edu/people", "SCCEI China Economy - People"),
    ("https://immigration.stanford.edu/people", "Immigration Policy Lab - People"),
    ("https://globalhealth.stanford.edu/people.html", "Stanford Global Health - People"),
    ("https://cardinalatwork.stanford.edu/", "Stanford Cardinal at Work"),
]

STUDENT_ORGS = [
    # ── ASSU ──
    ("https://assu.stanford.edu/", "ASSU - Main"),
    ("https://assu.stanford.edu/about/", "ASSU - About"),
    ("https://assu.stanford.edu/leadership/", "ASSU - Leadership"),
    ("https://assu.stanford.edu/officers/", "ASSU - Officers"),
    ("https://assu.stanford.edu/senate/", "ASSU - Senate"),
    ("https://assu.stanford.edu/executive/", "ASSU - Executive"),
    ("https://assu.stanford.edu/about-assu/", "ASSU - About v2"),
    ("https://assu.stanford.edu/team/", "ASSU - Team"),
    ("https://web.stanford.edu/group/assu/", "ASSU - Web Group"),

    # ── Stanford Daily ──
    ("https://stanforddaily.com/staff/", "Stanford Daily - Staff"),
    ("https://stanforddaily.com/about/", "Stanford Daily - About"),
    ("https://stanforddaily.com/contact/", "Stanford Daily - Contact"),
    ("https://stanforddaily.com/staff-list/", "Stanford Daily - Staff List"),

    # ── Student organizations directories ──
    ("https://studentaffairs.stanford.edu/student-organizations", "Stanford Student Orgs"),
    ("https://ose.stanford.edu/student-organizations", "OSE - Student Orgs"),
    ("https://ose.stanford.edu/get-involved/student-organizations", "OSE - Student Orgs v2"),
    ("https://studentaffairs.stanford.edu/student-orgs", "Stanford Student Orgs v2"),
    ("https://engage.stanford.edu/organizations", "Stanford Engage - Organizations"),
    ("https://engage.stanford.edu/", "Stanford Engage"),

    # ── Greek life ──
    ("https://studentaffairs.stanford.edu/greek-life", "Stanford Greek Life"),
    ("https://web.stanford.edu/group/fsl/", "Stanford FSL"),
    ("https://web.stanford.edu/group/ifc/", "Stanford IFC"),
    ("https://web.stanford.edu/group/panhellenic/", "Stanford Panhellenic"),

    # ── Graduate Student Council ──
    ("https://gsc.stanford.edu/", "GSC - Graduate Student Council"),
    ("https://gsc.stanford.edu/about/", "GSC - About"),
    ("https://gsc.stanford.edu/leadership/", "GSC - Leadership"),
    ("https://gsc.stanford.edu/people/", "GSC - People"),
    ("https://web.stanford.edu/group/gsc/", "GSC - Web Group"),

    # ── Other major student groups ──
    ("https://web.stanford.edu/group/sps/", "Stanford Political Society"),
    ("https://web.stanford.edu/group/sdr/", "Stanford Debate"),
    ("https://web.stanford.edu/group/mun/", "Stanford Model UN"),
    ("https://entrepreneurship.stanford.edu/students", "Stanford Entrepreneurship - Students"),
    ("https://bases.stanford.edu/team", "BASES - Team"),
    ("https://bases.stanford.edu/about", "BASES - About"),
    ("https://stanfordreview.org/about/", "Stanford Review"),
    ("https://stanfordreview.org/staff/", "Stanford Review - Staff"),
]

ATHLETICS = [
    # ── GoStanford coaching staff ──
    ("https://gostanford.com/staff-directory", "Stanford Athletics - Staff Directory"),
    ("https://gostanford.com/staff-directory/coaches", "Stanford Athletics - Coaches"),
    ("https://gostanford.com/sports/football/coaches", "Stanford Football - Coaches"),
    ("https://gostanford.com/sports/mens-basketball/coaches", "Stanford M. Basketball - Coaches"),
    ("https://gostanford.com/sports/womens-basketball/coaches", "Stanford W. Basketball - Coaches"),
    ("https://gostanford.com/sports/baseball/coaches", "Stanford Baseball - Coaches"),
    ("https://gostanford.com/sports/mens-soccer/coaches", "Stanford M. Soccer - Coaches"),
    ("https://gostanford.com/sports/womens-soccer/coaches", "Stanford W. Soccer - Coaches"),
    ("https://gostanford.com/sports/womens-volleyball/coaches", "Stanford Volleyball - Coaches"),
    ("https://gostanford.com/sports/mens-swimming-and-diving/coaches", "Stanford M. Swimming - Coaches"),
    ("https://gostanford.com/sports/womens-swimming-and-diving/coaches", "Stanford W. Swimming - Coaches"),
    ("https://gostanford.com/sports/mens-tennis/coaches", "Stanford M. Tennis - Coaches"),
    ("https://gostanford.com/sports/womens-tennis/coaches", "Stanford W. Tennis - Coaches"),
    ("https://gostanford.com/sports/mens-track-and-field/coaches", "Stanford M. Track - Coaches"),
    ("https://gostanford.com/sports/womens-track-and-field/coaches", "Stanford W. Track - Coaches"),
    ("https://gostanford.com/sports/wrestling/coaches", "Stanford Wrestling - Coaches"),
    ("https://gostanford.com/sports/mens-water-polo/coaches", "Stanford M. Water Polo - Coaches"),
    ("https://gostanford.com/sports/womens-water-polo/coaches", "Stanford W. Water Polo - Coaches"),
    ("https://gostanford.com/sports/mens-golf/coaches", "Stanford M. Golf - Coaches"),
    ("https://gostanford.com/sports/womens-golf/coaches", "Stanford W. Golf - Coaches"),
    ("https://gostanford.com/sports/mens-gymnastics/coaches", "Stanford M. Gymnastics - Coaches"),
    ("https://gostanford.com/sports/womens-gymnastics/coaches", "Stanford W. Gymnastics - Coaches"),
    ("https://gostanford.com/sports/field-hockey/coaches", "Stanford Field Hockey - Coaches"),
    ("https://gostanford.com/sports/womens-lacrosse/coaches", "Stanford W. Lacrosse - Coaches"),
    ("https://gostanford.com/sports/rowing/coaches", "Stanford Rowing - Coaches"),
    ("https://gostanford.com/sports/softball/coaches", "Stanford Softball - Coaches"),
    ("https://gostanford.com/sports/fencing/coaches", "Stanford Fencing - Coaches"),
    ("https://gostanford.com/sports/sailing/coaches", "Stanford Sailing - Coaches"),
    ("https://gostanford.com/sports/squash/coaches", "Stanford Squash - Coaches"),
    ("https://gostanford.com/sports/cross-country/coaches", "Stanford Cross Country - Coaches"),
    ("https://gostanford.com/sports/mens-rowing/coaches", "Stanford M. Rowing - Coaches"),

    # ── Athletics admin / general staff ──
    ("https://gostanford.com/staff-directory/administration", "Stanford Athletics - Administration"),
    ("https://gostanford.com/staff-directory/sports-medicine", "Stanford Athletics - Sports Medicine"),
    ("https://gostanford.com/staff-directory/strength-and-conditioning", "Stanford Athletics - S&C"),
    ("https://gostanford.com/staff-directory/compliance", "Stanford Athletics - Compliance"),
    ("https://gostanford.com/staff-directory/academic-support", "Stanford Athletics - Academic Support"),
]


def scrape_all_sources(source_list, category_name):
    """Scrape a list of (url, department) tuples."""
    print(f"\n{'═'*70}")
    print(f"  {category_name}")
    print(f"{'═'*70}")

    results = []
    seen = set()
    profile_links_to_follow = {}  # url -> department

    for url, department in source_list:
        page_results = scrape_page(url, department)

        new_count = 0
        for r in page_results:
            if r['email'] not in seen:
                seen.add(r['email'])
                results.append(r)
                new_count += 1

        if new_count > 0:
            print(f"    [{new_count:>3} new] {department}")
            for c in page_results[-min(3, new_count):]:
                nm = c['name'][:25] if c['name'] else '—'
                print(f"             {nm:<28} {c['email']}")
        else:
            print(f"    [  0    ] {department}")

        # Collect profile links from already-fetched pages
        # We re-fetch here but only to discover links - could optimize
        soup2, _ = get_page(url, timeout=10)
        if soup2:
            for a in soup2.find_all('a', href=True):
                href = a['href']
                full = urljoin(url, href)
                parsed = urlparse(full)
                if parsed.netloc and 'stanford.edu' in parsed.netloc:
                    if any(kw in href.lower() for kw in
                           ['/people/', '/person/', '/profile/']):
                        if full not in {u for u, _ in source_list}:
                            if full not in profile_links_to_follow:
                                profile_links_to_follow[full] = department

        time.sleep(0.3)

    # Phase 2: Follow discovered profile links (limited)
    if profile_links_to_follow:
        # Limit to 200 profile pages to keep runtime reasonable
        to_visit = sorted(profile_links_to_follow.items())[:200]
        print(f"\n    Following {len(to_visit)} profile sub-pages...")
        for sub_url, dept in to_visit:
            sub_results = scrape_page(sub_url, dept)
            for r in sub_results:
                if r['email'] not in seen:
                    seen.add(r['email'])
                    results.append(r)
                    nm = r['name'][:25] if r['name'] else '—'
                    print(f"      + {nm:<28} {r['email']}")
            time.sleep(0.25)

    print(f"    ── {category_name} subtotal: {len(results)} unique emails ──")
    return results


def scrape_gostanford_staff_directory():
    """Special handler for GoStanford staff directory - scrape main pages only (no sub-links)."""
    print(f"\n{'═'*70}")
    print(f"  ATHLETICS - GoStanford Staff & Coaches")
    print(f"{'═'*70}")

    results = []
    seen = set()

    for url, department in ATHLETICS:
        soup, raw_html = get_page(url, timeout=15)
        if not soup:
            print(f"    [SKIP  ] {department}")
            time.sleep(0.2)
            continue

        decoded = decode_html_entities(raw_html) if raw_html else ""
        emails = extract_stanford_emails(raw_html)

        # Also check CF emails
        for el in soup.find_all(attrs={'data-cfemail': True}):
            enc = el.get('data-cfemail', '')
            if enc:
                cf_decoded = decode_cf_email(enc)
                if cf_decoded and 'stanford.edu' in cf_decoded.lower():
                    e = cf_decoded.lower().strip()
                    if not is_skip_email(e):
                        emails.append(e)

        emails = list(set(emails))
        new_count = 0
        for email in emails:
            if email not in seen:
                name = extract_name_near_email(soup, decoded, email)
                seen.add(email)
                results.append({
                    'email': email,
                    'name': name,
                    'department': department,
                    'source_url': url,
                })
                new_count += 1

        if new_count > 0:
            print(f"    [{new_count:>3} new] {department}")
        else:
            print(f"    [  0    ] {department}")

        time.sleep(0.2)

    print(f"    ── Athletics subtotal: {len(results)} unique emails ──")
    return results


def main():
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║  STANFORD UNIVERSITY EMAIL SCRAPER                           ║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<62}║")
    print("╚════════════════════════════════════════════════════════════════╝")

    all_contacts = []
    master_seen = set()

    def merge(new_results):
        added = 0
        for r in new_results:
            e = r['email'].lower().strip()
            if e not in master_seen:
                master_seen.add(e)
                r['email'] = e
                all_contacts.append(r)
                added += 1
        return added

    # ── Phase 1: Professional Schools ──
    pro_results = scrape_all_sources(PROFESSIONAL_SCHOOLS, "PROFESSIONAL SCHOOLS & DEPARTMENTS")
    added = merge(pro_results)
    print(f"\n  -> Added {added} new emails (total: {len(all_contacts)})")

    # ── Phase 2: Research Centers ──
    rc_results = scrape_all_sources(RESEARCH_CENTERS, "RESEARCH CENTERS & INSTITUTES")
    added = merge(rc_results)
    print(f"\n  -> Added {added} new emails (total: {len(all_contacts)})")

    # ── Phase 3: Student Organizations ──
    org_results = scrape_all_sources(STUDENT_ORGS, "STUDENT ORGANIZATIONS")
    added = merge(org_results)
    print(f"\n  -> Added {added} new emails (total: {len(all_contacts)})")

    # ── Phase 4: Athletics ──
    ath_results = scrape_gostanford_staff_directory()
    added = merge(ath_results)
    print(f"\n  -> Added {added} new emails (total: {len(all_contacts)})")

    # ── Final cleanup & save ──
    print(f"\n{'═'*70}")
    print("  FINAL RESULTS")
    print(f"{'═'*70}")

    # Sort by department, then email
    all_contacts.sort(key=lambda x: (x['department'], x['email']))

    # Save CSV
    csv_path = '/Users/jaiashar/Documents/VoraBusinessFinder/stanford_pro_emails.csv'
    fieldnames = ['email', 'name', 'department', 'source_url']
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_contacts)

    # Save JSON too
    json_path = '/Users/jaiashar/Documents/VoraBusinessFinder/stanford_pro_emails.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(all_contacts, f, indent=2, ensure_ascii=False)

    # Summary
    print(f"\n  TOTAL UNIQUE EMAILS: {len(all_contacts)}")
    dept_counts = {}
    for c in all_contacts:
        d = c['department']
        dept_counts[d] = dept_counts.get(d, 0) + 1

    print(f"\n  By department/source:")
    for d, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        print(f"    {d:<55} {count}")

    # Print all emails
    print(f"\n  {'═'*70}")
    print(f"  ALL {len(all_contacts)} EMAILS")
    print(f"  {'═'*70}")

    by_dept = {}
    for c in all_contacts:
        d = c['department']
        if d not in by_dept:
            by_dept[d] = []
        by_dept[d].append(c)

    for dept in sorted(by_dept.keys()):
        contacts = by_dept[dept]
        print(f"\n  [{dept}] ({len(contacts)} contacts)")
        for c in contacts:
            nm = c['name'][:35] if c['name'] else '—'
            print(f"    {nm:<37} {c['email']}")

    print(f"\n{'═'*70}")
    print(f"  COMPLETE. {len(all_contacts)} unique @stanford.edu emails.")
    print(f"  CSV:  {csv_path}")
    print(f"  JSON: {json_path}")
    print(f"{'═'*70}")

    return all_contacts


if __name__ == "__main__":
    main()
