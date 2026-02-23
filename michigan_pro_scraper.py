#!/usr/bin/env python3
"""
University of Michigan Email Scraper (Playwright-based)
=========================================================
Uses a real browser to bypass UMich's bot protection (403 blocks).
Scrapes @umich.edu emails from:
  - Professional schools (Ross, Law, Medicine, SPH, Ford, SI, etc.)
  - Research labs & institutes
  - Student organizations (CSG, Michigan Daily, Greek)
  - Athletics (MGoBlue coaching/staff)

Outputs: michigan_pro_emails.csv
Columns: email, name, department, source_url
"""

import re
import csv
import json
import time
import sys
import html as html_mod
from urllib.parse import urljoin, urlparse
from datetime import datetime
from playwright.sync_api import sync_playwright

sys.stdout.reconfigure(line_buffering=True)

# ─── Email patterns ──────────────────────────────────────────────
EMAIL_RE = re.compile(
    r'[a-zA-Z0-9._%+\-]+@(?:[a-zA-Z0-9\-]+\.)*umich\.edu',
    re.IGNORECASE
)

# Generic / admin emails to skip
SKIP_EMAILS = {
    'webmaster@umich.edu', 'info@umich.edu', 'help@umich.edu',
    'admissions@umich.edu', 'registrar@umich.edu', 'noreply@umich.edu',
    'donotreply@umich.edu', 'feedback@umich.edu', 'abuse@umich.edu',
    'postmaster@umich.edu', 'root@umich.edu', 'security@umich.edu',
    'communications@umich.edu', 'contact@umich.edu', 'support@umich.edu',
    'news@umich.edu', 'media@umich.edu', 'giving@umich.edu',
    'alumni@umich.edu', 'development@umich.edu', 'hr@umich.edu',
    'privacy@umich.edu', 'copyright@umich.edu', 'dmca@umich.edu',
    'accessibility@umich.edu', 'its@umich.edu',
    'library@umich.edu', 'chancellor@umich.edu',
    'president@umich.edu', 'provost@umich.edu',
    'dean@umich.edu', 'events@umich.edu',
    'web-admin@umich.edu', 'web@umich.edu',
    'press@umich.edu', 'publicaffairs@umich.edu',
    'helpdesk@umich.edu', 'itservicedesk@umich.edu',
    'finaid@umich.edu', 'housing@umich.edu',
    'titleix@umich.edu', 'ecrt@umich.edu',
    'umichhr@umich.edu', 'careers@umich.edu',
    'marketing@umich.edu', 'ucomm@umich.edu',
    'ummedia@umich.edu', 'umweb@umich.edu',
    'enrollment@umich.edu', 'gradschool@umich.edu',
    'rackham@umich.edu', 'lsa@umich.edu',
    'michiganross@umich.edu', 'rossinfo@umich.edu',
    'ross.admissions@umich.edu', 'rossadmissions@umich.edu',
    'law.admissions@umich.edu', 'lawadmissions@umich.edu',
    'sph.inquiries@umich.edu', 'sph.admissions@umich.edu',
    'si.admissions@umich.edu', 'si.info@umich.edu',
    'fordschool@umich.edu', 'ford.admissions@umich.edu',
    'soe.info@umich.edu', 'soe.admissions@umich.edu',
    'ssw.info@umich.edu', 'ssw.admissions@umich.edu',
    'stamps.info@umich.edu', 'stamps.admissions@umich.edu',
    'smtd.info@umich.edu', 'smtd.admissions@umich.edu',
    'pharmacy@umich.edu', 'pharmacy.admissions@umich.edu',
    'dent.info@umich.edu', 'dentistry@umich.edu',
    'nursing.info@umich.edu', 'nursing@umich.edu',
    'seas.info@umich.edu', 'seas@umich.edu',
    'mgoblue@umich.edu', 'athletics@umich.edu',
    'compliance@umich.edu', 'umrecruiting@umich.edu',
    'wolverineaccess@umich.edu', 'online@umich.edu',
    'studentlife@umich.edu', 'deanofstudents@umich.edu',
    'sapac@umich.edu', 'oscr@umich.edu',
    'uhs@umich.edu', 'caps@umich.edu',
    'medical.admissions@umich.edu', 'medadmissions@umich.edu',
    'example@umich.edu', 'uniqname@umich.edu',
    'umich@umich.edu', 'test@umich.edu',
}

SKIP_PREFIXES = [
    'noreply', 'no-reply', 'donotreply', 'do-not-reply',
    'webmaster', 'wordpress', 'info@', 'admin@', 'office@',
    'help@', 'support@', 'contact@', 'registrar@',
    'admissions@', 'events@', 'media@', 'news@',
    'communications@', 'web-', 'web@', 'marketing@',
    'giving@', 'development@', 'advancement@',
    'recruitment@', 'recruit@', 'engage@',
]


def decode_html_entities(text):
    if not text:
        return text
    decoded = html_mod.unescape(text)
    decoded = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), decoded)
    decoded = re.sub(r'&#x([0-9a-fA-F]+);', lambda m: chr(int(m.group(1), 16)), decoded)
    return decoded


def decode_cf_email(encoded_string):
    try:
        r = int(encoded_string[:2], 16)
        return ''.join(chr(int(encoded_string[i:i+2], 16) ^ r)
                       for i in range(2, len(encoded_string), 2))
    except Exception:
        return ''


def is_skip_email(email):
    e = email.lower().strip()
    if e in SKIP_EMAILS:
        return True
    if any(e.startswith(p) for p in SKIP_PREFIXES):
        return True
    if any(ext in e for ext in ['.jpg', '.png', '.gif', '.pdf', '.css', '.js']):
        return True
    return False


def extract_umich_emails(text):
    if not text:
        return []
    decoded = decode_html_entities(text)
    emails = set()
    for m in EMAIL_RE.finditer(decoded):
        e = m.group(0).lower().strip().rstrip('.')
        if not is_skip_email(e):
            emails.add(e)
    for m in re.finditer(r'mailto:([a-zA-Z0-9._%+\-]+@(?:[a-zA-Z0-9\-]+\.)*umich\.edu)', decoded, re.IGNORECASE):
        e = m.group(1).lower().strip().rstrip('.')
        if not is_skip_email(e):
            emails.add(e)
    return list(emails)


def derive_name_from_email(email):
    local = email.split('@')[0]
    for sep in ['.', '_']:
        if sep in local:
            parts = local.split(sep)
            if all(len(p) > 1 for p in parts[:2]):
                cleaned = [re.sub(r'\d+', '', p) for p in parts[:2]]
                if all(len(p) > 1 for p in cleaned):
                    return ' '.join(p.capitalize() for p in cleaned)
    return ""


def extract_name_near_email(raw_html, email, window=600):
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
        r'<div[^>]*class="[^"]*(?:card|person|profile|member)[^"]*"[^>]*>.*?<[^>]*>\s*([A-Z][a-zA-ZÀ-ÿ\s.\-\']{3,40})\s*<',
        r'<td[^>]*>\s*([A-Z][a-zA-ZÀ-ÿ\s.\-\']{3,40})\s*</td>',
        r'<li[^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-ZÀ-ÿ\s.\-\']{3,40})\s*(?:</a>)?\s*.*?' + re.escape(email),
    ]
    for pat in patterns:
        m = re.search(pat, context, re.DOTALL)
        if m:
            name = m.group(1).strip()
            name = re.sub(r'<[^>]+>', '', name).strip()
            if 2 < len(name) < 60 and not re.search(r'[<>@{}\[\]]', name):
                return name
    return derive_name_from_email(email)


def fetch_page_playwright(page, url, wait_ms=3000):
    """Navigate to a URL using playwright, wait for content, return HTML."""
    try:
        resp = page.goto(url, timeout=30000, wait_until='domcontentloaded')
        if resp and resp.status >= 400:
            print(f"      [HTTP {resp.status}] {url[:80]}")
            return None
        page.wait_for_timeout(wait_ms)
        # Try waiting for body content
        try:
            page.wait_for_selector('body', timeout=5000)
        except Exception:
            pass
        html = page.content()
        return html
    except Exception as e:
        print(f"      [ERR] {url[:80]} — {str(e)[:60]}")
        return None


def scrape_page_pw(page, url, department):
    """Scrape a single URL for umich.edu emails using Playwright."""
    print(f"    Fetching: {url[:90]}...")
    raw_html = fetch_page_playwright(page, url)
    if not raw_html:
        return []

    decoded = decode_html_entities(raw_html)
    emails = extract_umich_emails(raw_html)

    # Check CF-protected emails
    cf_pattern = re.compile(r'data-cfemail="([a-f0-9]+)"', re.IGNORECASE)
    for m in cf_pattern.finditer(raw_html):
        cf_decoded = decode_cf_email(m.group(1))
        if cf_decoded and 'umich.edu' in cf_decoded.lower():
            e = cf_decoded.lower().strip()
            if not is_skip_email(e):
                emails.append(e)

    emails = list(set(emails))
    results = []
    for email in emails:
        name = extract_name_near_email(decoded, email)
        results.append({
            'email': email,
            'name': name,
            'department': department,
            'source_url': url,
        })
    return results


def collect_sub_links(raw_html, base_url, source_urls_set):
    """Find profile/people links to follow."""
    links = set()
    for m in re.finditer(r'href=["\']([^"\']+)["\']', raw_html):
        href = m.group(1)
        full = urljoin(base_url, href)
        parsed = urlparse(full)
        if parsed.netloc and 'umich.edu' in parsed.netloc:
            if any(kw in href.lower() for kw in ['/people/', '/person/', '/profile/', '/faculty/', '/directory/']):
                if full not in source_urls_set:
                    links.add(full)
    return links


# ═══════════════════════════════════════════════════════════════════
# SOURCE DEFINITIONS
# ═══════════════════════════════════════════════════════════════════

PROFESSIONAL_SCHOOLS = [
    # ── Ross School of Business ──
    ("https://michiganross.umich.edu/faculty-research/faculty", "Ross Business - Faculty"),
    ("https://michiganross.umich.edu/programs/phd/students", "Ross Business - PhD Students"),
    ("https://michiganross.umich.edu/programs/phd/current-students", "Ross Business - PhD Current"),
    ("https://michiganross.umich.edu/faculty-research/directory", "Ross Business - Directory"),
    ("https://michiganross.umich.edu/programs/phd/accounting", "Ross Business - PhD Accounting"),
    ("https://michiganross.umich.edu/programs/phd/finance", "Ross Business - PhD Finance"),
    ("https://michiganross.umich.edu/programs/phd/marketing", "Ross Business - PhD Marketing"),
    ("https://michiganross.umich.edu/programs/phd/management-and-organizations", "Ross Business - PhD Mgmt & Orgs"),
    ("https://michiganross.umich.edu/programs/phd/strategy", "Ross Business - PhD Strategy"),
    ("https://michiganross.umich.edu/programs/phd/technology-and-operations", "Ross Business - PhD Tech & Ops"),

    # ── Michigan Law School ──
    ("https://michigan.law.umich.edu/directory", "Michigan Law - Directory"),
    ("https://michigan.law.umich.edu/faculty-and-scholarship/our-faculty", "Michigan Law - Faculty"),
    ("https://michigan.law.umich.edu/student-life/student-organizations", "Michigan Law - Student Orgs"),
    ("https://www.law.umich.edu/FacultyBio/Pages/default.aspx", "Michigan Law - Faculty v2"),

    # ── Michigan Medicine / Medical School ──
    ("https://medicine.umich.edu/medschool/education/md-program/student-life", "Michigan Medicine - Student Life"),
    ("https://medicine.umich.edu/dept/human-genetics/people", "Michigan Med - Human Genetics"),
    ("https://medicine.umich.edu/dept/microbiology-immunology/people", "Michigan Med - Microbio"),
    ("https://medicine.umich.edu/dept/pharmacology/people", "Michigan Med - Pharmacology"),
    ("https://medicine.umich.edu/dept/biochemistry/people", "Michigan Med - Biochemistry"),
    ("https://medicine.umich.edu/dept/cell-developmental-biology/people", "Michigan Med - Cell Bio"),
    ("https://medicine.umich.edu/dept/computational-medicine-bioinformatics/people", "Michigan Med - Comp Med"),
    ("https://medicine.umich.edu/dept/biostatistics/people", "Michigan Med - Biostatistics"),
    ("https://medicine.umich.edu/dept/internal-medicine/people", "Michigan Med - Internal Med"),

    # ── School of Public Health ──
    ("https://sph.umich.edu/faculty-staff/faculty.html", "SPH - Faculty"),
    ("https://sph.umich.edu/epid/faculty.html", "SPH Epidemiology - Faculty"),
    ("https://sph.umich.edu/biostat/faculty.html", "SPH Biostatistics - Faculty"),
    ("https://sph.umich.edu/hbhe/faculty.html", "SPH Health Behavior - Faculty"),
    ("https://sph.umich.edu/hmp/faculty.html", "SPH Health Mgmt - Faculty"),
    ("https://sph.umich.edu/ehs/faculty.html", "SPH Env Health - Faculty"),
    ("https://sph.umich.edu/nutr/faculty.html", "SPH Nutrition - Faculty"),

    # ── Ford School of Public Policy ──
    ("https://fordschool.umich.edu/faculty", "Ford School - Faculty"),
    ("https://fordschool.umich.edu/people/phd-students", "Ford School - PhD Students"),
    ("https://fordschool.umich.edu/phd/students", "Ford School - PhD Students v2"),

    # ── School of Information ──
    ("https://www.si.umich.edu/people/directory", "School of Information - Directory"),
    ("https://www.si.umich.edu/people/faculty", "School of Information - Faculty"),
    ("https://www.si.umich.edu/people/phd-students", "School of Information - PhD Students"),

    # ── Marsal School of Education ──
    ("https://marsal.umich.edu/people/faculty", "Marsal Education - Faculty"),
    ("https://marsal.umich.edu/directory", "Marsal Education - Directory"),

    # ── School of Social Work ──
    ("https://ssw.umich.edu/faculty-staff", "Social Work - Faculty/Staff"),
    ("https://ssw.umich.edu/offices/phd-program/current-students", "Social Work - PhD Students"),

    # ── Stamps School of Art & Design ──
    ("https://stamps.umich.edu/people/faculty", "Stamps Art & Design - Faculty"),
    ("https://stamps.umich.edu/faculty-staff", "Stamps Art & Design - Faculty/Staff"),

    # ── School of Music, Theatre & Dance ──
    ("https://smtd.umich.edu/about/directory/", "SMTD - Directory"),
    ("https://smtd.umich.edu/about/people/", "SMTD - People"),

    # ── College of Pharmacy ──
    ("https://pharmacy.umich.edu/directory", "Pharmacy - Directory"),
    ("https://pharmacy.umich.edu/people/faculty", "Pharmacy - Faculty"),

    # ── School of Dentistry ──
    ("https://dent.umich.edu/about-school/directory", "Dentistry - Directory"),
    ("https://dent.umich.edu/about-school/faculty-staff", "Dentistry - Faculty/Staff"),

    # ── School of Nursing ──
    ("https://nursing.umich.edu/faculty-staff/directory", "Nursing - Directory"),
    ("https://nursing.umich.edu/people/faculty", "Nursing - Faculty"),

    # ── SEAS (Environment & Sustainability) ──
    ("https://seas.umich.edu/research/faculty", "SEAS - Faculty"),
    ("https://seas.umich.edu/about/directory", "SEAS - Directory"),

    # ── College of Engineering ──
    ("https://cse.engin.umich.edu/people/", "CSE Engineering - People"),
    ("https://cse.engin.umich.edu/people/graduate-students/", "CSE Engineering - Grad Students"),
    ("https://eecs.engin.umich.edu/people/", "EECS - People"),
    ("https://me.engin.umich.edu/people/", "Mechanical Engineering - People"),
    ("https://aero.engin.umich.edu/people/", "Aerospace Engineering - People"),
    ("https://che.engin.umich.edu/people/", "Chemical Engineering - People"),
    ("https://cee.engin.umich.edu/people/", "Civil & Env Engineering - People"),
    ("https://ioe.engin.umich.edu/people/", "IOE - People"),
    ("https://bme.umich.edu/people/", "Biomedical Engineering - People"),
    ("https://robotics.umich.edu/people/", "Robotics Institute - People"),
    ("https://robotics.umich.edu/people/students/", "Robotics Institute - Students"),

    # ── LSA Departments ──
    ("https://lsa.umich.edu/econ/people/phd-students.html", "LSA Economics - PhD Students"),
    ("https://lsa.umich.edu/polisci/people/graduate-students.html", "LSA PoliSci - Grad Students"),
    ("https://lsa.umich.edu/soc/people/graduate-students.html", "LSA Sociology - Grad Students"),
    ("https://lsa.umich.edu/psych/people/graduate-students.html", "LSA Psychology - Grad Students"),
    ("https://lsa.umich.edu/history/people/graduate-students.html", "LSA History - Grad Students"),
    ("https://lsa.umich.edu/stats/people/phd-students.html", "LSA Statistics - PhD Students"),
    ("https://lsa.umich.edu/math/people/phd-students.html", "LSA Mathematics - PhD Students"),
    ("https://lsa.umich.edu/physics/people/graduate-students.html", "LSA Physics - Grad Students"),
    ("https://lsa.umich.edu/chem/people/graduate-students.html", "LSA Chemistry - Grad Students"),
    ("https://lsa.umich.edu/bio/people/graduate-students.html", "LSA Biology - Grad Students"),
    ("https://lsa.umich.edu/philosophy/people/graduate-students.html", "LSA Philosophy - Grad Students"),
    ("https://lsa.umich.edu/linguistics/people/graduate-students.html", "LSA Linguistics - Grad Students"),
    ("https://lsa.umich.edu/english/people/graduate-students.html", "LSA English - Grad Students"),
    ("https://lsa.umich.edu/comm/people/graduate-students.html", "LSA Communication Studies - Grad"),
]

RESEARCH_LABS = [
    ("https://ai.umich.edu/people/", "Michigan AI Lab - People"),
    ("https://robotics.umich.edu/people/students/", "Robotics - Students"),
    ("https://isr.umich.edu/people/", "ISR - People"),
    ("https://midas.umich.edu/faculty-members/", "MIDAS - Faculty"),
    ("https://energy.umich.edu/people/", "Energy Institute - People"),
    ("https://poverty.umich.edu/people/", "Poverty Solutions - People"),
    ("https://www.lsi.umich.edu/science/our-labs", "LSI - Labs"),
    ("https://www.lsi.umich.edu/people", "LSI - People"),
    ("https://lsa.umich.edu/cscs/people.html", "Complex Systems - People"),
    ("https://cps.isr.umich.edu/people/", "CPS - People"),
]

STUDENT_ORGS = [
    ("https://csg.umich.edu/", "CSG - Main"),
    ("https://csg.umich.edu/about/", "CSG - About"),
    ("https://csg.umich.edu/leadership/", "CSG - Leadership"),
    ("https://csg.umich.edu/officers/", "CSG - Officers"),
    ("https://csg.umich.edu/executive/", "CSG - Executive"),
    ("https://csg.umich.edu/team/", "CSG - Team"),
    ("https://www.michigandaily.com/staff/", "Michigan Daily - Staff"),
    ("https://www.michigandaily.com/about/", "Michigan Daily - About"),
    ("https://www.michigandaily.com/contact/", "Michigan Daily - Contact"),
    ("https://rackham.umich.edu/rackham-life/student-government/", "Rackham Student Gov"),
    ("https://maizepages.umich.edu/organizations", "MaizePages - Organizations"),
    ("https://studentlife.umich.edu/", "Student Life"),
    ("https://campusinvolvement.umich.edu/greek-life", "Campus Involvement - Greek"),
    ("https://engin.umich.edu/students/student-organizations/", "Engineering Student Orgs"),
    ("https://msa.umich.edu/", "MSA - Main"),
    ("https://msa.umich.edu/leadership/", "MSA - Leadership"),
]

ATHLETICS = [
    ("https://mgoblue.com/staff-directory", "Michigan Athletics - Staff Directory"),
    ("https://mgoblue.com/sports/football/coaches", "Michigan Football - Coaches"),
    ("https://mgoblue.com/sports/mens-basketball/coaches", "Michigan M. Basketball - Coaches"),
    ("https://mgoblue.com/sports/womens-basketball/coaches", "Michigan W. Basketball - Coaches"),
    ("https://mgoblue.com/sports/ice-hockey/coaches", "Michigan Hockey - Coaches"),
    ("https://mgoblue.com/sports/baseball/coaches", "Michigan Baseball - Coaches"),
    ("https://mgoblue.com/sports/softball/coaches", "Michigan Softball - Coaches"),
    ("https://mgoblue.com/sports/mens-soccer/coaches", "Michigan M. Soccer - Coaches"),
    ("https://mgoblue.com/sports/womens-soccer/coaches", "Michigan W. Soccer - Coaches"),
    ("https://mgoblue.com/sports/womens-volleyball/coaches", "Michigan Volleyball - Coaches"),
    ("https://mgoblue.com/sports/mens-swimming-and-diving/coaches", "Michigan M. Swimming - Coaches"),
    ("https://mgoblue.com/sports/womens-swimming-and-diving/coaches", "Michigan W. Swimming - Coaches"),
    ("https://mgoblue.com/sports/mens-tennis/coaches", "Michigan M. Tennis - Coaches"),
    ("https://mgoblue.com/sports/womens-tennis/coaches", "Michigan W. Tennis - Coaches"),
    ("https://mgoblue.com/sports/mens-track-and-field/coaches", "Michigan M. Track - Coaches"),
    ("https://mgoblue.com/sports/womens-track-and-field/coaches", "Michigan W. Track - Coaches"),
    ("https://mgoblue.com/sports/wrestling/coaches", "Michigan Wrestling - Coaches"),
    ("https://mgoblue.com/sports/mens-gymnastics/coaches", "Michigan M. Gymnastics - Coaches"),
    ("https://mgoblue.com/sports/womens-gymnastics/coaches", "Michigan W. Gymnastics - Coaches"),
    ("https://mgoblue.com/sports/mens-golf/coaches", "Michigan M. Golf - Coaches"),
    ("https://mgoblue.com/sports/womens-golf/coaches", "Michigan W. Golf - Coaches"),
    ("https://mgoblue.com/sports/rowing/coaches", "Michigan Rowing - Coaches"),
    ("https://mgoblue.com/sports/field-hockey/coaches", "Michigan Field Hockey - Coaches"),
    ("https://mgoblue.com/sports/womens-lacrosse/coaches", "Michigan W. Lacrosse - Coaches"),
]


def scrape_source_list(page, source_list, category_name):
    """Scrape a list of (url, department) tuples using Playwright."""
    print(f"\n{'═'*70}")
    print(f"  {category_name}")
    print(f"{'═'*70}")

    results = []
    seen = set()
    all_source_urls = {u for u, _ in source_list}
    profile_links_to_follow = {}

    for url, department in source_list:
        page_results = scrape_page_pw(page, url, department)
        raw_html = fetch_page_playwright(page, url, wait_ms=1000) if not page_results else None

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

        # Collect profile sub-links from the page we just fetched
        # We need to get the HTML from the already-loaded page
        try:
            current_html = page.content()
            sub_links = collect_sub_links(current_html, url, all_source_urls)
            for sl in sub_links:
                if sl not in profile_links_to_follow:
                    profile_links_to_follow[sl] = department
        except Exception:
            pass

        time.sleep(0.5)

    # Phase 2: Follow discovered sub-links (limited)
    if profile_links_to_follow:
        to_visit = sorted(profile_links_to_follow.items())[:100]
        print(f"\n    Following {len(to_visit)} profile sub-pages...")
        for sub_url, dept in to_visit:
            sub_results = scrape_page_pw(page, sub_url, dept)
            for r in sub_results:
                if r['email'] not in seen:
                    seen.add(r['email'])
                    results.append(r)
                    nm = r['name'][:25] if r['name'] else '—'
                    print(f"      + {nm:<28} {r['email']}")
            time.sleep(0.3)

    print(f"    ── {category_name} subtotal: {len(results)} unique emails ──")
    return results


def scrape_athletics(page):
    """Special handler for MGoBlue — also follow individual staff profile links."""
    print(f"\n{'═'*70}")
    print(f"  ATHLETICS - MGoBlue Staff & Coaches")
    print(f"{'═'*70}")

    results = []
    seen = set()

    for url, department in ATHLETICS:
        page_results = scrape_page_pw(page, url, department)
        new_count = 0
        for r in page_results:
            if r['email'] not in seen:
                seen.add(r['email'])
                results.append(r)
                new_count += 1

        if new_count > 0:
            print(f"    [{new_count:>3} new] {department}")
        else:
            print(f"    [  0    ] {department}")
        time.sleep(0.3)

    # Follow individual staff/coach profile links from main staff directory
    print(f"\n    Collecting staff profile links from staff directory...")
    html = fetch_page_playwright(page, "https://mgoblue.com/staff-directory", wait_ms=3000)
    if html:
        staff_links = set()
        for m in re.finditer(r'href=["\']([^"\']*(?:/staff-directory/|/coaches/)[^"\']*)["\']', html):
            full = urljoin("https://mgoblue.com", m.group(1))
            if full not in {u for u, _ in ATHLETICS}:
                staff_links.add(full)

        if staff_links:
            print(f"    Following {len(staff_links)} staff profile pages...")
            for staff_url in sorted(staff_links)[:80]:
                sub_results = scrape_page_pw(page, staff_url, "Michigan Athletics - Staff")
                for r in sub_results:
                    if r['email'] not in seen:
                        seen.add(r['email'])
                        results.append(r)
                        nm = r['name'][:25] if r['name'] else '—'
                        print(f"      + {nm:<28} {r['email']}")
                time.sleep(0.2)

    print(f"    ── Athletics subtotal: {len(results)} unique emails ──")
    return results


def main():
    print("╔════════════════════════════════════════════════════════════════╗")
    print("║  UNIVERSITY OF MICHIGAN EMAIL SCRAPER (Playwright)           ║")
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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/124.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
        )
        page = context.new_page()

        # ── Phase 1: Professional Schools ──
        pro_results = scrape_source_list(page, PROFESSIONAL_SCHOOLS, "PROFESSIONAL SCHOOLS & DEPARTMENTS")
        added = merge(pro_results)
        print(f"\n  -> Added {added} new emails (total: {len(all_contacts)})")

        # ── Phase 2: Research Labs ──
        lab_results = scrape_source_list(page, RESEARCH_LABS, "RESEARCH LABS & INSTITUTES")
        added = merge(lab_results)
        print(f"\n  -> Added {added} new emails (total: {len(all_contacts)})")

        # ── Phase 3: Student Organizations ──
        org_results = scrape_source_list(page, STUDENT_ORGS, "STUDENT ORGANIZATIONS")
        added = merge(org_results)
        print(f"\n  -> Added {added} new emails (total: {len(all_contacts)})")

        # ── Phase 4: Athletics ──
        ath_results = scrape_athletics(page)
        added = merge(ath_results)
        print(f"\n  -> Added {added} new emails (total: {len(all_contacts)})")

        browser.close()

    # ── Final cleanup & save ──
    print(f"\n{'═'*70}")
    print("  FINAL RESULTS")
    print(f"{'═'*70}")

    all_contacts.sort(key=lambda x: (x['department'], x['email']))

    csv_path = '/Users/jaiashar/Documents/VoraBusinessFinder/michigan_pro_emails.csv'
    fieldnames = ['email', 'name', 'department', 'source_url']
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_contacts)

    json_path = '/Users/jaiashar/Documents/VoraBusinessFinder/michigan_pro_emails.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(all_contacts, f, indent=2, ensure_ascii=False)

    print(f"\n  TOTAL UNIQUE EMAILS: {len(all_contacts)}")
    dept_counts = {}
    for c in all_contacts:
        d = c['department']
        dept_counts[d] = dept_counts.get(d, 0) + 1

    print(f"\n  By department/source:")
    for d, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        print(f"    {d:<55} {count}")

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
    print(f"  COMPLETE. {len(all_contacts)} unique @umich.edu emails.")
    print(f"  CSV:  {csv_path}")
    print(f"  JSON: {json_path}")
    print(f"{'═'*70}")

    return all_contacts


if __name__ == "__main__":
    main()
