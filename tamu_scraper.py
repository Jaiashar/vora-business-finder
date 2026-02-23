#!/usr/bin/env python3
"""
Texas A&M University (TAMU) Email Scraper - V2
===============================================
Scrapes @tamu.edu and @*.tamu.edu emails from:
  - Arts and Sciences graduate student directories (JSON API)
  - Engineering department directories (JSON API)
  - Professional schools (Mays, Law, Education, Bush School, SPH, Vet Med)
  - Athletics staff directory (12thman.com)
  - Student organizations (Student Government, The Battalion)
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import json
import time
from urllib.parse import urljoin


def log(msg):
    print(msg, flush=True)


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/tamu_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/tamu_dept_emails.json'


# ============================================================
# EMAIL UTILITIES
# ============================================================

def decode_cf_email(encoded_string):
    try:
        r = int(encoded_string[:2], 16)
        email = ''
        for i in range(2, len(encoded_string), 2):
            email += chr(int(encoded_string[i:i+2], 16) ^ r)
        return email
    except Exception:
        return None


def extract_tamu_emails(text):
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*tamu\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip().rstrip('.')
        m = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*tamu\.edu)', e)
        if m:
            cleaned.add(m.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    emails = []
    for a in soup.find_all('a', href=True):
        href = a.get('href', '')
        if 'mailto:' in href.lower():
            m = re.search(r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*tamu\.edu)', href, re.IGNORECASE)
            if m:
                emails.append(m.group(1).lower().strip())
    for el in soup.find_all('a', class_='__cf_email__'):
        enc = el.get('data-cfemail', '')
        if enc:
            dec = decode_cf_email(enc)
            if dec and 'tamu.edu' in dec.lower():
                emails.append(dec.lower().strip())
    for el in soup.find_all(attrs={'data-cfemail': True}):
        enc = el.get('data-cfemail', '')
        if enc:
            dec = decode_cf_email(enc)
            if dec and 'tamu.edu' in dec.lower():
                emails.append(dec.lower().strip())
    for a in soup.find_all('a', href=True):
        href = a.get('href', '')
        m = re.search(r'/cdn-cgi/l/email-protection#([0-9a-fA-F]+)', href)
        if m:
            dec = decode_cf_email(m.group(1))
            if dec and 'tamu.edu' in dec.lower():
                emails.append(dec.lower().strip())
    return list(set(emails))


ADMIN_PREFIXES = [
    'info@', 'admin@', 'office@', 'dept@', 'webmaster@', 'help@',
    'support@', 'contact@', 'registrar@', 'grad@', 'gradoffice@',
    'department@', 'chair@', 'advising@', 'undergrad@', 'dean@',
    'reception@', 'main@', 'general@', 'staff@', 'gradadmit@',
    'calendar@', 'events@', 'news@', 'newsletter@', 'web@',
    'marketing@', 'media@', 'communications@', 'hr@', 'hiring@',
    'jobs@', 'career@', 'alumni@', 'development@', 'giving@',
    'feedback@', 'safety@', 'security@', 'facilities@', 'it@',
    'tech@', 'helpdesk@', 'library@', 'gradapp@', 'apply@',
    'admissions@', 'enrollment@', 'records@', 'bursar@',
    'finaid@', 'housing@', 'dining@', 'parking@', 'police@',
    'noreply@', 'do-not-reply@', 'donotreply@', 'postmaster@',
    'anthonytravel@', 'travel@', 'compliance@',
    'studentaffairs@', 'provost@', 'president@',
    'gradschool@', 'ogs@', 'ogaps@', 'sociadvising@',
    'polsconnect@', 'bushschoolcgs@',
]


def is_admin_email(email):
    return any(email.lower().startswith(p) for p in ADMIN_PREFIXES)


def get_soup(url, session):
    try:
        resp = session.get(url, headers=HEADERS, timeout=25, allow_redirects=True)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, 'html.parser'), resp.url
        return None, None
    except Exception:
        return None, None


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


def try_get_name_for_email(soup, email):
    for a in soup.find_all('a', href=True):
        if email in a.get('href', '').lower():
            parent = a.parent
            for _ in range(6):
                if parent is None:
                    break
                for tag in parent.find_all(['h2','h3','h4','h5','strong','b','a','span']):
                    txt = tag.get_text(strip=True)
                    if txt and '@' not in txt and 2 < len(txt) < 80:
                        skip = ['email','contact','phone','http','department',
                                'graduate','student','people','faculty','office',
                                'read more','view profile','website','lab',
                                'full bio','biography']
                        if not any(x in txt.lower() for x in skip):
                            return txt
                parent = parent.parent
    elems = soup.find_all(string=re.compile(re.escape(email), re.IGNORECASE))
    for elem in elems:
        parent = elem.parent
        for _ in range(6):
            if parent is None:
                break
            for tag in parent.find_all(['h2','h3','h4','h5','strong','b','a','span']):
                name = tag.get_text(strip=True)
                if name and '@' not in name and 2 < len(name) < 80:
                    skip = ['email','contact','@','student','people','phone',
                            'read more','department','faculty','office','http',
                            'website','full bio']
                    if not any(x in name.lower() for x in skip):
                        return name
            parent = parent.parent
    return derive_name_from_email(email)


# ============================================================
# PHASE 1: ARTS AND SCIENCES (JSON API)
# ============================================================

ARTSCI_JSON = [
    {"department": "Economics (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/economics/contact/profiles/dept-profile-data.json"},
    {"department": "Sociology (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/sociology/people/profiles/dept-profile-data.json"},
    {"department": "Psychology and Brain Sciences (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/psychological-brain-sciences/contact/profiles/dept-profile-data.json"},
    {"department": "History (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/history/contact/profiles/dept-profile-data.json"},
    {"department": "English (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/english/contact/profiles/dept-profile-data.json"},
    {"department": "Philosophy and Humanities (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/philosophy/contact/profiles/dept-profile-data.json"},
    {"department": "Mathematics (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/mathematics/contact/profiles/dept-profile-data.json"},
    {"department": "Statistics (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/statistics/contact/profiles/dept-profile-data.json"},
    {"department": "Physics and Astronomy (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/physics-astronomy/contact/profiles/dept-profile-data.json"},
    {"department": "Chemistry (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/chemistry/contact/profiles/dept-profile-data.json"},
    {"department": "Geology and Geophysics (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/geology-geophysics/contact/profiles/dept-profile-data.json"},
    {"department": "Biology (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/biology/contact/profiles/dept-profile-data.json"},
    {"department": "Anthropology (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/anthropology/contact/profiles/dept-profile-data.json"},
    {"department": "Geography (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/geography/contact/profiles/dept-profile-data.json"},
    {"department": "Communication and Journalism (Arts and Sciences)",
     "json_url": "https://artsci.tamu.edu/comm-journalism/contact/profiles/dept-profile-data.json"},
]

ARTSCI_HTML = [
    {"department": "Economics Grad Students (Arts and Sciences)",
     "urls": ["https://economics.tamu.edu/people/graduate-students/"]},
    {"department": "Political Science Grad Students (Arts and Sciences)",
     "urls": ["https://pols.tamu.edu/people/graduate-students/"]},
    {"department": "Sociology Grad Students (Arts and Sciences)",
     "urls": ["https://sociology.tamu.edu/people/graduate-students/"]},
    {"department": "Psychology Grad Students (Arts and Sciences)",
     "urls": ["https://psychology.tamu.edu/people/graduate-students/"]},
    {"department": "History Grad Students (Arts and Sciences)",
     "urls": ["https://history.tamu.edu/people/graduate-students/"]},
    {"department": "English Grad Students (Arts and Sciences)",
     "urls": ["https://english.tamu.edu/people/graduate-students/"]},
    {"department": "Philosophy Grad Students (Arts and Sciences)",
     "urls": ["https://philosophy.tamu.edu/people/graduate-students/"]},
    {"department": "Mathematics Grad Directory (Arts and Sciences)",
     "urls": ["https://www.math.tamu.edu/directory/graduate.html",
              "https://math.tamu.edu/people/graduate-students/"]},
    {"department": "Statistics Grad Students (Arts and Sciences)",
     "urls": ["https://stat.tamu.edu/people/graduate-students/"]},
    {"department": "Physics Grad Students (Arts and Sciences)",
     "urls": ["https://physics.tamu.edu/dir-search/?search_field=job_category&value=student",
              "https://physics.tamu.edu/dir-search/",
              "https://physics.tamu.edu/people/graduate-students/"]},
    {"department": "Chemistry Grad Students (Arts and Sciences)",
     "urls": ["https://chemistry.tamu.edu/people/graduate-students/"]},
    {"department": "Geology Grad Students (Arts and Sciences)",
     "urls": ["https://geology.tamu.edu/people/graduate-students/"]},
    {"department": "Biology Grad Students (Arts and Sciences)",
     "urls": ["https://www.bio.tamu.edu/full-directory/graduate-student-directory/",
              "https://biology.tamu.edu/people/graduate-students/"]},
    {"department": "Anthropology Grad Students (Arts and Sciences)",
     "urls": ["https://anthropology.tamu.edu/people/graduate-students/"]},
    {"department": "Geography Grad Students (Arts and Sciences)",
     "urls": ["https://geography.tamu.edu/people/graduate-students/"]},
    {"department": "Communication Grad Students (Arts and Sciences)",
     "urls": ["https://comm.tamu.edu/people/graduate-students/"]},
    {"department": "Biochemistry and Biophysics (Arts and Sciences)",
     "urls": ["https://bcbp.tamu.edu/types/graduate-students/"]},
    {"department": "Ecology and Conservation Biology (Arts and Sciences)",
     "urls": ["https://eeb.tamu.edu/people/student-roster/"]},
    {"department": "Political Science (Bush School)",
     "urls": ["https://bush.tamu.edu/pols/people/"]},
]


def scrape_artsci_json(config, session):
    department = config['department']
    json_url = config['json_url']
    results = []
    seen = set()

    log(f"\n{'='*60}")
    log(f"Department: {department}")
    log(f"{'='*60}")
    log(f"  JSON: {json_url}")

    try:
        resp = session.get(json_url, headers=HEADERS, timeout=25)
        if resp.status_code != 200:
            log(f"    HTTP {resp.status_code}")
            return results
        data = resp.json()
        log(f"    -> {len(data)} records in JSON")
    except Exception as e:
        log(f"    Error: {e}")
        return results

    for person in data:
        email = person.get('email', '').lower().strip()
        if not email or 'tamu.edu' not in email or is_admin_email(email) or email in seen:
            continue
        seen.add(email)
        name = person.get('name', '').strip()
        link = person.get('link', '')
        source = urljoin(json_url, link) if link else json_url
        results.append({'email': email, 'name': name,
                        'department': department, 'source_url': source})

    log(f"    -> {len(results)} TAMU emails extracted")
    return results


# ============================================================
# PHASE 2: ENGINEERING (JSON API)
# ============================================================

ENG_JSON_URL = "https://engineering.tamu.edu/profile-data.json"

ENG_TAG_MAP = {
    'csce': 'Computer Science and Engineering (Engineering)',
    'electrical': 'Electrical and Computer Engineering (Engineering)',
    'mechanical': 'Mechanical Engineering (Engineering)',
    'civil': 'Civil and Environmental Engineering (Engineering)',
    'biomedical': 'Biomedical Engineering (Engineering)',
    'chemical': 'Chemical Engineering (Engineering)',
    'aerospace': 'Aerospace Engineering (Engineering)',
    'petroleum': 'Petroleum Engineering (Engineering)',
    'nuclear': 'Nuclear Engineering (Engineering)',
    'ocean': 'Ocean Engineering (Engineering)',
    'industrial': 'Industrial and Systems Engineering (Engineering)',
    'materials': 'Materials Science and Engineering (Engineering)',
    'etid': 'Engineering Technology (Engineering)',
    'mtde': 'Multidisciplinary Engineering (Engineering)',
}


def scrape_engineering_json(session):
    results = []
    seen = set()

    log(f"\n{'='*60}")
    log(f"ENGINEERING (Single JSON API)")
    log(f"{'='*60}")
    log(f"  JSON: {ENG_JSON_URL}")

    try:
        resp = session.get(ENG_JSON_URL, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            log(f"    HTTP {resp.status_code}")
            return results
        data = resp.json()
        log(f"    -> {len(data)} total records")
    except Exception as e:
        log(f"    Error: {e}")
        return results

    dept_counts = {}
    for person in data:
        email = person.get('email', '').lower().strip()
        if not email or 'tamu.edu' not in email or is_admin_email(email) or email in seen:
            continue
        seen.add(email)
        tags = [t.lower() for t in person.get('tag', [])]
        name = person.get('name', '').strip()
        dept = None
        for tkey, dname in ENG_TAG_MAP.items():
            if tkey in tags:
                dept = dname
                break
        if not dept:
            dept = 'Engineering (General)'
        link = person.get('link', '')
        source = urljoin(ENG_JSON_URL, link) if link else ENG_JSON_URL
        results.append({'email': email, 'name': name,
                        'department': dept, 'source_url': source})
        dept_counts[dept] = dept_counts.get(dept, 0) + 1

    log(f"    -> {len(results)} unique TAMU emails")
    for d, c in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"       {d}: {c}")
    return results


# ============================================================
# PHASE 3: PROFESSIONAL SCHOOLS (HTML)
# ============================================================

PROFESSIONAL = [
    {"department": "Mays Business School",
     "urls": [f"https://mays.tamu.edu/directory/?drole=&ddept=&page={p}" for p in range(1, 30)]
             + ["https://mays.tamu.edu/phd/"]},
    {"department": "School of Law",
     "urls": ["https://law.tamu.edu/", "https://law.tamu.edu/faculty-staff",
              "https://law.tamu.edu/faculty-staff/find-people",
              "https://law.tamu.edu/student-life",
              "https://law.tamu.edu/student-life/student-organizations"]},
    {"department": "School of Education and Human Development",
     "urls": [f"https://directory.education.tamu.edu/?page={p}&number_per_page=50&d=&et="
              for p in range(1, 15)]
             + ["https://education.tamu.edu/people/students"]},
    {"department": "Bush School of Government and Public Service",
     "urls": ["https://bush.tamu.edu/faculty/", "https://bush.tamu.edu/people/",
              "https://bush.tamu.edu/about/", "https://bush.tamu.edu/pols/people/",
              "https://bush.tamu.edu/people/students"]},
    {"department": "School of Public Health",
     "urls": ["https://sph.tamu.edu/", "https://sph.tamu.edu/about/index.html",
              "https://publichealth.tamu.edu/people/students",
              "https://publichealth.tamu.edu/"]},
    {"department": "College of Veterinary Medicine",
     "urls": ["https://vetmed.tamu.edu/", "https://vetmed.tamu.edu/directory/",
              "https://vet.tamu.edu/"]},
]


def scrape_html_dept(config, session):
    department = config['department']
    urls = config['urls']
    results = []
    seen = set()

    log(f"\n{'='*60}")
    log(f"Department: {department}")
    log(f"{'='*60}")

    for url in urls:
        soup, final_url = get_soup(url, session)
        if soup is None:
            continue
        surl = final_url or url
        text = soup.get_text(separator=' ', strip=True)
        text_emails = extract_tamu_emails(text)
        mailto_emails = extract_mailto_emails(soup)
        for script in soup.find_all('script'):
            if script.string:
                text_emails.extend(extract_tamu_emails(script.string))
        all_emails = list(set(text_emails + mailto_emails))
        new_ct = 0
        for email in all_emails:
            if email in seen or is_admin_email(email):
                continue
            seen.add(email)
            name = try_get_name_for_email(soup, email)
            if not name:
                name = derive_name_from_email(email)
            results.append({'email': email, 'name': name,
                            'department': department, 'source_url': surl})
            new_ct += 1
        if new_ct > 0:
            log(f"  {surl}: +{new_ct} emails")
        time.sleep(0.5)

    log(f"  TOTAL for {department}: {len(results)} emails")
    return results


# ============================================================
# PHASE 4: ATHLETICS
# ============================================================

def scrape_athletics(session):
    results = []
    seen = set()
    department = "Aggies Athletics (Staff)"

    log(f"\n{'='*60}")
    log(f"Department: {department}")
    log(f"{'='*60}")

    url = "https://12thman.com/staff-directory"
    log(f"  Trying: {url}")
    soup, final_url = get_soup(url, session)
    if soup is None:
        return results

    for a in soup.find_all('a', href=True):
        href = a.get('href', '')
        if 'mailto:' in href.lower():
            m = re.search(r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*tamu\.edu)', href, re.IGNORECASE)
            if m:
                email = m.group(1).lower().strip()
                if email in seen or is_admin_email(email):
                    continue
                name = ""
                parent = a.parent
                for _ in range(6):
                    if parent is None:
                        break
                    for tag in parent.find_all(['h4','h3','h2','strong','b','a']):
                        txt = tag.get_text(strip=True)
                        tag_href = tag.get('href', '') if tag.name == 'a' else ''
                        if 'mailto:' in tag_href:
                            continue
                        if txt and '@' not in txt and 2 < len(txt) < 80:
                            skip = ['email','phone','contact','http','full bio',
                                    'staff directory','administration']
                            if not any(x in txt.lower() for x in skip):
                                name = txt
                                break
                    if name:
                        break
                    parent = parent.parent
                seen.add(email)
                results.append({'email': email, 'name': name,
                                'department': department, 'source_url': final_url or url})

    text = soup.get_text(separator=' ', strip=True)
    for email in extract_tamu_emails(text):
        if email not in seen and not is_admin_email(email):
            seen.add(email)
            name = try_get_name_for_email(soup, email)
            results.append({'email': email, 'name': name,
                            'department': department, 'source_url': final_url or url})

    staff_links = set()
    for a in soup.find_all('a', href=True):
        href = a.get('href', '')
        if '/staff-directory/' in href and href not in ('/staff-directory', '/staff-directory/'):
            full = urljoin(url, href)
            if 'staff-directory/' in full:
                staff_links.add(full)

    log(f"    -> {len(staff_links)} staff profile links")
    visited = 0
    for surl in sorted(staff_links):
        if visited >= 100:
            break
        ssoup, sfinal = get_soup(surl, session)
        if ssoup:
            stxt = ssoup.get_text(separator=' ', strip=True)
            se = extract_tamu_emails(stxt)
            sm = extract_mailto_emails(ssoup)
            for email in list(set(se + sm)):
                if email not in seen and not is_admin_email(email):
                    seen.add(email)
                    name = try_get_name_for_email(ssoup, email)
                    results.append({'email': email, 'name': name,
                                    'department': department, 'source_url': sfinal or surl})
        visited += 1
        time.sleep(0.3)

    log(f"  TOTAL for {department}: {len(results)} emails")
    return results


# ============================================================
# PHASE 5: STUDENT ORGS
# ============================================================

STUDENT_ORGS = [
    {"department": "Student Government Association",
     "urls": ["https://sga.tamu.edu/", "https://studentactivities.tamu.edu/",
              "https://stuact.tamu.edu/"]},
    {"department": "The Battalion (Student Newspaper)",
     "urls": ["https://www.thebatt.com/", "https://www.thebatt.com/staff/",
              "https://www.thebatt.com/about/", "https://www.thebatt.com/contact/",
              "https://thebatt.com/", "https://thebatt.com/staff/"]},
    {"department": "Graduate and Professional Student Government",
     "urls": ["https://gpsg.tamu.edu/", "https://gpsg.tamu.edu/about/",
              "https://gpsg.tamu.edu/officers/", "https://gpsg.tamu.edu/leadership/"]},
]


# ============================================================
# MAIN
# ============================================================

def main():
    session = requests.Session()
    all_results = []
    global_seen = set()

    def add_results(dept_results):
        ct = 0
        for r in dept_results:
            email = r['email'].lower().strip()
            if email and email not in global_seen:
                global_seen.add(email)
                all_results.append(r)
                ct += 1
        return ct

    log("=" * 70)
    log("TEXAS A&M UNIVERSITY (TAMU) EMAIL SCRAPER - V2")
    log("Domains: @tamu.edu, @*.tamu.edu")
    log("=" * 70)

    # Phase 1: Arts and Sciences (JSON)
    log("\n\n" + "=" * 70)
    log("PHASE 1: COLLEGE OF ARTS AND SCIENCES (JSON API)")
    log("=" * 70)
    for cfg in ARTSCI_JSON:
        try:
            res = scrape_artsci_json(cfg, session)
            n = add_results(res)
            log(f"  => {n} new unique emails (total: {len(all_results)})")
            time.sleep(0.5)
        except Exception as e:
            log(f"  ERROR {cfg['department']}: {e}")

    # Phase 1b: Arts and Sciences HTML fallbacks
    log("\n\n" + "=" * 70)
    log("PHASE 1b: ARTS AND SCIENCES (HTML Fallbacks)")
    log("=" * 70)
    for cfg in ARTSCI_HTML:
        try:
            res = scrape_html_dept(cfg, session)
            n = add_results(res)
            log(f"  => {n} new unique emails (total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR {cfg['department']}: {e}")

    # Phase 2: Engineering (JSON)
    log("\n\n" + "=" * 70)
    log("PHASE 2: COLLEGE OF ENGINEERING (JSON API)")
    log("=" * 70)
    try:
        res = scrape_engineering_json(session)
        n = add_results(res)
        log(f"  => {n} new unique emails (total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR engineering: {e}")

    # Phase 2b: Engineering HTML fallbacks
    log("\n\n" + "=" * 70)
    log("PHASE 2b: ENGINEERING (HTML Grad Student Pages)")
    log("=" * 70)
    ENG_HTML = [
        {"department": "Computer Science Grad Students (Engineering)",
         "urls": ["https://cse.tamu.edu/people/graduate-students/"]},
        {"department": "ECE Grad Students (Engineering)",
         "urls": ["https://ece.tamu.edu/people/graduate-students/"]},
        {"department": "Mechanical Engineering Grad Students (Engineering)",
         "urls": ["https://me.tamu.edu/people/graduate-students/"]},
        {"department": "Civil Engineering Grad Students (Engineering)",
         "urls": ["https://ce.tamu.edu/people/graduate-students/"]},
        {"department": "Biomedical Engineering Grad Students (Engineering)",
         "urls": ["https://bme.tamu.edu/people/graduate-students/"]},
        {"department": "Chemical Engineering Grad Students (Engineering)",
         "urls": ["https://che.tamu.edu/people/graduate-students/"]},
        {"department": "Aerospace Engineering Grad Students (Engineering)",
         "urls": ["https://aero.tamu.edu/people/graduate-students/"]},
        {"department": "Petroleum Engineering Grad Students (Engineering)",
         "urls": ["https://pete.tamu.edu/people/graduate-students/"]},
        {"department": "Nuclear Engineering Grad Students (Engineering)",
         "urls": ["https://nuen.tamu.edu/people/graduate-students/"]},
        {"department": "Ocean Engineering Grad Students (Engineering)",
         "urls": ["https://ocean.tamu.edu/people/graduate-students/"]},
    ]
    for cfg in ENG_HTML:
        try:
            res = scrape_html_dept(cfg, session)
            n = add_results(res)
            log(f"  => {n} new unique emails (total: {len(all_results)})")
            time.sleep(0.5)
        except Exception as e:
            log(f"  ERROR {cfg['department']}: {e}")

    # Phase 3: Professional Schools
    log("\n\n" + "=" * 70)
    log("PHASE 3: PROFESSIONAL SCHOOLS")
    log("=" * 70)
    for cfg in PROFESSIONAL:
        try:
            res = scrape_html_dept(cfg, session)
            n = add_results(res)
            log(f"  => {n} new unique emails (total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR {cfg['department']}: {e}")

    # Phase 4: Athletics
    log("\n\n" + "=" * 70)
    log("PHASE 4: ATHLETICS (12thMan.com)")
    log("=" * 70)
    try:
        res = scrape_athletics(session)
        n = add_results(res)
        log(f"  => {n} new unique emails (total: {len(all_results)})")
    except Exception as e:
        log(f"  ERROR athletics: {e}")

    # Phase 5: Student Orgs
    log("\n\n" + "=" * 70)
    log("PHASE 5: STUDENT ORGANIZATIONS")
    log("=" * 70)
    for cfg in STUDENT_ORGS:
        try:
            res = scrape_html_dept(cfg, session)
            n = add_results(res)
            log(f"  => {n} new unique emails (total: {len(all_results)})")
            time.sleep(1)
        except Exception as e:
            log(f"  ERROR {cfg['department']}: {e}")

    # Save results
    log(f"\n\n{'='*70}")
    log(f"RESULTS SUMMARY")
    log(f"{'='*70}")
    log(f"Total unique TAMU emails: {len(all_results)}")

    tamu_main = [c for c in all_results if c['email'].endswith('@tamu.edu')]
    tamu_sub = [c for c in all_results
                if 'tamu.edu' in c['email'] and not c['email'].endswith('@tamu.edu')]
    log(f"  @tamu.edu: {len(tamu_main)}")
    if tamu_sub:
        log(f"  @*.tamu.edu (subdomains): {len(tamu_sub)}")

    # CSV with proper quoting to handle commas in names
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'],
                                quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for r in sorted(all_results, key=lambda x: (x['department'], x['email'])):
            writer.writerow(r)
    log(f"\nSaved to {OUTPUT_CSV}")

    with open(OUTPUT_JSON, 'w') as f:
        json.dump(all_results, f, indent=2)
    log(f"Saved to {OUTPUT_JSON}")

    # Department summary
    log(f"\n{'='*70}")
    log("SUMMARY BY DEPARTMENT:")
    log(f"{'='*70}")
    dept_counts = {}
    for r in all_results:
        d = r['department']
        dept_counts[d] = dept_counts.get(d, 0) + 1
    for d, c in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"  {d}: {c} emails")

    log(f"\n{'='*70}")
    log(f"ALL {len(all_results)} EMAILS")
    log(f"{'='*70}")
    by_dept = {}
    for c in all_results:
        d = c['department']
        if d not in by_dept:
            by_dept[d] = []
        by_dept[d].append(c)
    for dept in sorted(by_dept.keys()):
        contacts = by_dept[dept]
        log(f"\n  [{dept}] ({len(contacts)} contacts)")
        for c in contacts:
            nm = c['name'][:35] if c['name'] else '-'
            log(f"    {nm:<37} {c['email']}")

    log(f"\n{'='*70}")
    log(f"COMPLETE. {len(all_results)} unique TAMU emails scraped.")
    log(f"{'='*70}")
    return all_results


if __name__ == '__main__':
    main()
