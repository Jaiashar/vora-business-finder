#!/usr/bin/env python3
"""
University of Iowa Email Scraper v2
Scrapes @uiowa.edu emails from graduate student directories, professional schools,
athletics, and student organizations. Merges with existing iowa_dept_emails.csv.
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
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

OUTPUT_CSV = '/Users/jaiashar/Documents/VoraBusinessFinder/iowa_dept_emails.csv'
OUTPUT_JSON = '/Users/jaiashar/Documents/VoraBusinessFinder/iowa_dept_emails.json'

CLAS_DEPARTMENTS = [
    {"department": "Economics", "urls": [
        "https://economics.uiowa.edu/people/graduate-students",
        "https://economics.uiowa.edu/people/students",
        "https://economics.uiowa.edu/people",
        "https://tippie.uiowa.edu/people/economics",
    ]},
    {"department": "Political Science", "urls": [
        "https://polisci.uiowa.edu/people/graduate-students",
        "https://polisci.uiowa.edu/people",
        "https://politicalscience.uiowa.edu/people",
        "https://politicalscience.uiowa.edu/people/graduate-students",
    ]},
    {"department": "Sociology", "urls": [
        "https://sociology.uiowa.edu/people/graduate-students",
        "https://sociology.uiowa.edu/people/students",
        "https://sociology.uiowa.edu/people",
    ]},
    {"department": "Psychology", "urls": [
        "https://psychology.uiowa.edu/people/graduate-students",
        "https://psychology.uiowa.edu/people/students",
        "https://psychology.uiowa.edu/people",
    ]},
    {"department": "History", "urls": [
        "https://history.uiowa.edu/people/graduate-students",
        "https://history.uiowa.edu/people/students",
        "https://history.uiowa.edu/people",
    ]},
    {"department": "English", "urls": [
        "https://english.uiowa.edu/people/graduate-students",
        "https://english.uiowa.edu/people/students",
        "https://english.uiowa.edu/people",
    ]},
    {"department": "Philosophy", "urls": [
        "https://philosophy.uiowa.edu/people/graduate-students",
        "https://philosophy.uiowa.edu/people/students",
        "https://philosophy.uiowa.edu/people",
    ]},
    {"department": "Linguistics", "urls": [
        "https://linguistics.uiowa.edu/people/graduate-students",
        "https://linguistics.uiowa.edu/people",
        "https://lllc.uiowa.edu/people",
        "https://lllc.uiowa.edu/people/graduate-students",
    ]},
    {"department": "Mathematics", "urls": [
        "https://math.uiowa.edu/people/graduate-students",
        "https://math.uiowa.edu/people/students",
        "https://math.uiowa.edu/people",
    ]},
    {"department": "Statistics & Actuarial Science", "urls": [
        "https://stat.uiowa.edu/people/graduate-students",
        "https://stat.uiowa.edu/people/students",
        "https://stat.uiowa.edu/people",
    ]},
    {"department": "Physics & Astronomy", "urls": [
        "https://physics.uiowa.edu/people/graduate-students",
        "https://physics.uiowa.edu/people/students",
        "https://physics.uiowa.edu/people",
    ]},
    {"department": "Chemistry", "urls": [
        "https://chem.uiowa.edu/people/graduate-students",
        "https://chem.uiowa.edu/people/students",
        "https://chem.uiowa.edu/people",
    ]},
    {"department": "Earth & Environmental Sciences", "urls": [
        "https://sees.uiowa.edu/people/graduate-students",
        "https://sees.uiowa.edu/people",
        "https://geoscience.uiowa.edu/people/graduate-students",
        "https://geoscience.uiowa.edu/people",
        "https://ees.uiowa.edu/people/graduate-students",
        "https://ees.uiowa.edu/people",
    ]},
    {"department": "Biology", "urls": [
        "https://biology.uiowa.edu/people/graduate-students",
        "https://biology.uiowa.edu/people/students",
        "https://biology.uiowa.edu/people",
    ]},
    {"department": "Anthropology", "urls": [
        "https://anthropology.uiowa.edu/people/graduate-students",
        "https://anthropology.uiowa.edu/people/students",
        "https://anthropology.uiowa.edu/people",
    ]},
    {"department": "Geography & Sustainability Sciences", "urls": [
        "https://sees.uiowa.edu/people",
        "https://geography.uiowa.edu/people/graduate-students",
        "https://geography.uiowa.edu/people",
    ]},
    {"department": "Journalism & Mass Communication", "urls": [
        "https://journalism.uiowa.edu/people/graduate-students",
        "https://journalism.uiowa.edu/people/students",
        "https://journalism.uiowa.edu/people",
    ]},
    {"department": "Communication Studies", "urls": [
        "https://communication-studies.uiowa.edu/people/graduate-students",
        "https://communication-studies.uiowa.edu/people",
        "https://communicationstudies.uiowa.edu/people",
        "https://communicationstudies.uiowa.edu/people/graduate-students",
    ]},
    {"department": "Music", "urls": [
        "https://music.uiowa.edu/people/graduate-students",
        "https://music.uiowa.edu/people/students",
        "https://music.uiowa.edu/people",
    ]},
]

ENGINEERING = [
    {"department": "Computer Science", "urls": [
        "https://cs.uiowa.edu/people/graduate-students",
        "https://cs.uiowa.edu/people/students",
        "https://cs.uiowa.edu/people",
    ]},
    {"department": "Electrical & Computer Engineering", "urls": [
        "https://ece.uiowa.edu/people/graduate-students",
        "https://ece.uiowa.edu/people",
        "https://engineering.uiowa.edu/ece-people",
    ]},
    {"department": "Mechanical Engineering", "urls": [
        "https://me.uiowa.edu/people/graduate-students",
        "https://me.uiowa.edu/people",
        "https://engineering.uiowa.edu/me-people",
    ]},
    {"department": "Civil & Environmental Engineering", "urls": [
        "https://cee.uiowa.edu/people/graduate-students",
        "https://cee.uiowa.edu/people",
        "https://engineering.uiowa.edu/cee/cee-people",
    ]},
    {"department": "Biomedical Engineering", "urls": [
        "https://bme.uiowa.edu/people/graduate-students",
        "https://bme.uiowa.edu/people",
        "https://engineering.uiowa.edu/bme/people/bme-graduate-students",
        "https://engineering.uiowa.edu/bme/people",
    ]},
    {"department": "Chemical & Biochemical Engineering", "urls": [
        "https://che.uiowa.edu/people/graduate-students",
        "https://che.uiowa.edu/people",
        "https://engineering.uiowa.edu/cbe/people",
    ]},
]

PROFESSIONAL = [
    {"department": "Tippie College of Business (PhD)", "urls": [
        "https://tippie.uiowa.edu/phd",
        "https://tippie.uiowa.edu/phd/students",
        "https://tippie.uiowa.edu/phd/current-students",
        "https://tippie.uiowa.edu/people/students",
        "https://tippie.uiowa.edu/people/graduate-students",
        "https://tippie.uiowa.edu/people/phd-students",
        "https://tippie.uiowa.edu/accounting/phd",
        "https://tippie.uiowa.edu/finance/phd",
        "https://tippie.uiowa.edu/management-organizations/phd",
        "https://tippie.uiowa.edu/marketing/phd",
        "https://tippie.uiowa.edu/management-sciences/phd",
        "https://tippie.uiowa.edu/business-analytics-information-systems/phd",
    ]},
    {"department": "College of Law", "urls": [
        "https://law.uiowa.edu/student-organizations",
        "https://law.uiowa.edu/students",
        "https://law.uiowa.edu/about/people",
        "https://law.uiowa.edu/people",
        "https://law.uiowa.edu/student-life/student-organizations",
        "https://law.uiowa.edu/",
        "https://law.uiowa.edu/directory",
    ]},
    {"department": "College of Education", "urls": [
        "https://education.uiowa.edu/people/students",
        "https://education.uiowa.edu/people/graduate-students",
        "https://education.uiowa.edu/people",
        "https://education.uiowa.edu/directory",
        "https://education.uiowa.edu/directory?cohort=519554",
    ]},
    {"department": "College of Public Health", "urls": [
        "https://www.public-health.uiowa.edu/people/",
        "https://www.public-health.uiowa.edu/people/students/",
        "https://www.public-health.uiowa.edu/directory/",
        "https://www.public-health.uiowa.edu/",
        "https://cph.uiowa.edu/people",
    ]},
    {"department": "College of Pharmacy", "urls": [
        "https://pharmacy.uiowa.edu/people/students",
        "https://pharmacy.uiowa.edu/people/graduate-students",
        "https://pharmacy.uiowa.edu/people",
    ]},
    {"department": "College of Nursing", "urls": [
        "https://nursing.uiowa.edu/people/students",
        "https://nursing.uiowa.edu/people/graduate-students",
        "https://nursing.uiowa.edu/people",
    ]},
    {"department": "Iowa Writers Workshop / Nonfiction Writing Program", "urls": [
        "https://clas.uiowa.edu/nonfiction-writing-program/",
        "https://clas.uiowa.edu/nonfiction-writing-program/people",
        "https://writersworkshop.uiowa.edu/",
        "https://writersworkshop.uiowa.edu/people",
        "https://writersworkshop.uiowa.edu/people/students",
    ]},
]

ATHLETICS_URLS = [
    "https://hawkeyesports.com/staff-directory/",
    "https://hawkeyesports.com/staff/",
]

STUDENT_ORGS = [
    {"department": "University of Iowa Student Government (USG)", "urls": [
        "https://usg.uiowa.edu/people",
        "https://usg.uiowa.edu/people?page=1",
        "https://usg.uiowa.edu/people?page=2",
        "https://usg.uiowa.edu/about-us/contact-usg",
    ]},
    {"department": "The Daily Iowan (Student Newspaper)", "urls": [
        "https://dailyiowan.com/contact/",
        "https://dailyiowan.com/staff/",
        "https://dailyiowan.com/about/",
    ]},
]


def extract_uiowa_emails(text):
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*uiowa\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        m = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*uiowa\.edu)', e)
        cleaned.add(m.group(1).lower() if m else e)
    return list(cleaned)

def extract_mailto_emails(soup):
    emails = []
    for a in soup.find_all('a', href=True):
        if 'mailto:' in a['href'].lower():
            m = re.search(r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*uiowa\.edu)', a['href'], re.IGNORECASE)
            if m: emails.append(m.group(1).lower().strip())
    return list(set(emails))

def decode_cf_email(enc):
    try:
        r = int(enc[:2], 16)
        return ''.join(chr(int(enc[i:i+2], 16) ^ r) for i in range(2, len(enc), 2)).lower()
    except Exception:
        return None

def extract_cf_emails(soup):
    emails = []
    for tag in soup.find_all(attrs={'data-cfemail': True}):
        d = decode_cf_email(tag.get('data-cfemail', ''))
        if d and '@' in d: emails.append(d)
    for a in soup.find_all('a', href=True):
        m = re.search(r'/email-protection#([a-f0-9]+)', a.get('href', ''))
        if m:
            d = decode_cf_email(m.group(1))
            if d and '@' in d: emails.append(d)
    return [e for e in set(emails) if 'uiowa.edu' in e]

ADMIN_PREFIXES = [
    'info@','admin@','office@','dept@','webmaster@','help@','support@','contact@',
    'registrar@','grad@','gradoffice@','department@','chair@','advising@','undergrad@',
    'dean@','reception@','main@','general@','staff@','gradadmit@','calendar@','events@',
    'news@','newsletter@','web@','marketing@','media@','communications@','hr@','hiring@',
    'jobs@','career@','alumni@','development@','giving@','feedback@','safety@','security@',
    'facilities@','it@','tech@','helpdesk@','library@','gradapp@','apply@','admissions@',
    'enrollment@','records@','bursar@','finaid@','housing@','dining@','parking@','police@',
    'noreply@','do-not-reply@','donotreply@','president@','provost@','gradschool@',
    'gradstudies@','law-admissions@','law-career@','law-library@','athletics@','compliance@',
    'titleix@','title-ix@','conduct@','counseling@','disability@','veterans@','international@',
    'multicultural@','equity@','clas@','coe@','cph@','tippie@','nursing@','ask-',
    'its-helpdesk@','its@','hawkeyesports@','uifoundation@','ui-usg@','usg-','usg@',
    'con-','nursing-','sociology@','sociology-','actuarial-science@','writers-workshop@',
    'uss-sociology@','tippie-phd@','pharmacy-','pharmacy@','education@','daily-iowan@',
    'geology@','clas-','engineering@','law-','uiowa-',
]

def is_admin(email):
    el = email.lower()
    return any(el.startswith(p) for p in ADMIN_PREFIXES)

def get_soup(url, session):
    try:
        r = session.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if r.status_code == 200:
            return BeautifulSoup(r.text, 'html.parser'), r.url
        log(f"    HTTP {r.status_code}")
        return None, None
    except Exception as e:
        log(f"    Error: {e}")
        return None, None

SKIP = ['email','contact','phone','http','department','graduate','student','people',
        'faculty','office','read more','view profile','website','lab','research',
        'iowa','uiowa','university']

def get_name(soup, email):
    for a in soup.find_all('a', href=True):
        if email in a.get('href','').lower():
            p = a.parent
            for _ in range(6):
                if not p: break
                for t in p.find_all(['h2','h3','h4','h5','strong','b','a','span']):
                    tx = t.get_text(strip=True)
                    if tx and '@' not in tx and 2<len(tx)<80 and not any(x in tx.lower() for x in SKIP):
                        return tx
                p = p.parent
    for elem in soup.find_all(string=re.compile(re.escape(email), re.IGNORECASE)):
        p = elem.parent
        for _ in range(6):
            if not p: break
            for t in p.find_all(['h2','h3','h4','h5','strong','b','a','span']):
                tx = t.get_text(strip=True)
                if tx and '@' not in tx and 2<len(tx)<80 and not any(x in tx.lower() for x in SKIP):
                    return tx
            p = p.parent
    return ""

def cards(soup, url, dept):
    results, seen = [], set()
    sels = ['.views-row','[class*="person"]','[class*="profile"]','[class*="people"]',
            '[class*="member"]','[class*="student"]','[class*="card"]','[class*="directory"]',
            '.field-content','article','.node--type-person','tr','.vcard','.teaser',
            '.person-listing','.bio-block','.staff-member','.team-member','.people-item',
            '.person-card','.people-card','.view-people .view-content > div',
            '.view-directory .view-content > div']
    for sel in sels:
        try:
            for c in soup.select(sel):
                tx = c.get_text(separator=' ', strip=True)
                for e in set(extract_uiowa_emails(tx)+extract_mailto_emails(c)+extract_cf_emails(c)):
                    if e in seen or is_admin(e): continue
                    seen.add(e)
                    nm = ""
                    for t in c.find_all(['h2','h3','h4','h5','strong','b','a','span']):
                        tn = t.get_text(strip=True)
                        if tn and '@' not in tn and 2<len(tn)<80 and not any(x in tn.lower() for x in SKIP):
                            nm=tn; break
                    results.append({'email':e,'name':nm,'department':dept,'source_url':url})
        except: continue
    return results

def pagination(soup, base):
    pgs = set()
    for a in soup.find_all('a', href=True):
        full = urljoin(base, a['href'])
        if re.search(r'[?&]page=\d+', full) or re.search(r'/page/\d+/?$', full):
            pgs.add(full)
    return sorted(pgs)

def profiles(soup, base):
    ps, seen = [], set()
    for a in soup.find_all('a', href=True):
        full = urljoin(base, a['href'])
        if full in seen: continue
        if re.search(r'/(people|directory)/[a-z][\w-]+/?$', full, re.IGNORECASE):
            nm = a.get_text(strip=True)
            if nm and '@' not in nm and 2<len(nm)<80:
                if not any(x in nm.lower() for x in ['graduate','student','people','faculty','all','home','search','more','view','page','next','previous','department','staff','back']):
                    seen.add(full); ps.append({'name':nm,'url':full})
    return ps

def scrape_profile(url, session):
    soup, _ = get_soup(url, session)
    if not soup: return None
    es = list(set(extract_mailto_emails(soup)+extract_uiowa_emails(soup.get_text(separator=' ',strip=True))+extract_cf_emails(soup)))
    ps = [e for e in es if not is_admin(e)]
    return ps[0] if ps else None

def scrape_dept(cfg, session):
    dept, results, seen, done = cfg['department'], [], set(), set()
    log(f"\n{'='*60}\nDept: {dept}\n{'='*60}")
    for url in cfg['urls']:
        if url in done: continue
        log(f"  Trying: {url}")
        soup, final = get_soup(url, session)
        if not soup: log("    -> Failed"); time.sleep(0.5); continue
        done.add(url)
        if final: done.add(final)
        src = final or url
        text = soup.get_text(separator=' ', strip=True)
        log(f"    -> OK ({final})")

        for r in cards(soup, src, dept):
            if r['email'] not in seen: seen.add(r['email']); results.append(r)
        for e in set(extract_uiowa_emails(text)+extract_mailto_emails(soup)+extract_cf_emails(soup)):
            if e not in seen and not is_admin(e):
                seen.add(e); results.append({'email':e,'name':get_name(soup,e),'department':dept,'source_url':src})
        for pf,dm in re.findall(r'([\w.+-]+)\s*(?:\[at\]|\(at\)|&#64;)\s*((?:[\w-]+\.)*uiowa\.edu)',text,re.IGNORECASE):
            e = f"{pf}@{dm}".lower()
            if e not in seen and not is_admin(e):
                seen.add(e); results.append({'email':e,'name':'','department':dept,'source_url':src})
        for sc in soup.find_all('script'):
            if sc.string:
                for e in extract_uiowa_emails(sc.string):
                    if e not in seen and not is_admin(e):
                        seen.add(e); results.append({'email':e,'name':'','department':dept,'source_url':src})

        pgs = pagination(soup, src)
        if pgs:
            log(f"    -> {len(pgs)} pages")
            for pg in pgs:
                if pg in done: continue
                done.add(pg)
                ps, _ = get_soup(pg, session)
                if not ps: continue
                for r in cards(ps, pg, dept):
                    if r['email'] not in seen: seen.add(r['email']); results.append(r)
                for e in set(extract_uiowa_emails(ps.get_text(separator=' ',strip=True))+extract_mailto_emails(ps)+extract_cf_emails(ps)):
                    if e not in seen and not is_admin(e):
                        seen.add(e); results.append({'email':e,'name':get_name(ps,e),'department':dept,'source_url':pg})
                time.sleep(0.5)

        if len(results) < 5:
            pls = profiles(soup, src)
            if pls:
                log(f"    -> {len(pls)} profiles")
                for p in pls[:80]:
                    e = scrape_profile(p['url'], session)
                    if e and e not in seen and not is_admin(e):
                        seen.add(e); results.append({'email':e,'name':p['name'],'department':dept,'source_url':p['url']})
                    time.sleep(0.3)
        if results: log(f"    -> {len(results)} emails")
        time.sleep(0.5)

    log(f"  TOTAL {dept}: {len(results)}")
    for r in results[:10]:
        em = r['email']
        nm = r['name']
        log(f"    {em:<42} | {nm}")
    if len(results)>10: log(f"    ...+{len(results)-10}")
    return results

def scrape_athletics(session):
    results, seen, dept = [], set(), "Hawkeyes Athletics (Staff)"
    log(f"\n{'='*60}\nDept: {dept}\n{'='*60}")
    for url in ATHLETICS_URLS:
        log(f"  Trying: {url}")
        soup, final = get_soup(url, session)
        if not soup: continue
        text = soup.get_text(separator=' ', strip=True)
        all_e = list(set(extract_uiowa_emails(text)+extract_mailto_emails(soup)+extract_cf_emails(soup)))
        log(f"    -> {len(all_e)} emails")
        for a in soup.find_all('a', href=True):
            if 'mailto:' not in a['href'].lower(): continue
            m = re.search(r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*uiowa\.edu)', a['href'], re.IGNORECASE)
            if not m: continue
            e = m.group(1).lower()
            if e in seen or is_admin(e): continue
            nm, p = "", a.parent
            for _ in range(5):
                if not p: break
                for t in p.find_all(['h2','h3','h4','h5','strong','b','span','a']):
                    tx = t.get_text(strip=True)
                    if tx and '@' not in tx and 2<len(tx)<80 and not any(x in tx.lower() for x in ['email','phone','contact','http']):
                        nm=tx; break
                if nm: break
                p = p.parent
            seen.add(e); results.append({'email':e,'name':nm,'department':dept,'source_url':final or url})
        for e in all_e:
            if e not in seen and not is_admin(e):
                seen.add(e); results.append({'email':e,'name':get_name(soup,e),'department':dept,'source_url':final or url})
        for pg in pagination(soup, final or url):
            ps, _ = get_soup(pg, session)
            if ps:
                for e in set(extract_uiowa_emails(ps.get_text(separator=' ',strip=True))+extract_mailto_emails(ps)):
                    if e not in seen and not is_admin(e):
                        seen.add(e); results.append({'email':e,'name':get_name(ps,e),'department':dept,'source_url':pg})
            time.sleep(0.5)
        if results: break
        time.sleep(0.5)
    log(f"  TOTAL {dept}: {len(results)}")
    return results

def main():
    session = requests.Session()
    existing, ex_emails = [], set()
    try:
        with open(OUTPUT_CSV) as f:
            for row in csv.DictReader(f):
                existing.append(row); ex_emails.add(row['email'].lower().strip())
        log(f"Loaded {len(existing)} existing")
    except FileNotFoundError:
        log("Starting fresh")

    all_r, seen = list(existing), set(ex_emails)
    def add(rs):
        n=0
        for r in rs:
            e=r['email'].lower().strip()
            if e and e not in seen: seen.add(e); all_r.append(r); n+=1
        return n

    log("="*70+f"\nUIowa v2 | {len(existing)} existing\n"+"="*70)

    log("\nPHASE 1: CLAS\n"+"="*70)
    for c in CLAS_DEPARTMENTS:
        try: n=add(scrape_dept(c,session)); log(f"  => +{n} (total: {len(all_r)})"); time.sleep(1)
        except Exception as e: log(f"  ERR {c['department']}: {e}")

    log("\nPHASE 2: ENGINEERING\n"+"="*70)
    for c in ENGINEERING:
        try: n=add(scrape_dept(c,session)); log(f"  => +{n} (total: {len(all_r)})"); time.sleep(1)
        except Exception as e: log(f"  ERR {c['department']}: {e}")

    log("\nPHASE 3: PROFESSIONAL\n"+"="*70)
    for c in PROFESSIONAL:
        try: n=add(scrape_dept(c,session)); log(f"  => +{n} (total: {len(all_r)})"); time.sleep(1)
        except Exception as e: log(f"  ERR {c['department']}: {e}")

    log("\nPHASE 4: ATHLETICS\n"+"="*70)
    try: n=add(scrape_athletics(session)); log(f"  => +{n} (total: {len(all_r)})")
    except Exception as e: log(f"  ERR athletics: {e}")

    log("\nPHASE 5: STUDENT ORGS\n"+"="*70)
    for c in STUDENT_ORGS:
        try: n=add(scrape_dept(c,session)); log(f"  => +{n} (total: {len(all_r)})"); time.sleep(1)
        except Exception as e: log(f"  ERR {c['department']}: {e}")

    log(f"\n{'='*70}\nTOTAL: {len(all_r)} ({len(all_r)-len(existing)} new)\n{'='*70}")

    with open(OUTPUT_CSV, 'w', newline='') as f:
        w=csv.DictWriter(f, fieldnames=['email','name','department','source_url'])
        w.writeheader()
        for r in sorted(all_r, key=lambda x:(x['department'],x['email'])): w.writerow(r)
    log(f"Saved {OUTPUT_CSV}")

    with open(OUTPUT_JSON, 'w') as f: json.dump(all_r, f, indent=2)
    log(f"Saved {OUTPUT_JSON}")

    dc={}
    for r in all_r: dc[r['department']]=dc.get(r['department'],0)+1
    log(f"\nBY DEPT:")
    for d,c in sorted(dc.items(), key=lambda x:-x[1]): log(f"  {d}: {c}")

    all_d = set([c['department'] for c in CLAS_DEPARTMENTS+ENGINEERING+PROFESSIONAL+STUDENT_ORGS]+['Hawkeyes Athletics (Staff)'])
    zeros = [d for d in all_d if d not in dc]
    if zeros:
        log("\n0 emails:")
        for d in sorted(zeros): log(f"  - {d}")
    return all_r

if __name__ == '__main__':
    main()
