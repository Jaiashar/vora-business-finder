#!/usr/bin/env python3
"""
Michigan State University (MSU) Email Scraper - v3
Scrapes @msu.edu (and @ath.msu.edu) emails from:
 - Arts & Sciences grad-student directories (NatSci / Social Science / Arts & Letters)
 - Engineering department grad-student pages
 - Professional school directories (Broad, Law, Education, Journalism, Social Work, Vet Med)
 - Athletics staff directory (msuspartans.com)
 - Student organizations (ASMSU, The State News)
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import json
import time
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright


def log(msg):
    print(msg, flush=True)


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

OUTPUT_CSV = "/Users/jaiashar/Documents/VoraBusinessFinder/msu_dept_emails.csv"
OUTPUT_JSON = "/Users/jaiashar/Documents/VoraBusinessFinder/msu_dept_emails.json"


# ============================================================
# EMAIL EXTRACTION UTILITIES
# ============================================================

def extract_msu_emails(text):
    """Extract @msu.edu and @ath.msu.edu emails from text."""
    pattern = r"[\w.+-]+@(?:[\w-]+\.)*msu\.edu"
    raw = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in raw:
        e = e.lower().strip()
        # Strip leading digits/dashes that sometimes get captured
        m = re.match(r"(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*msu\.edu)", e)
        if m:
            cleaned.add(m.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract @msu.edu emails from mailto: links."""
    emails = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "mailto:" in href.lower():
            m = re.search(r"mailto:\s*([\w.+-]+@(?:[\w-]+\.)*msu\.edu)", href, re.IGNORECASE)
            if m:
                emails.append(m.group(1).lower().strip())
    return list(set(emails))


ADMIN_PREFIXES = [
    "info@", "admin@", "office@", "dept@", "webmaster@", "help@",
    "support@", "contact@", "registrar@", "grad@", "gradoffice@",
    "department@", "chair@", "advising@", "undergrad@", "dean@",
    "reception@", "main@", "general@", "staff@", "gradadmit@",
    "calendar@", "events@", "news@", "newsletter@", "web@",
    "marketing@", "media@", "communications@", "hr@", "hiring@",
    "jobs@", "career@", "alumni@", "development@", "giving@",
    "feedback@", "safety@", "security@", "facilities@", "it@",
    "tech@", "helpdesk@", "library@", "gradapp@", "apply@",
    "admissions@", "enrollment@", "records@", "bursar@",
    "finaid@", "housing@", "dining@", "parking@", "police@",
    "noreply@", "do-not-reply@", "donotreply@",
    "spartanfund@", "ticket@", "tickets@", "websupport@",
    "msulaw@", "vetmed@", "broad@", "education@",
    "comarts@", "socialwork@", "journalism@",
    "stt.staff@", "stt.undergradoffice@", "stt.gradoffice@", "stt.ithelp@",
    "spartans@", "gogreen@", "msuspartans@", "spartanfund@",
    "msuathletics@", "compliance@", "studentlife@",
]


def is_admin_email(email):
    el = email.lower()
    return any(el.startswith(p) for p in ADMIN_PREFIXES)


# ============================================================
# PAGE LOADING
# ============================================================

def pw_load(url, page, wait_ms=5000):
    """Load a URL using Playwright, return (BeautifulSoup, final_url)."""
    try:
        resp = page.goto(url, timeout=30000, wait_until="domcontentloaded")
        if resp is None or resp.status >= 400:
            s = resp.status if resp else "no response"
            log(f"    HTTP {s}")
            return None, None
        page.wait_for_timeout(wait_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        # Scroll down to trigger lazy-loaded content
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)
        html = page.content()
        return BeautifulSoup(html, "html.parser"), page.url
    except Exception as e:
        if "Timeout" in str(e):
            try:
                html = page.content()
                if html and len(html) > 500:
                    return BeautifulSoup(html, "html.parser"), url
            except Exception:
                pass
        log(f"    PW error: {e}")
        return None, None


def req_load(url, session=None):
    """Load a URL using requests, return (BeautifulSoup, final_url)."""
    s = session or requests.Session()
    try:
        r = s.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if r.status_code >= 400:
            log(f"    HTTP {r.status_code}")
            return None, None
        return BeautifulSoup(r.text, "html.parser"), r.url
    except Exception as e:
        log(f"    Request error: {e}")
        return None, None


# ============================================================
# NAME EXTRACTION
# ============================================================

SKIP_WORDS = {
    "email", "contact", "phone", "http", "department", "graduate",
    "student", "people", "faculty", "office", "read more", "view profile",
    "website", "lab", "full bio", "view", "more info", "directory",
    "staff", "undergraduate", "home", "search", "news", "events",
    "research", "about", "explore program", "learn more", "apply now",
    "skip to", "menu", "close", "toggle", "navigation",
}


def is_name(text):
    if not text or "@" in text:
        return False
    if len(text) < 3 or len(text) > 80:
        return False
    return not any(w in text.lower() for w in SKIP_WORDS)


def try_get_name(soup, email):
    """Try to find a person's name near their email on the page."""
    # Look in mailto links first
    for a in soup.find_all("a", href=True):
        if email in a.get("href", "").lower():
            parent = a.parent
            for _ in range(6):
                if parent is None:
                    break
                for tag in parent.find_all(["h2", "h3", "h4", "h5", "strong", "b", "a"]):
                    t = tag.get_text(strip=True)
                    if is_name(t) and "mailto" not in t.lower():
                        return t
                parent = parent.parent
    # Search for email in text
    for elem in soup.find_all(string=re.compile(re.escape(email), re.IGNORECASE)):
        parent = elem.parent
        for _ in range(6):
            if parent is None:
                break
            for tag in parent.find_all(["h2", "h3", "h4", "h5", "strong", "b", "a"]):
                t = tag.get_text(strip=True)
                if is_name(t) and "mailto" not in t.lower():
                    return t
            parent = parent.parent
    return ""


# ============================================================
# GENERIC SCRAPING METHODS
# ============================================================

def scrape_page_with_profiles(url, department, pw_page, session=None):
    """
    Scrape a page for emails. If few/no emails found on the listing page,
    follow profile links to get individual emails.
    Handles WordPress, static HTML, and many MSU department pages.
    """
    results = []
    seen = set()
    s = session or requests.Session()

    log(f"    Fetching: {url}")
    # Try requests first (faster), fall back to Playwright
    soup, final = req_load(url, s)
    if soup is None or len(soup.get_text(strip=True)) < 200:
        log(f"    Trying Playwright for: {url}")
        soup, final = pw_load(url, pw_page, wait_ms=8000)
    if soup is None:
        return results

    # Extract emails from listing page
    page_text = soup.get_text(separator=" ", strip=True)
    emails = extract_msu_emails(page_text)
    mailto_emails = extract_mailto_emails(soup)
    all_emails = list(set(emails + mailto_emails))

    for email in all_emails:
        if email in seen or is_admin_email(email):
            continue
        seen.add(email)
        name = try_get_name(soup, email)
        results.append({
            "email": email, "name": name,
            "department": department, "source_url": final or url,
        })

    if results:
        log(f"    -> {len(results)} emails from listing page")

    # Collect profile links - always look, even if we found some emails
    profile_links = []
    base = final or url
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        text = a.get_text(strip=True)
        full = urljoin(base, href)
        # Skip self-links, anchors, external links
        if full == base or full.startswith("#") or not full.startswith("http"):
            continue
        # Match profile patterns
        if (re.search(r"/people/[\w-]+/?$", full)
            or re.search(r"/graduate-students/[\w-]+/?$", full)
            or re.search(r"/directory/[\w-]+/?$", full)
            or re.search(r"/students/[\w-]+/?$", full)):
            if is_name(text):
                profile_links.append({"name": text, "url": full})
        # Some sites use /<dept>/<name-slug>/ pattern
        elif re.search(r"msu\.edu/[\w-]+/$", full) and is_name(text) and len(text.split()) >= 2:
            # Must look like a person name (at least 2 words)
            profile_links.append({"name": text, "url": full})

    # If we found fewer than 3 emails on the listing, try profile links
    if len(results) < 3 and profile_links:
        log(f"    -> Following {len(profile_links)} profile links...")
        for prof in profile_links[:120]:
            try:
                pr, _ = req_load(prof["url"], s)
                if pr is None:
                    pr, _ = pw_load(prof["url"], pw_page, wait_ms=4000)
                if pr is None:
                    continue
                p_text = pr.get_text(separator=" ", strip=True)
                p_emails = extract_msu_emails(p_text)
                p_emails += extract_mailto_emails(pr)
                for email in set(p_emails):
                    if email not in seen and not is_admin_email(email):
                        seen.add(email)
                        results.append({
                            "email": email, "name": prof["name"],
                            "department": department, "source_url": prof["url"],
                        })
            except Exception as e:
                log(f"    Profile error: {e}")
            time.sleep(0.2)

    return results


def scrape_multi_urls(urls, department, pw_page, session=None):
    """Scrape multiple URLs for a single department, deduplicating."""
    results = []
    seen = set()
    for url in urls:
        page_results = scrape_page_with_profiles(url, department, pw_page, session)
        for r in page_results:
            if r["email"] not in seen:
                seen.add(r["email"])
                results.append(r)
        time.sleep(0.3)
    return results


# ============================================================
# ASPX DIRECTORY SCRAPING (NatSci departmental directories)
# ============================================================

def scrape_aspx_directory(url, department, session=None):
    """Scrape ASP.NET-style directory pages with pagination."""
    results = []
    seen = set()
    s = session or requests.Session()
    pages_to_visit = [url]
    visited = set()

    while pages_to_visit:
        page_url = pages_to_visit.pop(0)
        if page_url in visited:
            continue
        visited.add(page_url)

        log(f"    Fetching: {page_url}")
        soup, final = req_load(page_url, s)
        if soup is None:
            continue

        page_text = soup.get_text(separator=" ", strip=True)
        emails = extract_msu_emails(page_text)
        mailto_emails = extract_mailto_emails(soup)
        all_emails = list(set(emails + mailto_emails))

        for email in all_emails:
            if email in seen or is_admin_email(email):
                continue
            seen.add(email)
            name = try_get_name(soup, email)
            results.append({
                "email": email, "name": name,
                "department": department, "source_url": page_url,
            })

        # Check for pagination links
        if len(visited) <= 1:
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                text = a.get_text(strip=True)
                if "page=" in href and text.isdigit():
                    full = urljoin(page_url, href)
                    if full not in visited:
                        pages_to_visit.append(full)

    return results


# ============================================================
# ATHLETICS
# ============================================================

def scrape_athletics(pw_page):
    """Scrape MSU athletics staff directory."""
    results = []
    seen = set()
    url = "https://msuspartans.com/staff-directory"
    department = "Athletics (Staff)"

    log(f"    Loading: {url}")
    soup, final = pw_load(url, pw_page, wait_ms=12000)
    if soup is None:
        return results

    # Get mailto emails
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "mailto:" in href.lower():
            m = re.search(r"mailto:\s*([\w.+-]+@[\w.+-]+)", href, re.IGNORECASE)
            if m:
                email = m.group(1).lower().strip()
                if "msu.edu" in email and email not in seen and not is_admin_email(email):
                    seen.add(email)
                    name = ""
                    parent = a.parent
                    for _ in range(6):
                        if parent is None:
                            break
                        for tag in parent.find_all(["h4", "h3", "h2", "h5", "strong", "a"]):
                            t = tag.get_text(strip=True)
                            if is_name(t) and "mailto" not in t.lower():
                                name = t
                                break
                        if name:
                            break
                        parent = parent.parent
                    results.append({
                        "email": email, "name": name,
                        "department": department, "source_url": url,
                    })

    # Also extract from page text
    page_text = soup.get_text(separator=" ", strip=True)
    for email in extract_msu_emails(page_text):
        if email not in seen and not is_admin_email(email):
            seen.add(email)
            results.append({
                "email": email, "name": try_get_name(soup, email),
                "department": department, "source_url": url,
            })

    # Try staff detail pages too
    staff_links = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        text = a.get_text(strip=True)
        full = urljoin(url, href)
        if "/staff-directory/" in href and is_name(text):
            staff_links.append({"name": text, "url": full})

    if staff_links:
        log(f"    -> Following {len(staff_links)} staff profile links...")
        for prof in staff_links[:150]:
            try:
                p_soup, _ = pw_load(prof["url"], pw_page, wait_ms=4000)
                if p_soup is None:
                    continue
                p_emails = extract_msu_emails(p_soup.get_text(separator=" ", strip=True))
                p_emails += extract_mailto_emails(p_soup)
                for email in set(p_emails):
                    if email not in seen and not is_admin_email(email):
                        seen.add(email)
                        results.append({
                            "email": email, "name": prof["name"],
                            "department": department, "source_url": prof["url"],
                        })
            except Exception:
                pass
            time.sleep(0.2)

    return results


# ============================================================
# STUDENT ORGS
# ============================================================

def scrape_student_orgs(pw_page, session=None):
    """Scrape ASMSU and The State News for emails."""
    results = []
    seen = set()
    s = session or requests.Session()

    orgs = [
        {
            "name": "ASMSU (Student Government)",
            "urls": [
                "https://asmsu.msu.edu/",
                "https://asmsu.msu.edu/about",
                "https://asmsu.msu.edu/leadership",
                "https://asmsu.msu.edu/contact",
                "https://asmsu.msu.edu/officers",
                "https://asmsu.msu.edu/executive-board",
                "https://asmsu.msu.edu/general-assembly",
            ],
        },
        {
            "name": "The State News",
            "urls": [
                "https://statenews.com/staff",
                "https://statenews.com/page/contact",
                "https://statenews.com/page/about",
                "https://statenews.com/",
            ],
        },
    ]

    for org in orgs:
        dept = org["name"]
        log(f"  {dept}")
        for url in org["urls"]:
            log(f"    Trying: {url}")
            # Try requests first
            soup, final = req_load(url, s)
            if soup is None or len(soup.get_text(strip=True)) < 100:
                soup, final = pw_load(url, pw_page, wait_ms=6000)
            if soup is None:
                continue
            page_text = soup.get_text(separator=" ", strip=True)
            emails = extract_msu_emails(page_text)
            mailto_emails = extract_mailto_emails(soup)
            all_emails = list(set(emails + mailto_emails))
            for email in all_emails:
                if email in seen or is_admin_email(email):
                    continue
                seen.add(email)
                results.append({
                    "email": email, "name": try_get_name(soup, email),
                    "department": dept, "source_url": final or url,
                })
            if all_emails:
                log(f"    -> {len(all_emails)} emails found")
            time.sleep(0.5)

    return results


# ============================================================
# DEPARTMENT CONFIGS - using exact user-provided URLs
# ============================================================

# Arts & Sciences: many MSU departments use the same /people/graduate-students pattern
# Some use ASP.NET directories, some WordPress, some static HTML, some need Playwright
# We'll try requests first and fall back to Playwright for all of them.

ARTS_SCIENCES_DEPARTMENTS = [
    # === Social Science ===
    {
        "department": "Economics",
        "urls": [
            "https://econ.msu.edu/people/graduate-students",
            "https://econ.msu.edu/about/directory",
            "https://econ.msu.edu/people",
        ],
    },
    {
        "department": "Political Science",
        "urls": [
            "https://polisci.msu.edu/people/graduate-students",
            "https://polisci.msu.edu/people/phd-students",
            "https://polisci.msu.edu/people/mpp-students",
            "https://polisci.msu.edu/people/index.html",
        ],
    },
    {
        "department": "Sociology",
        "urls": [
            "https://sociology.msu.edu/people/graduate-students",
            "https://sociology.msu.edu/people/grad_students.html",
            "https://sociology.msu.edu/people/index.html",
        ],
    },
    {
        "department": "Psychology",
        "urls": [
            "https://psychology.msu.edu/people/graduate-students",
            "https://psychology.msu.edu/directory/index.html",
        ],
    },
    {
        "department": "History",
        "urls": [
            "https://history.msu.edu/people/graduate-students",
            "https://history.msu.edu/people/graduate-students/",
        ],
    },
    {
        "department": "English",
        "urls": [
            "https://english.msu.edu/people/graduate-students",
            "https://english.msu.edu/graduate-students/",
        ],
    },
    {
        "department": "Philosophy",
        "urls": [
            "https://philosophy.msu.edu/people/graduate-students",
            "https://philosophy.msu.edu/graduate-students/",
        ],
    },
    {
        "department": "Linguistics",
        "urls": [
            "https://linguistics.msu.edu/people/graduate-students",
            "https://lilac.msu.edu/faculty-staff/",
            "https://lilac.msu.edu/",
        ],
    },
    {
        "department": "Anthropology",
        "urls": [
            "https://anthropology.msu.edu/people/graduate-students",
            "https://anthropology.msu.edu/people/graduate-students/",
        ],
    },
    {
        "department": "Geography",
        "urls": [
            "https://geo.msu.edu/people/graduate-students",
            "https://geo.msu.edu/directory/index.html",
        ],
    },
    # === NatSci ===
    {
        "department": "Mathematics",
        "urls": [
            "https://math.msu.edu/people/graduate-students",
        ],
        "aspx_fallback": "https://math.msu.edu/People/index.aspx?group=241",
    },
    {
        "department": "Statistics & Probability",
        "urls": [
            "https://stt.natsci.msu.edu/people/graduate-students",
        ],
        "aspx_fallback": "https://directory.natsci.msu.edu/Directory/Directory/Department/2?group=188",
    },
    {
        "department": "Physics & Astronomy",
        "urls": [
            "https://pa.msu.edu/people/graduate-students",
        ],
        "aspx_fallback": "https://pa.msu.edu/directory/index.aspx?group=185",
    },
    {
        "department": "Chemistry",
        "urls": [
            "https://chemistry.msu.edu/people/graduate-students",
        ],
        "aspx_fallback": "https://chemistry.natsci.msu.edu/directory/index.aspx?group=180",
    },
    {
        "department": "Earth & Environmental Sciences",
        "urls": [
            "https://ees.natsci.msu.edu/people/graduate-students",
        ],
        "aspx_fallback": "https://directory.natsci.msu.edu/Directory/Directory/Department/20?group=182",
    },
    {
        "department": "Biology (Integrative)",
        "urls": [
            "https://biology.msu.edu/people/graduate-students",
        ],
        "aspx_fallback": "https://directory.natsci.msu.edu/Directory/Directory/Department/4?group=183",
    },
    {
        "department": "Computational Math, Science & Engineering",
        "urls": [
            "https://cmse.msu.edu/people/graduate-students",
        ],
        "aspx_fallback": "https://cmse.msu.edu/Directory/index.aspx?group=181",
    },
    # === Arts & Letters ===
    {
        "department": "Music",
        "urls": [
            "https://music.msu.edu/people/graduate-students",
            "https://music.msu.edu/faculty/",
        ],
    },
]

ENGINEERING_DEPARTMENTS = [
    {
        "department": "Computer Science & Engineering",
        "urls": [
            "https://cse.msu.edu/people/graduate-students",
            "https://cse.msu.edu/People",
        ],
    },
    {
        "department": "Electrical & Computer Engineering",
        "urls": [
            "https://ece.msu.edu/people/graduate-students",
            "https://ece.msu.edu/People",
        ],
    },
    {
        "department": "Mechanical Engineering",
        "urls": [
            "https://me.msu.edu/people/graduate-students",
            "https://me.msu.edu/People",
        ],
    },
    {
        "department": "Civil & Environmental Engineering",
        "urls": [
            "https://egr.msu.edu/cee/people/graduate-students",
            "https://egr.msu.edu/cee/people",
        ],
    },
    {
        "department": "Biomedical Engineering",
        "urls": [
            "https://bme.msu.edu/people/graduate-students",
            "https://bme.msu.edu/People",
        ],
    },
    {
        "department": "Chemical Engineering",
        "urls": [
            "https://che.msu.edu/people/graduate-students",
            "https://che.msu.edu/People",
        ],
    },
]

PROFESSIONAL_SCHOOLS = [
    {
        "department": "Broad College of Business",
        "urls": [
            "https://broad.msu.edu/phd/",
            "https://broad.msu.edu/directory/",
            "https://broad.msu.edu/phd/students/",
        ],
    },
    {
        "department": "College of Law",
        "urls": [
            "https://law.msu.edu/student-organizations",
            "https://law.msu.edu/students",
            "https://law.msu.edu/directory",
            "https://law.msu.edu/",
        ],
    },
    {
        "department": "College of Education",
        "urls": [
            "https://education.msu.edu/people/students",
            "https://education.msu.edu/people",
        ],
    },
    {
        "department": "Journalism",
        "urls": [
            "https://journalism.msu.edu/people/students",
            "https://comartsci.msu.edu/people",
            "https://journalism.msu.edu/",
        ],
    },
    {
        "department": "School of Social Work",
        "urls": [
            "https://socialwork.msu.edu/people/students",
            "https://socialwork.msu.edu/students/index.html",
            "https://socialwork.msu.edu/",
        ],
    },
    {
        "department": "College of Veterinary Medicine",
        "urls": [
            "https://cvm.msu.edu/directory",
            "https://cvm.msu.edu/",
            "https://vet.msu.edu/",
        ],
    },
]


# ============================================================
# ADDITIONAL NATSCI ASP.NET DIRECTORIES (fallback data)
# ============================================================

NATSCI_ASPX_EXTRA = [
    {"department": "Biochemistry & Molecular Biology",
     "url": "https://directory.natsci.msu.edu/Directory/Directory/Department/6?group=179"},
    {"department": "Microbiology, Genetics & Immunology",
     "url": "https://directory.natsci.msu.edu/Directory/Directory/Department/26?group=184"},
    {"department": "Plant Biology",
     "url": "https://directory.natsci.msu.edu/Directory/Directory/Department/45?group=186"},
    {"department": "Physiology",
     "url": "https://directory.natsci.msu.edu/Directory/Directory/Department/35?group=187"},
]


# ============================================================
# ENGINEERING CENTRAL DIRECTORY (supplementary)
# ============================================================

def scrape_engineering_central(pw_page):
    """Scrape the central engineering directory for additional emails."""
    results = []
    seen = set()
    url = "https://engineering.msu.edu/directory"

    log(f"    Loading: {url}")
    soup, final = pw_load(url, pw_page, wait_ms=12000)
    if soup is None:
        return results

    addresses = soup.find_all("address")
    log(f"    -> Found {len(addresses)} directory entries")

    for addr in addresses:
        text = addr.get_text(separator=" ", strip=True)
        emails = extract_msu_emails(text)
        if not emails:
            continue

        row = addr.parent
        while row and "row" not in " ".join(row.get("class", [])):
            row = row.parent

        name = ""
        dept_info = "Engineering"

        if row:
            for a in row.find_all("a", href=True):
                href = a.get("href", "")
                t = a.get_text(strip=True)
                if "/directory/" in href and is_name(t):
                    name = t
                    break

            for tag in row.find_all(["span", "p", "small", "div"]):
                t = tag.get_text(strip=True)
                if t and len(t) < 120:
                    for dname in [
                        "Computer Science and Engineering",
                        "Electrical and Computer Engineering",
                        "Mechanical Engineering",
                        "Chemical Engineering and Materials Science",
                        "Civil and Environmental Engineering",
                        "Biomedical Engineering",
                    ]:
                        if dname.lower() in t.lower():
                            dept_info = dname
                            break

        for email in emails:
            if email in seen or is_admin_email(email):
                continue
            seen.add(email)
            results.append({
                "email": email, "name": name,
                "department": dept_info, "source_url": url,
            })

    return results


# ============================================================
# MAIN
# ============================================================

def main():
    all_results = []
    global_seen = set()

    def add_results(results):
        count = 0
        for r in results:
            email = r["email"].lower().strip()
            if email and email not in global_seen:
                global_seen.add(email)
                all_results.append(r)
                count += 1
        return count

    log("=" * 70)
    log("MICHIGAN STATE UNIVERSITY EMAIL SCRAPER v3")
    log("Domains: @msu.edu, @ath.msu.edu")
    log("=" * 70)

    session = requests.Session()

    # PHASE 1: NatSci ASP.NET directories (extra depts not in main list)
    log("\n\nPHASE 1: EXTRA NATSCI ASP.NET DIRECTORIES")
    log("=" * 70)
    for config in NATSCI_ASPX_EXTRA:
        dept = config["department"]
        url = config["url"]
        log(f"\n  {dept}")
        try:
            results = scrape_aspx_directory(url, dept, session)
            n = add_results(results)
            log(f"    => {n} new emails (total: {len(all_results)})")
        except Exception as e:
            log(f"    ERROR: {e}")
        time.sleep(0.3)

    # Phases 2+ need Playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()

        # PHASE 2: Arts & Sciences
        log("\n\nPHASE 2: ARTS & SCIENCES (NatSci + Social Science + Arts & Letters)")
        log("=" * 70)
        for config in ARTS_SCIENCES_DEPARTMENTS:
            dept = config["department"]
            urls = config["urls"]
            log(f"\n  {dept}")
            try:
                results = scrape_multi_urls(urls, dept, page, session)
                n = add_results(results)
                log(f"    => {n} new emails (total: {len(all_results)})")

                # If we got very few, try ASP.NET fallback
                if n < 3 and "aspx_fallback" in config:
                    log(f"    -> Trying ASP.NET fallback...")
                    fb_results = scrape_aspx_directory(config["aspx_fallback"], dept, session)
                    n2 = add_results(fb_results)
                    log(f"    => {n2} additional from ASP.NET (total: {len(all_results)})")
            except Exception as e:
                log(f"    ERROR: {e}")
            time.sleep(0.3)

        # PHASE 3: Engineering departments
        log("\n\nPHASE 3: ENGINEERING DEPARTMENTS")
        log("=" * 70)
        for config in ENGINEERING_DEPARTMENTS:
            dept = config["department"]
            urls = config["urls"]
            log(f"\n  {dept}")
            try:
                results = scrape_multi_urls(urls, dept, page, session)
                n = add_results(results)
                log(f"    => {n} new emails (total: {len(all_results)})")
            except Exception as e:
                log(f"    ERROR: {e}")
            time.sleep(0.3)

        # PHASE 3b: Engineering central directory (supplementary)
        log("\n  Engineering Central Directory (supplement)")
        try:
            results = scrape_engineering_central(page)
            n = add_results(results)
            log(f"    => {n} new engineering emails (total: {len(all_results)})")
        except Exception as e:
            log(f"    ERROR: {e}")

        # PHASE 4: Professional Schools
        log("\n\nPHASE 4: PROFESSIONAL SCHOOLS")
        log("=" * 70)
        for config in PROFESSIONAL_SCHOOLS:
            dept = config["department"]
            urls = config["urls"]
            log(f"\n  {dept}")
            try:
                results = scrape_multi_urls(urls, dept, page, session)
                n = add_results(results)
                log(f"    => {n} new emails (total: {len(all_results)})")
            except Exception as e:
                log(f"    ERROR: {e}")
            time.sleep(0.5)

        # PHASE 5: Athletics
        log("\n\nPHASE 5: ATHLETICS")
        log("=" * 70)
        try:
            results = scrape_athletics(page)
            n = add_results(results)
            log(f"  => {n} new athletics emails (total: {len(all_results)})")
        except Exception as e:
            log(f"  ERROR: {e}")

        # PHASE 6: Student Organizations
        log("\n\nPHASE 6: STUDENT ORGANIZATIONS")
        log("=" * 70)
        try:
            results = scrape_student_orgs(page, session)
            n = add_results(results)
            log(f"  => {n} new student org emails (total: {len(all_results)})")
        except Exception as e:
            log(f"  ERROR: {e}")

        browser.close()

    # Save results
    log(f"\n\n{'=' * 70}")
    log("RESULTS SUMMARY")
    log(f"{'=' * 70}")
    log(f"Total unique MSU emails: {len(all_results)}")

    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["email", "name", "department", "source_url"])
        writer.writeheader()
        for r in sorted(all_results, key=lambda x: (x["department"], x["email"])):
            writer.writerow(r)
    log(f"\nSaved to {OUTPUT_CSV}")

    with open(OUTPUT_JSON, "w") as f:
        json.dump(all_results, f, indent=2)
    log(f"Saved to {OUTPUT_JSON}")

    log(f"\n{'=' * 70}")
    log("SUMMARY BY DEPARTMENT:")
    log(f"{'=' * 70}")
    dept_counts = {}
    for r in all_results:
        dept = r["department"]
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"  {dept}: {count} emails")

    # Report departments with 0 emails
    all_depts = set(
        [c["department"] for c in ARTS_SCIENCES_DEPARTMENTS]
        + [c["department"] for c in ENGINEERING_DEPARTMENTS]
        + [c["department"] for c in PROFESSIONAL_SCHOOLS]
        + [c["department"] for c in NATSCI_ASPX_EXTRA]
        + ["Engineering", "Athletics (Staff)", "ASMSU (Student Government)", "The State News"]
    )
    zeros = sorted(all_depts - set(dept_counts.keys()))
    if zeros:
        log(f"\nDepartments with 0 emails:")
        for d in zeros:
            log(f"  - {d}")

    return all_results


if __name__ == "__main__":
    main()
