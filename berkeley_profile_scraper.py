#!/usr/bin/env python3
"""Visit individual Berkeley profile pages to extract emails."""
import os, re, csv, json, time, ssl, html, signal, sys
import urllib.request
from urllib.parse import urljoin
from datetime import datetime

sys.stdout.reconfigure(line_buffering=True)

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@(?:[\w\-]+\.)*berkeley\.edu', re.IGNORECASE)

SKIP_PREFIXES = [
    'info@','admin@','office@','dept@','webmaster@','help@','support@',
    'contact@','registrar@','grad@','gradoffice@','department@','chair@',
    'advising@','undergrad@','dean@','reception@','main@','general@',
    'staff@','gradadmit@','calendar@','events@','news@','newsletter@',
    'web@','marketing@','media@','communications@','hr@','hiring@',
    'jobs@','career@','alumni@','development@','giving@','feedback@',
    'safety@','security@','facilities@','it@','tech@','helpdesk@',
    'library@','gradapp@','apply@','noreply@','no-reply@','donotreply@',
    'postmaster@','abuse@','root@','privacy@','records@','copyright@',
    'accessibility@','editor@','subscription@','are@','polisci@','english@',
    'music@','optometry@','bioeng@','astro@','publichealth@','haas@',
]


def is_admin(email):
    e = email.lower()
    return any(e.startswith(p) for p in SKIP_PREFIXES)


def fetch(url, timeout=15):
    def handler(s, f): raise TimeoutError("timeout")
    try:
        old = signal.signal(signal.SIGALRM, handler)
        signal.alarm(timeout + 5)
        req = urllib.request.Request(url, headers=HEADERS)
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        data = resp.read().decode('utf-8', errors='ignore')
        signal.alarm(0); signal.signal(signal.SIGALRM, old)
        return data
    except Exception as e:
        signal.alarm(0)
        try: signal.signal(signal.SIGALRM, old)
        except: pass
        return None


def get_emails(raw):
    if not raw: return []
    d = html.unescape(raw)
    emails = set()
    for m in EMAIL_RE.finditer(d):
        emails.add(m.group(0).lower().strip().rstrip('.'))
    for m in re.finditer(r'mailto:\s*([a-zA-Z0-9._%+\-]+@(?:[\w\-]+\.)*berkeley\.edu)', d, re.I):
        emails.add(m.group(1).lower().strip().rstrip('.'))
    return [e for e in emails if not is_admin(e)]


def find_profile_links(raw_html, base_url):
    """Find individual person profile links from a listing page."""
    links = set()
    skip = ['graduate-students', 'faculty', 'staff', 'postdoc', 'emerit',
            'in-memoriam', 'visitor', 'undergraduate', 'students',
            'phd-students', 'researchers', 'alumni', 'adjunct']
    
    for m in re.finditer(r'href="(/(?:people|faculty|person)/([^"]+))"', raw_html):
        path = m.group(1)
        slug = m.group(2).rstrip('/').split('?')[0]
        if slug and not any(slug == s or slug.startswith(s + '/') for s in skip):
            if len(slug) > 2 and slug[0].isalpha():
                url = base_url + path
                links.add(url)
    return sorted(links)


DEPARTMENTS = [
    ("https://are.berkeley.edu/people/graduate-students", "https://are.berkeley.edu", "Agricultural & Resource Economics"),
    ("https://english.berkeley.edu/people/graduate-students", "https://english.berkeley.edu", "English"),
    ("https://music.berkeley.edu/people/graduate-students", "https://music.berkeley.edu", "Music"),
    ("https://polisci.berkeley.edu/people/graduate-students", "https://polisci.berkeley.edu", "Political Science"),
    ("https://optometry.berkeley.edu/people/", "https://optometry.berkeley.edu", "School of Optometry"),
    ("https://publichealth.berkeley.edu/people/", "https://publichealth.berkeley.edu", "School of Public Health"),
    ("https://astro.berkeley.edu/people/graduate-student-directory/", "https://astro.berkeley.edu", "Astronomy"),
    ("https://bioeng.berkeley.edu/people/", "https://bioeng.berkeley.edu", "Bioengineering"),
    ("https://haas.berkeley.edu/faculty/", "https://haas.berkeley.edu", "Haas Business School"),
    ("https://are.berkeley.edu/people/faculty", "https://are.berkeley.edu", "Agricultural & Resource Economics"),
    ("https://art.berkeley.edu/people/graduate-students", "https://art.berkeley.edu", "Art Practice"),
    ("https://plantbiology.berkeley.edu/people/graduate-students", "https://plantbiology.berkeley.edu", "Plant Biology"),
]


def main():
    print("=" * 70)
    print("  UC BERKELEY PROFILE PAGE SCRAPER")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    base = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base, "berkeley_pro_emails.csv")
    existing, existing_emails = [], set()

    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                existing.append(row)
                existing_emails.add(row['email'].lower())
        print(f"\n  Loaded {len(existing)} existing contacts")

    all_new = []

    for listing_url, base_url, dept in DEPARTMENTS:
        print(f"\n  {dept} ({listing_url})")
        raw = fetch(listing_url)
        if not raw:
            print(f"    Could not fetch listing")
            continue

        # First extract emails directly from listing page
        listing_emails = get_emails(raw)
        for email in listing_emails:
            if email not in existing_emails:
                existing_emails.add(email)
                all_new.append({'email': email, 'name': '', 'department': dept, 'source_url': listing_url})

        # Find and visit profile pages
        profile_urls = find_profile_links(raw, base_url)
        
        # Also check pagination
        for pg_m in re.finditer(r'href="([^"]*[?&]page=\d+[^"]*)"', raw):
            pg_url = urljoin(listing_url, pg_m.group(1))
            if pg_url != listing_url:
                pg_raw = fetch(pg_url)
                if pg_raw:
                    more = find_profile_links(pg_raw, base_url)
                    for u in more:
                        if u not in profile_urls:
                            profile_urls.append(u)
                    for email in get_emails(pg_raw):
                        if email not in existing_emails:
                            existing_emails.add(email)
                            all_new.append({'email': email, 'name': '', 'department': dept, 'source_url': pg_url})
                time.sleep(0.3)

        print(f"    Found {len(profile_urls)} profiles, visiting...")
        count = 0
        dept_new = 0
        for url in profile_urls[:100]:
            count += 1
            pg = fetch(url, timeout=10)
            if pg:
                emails = get_emails(pg)
                for email in emails:
                    if email not in existing_emails:
                        existing_emails.add(email)
                        name = ""
                        h1 = re.search(r'<h1[^>]*>\s*(?:<[^>]+>\s*)*([^<]+)', pg)
                        if h1:
                            name = re.sub(r'\s+', ' ', h1.group(1).strip())
                            if len(name) > 60 or '@' in name: name = ""
                        if not name:
                            title = re.search(r'<title>\s*([^|<]+)', pg)
                            if title:
                                name = title.group(1).strip().split(' | ')[0].strip()
                        all_new.append({'email': email, 'name': name, 'department': dept, 'source_url': url})
                        dept_new += 1
            if count % 25 == 0:
                print(f"    [{count}/{min(100,len(profile_urls))}] visited...")
            time.sleep(0.2)
        print(f"    -> {dept_new} new emails from {dept}")
        time.sleep(0.3)

    final = existing + all_new
    print(f"\n{'=' * 70}")
    print(f"  Previously: {len(existing)} | New: {len(all_new)} | Total: {len(final)}")
    print(f"{'=' * 70}")

    if all_new:
        depts = {}
        for c in all_new:
            d = c['department']
            depts[d] = depts.get(d, 0) + 1
        for d, cnt in sorted(depts.items(), key=lambda x: -x[1]):
            print(f"    {d:<55} {cnt}")

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=["email","name","department","source_url"])
        w.writeheader()
        w.writerows(final)

    json_path = os.path.join(base, "berkeley_pro_emails.json")
    with open(json_path, 'w') as f:
        json.dump(final, f, indent=2)

    print(f"\n  CSV: {csv_path}")
    print(f"  COMPLETE. {len(final)} total emails.")


if __name__ == "__main__":
    main()
