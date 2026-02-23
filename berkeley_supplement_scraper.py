#!/usr/bin/env python3
"""Supplementary Berkeley Scraper - gets sources missed by main scraper."""
import os, re, csv, json, time, ssl, html, signal, sys
import urllib.request, urllib.error
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
    "Accept-Language": "en-US,en;q=0.5",
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
    'accessibility@','editor@','op-ed@','letters@','studentaffairs@',
    'financial-aid@','finaid@','subscription@',
]

SKIP_EMAILS = {
    'webmaster@berkeley.edu','info@berkeley.edu','help@berkeley.edu',
    'registrar@berkeley.edu','chancellor@berkeley.edu',
    'publichealth@berkeley.edu','ieee@berkeley.edu',
    'bair-admin@berkeley.edu','bair-website@berkeley.edu',
}


def is_admin(email):
    e = email.lower()
    if e in SKIP_EMAILS: return True
    return any(e.startswith(p) for p in SKIP_PREFIXES)


def fetch(url, timeout=20):
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
        print(f"      [WARN] {url[:80]} - {str(e)[:60]}")
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


def derive_name(email):
    local = email.split('@')[0]
    for sep in ['.', '_']:
        if sep in local:
            parts = local.split(sep)
            if all(len(p) > 1 for p in parts[:2]):
                cleaned = [re.sub(r'\d+', '', p) for p in parts[:2]]
                if all(len(p) > 1 for p in cleaned):
                    return ' '.join(p.capitalize() for p in cleaned)
    return ""


def find_name(text, email):
    idx = text.lower().find(email.lower())
    if idx < 0: return derive_name(email)
    ctx = text[max(0,idx-500):idx+200]
    for pat in [
        r'<h[1-5][^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][a-zA-Z\s.\'-]+?)(?:</a>)?\s*</h[1-5]>',
        r'<strong[^>]*>\s*([A-Z][a-zA-Z\s.\'-]+?)\s*</strong>',
        r'class="[^"]*name[^"]*"[^>]*>\s*([A-Z][a-zA-Z\s.\'-]{3,50})',
    ]:
        m = re.search(pat, ctx)
        if m:
            n = re.sub(r'\s+', ' ', m.group(1).strip())
            if 2 < len(n) < 60 and '@' not in n: return n
    return derive_name(email)


def scrape_url(url, dept):
    raw = fetch(url)
    if not raw: return []
    emails = get_emails(raw)
    d = html.unescape(raw)
    results, seen = [], set()
    for email in emails:
        if email not in seen:
            seen.add(email)
            results.append({'email': email, 'name': find_name(d, email),
                          'department': dept, 'source_url': url})
    # pagination
    for m in re.finditer(r'href="([^"]*[?&]page=\d+[^"]*)"', raw):
        pg = urljoin(url, m.group(1))
        if pg != url:
            time.sleep(0.3)
            pr = fetch(pg)
            if pr:
                for email in get_emails(pr):
                    if email not in seen:
                        seen.add(email)
                        results.append({'email': email, 'name': find_name(html.unescape(pr), email),
                                      'department': dept, 'source_url': pg})
    return results


# ---- Cal Bears Profiles ----
def scrape_calbears_profiles():
    print("\n  Phase 1: Cal Bears Staff Profiles")
    raw = fetch("https://calbears.com/staff-directory")
    if not raw: return []
    profiles = {}
    for m in re.finditer(r'href="(/staff-directory/([a-zA-Z0-9\-]+)/\d+)"', raw):
        path, slug = m.group(1), m.group(2)
        url = f"https://calbears.com{path}"
        if url not in profiles and 'tba' not in slug and 'tbd' not in slug:
            profiles[url] = slug.replace('-', ' ').title()
    print(f"    Found {len(profiles)} profiles")
    results, seen, count = [], set(), 0
    for url, name in list(profiles.items())[:400]:
        count += 1
        if count % 50 == 0:
            print(f"    [{count}/{min(400,len(profiles))}] scraped, {len(results)} emails...")
        pg = fetch(url, timeout=10)
        if pg:
            for email in get_emails(pg):
                if email not in seen:
                    seen.add(email)
                    results.append({'email': email, 'name': name,
                                  'department': 'Cal Bears Athletics Staff', 'source_url': url})
        time.sleep(0.15)
    print(f"    Cal Bears profiles: {len(results)} emails")
    return results


# ---- Cal Bears All Sports ----
def scrape_calbears_sports():
    print("\n  Phase 2: Cal Bears All Sports")
    sports = [
        "baseball","beach-volleyball","cross-country","field-hockey",
        "mens-golf","womens-golf","mens-gymnastics","womens-gymnastics",
        "lacrosse","mens-rowing","womens-rowing","rugby",
        "mens-soccer","womens-soccer","softball",
        "mens-tennis","womens-tennis","volleyball",
        "mens-water-polo","womens-water-polo",
    ]
    results, seen = [], set()
    for sport in sports:
        url = f"https://calbears.com/sports/{sport}/coaches"
        pg = fetch(url, timeout=10)
        if pg:
            emails = get_emails(pg)
            for email in emails:
                if email not in seen:
                    seen.add(email)
                    results.append({'email': email, 'name': find_name(html.unescape(pg), email),
                                  'department': f"Cal Bears - {sport.replace('-',' ').title()}",
                                  'source_url': url})
            if emails: print(f"    {sport}: {len(emails)} emails")
        time.sleep(0.3)
    print(f"    All sports: {len(results)} emails")
    return results


# ---- HKN ----
def scrape_hkn():
    print("\n  Phase 3: HKN Officers")
    results, seen = [], set()
    for page_url in ["https://hkn.eecs.berkeley.edu/about/officers",
                     "https://hkn.eecs.berkeley.edu/about/cmembers"]:
        pg = fetch(page_url)
        if not pg: continue
        d = html.unescape(pg)
        for m in re.finditer(r'([A-Z][a-zA-Z\s.\'-]{2,40}?)\s*\n?\s*([a-z_][a-z0-9_\.]*?)@hkn\b', d):
            name = re.sub(r'\s+', ' ', m.group(1).strip())
            username = m.group(2).strip()
            email = f"{username}@hkn.eecs.berkeley.edu"
            if email not in seen and not is_admin(email) and 2 < len(name) < 60 and '@' not in name:
                seen.add(email)
                results.append({'email': email, 'name': name,
                              'department': 'HKN (Eta Kappa Nu) - EECS', 'source_url': page_url})
        time.sleep(0.3)
    print(f"    HKN: {len(results)} emails")
    for r in results[:5]: print(f"      {r['name']:<30} {r['email']}")
    return results


# ---- Additional Sources ----
def scrape_additional():
    print("\n  Phase 4: Additional Sources")
    sources = [
        ("https://autolab.berkeley.edu/members", "AUTOLab"),
        ("https://autolab.berkeley.edu/people", "AUTOLab"),
        ("https://redwood.berkeley.edu/people/", "Redwood Center"),
        ("https://humancompatible.ai/people/", "CHAI"),
        ("https://humancompatible.ai/people", "CHAI"),
        ("https://bwrc.eecs.berkeley.edu/people", "Berkeley Wireless Research Center"),
        ("https://nuc.berkeley.edu/people/graduate-students", "Nuclear Engineering"),
        ("https://mse.berkeley.edu/people/graduate-students/", "Materials Science"),
        ("https://matrix.berkeley.edu/people/", "Social Science Matrix"),
        ("https://haas.berkeley.edu/phd/academics/accounting/", "Haas - Accounting PhD"),
        ("https://haas.berkeley.edu/phd/academics/finance/", "Haas - Finance PhD"),
        ("https://haas.berkeley.edu/phd/academics/marketing/", "Haas - Marketing PhD"),
        ("https://haas.berkeley.edu/faculty/", "Haas Business School"),
        ("https://gspp.berkeley.edu/", "Goldman School of Public Policy"),
        ("https://gspp.berkeley.edu/academics/doctoral-program", "GSPP Doctoral"),
        ("https://swe.berkeley.edu/", "Society of Women Engineers"),
        ("https://asuc.org/", "ASUC"),
        ("https://asuc.org/about", "ASUC"),
        ("https://optometry.berkeley.edu/research/", "School of Optometry"),
        ("https://optometry.berkeley.edu/faculty/", "School of Optometry"),
        ("https://ischool.berkeley.edu/people/phd-students", "School of Information"),
        ("https://ischool.berkeley.edu/people", "School of Information"),
        ("https://publichealth.berkeley.edu/people/faculty", "Public Health"),
        ("https://bioeng.berkeley.edu/people/graduate-students/", "Bioengineering"),
        ("https://ieor.berkeley.edu/people/graduate-students/", "IEOR"),
        ("https://are.berkeley.edu/people/graduate-students", "ARE"),
        ("https://are.berkeley.edu/people/", "ARE"),
        ("https://sociology.berkeley.edu/people/graduate-students", "Sociology"),
        ("https://polisci.berkeley.edu/people/graduate-students", "Political Science"),
        ("https://economics.berkeley.edu/people/grad-students", "Economics"),
        ("https://history.berkeley.edu/people/graduate-students", "History"),
        ("https://english.berkeley.edu/people/graduate-students", "English"),
        ("https://linguistics.berkeley.edu/people/graduate-students", "Linguistics"),
        ("https://music.berkeley.edu/people/graduate-students", "Music"),
        ("https://astro.berkeley.edu/people/graduate-student-directory/", "Astronomy"),
        ("https://integbi.berkeley.edu/people/graduate-students", "Integrative Biology"),
        ("https://mcb.berkeley.edu/people/graduate-students", "Molecular & Cell Biology"),
        ("https://plantbiology.berkeley.edu/people/graduate-students", "Plant Biology"),
        ("https://data.berkeley.edu/people", "Data Science"),
        ("https://cdss.berkeley.edu/people", "CDSS"),
    ]
    results, seen = [], set()
    for url, dept in sources:
        r = scrape_url(url, dept)
        new = 0
        for item in r:
            if item['email'] not in seen:
                seen.add(item['email'])
                results.append(item)
                new += 1
        if new > 0: print(f"    [{new:>3} new] {dept}")
        time.sleep(0.3)
    print(f"    Additional: {len(results)} emails")
    return results


def main():
    print("=" * 70)
    print("  UC BERKELEY SUPPLEMENTARY SCRAPER")
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
    def add(results):
        n = 0
        for r in results:
            e = r['email'].lower()
            if e not in existing_emails:
                existing_emails.add(e)
                all_new.append(r)
                n += 1
        return n

    for phase_fn in [scrape_calbears_profiles, scrape_calbears_sports, scrape_hkn, scrape_additional]:
        try:
            n = add(phase_fn())
            print(f"    -> {n} new unique")
        except Exception as e:
            print(f"    ERROR: {e}")

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
    print(f"  JSON: {json_path}")
    print(f"\n  COMPLETE. {len(final)} total emails.")


if __name__ == "__main__":
    main()
