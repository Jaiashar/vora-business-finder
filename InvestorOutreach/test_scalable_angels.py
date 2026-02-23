#!/usr/bin/env python3
"""
The actual scalable approach for angel investor emails:

SOURCE A: AngelList data (400 names + websites) -> extract domains -> email patterns
SOURCE B: Playwright scrape of angelmatch.io health investors (structured data)
SOURCE C: Playwright scrape of ramp.com health/wellness investor database
SOURCE D: GitHub curated email lists (swyxio, contact-angel-investors)
SOURCE E: Our Crunchbase Bing pipeline for additional health angel names

For each source we extract: name, company/fund, domain, email (if available)
For those with domains but no email, we generate first.last@domain patterns.
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import csv
import io
import time
from urllib.parse import urlparse

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}


def extract_domain(url):
    """Extract clean domain from URL."""
    if not url:
        return None
    try:
        if not url.startswith('http'):
            url = 'http://' + url
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        # Skip generic domains
        skip = {'angel.co', 'linkedin.com', 'twitter.com', 'facebook.com',
                'github.com', 'medium.com', 'wordpress.com', 'blogspot.com',
                'youtube.com', 'instagram.com', 'crunchbase.com', 'wellfound.com',
                'google.com', 'about.me', 'substack.com', ''}
        if domain in skip:
            return None
        if not '.' in domain:
            return None
        return domain
    except Exception:
        return None


def parse_name(full_name):
    """Split a full name into first and last."""
    if not full_name:
        return None, None
    parts = full_name.strip().split()
    if len(parts) < 2:
        return full_name, None
    return parts[0], parts[-1]


def generate_patterns(first, last, domain):
    """Generate email pattern candidates."""
    if not first or not last or not domain:
        return []
    f = first.lower().strip()
    l = last.lower().strip()
    return [
        f'{f}.{l}@{domain}',
        f'{f}@{domain}',
        f'{f[0]}{l}@{domain}',
        f'{f}{l}@{domain}',
        f'{f}{l[0]}@{domain}',
        f'{f}_{l}@{domain}',
    ]


def main():
    print(f"\n{'=' * 65}")
    print(f"  SCALABLE ANGEL INVESTOR EMAIL PIPELINE")
    print(f"  Multi-source aggregation approach")
    print(f"{'=' * 65}")

    all_investors = []  # List of {name, company, domain, email, source, patterns}

    # ══════════════════════════════════════════════════════════════════
    # SOURCE A: AngelList GitHub dataset (400 names + websites)
    # ══════════════════════════════════════════════════════════════════
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE A: AngelList GitHub Dataset (400 names)")
    print(f"  {'═' * 55}\n")

    url = 'https://raw.githubusercontent.com/VirenMohindra/AngelList/master/investor_names.csv'
    resp = requests.get(url, timeout=15)
    if resp.status_code == 200:
        reader = csv.DictReader(io.StringIO(resp.text))
        count = 0
        with_domain = 0
        for row in reader:
            name = row.get('Name', '').strip()
            website = row.get('Website', '').strip()
            angellist_url = row.get('AngelList URL', '').strip()
            domain = extract_domain(website)

            first, last = parse_name(name)
            patterns = generate_patterns(first, last, domain) if domain else []

            inv = {
                'name': name,
                'first': first,
                'last': last,
                'company': None,
                'domain': domain,
                'website': website,
                'email': None,
                'patterns': patterns,
                'source': 'angellist_github',
            }
            all_investors.append(inv)
            count += 1
            if domain:
                with_domain += 1

        print(f"    Loaded {count} investors, {with_domain} with usable domains ({with_domain/count*100:.0f}%)")
        # Show some examples
        with_dom = [i for i in all_investors if i['domain']]
        for inv in with_dom[:10]:
            print(f"      {inv['name']:30s} | {inv['domain']:25s} | {inv['patterns'][0] if inv['patterns'] else 'no pattern'}")
    else:
        print(f"    Failed to fetch: {resp.status_code}")

    # ══════════════════════════════════════════════════════════════════
    # SOURCE B: Playwright scrape of angelmatch.io health investors
    # ══════════════════════════════════════════════════════════════════
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE B: Angelmatch.io Health Investors (Playwright)")
    print(f"  {'═' * 55}\n")

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')

            urls_to_scrape = [
                'https://angelmatch.io/investors/by-market/health-care',
                'https://angelmatch.io/investors/by-market/wellness',
                'https://angelmatch.io/investors/by-market/fitness',
                'https://angelmatch.io/investors/by-market/digital-health',
            ]

            angelmatch_count = 0
            for url in urls_to_scrape:
                try:
                    page.goto(url, wait_until='networkidle', timeout=20000)
                    time.sleep(2)

                    # Scroll to load all content
                    for _ in range(5):
                        page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                        time.sleep(1)

                    # Extract investor cards/rows
                    content = page.content()
                    soup = BeautifulSoup(content, 'html.parser')

                    # Look for investor name elements (common patterns)
                    for tag in soup.find_all(['h2', 'h3', 'h4', 'a']):
                        text = tag.get_text(strip=True)
                        href = tag.get('href', '')
                        words = text.split()
                        if 2 <= len(words) <= 4 and all(w[0].isupper() for w in words if len(w) > 1):
                            first, last = parse_name(text)
                            if first and last:
                                all_investors.append({
                                    'name': text,
                                    'first': first,
                                    'last': last,
                                    'company': None,
                                    'domain': None,
                                    'website': href if href.startswith('http') else None,
                                    'email': None,
                                    'patterns': [],
                                    'source': 'angelmatch',
                                })
                                angelmatch_count += 1

                    # Also look for table rows
                    for tr in soup.find_all('tr'):
                        cells = tr.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            name_text = cells[0].get_text(strip=True)
                            words = name_text.split()
                            if 2 <= len(words) <= 4:
                                first, last = parse_name(name_text)
                                if first and last:
                                    company = cells[1].get_text(strip=True) if len(cells) > 1 else None
                                    all_investors.append({
                                        'name': name_text,
                                        'first': first,
                                        'last': last,
                                        'company': company,
                                        'domain': None,
                                        'website': None,
                                        'email': None,
                                        'patterns': [],
                                        'source': 'angelmatch',
                                    })
                                    angelmatch_count += 1

                    print(f"    {url.split('/')[-1]}: extracted {angelmatch_count} investor names so far")
                except Exception as e:
                    print(f"    Error on {url}: {str(e)[:50]}")
                time.sleep(1)

            # ══════════════════════════════════════════════════════════════════
            # SOURCE C: Ramp.com health/wellness investor database
            # ══════════════════════════════════════════════════════════════════
            print(f"\n  {'═' * 55}")
            print(f"  SOURCE C: Ramp.com Health/Wellness Investor DB (Playwright)")
            print(f"  {'═' * 55}\n")

            ramp_urls = [
                'https://ramp.com/vc-database/health-wellness-vc-angel-list',
                'https://ramp.com/vc-database/angel-investor-list',
            ]

            ramp_count = 0
            for url in ramp_urls:
                try:
                    page.goto(url, wait_until='networkidle', timeout=20000)
                    time.sleep(3)

                    # Scroll to load
                    for _ in range(8):
                        page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                        time.sleep(1)

                    content = page.content()
                    soup = BeautifulSoup(content, 'html.parser')

                    # Ramp usually has structured investor cards
                    # Look for name patterns in various elements
                    for tag in soup.find_all(['h2', 'h3', 'h4', 'p', 'span', 'div']):
                        text = tag.get_text(strip=True)
                        # Look for "Name at Company" or "Name - Title" patterns
                        if ' at ' in text or ' - ' in text:
                            parts = text.replace(' - ', ' at ').split(' at ')
                            if len(parts) >= 2:
                                name = parts[0].strip()
                                company = parts[1].strip()
                                words = name.split()
                                if 2 <= len(words) <= 3:
                                    first, last = parse_name(name)
                                    if first and last:
                                        all_investors.append({
                                            'name': name,
                                            'first': first,
                                            'last': last,
                                            'company': company,
                                            'domain': None,
                                            'website': None,
                                            'email': None,
                                            'patterns': [],
                                            'source': 'ramp',
                                        })
                                        ramp_count += 1

                    # Also try extracting from any table structure
                    for table in soup.find_all('table'):
                        rows = table.find_all('tr')
                        for row in rows[1:]:  # Skip header
                            cells = row.find_all(['td', 'th'])
                            if len(cells) >= 2:
                                name_text = cells[0].get_text(strip=True)
                                words = name_text.split()
                                if 2 <= len(words) <= 4:
                                    first, last = parse_name(name_text)
                                    company = cells[1].get_text(strip=True) if len(cells) > 1 else None
                                    domain_text = None
                                    # Check cells for website/domain
                                    for cell in cells:
                                        for a in cell.find_all('a', href=True):
                                            d = extract_domain(a['href'])
                                            if d:
                                                domain_text = d
                                                break

                                    patterns = generate_patterns(first, last, domain_text) if domain_text else []
                                    all_investors.append({
                                        'name': name_text,
                                        'first': first,
                                        'last': last,
                                        'company': company,
                                        'domain': domain_text,
                                        'website': None,
                                        'email': None,
                                        'patterns': patterns,
                                        'source': 'ramp',
                                    })
                                    ramp_count += 1

                    print(f"    {url.split('/')[-1]}: extracted {ramp_count} investors so far")
                except Exception as e:
                    print(f"    Error on {url}: {str(e)[:50]}")
                time.sleep(1)

            browser.close()
    except ImportError:
        print("    Playwright not available")

    # ══════════════════════════════════════════════════════════════════
    # SOURCE D: GitHub curated email lists
    # ══════════════════════════════════════════════════════════════════
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE D: GitHub Curated Email Lists")
    print(f"  {'═' * 55}\n")

    # swyxio/devtools-angels
    url = 'https://raw.githubusercontent.com/swyxio/devtools-angels/main/README.md'
    resp = requests.get(url, timeout=10)
    if resp.status_code == 200:
        emails = EMAIL_RE.findall(resp.text)
        for email in emails:
            # Try to extract the name associated with this email from surrounding context
            idx = resp.text.find(email)
            context = resp.text[max(0, idx-200):idx]
            name_match = re.findall(r'\[([A-Z][a-z]+ [A-Z][a-z]+)\]', context)
            name = name_match[-1] if name_match else 'Unknown'
            first, last = parse_name(name)
            domain = email.split('@')[1]
            all_investors.append({
                'name': name,
                'first': first,
                'last': last,
                'company': None,
                'domain': domain,
                'website': None,
                'email': email.lower(),
                'patterns': [],
                'source': 'github_swyxio',
            })
        print(f"    swyxio/devtools-angels: {len(emails)} emails")
    else:
        print(f"    swyxio: status {resp.status_code}")

    # ishandutta2007/contact-angel-investors
    url = 'https://raw.githubusercontent.com/ishandutta2007/contact-angel-investors/master/email-list.csv'
    resp = requests.get(url, timeout=10)
    if resp.status_code == 200:
        emails = list(set(EMAIL_RE.findall(resp.text)))
        for email in emails:
            all_investors.append({
                'name': email.split('@')[0],
                'first': None,
                'last': None,
                'company': None,
                'domain': email.split('@')[1],
                'website': None,
                'email': email.lower(),
                'patterns': [],
                'source': 'github_contact_angels',
            })
        print(f"    contact-angel-investors: {len(emails)} emails")
    else:
        print(f"    contact-angel-investors: status {resp.status_code}")

    # ══════════════════════════════════════════════════════════════════
    # SOURCE E: Bing Crunchbase + health angel search
    # ══════════════════════════════════════════════════════════════════
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE E: Bing Search for Health Angel Investors")
    print(f"  {'═' * 55}\n")

    from ddgs import DDGS

    queries = [
        'site:crunchbase.com/person angel investor health',
        'site:crunchbase.com/person angel investor wellness longevity',
        'site:crunchbase.com/person angel investor "digital health"',
        'site:crunchbase.com/person angel investor fitness wearable',
        'site:crunchbase.com/person angel investor seed health',
        '"angel investor" "health" OR "wellness" portfolio investments email',
    ]

    bing_names = set()
    for q in queries:
        try:
            results = DDGS().text(q, max_results=20, backend='bing')
            for r in results:
                url = r.get('href', '')
                title = r.get('title', '')
                if 'crunchbase.com/person' in url:
                    name = title.split(' - ')[0].split(' | ')[0].strip()
                    if name and len(name.split()) >= 2:
                        bing_names.add(name)
            print(f"    '{q[:50]}...' -> {sum(1 for r in results if 'crunchbase.com/person' in r.get('href',''))} profiles")
        except Exception:
            pass
        time.sleep(1.5)

    for name in bing_names:
        first, last = parse_name(name)
        all_investors.append({
            'name': name,
            'first': first,
            'last': last,
            'company': None,
            'domain': None,
            'website': None,
            'email': None,
            'patterns': [],
            'source': 'bing_crunchbase',
        })
    print(f"    Total unique names from Bing: {len(bing_names)}")

    # ══════════════════════════════════════════════════════════════════
    # ANALYSIS & SUMMARY
    # ══════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 65}")
    print(f"  AGGREGATION RESULTS")
    print(f"{'=' * 65}\n")

    # Deduplicate by name
    seen_names = set()
    unique_investors = []
    for inv in all_investors:
        key = inv['name'].lower().strip()
        if key not in seen_names and len(key) > 3:
            seen_names.add(key)
            unique_investors.append(inv)

    # Categorize
    with_email = [i for i in unique_investors if i['email']]
    with_domain = [i for i in unique_investors if i['domain'] and not i['email']]
    with_patterns = [i for i in unique_investors if i['patterns'] and not i['email']]
    names_only = [i for i in unique_investors if not i['email'] and not i['domain']]

    by_source = {}
    for inv in unique_investors:
        src = inv['source']
        by_source[src] = by_source.get(src, 0) + 1

    print(f"  Total unique investors: {len(unique_investors)}")
    print(f"  With direct email:     {len(with_email)}")
    print(f"  With domain + patterns:{len(with_patterns)}")
    print(f"  Names only (no domain):{len(names_only)}")
    print()
    print(f"  By source:")
    for src, count in sorted(by_source.items(), key=lambda x: -x[1]):
        print(f"    {src:25s}: {count}")

    print(f"\n  Direct emails:")
    for inv in with_email:
        print(f"    {inv['name']:30s} | {inv['email']}")

    print(f"\n  With domain patterns (first 20):")
    for inv in with_patterns[:20]:
        print(f"    {inv['name']:30s} | {inv['domain']:25s} | {inv['patterns'][0]}")

    # Save everything
    with open('test_scalable_results.json', 'w') as f:
        json.dump({
            'summary': {
                'total_unique': len(unique_investors),
                'with_email': len(with_email),
                'with_patterns': len(with_patterns),
                'names_only': len(names_only),
                'by_source': by_source,
            },
            'investors': unique_investors,
        }, f, indent=2)

    # Save CSV for easy review
    with open('test_scalable_results.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['name', 'first', 'last', 'company', 'domain', 'email', 'best_guess_email', 'source'])
        for inv in unique_investors:
            best_guess = inv['email'] or (inv['patterns'][0] if inv['patterns'] else '')
            writer.writerow([
                inv['name'], inv['first'], inv['last'], inv['company'],
                inv['domain'], inv['email'], best_guess, inv['source'],
            ])

    print(f"\n  Saved to test_scalable_results.json and test_scalable_results.csv\n")


if __name__ == '__main__':
    main()
