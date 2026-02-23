#!/usr/bin/env python3
"""Test scraping VC firm team pages for partner emails."""

import requests
from bs4 import BeautifulSoup
import re
import json
import time

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
JUNK_PREFIXES = {'info@', 'hello@', 'contact@', 'support@', 'admin@', 'press@',
                 'media@', 'jobs@', 'careers@', 'hr@', 'legal@', 'privacy@',
                 'noreply@', 'no-reply@', 'webmaster@', 'feedback@', 'team@',
                 'office@', 'general@', 'inquiries@', 'help@'}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

VC_FIRMS = [
    {
        'name': 'a16z (Andreessen Horowitz)',
        'urls': [
            'https://a16z.com/about/',
            'https://a16z.com/team/',
        ],
    },
    {
        'name': 'Y Combinator',
        'urls': [
            'https://www.ycombinator.com/people',
            'https://www.ycombinator.com/about',
        ],
    },
    {
        'name': 'Sequoia Capital',
        'urls': [
            'https://www.sequoiacap.com/our-team/',
            'https://www.sequoiacap.com/people/',
        ],
    },
    {
        'name': 'First Round Capital',
        'urls': [
            'https://firstround.com/team/',
            'https://firstround.com/about/',
        ],
    },
    {
        'name': 'Greylock Partners',
        'urls': [
            'https://greylock.com/team/',
            'https://greylock.com/about/',
        ],
    },
    {
        'name': 'Founders Fund',
        'urls': [
            'https://foundersfund.com/team/',
            'https://foundersfund.com/about/',
        ],
    },
    {
        'name': 'Lightspeed Venture Partners',
        'urls': [
            'https://lsvp.com/team/',
            'https://lsvp.com/people/',
        ],
    },
    {
        'name': 'Bessemer Venture Partners',
        'urls': [
            'https://www.bvp.com/team',
            'https://www.bvp.com/people',
        ],
    },
    {
        'name': 'Khosla Ventures',
        'urls': [
            'https://www.khoslaventures.com/team/',
            'https://www.khoslaventures.com/about/',
        ],
    },
    {
        'name': 'Accel',
        'urls': [
            'https://www.accel.com/team',
            'https://www.accel.com/people',
        ],
    },
    {
        'name': 'NEA',
        'urls': [
            'https://www.nea.com/team',
            'https://www.nea.com/people',
        ],
    },
    {
        'name': 'Initialized Capital',
        'urls': [
            'https://initialized.com/team',
            'https://initialized.com/about',
        ],
    },
    {
        'name': 'Craft Ventures',
        'urls': [
            'https://www.craftventures.com/team',
            'https://www.craftventures.com/about',
        ],
    },
    {
        'name': 'Floodgate',
        'urls': [
            'https://floodgate.com/team/',
            'https://floodgate.com/about/',
        ],
    },
    {
        'name': 'Lux Capital',
        'urls': [
            'https://luxcapital.com/team/',
            'https://luxcapital.com/people/',
        ],
    },
]


def is_junk_email(email):
    email_lower = email.lower()
    for prefix in JUNK_PREFIXES:
        if email_lower.startswith(prefix):
            return True
    junk_domains = {'example.com', 'sentry.io', 'gmail.com', 'yahoo.com',
                    'hotmail.com', 'googlemail.com', 'w3.org', 'schema.org',
                    'wordpress.org', 'gravatar.com', 'wixpress.com'}
    domain = email_lower.split('@')[1] if '@' in email_lower else ''
    if domain in junk_domains:
        return True
    if email_lower.endswith('.png') or email_lower.endswith('.jpg') or email_lower.endswith('.svg'):
        return True
    return False


def extract_emails_from_page(url, session):
    """Extract emails from a single page using multiple strategies."""
    emails = set()
    names_for_emails = {}

    try:
        resp = session.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        if resp.status_code != 200:
            return emails, names_for_emails, resp.status_code, len(resp.text)

        soup = BeautifulSoup(resp.text, 'html.parser')
        page_text = soup.get_text()
        html_text = resp.text

        # Strategy 1: mailto links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if 'mailto:' in href:
                email = href.split('mailto:')[1].split('?')[0].strip()
                if EMAIL_RE.match(email) and not is_junk_email(email):
                    emails.add(email.lower())
                    # Try to get name from nearby text
                    parent = a_tag.find_parent(['div', 'li', 'td', 'article', 'section'])
                    if parent:
                        for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'span']):
                            name_text = tag.get_text(strip=True)
                            if name_text and len(name_text) < 50 and '@' not in name_text:
                                names_for_emails[email.lower()] = name_text
                                break

        # Strategy 2: regex on page text
        for match in EMAIL_RE.findall(page_text):
            if not is_junk_email(match):
                emails.add(match.lower())

        # Strategy 3: regex on raw HTML (catches emails in attributes, scripts, etc.)
        for match in EMAIL_RE.findall(html_text):
            if not is_junk_email(match):
                emails.add(match.lower())

        # Strategy 4: obfuscated emails [at] [dot]
        obfuscated = re.findall(r'[\w.+-]+\s*\[?\s*(?:at|AT)\s*\]?\s*[\w.-]+\s*\[?\s*(?:dot|DOT)\s*\]?\s*\w+', page_text)
        for ob in obfuscated:
            cleaned = re.sub(r'\s*\[?\s*(?:at|AT)\s*\]?\s*', '@', ob)
            cleaned = re.sub(r'\s*\[?\s*(?:dot|DOT)\s*\]?\s*', '.', cleaned)
            if EMAIL_RE.match(cleaned) and not is_junk_email(cleaned):
                emails.add(cleaned.lower())

        # Strategy 5: Cloudflare email protection
        for tag in soup.find_all(attrs={'data-cfemail': True}):
            encoded = tag['data-cfemail']
            try:
                key = int(encoded[:2], 16)
                decoded = ''.join(chr(int(encoded[i:i+2], 16) ^ key) for i in range(2, len(encoded), 2))
                if EMAIL_RE.match(decoded) and not is_junk_email(decoded):
                    emails.add(decoded.lower())
            except Exception:
                pass

        return emails, names_for_emails, resp.status_code, len(resp.text)

    except Exception as e:
        return emails, names_for_emails, str(e), 0


def extract_people_names(url, session):
    """Extract names from a team page even if no emails found."""
    names = []
    try:
        resp = session.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        if resp.status_code != 200:
            return names

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Look for common team page patterns
        for card in soup.find_all(['div', 'li', 'article'], class_=re.compile(r'team|member|person|partner|people|staff|bio', re.I)):
            name_tag = card.find(['h2', 'h3', 'h4', 'h5', 'strong', 'a'])
            if name_tag:
                name = name_tag.get_text(strip=True)
                if name and len(name) < 60 and '@' not in name and not name.startswith('http'):
                    title_tag = card.find(['p', 'span', 'div'], class_=re.compile(r'title|role|position|job', re.I))
                    title = title_tag.get_text(strip=True) if title_tag else ''
                    names.append({'name': name, 'title': title})

        return names
    except Exception:
        return names


def main():
    session = requests.Session()
    results = {}

    print(f"\n{'=' * 70}")
    print(f"  VC FIRM TEAM PAGE SCRAPING TEST")
    print(f"  Testing {len(VC_FIRMS)} firms")
    print(f"{'=' * 70}\n")

    for firm in VC_FIRMS:
        firm_name = firm['name']
        all_emails = set()
        all_names = {}
        people_found = []
        url_results = []

        print(f"  {firm_name}")

        for url in firm['urls']:
            emails, names, status, size = extract_emails_from_page(url, session)
            all_emails.update(emails)
            all_names.update(names)

            # Also try to get people names
            people = extract_people_names(url, session)
            people_found.extend(people)

            url_results.append({
                'url': url,
                'status': status,
                'page_size': size,
                'emails_found': len(emails),
            })

            print(f"    {url}")
            print(f"      Status: {status}, Size: {size:,} bytes, Emails: {len(emails)}")

            time.sleep(0.5)

        # Dedupe people
        seen_names = set()
        unique_people = []
        for p in people_found:
            if p['name'] not in seen_names:
                seen_names.add(p['name'])
                unique_people.append(p)

        results[firm_name] = {
            'emails': list(all_emails),
            'email_names': all_names,
            'people': unique_people[:20],  # cap at 20
            'url_results': url_results,
        }

        if all_emails:
            print(f"    EMAILS FOUND: {list(all_emails)[:10]}")
        else:
            print(f"    NO EMAILS FOUND")
            if unique_people:
                print(f"    But found {len(unique_people)} people names (could guess emails)")
                for p in unique_people[:5]:
                    print(f"      - {p['name']} {('(' + p['title'] + ')') if p['title'] else ''}")

        print()

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  SUMMARY")
    print(f"{'=' * 70}")

    total_emails = 0
    firms_with_emails = 0
    firms_with_people = 0

    for firm_name, data in results.items():
        n_emails = len(data['emails'])
        n_people = len(data['people'])
        total_emails += n_emails
        if n_emails > 0:
            firms_with_emails += 1
        if n_people > 0:
            firms_with_people += 1

        status = f"{n_emails} emails" if n_emails > 0 else f"0 emails, {n_people} names"
        print(f"  {firm_name}: {status}")

    print(f"\n  Total: {total_emails} emails from {firms_with_emails}/{len(VC_FIRMS)} firms")
    print(f"  Firms with people names (for email guessing): {firms_with_people}/{len(VC_FIRMS)}")
    print()

    # Save results
    with open('test_vc_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    print(f"  Detailed results saved to test_vc_results.json\n")


if __name__ == '__main__':
    main()
