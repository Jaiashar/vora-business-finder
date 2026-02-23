#!/usr/bin/env python3
"""
Fetch actual data from the goldmine sources found:
1. GitHub: ishandutta2007/contact-angel-investors (has email-list.csv!)
2. GitHub: nexly-learning-solutions/vc (has angel investor CSVs!)
3. GitHub: Nutlope/aiangels (AI angel investors directory)
4. GitHub: swyxio/devtools-angels (active angel investors)
5. Public investor databases: 50pros, angelmatch, eqvista, neerajkroy, ramp
6. Google Sheets: "Venture Capital and Angel Investor List 3000+"
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import csv
import io
import time

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


def fetch_raw(url, max_size=5000000):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        if resp.status_code == 200 and len(resp.content) <= max_size:
            return resp.text, resp.status_code
        return '', resp.status_code
    except Exception as e:
        return '', str(e)[:50]


def main():
    print(f"\n{'=' * 65}")
    print(f"  GOLDMINE SOURCES - Fetching Actual Data")
    print(f"{'=' * 65}")

    all_data = {}

    # ──────────────────────────────────────────────────────────────
    # SOURCE 1: ishandutta2007/contact-angel-investors
    # ──────────────────────────────────────────────────────────────
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE 1: contact-angel-investors (GitHub)")
    print(f"  {'═' * 55}\n")

    # Get repo file list first
    repo_api = 'https://api.github.com/repos/ishandutta2007/contact-angel-investors/contents'
    resp = requests.get(repo_api, timeout=10)
    if resp.status_code == 200:
        files = resp.json()
        data_files = [f for f in files if f['name'].endswith(('.csv', '.json', '.txt'))]
        print(f"    Data files in repo:")
        for f in data_files:
            print(f"      {f['name']} ({f.get('size', '?')} bytes)")

        # Fetch email-list.csv
        for f in data_files:
            if 'email' in f['name'].lower() or f['name'].endswith('.csv'):
                raw_url = f.get('download_url', '')
                if raw_url:
                    content, status = fetch_raw(raw_url)
                    if content:
                        lines = content.strip().split('\n')
                        emails = []
                        for line in lines:
                            for match in EMAIL_RE.findall(line):
                                emails.append(match.lower())
                        print(f"\n    {f['name']}: {len(lines)} lines, {len(emails)} emails")
                        if emails:
                            for e in emails[:20]:
                                print(f"      {e}")
                            if len(emails) > 20:
                                print(f"      ... and {len(emails) - 20} more")
                            all_data['contact_angel_investors'] = emails
    else:
        print(f"    API status: {resp.status_code}")

    time.sleep(1)

    # ──────────────────────────────────────────────────────────────
    # SOURCE 2: nexly-learning-solutions/vc
    # ──────────────────────────────────────────────────────────────
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE 2: nexly-learning-solutions/vc (GitHub)")
    print(f"  {'═' * 55}\n")

    repo_api = 'https://api.github.com/repos/nexly-learning-solutions/vc/contents'
    resp = requests.get(repo_api, timeout=10)
    if resp.status_code == 200:
        files = resp.json()
        csv_files = [f for f in files if f['name'].endswith('.csv')]
        print(f"    CSV files: {len(csv_files)}")
        for f in csv_files:
            print(f"      {f['name']} ({f.get('size', '?')} bytes)")

        # Fetch angel-related CSVs
        all_angel_data = []
        for f in csv_files:
            name_lower = f['name'].lower()
            if 'angel' in name_lower or 'health' in name_lower or 'seed' in name_lower:
                raw_url = f.get('download_url', '')
                if raw_url:
                    content, status = fetch_raw(raw_url)
                    if content:
                        reader = csv.reader(io.StringIO(content))
                        rows = list(reader)
                        headers_row = rows[0] if rows else []
                        print(f"\n    {f['name']}:")
                        print(f"      Headers: {headers_row[:8]}")
                        print(f"      Rows: {len(rows) - 1}")

                        # Find email column
                        email_col = None
                        for i, h in enumerate(headers_row):
                            if 'email' in h.lower():
                                email_col = i
                                break

                        # Find name columns
                        name_col = None
                        for i, h in enumerate(headers_row):
                            if 'name' in h.lower() and 'company' not in h.lower():
                                name_col = i
                                break

                        emails_found = []
                        for row in rows[1:]:
                            email = row[email_col].strip() if email_col is not None and email_col < len(row) else ''
                            name = row[name_col].strip() if name_col is not None and name_col < len(row) else ''
                            if email and EMAIL_RE.match(email):
                                emails_found.append({'name': name, 'email': email.lower()})
                                all_angel_data.append({'name': name, 'email': email.lower(), 'source': f['name']})

                        print(f"      Emails found: {len(emails_found)}")
                        for item in emails_found[:10]:
                            print(f"        {item['name']:30s} | {item['email']}")
                        if len(emails_found) > 10:
                            print(f"        ... and {len(emails_found) - 10} more")

                time.sleep(0.5)

        all_data['nexly_vc'] = all_angel_data
    else:
        print(f"    API status: {resp.status_code}")

    time.sleep(1)

    # ──────────────────────────────────────────────────────────────
    # SOURCE 3: Nutlope/aiangels
    # ──────────────────────────────────────────────────────────────
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE 3: Nutlope/aiangels (GitHub)")
    print(f"  {'═' * 55}\n")

    repo_api = 'https://api.github.com/repos/Nutlope/aiangels/contents'
    resp = requests.get(repo_api, timeout=10)
    if resp.status_code == 200:
        files = resp.json()
        print(f"    Files:")
        for f in files:
            print(f"      {f['name']} ({f.get('size', '?')} bytes)")

        # Look for data or README
        for f in files:
            if f['name'].lower().startswith('readme') or f['name'].endswith(('.csv', '.json', '.md')):
                raw_url = f.get('download_url', '')
                if raw_url:
                    content, _ = fetch_raw(raw_url, 200000)
                    if content:
                        emails = [e.lower() for e in EMAIL_RE.findall(content)]
                        # Also extract names from markdown headers
                        names = re.findall(r'#+\s*\[?([A-Z][a-z]+ [A-Z][a-z]+)', content)
                        print(f"\n    {f['name']}: {len(emails)} emails, {len(names)} names")
                        for e in emails[:10]:
                            print(f"      EMAIL: {e}")
                        for n in names[:10]:
                            print(f"      NAME: {n}")
                        if names:
                            all_data['aiangels_names'] = names
    else:
        print(f"    API status: {resp.status_code}")

    time.sleep(1)

    # ──────────────────────────────────────────────────────────────
    # SOURCE 4: swyxio/devtools-angels
    # ──────────────────────────────────────────────────────────────
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE 4: swyxio/devtools-angels (GitHub)")
    print(f"  {'═' * 55}\n")

    repo_api = 'https://api.github.com/repos/swyxio/devtools-angels/contents'
    resp = requests.get(repo_api, timeout=10)
    if resp.status_code == 200:
        files = resp.json()
        for f in files:
            if f['name'].lower().startswith('readme') or f['name'].endswith(('.csv', '.json', '.md')):
                raw_url = f.get('download_url', '')
                if raw_url:
                    content, _ = fetch_raw(raw_url, 200000)
                    if content:
                        emails = [e.lower() for e in EMAIL_RE.findall(content)]
                        names = re.findall(r'\[([A-Z][a-z]+ [A-Z][a-z]+)\]', content)
                        print(f"    {f['name']}: {len(emails)} emails, {len(names)} names")
                        for e in emails[:10]:
                            print(f"      EMAIL: {e}")
                        for n in names[:10]:
                            print(f"      NAME: {n}")
    else:
        print(f"    API status: {resp.status_code}")

    time.sleep(1)

    # ──────────────────────────────────────────────────────────────
    # SOURCE 5: Public Investor List Websites
    # ──────────────────────────────────────────────────────────────
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE 5: Public Investor List Websites")
    print(f"  {'═' * 55}\n")

    websites = [
        ('50pros', 'https://www.50pros.com/data/investor-database'),
        ('angelmatch health', 'https://angelmatch.io/investors/by-market/health-care'),
        ('eqvista top100', 'https://eqvista.com/top-100-active-angel-investors-list-for-startups/'),
        ('neerajkroy 1000', 'https://www.neerajkroy.com/post/1000-active-angel-and-vc-investors'),
        ('startupmodels', 'https://startupmodels.io/startup-investor-list'),
        ('ramp angels', 'https://ramp.com/vc-database/angel-investor-list'),
        ('ramp health', 'https://ramp.com/vc-database/health-wellness-vc-angel-list'),
        ('funderintel', 'https://www.funderintel.com/download-angel-investor-vc-list'),
    ]

    for name, url in websites:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
            if resp.status_code != 200:
                print(f"    {name}: status {resp.status_code}")
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')
            text = soup.get_text()

            # Extract emails
            emails = set()
            for match in EMAIL_RE.findall(resp.text):
                emails.add(match.lower())
            for a in soup.find_all('a', href=True):
                href = a.get('href', '')
                if 'mailto:' in href:
                    email = href.split('mailto:')[1].split('?')[0].strip()
                    if EMAIL_RE.match(email):
                        emails.add(email.lower())

            # Count investor names (rough estimate from h2/h3/bold text)
            investor_names = []
            for tag in soup.find_all(['h2', 'h3', 'h4', 'strong', 'b']):
                t = tag.get_text(strip=True)
                words = t.split()
                if 2 <= len(words) <= 4 and all(w[0].isupper() for w in words[:2] if len(w) > 1):
                    investor_names.append(t)

            # Also check for table data
            tables = soup.find_all('table')
            table_rows = 0
            for table in tables:
                rows = table.find_all('tr')
                table_rows += len(rows)

            print(f"    {name:20s}: {len(emails):3d} emails, ~{len(investor_names)} names, {table_rows} table rows, {len(resp.text):,} bytes")
            if emails:
                for e in list(emails)[:5]:
                    print(f"      {e}")

        except Exception as e:
            print(f"    {name}: Error - {str(e)[:50]}")
        time.sleep(0.5)

    # ──────────────────────────────────────────────────────────────
    # SOURCE 6: Google Sheets
    # ──────────────────────────────────────────────────────────────
    print(f"\n  {'═' * 55}")
    print(f"  SOURCE 6: Google Sheets Investor Lists")
    print(f"  {'═' * 55}\n")

    sheets = [
        ('3000+ VC/Angels', 'https://docs.google.com/spreadsheets/d/1vORRulYePR9jX2evoqqaTCH7blVouou0v3OYQJnWrds/gviz/tq?tqx=out:csv'),
        ('Angel Investing', 'https://docs.google.com/spreadsheets/d/13z_k0yGjdrJFZ3BFjbT2zFpKHqKseQnRfhTbs55bbvQ/gviz/tq?tqx=out:csv'),
        ('All Investors', 'https://docs.google.com/spreadsheets/d/1zNueN4zkaS-_1KH8JqKCgDxltGgcb2kc--HmGGQZe-4/gviz/tq?tqx=out:csv'),
    ]

    for name, url in sheets:
        content, status = fetch_raw(url)
        if content:
            reader = csv.reader(io.StringIO(content))
            rows = list(reader)
            headers_row = rows[0] if rows else []

            # Find email column
            email_col = None
            name_cols = []
            for i, h in enumerate(headers_row):
                h_lower = h.lower()
                if 'email' in h_lower:
                    email_col = i
                if 'name' in h_lower:
                    name_cols.append(i)

            emails = []
            for row in rows[1:]:
                if email_col is not None and email_col < len(row):
                    email = row[email_col].strip()
                    if email and EMAIL_RE.match(email):
                        name_parts = []
                        for nc in name_cols:
                            if nc < len(row):
                                name_parts.append(row[nc].strip())
                        emails.append({'name': ' '.join(name_parts), 'email': email.lower()})

            print(f"    {name:20s}: {len(rows)-1} rows, headers: {headers_row[:6]}")
            print(f"      Email col: {email_col}, emails found: {len(emails)}")
            if emails:
                for item in emails[:10]:
                    print(f"        {item['name']:30s} | {item['email']}")
                if len(emails) > 10:
                    print(f"        ... and {len(emails) - 10} more")
                all_data[f'sheet_{name}'] = emails
        else:
            print(f"    {name}: status {status}")

        time.sleep(1)

    # ──────────────────────────────────────────────────────────────
    # FINAL SUMMARY
    # ──────────────────────────────────────────────────────────────
    print(f"\n{'=' * 65}")
    print(f"  GOLDMINE SOURCES SUMMARY")
    print(f"{'=' * 65}\n")

    total_emails = set()
    for source, data in all_data.items():
        if isinstance(data, list) and data:
            if isinstance(data[0], dict):
                emails = [d['email'] for d in data if 'email' in d]
            else:
                emails = data
            total_emails.update(emails)
            print(f"  {source:35s}: {len(emails)} emails/contacts")

    print(f"\n  TOTAL UNIQUE EMAILS FROM GOLDMINE SOURCES: {len(total_emails)}")

    with open('test_goldmine_results.json', 'w') as f:
        json.dump(all_data, f, indent=2, default=str)
    print(f"  Saved to test_goldmine_results.json\n")


if __name__ == '__main__':
    main()
