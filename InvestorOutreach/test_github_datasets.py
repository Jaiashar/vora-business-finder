#!/usr/bin/env python3
"""
Search GitHub for public angel investor datasets, investor lists, and scraping tools.
Also search for curated investor spreadsheets and CSV data dumps.
"""

import requests
import json
import time
import re

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept': 'application/vnd.github.v3+json',
}

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')


def search_github_repos(query, per_page=30):
    """Search GitHub repos via API."""
    try:
        resp = requests.get(
            'https://api.github.com/search/repositories',
            params={'q': query, 'sort': 'stars', 'per_page': per_page},
            headers=HEADERS,
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json().get('items', [])
        else:
            print(f"    GitHub API {resp.status_code}: {resp.text[:100]}")
            return []
    except Exception as e:
        print(f"    Error: {e}")
        return []


def search_github_code(query, per_page=30):
    """Search GitHub code."""
    try:
        resp = requests.get(
            'https://api.github.com/search/code',
            params={'q': query, 'per_page': per_page},
            headers=HEADERS,
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json().get('items', [])
        else:
            print(f"    Code search {resp.status_code}")
            return []
    except Exception as e:
        print(f"    Error: {e}")
        return []


def fetch_raw_content(url, max_size=500000):
    """Fetch raw file content from GitHub."""
    try:
        resp = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if resp.status_code == 200 and len(resp.content) <= max_size:
            return resp.text
        return ''
    except Exception:
        return ''


def main():
    session = requests.Session()

    print(f"\n{'=' * 65}")
    print(f"  GITHUB DATASET SEARCH")
    print(f"{'=' * 65}")

    # ──────────────────────────────────────────────────────────────
    # Part 1: Search for repos with angel investor data
    # ──────────────────────────────────────────────────────────────
    print(f"\n  {'─' * 55}")
    print(f"  PART 1: GitHub Repo Search")
    print(f"  {'─' * 55}\n")

    queries = [
        'angel investors list',
        'investor database email',
        'vc angel investor dataset',
        'startup investor contacts',
        'crunchbase investors scraper',
        'angel investor scraper',
        'investor email list',
        'health investor contacts',
    ]

    all_repos = []
    for q in queries:
        repos = search_github_repos(q, 10)
        for repo in repos:
            all_repos.append({
                'name': repo['full_name'],
                'description': (repo.get('description') or '')[:100],
                'stars': repo['stargazers_count'],
                'url': repo['html_url'],
                'language': repo.get('language', '?'),
            })
        print(f"    '{q}': {len(repos)} repos")
        time.sleep(2)  # GitHub rate limit

    # Dedupe and sort by stars
    seen = set()
    unique_repos = []
    for r in sorted(all_repos, key=lambda x: x['stars'], reverse=True):
        if r['name'] not in seen:
            seen.add(r['name'])
            unique_repos.append(r)

    print(f"\n    Top repos by stars:")
    for r in unique_repos[:25]:
        print(f"      [{r['stars']:>5} *] {r['name']}")
        if r['description']:
            print(f"              {r['description'][:80]}")

    # ──────────────────────────────────────────────────────────────
    # Part 2: Search for CSV/JSON files with investor data
    # ──────────────────────────────────────────────────────────────
    print(f"\n  {'─' * 55}")
    print(f"  PART 2: GitHub Code/File Search")
    print(f"  {'─' * 55}\n")

    code_queries = [
        'angel investor email filename:csv',
        'investor contacts filename:json',
        'angel investor filename:csv',
        'vc investor email list filename:csv',
    ]

    interesting_files = []
    for q in code_queries:
        results = search_github_code(q, 10)
        for item in results:
            repo = item.get('repository', {}).get('full_name', '?')
            path = item.get('path', '?')
            interesting_files.append({
                'repo': repo,
                'path': path,
                'url': item.get('html_url', ''),
            })
        print(f"    '{q}': {len(results)} files")
        time.sleep(5)  # Stricter rate limit for code search

    for f in interesting_files[:15]:
        print(f"      {f['repo']}/{f['path']}")

    # ──────────────────────────────────────────────────────────────
    # Part 3: Bing search for GitHub investor datasets
    # ──────────────────────────────────────────────────────────────
    print(f"\n  {'─' * 55}")
    print(f"  PART 3: Bing Search for GitHub Investor Data")
    print(f"  {'─' * 55}\n")

    from ddgs import DDGS

    bing_queries = [
        'site:github.com angel investor email list csv',
        'site:github.com "angel investors" dataset contacts',
        'site:github.com investor database scraper email',
        'site:github.com crunchbase scraper investor',
        'site:github.com vc investor email scraper',
        'site:github.com "angel investor" scraper python',
        'site:github.com investor leads scraper email',
        'site:github.com startup investor contacts database',
        'site:github.com wealthy individuals email scraper',
        'site:github.com "seed investor" email',
    ]

    github_urls = set()
    for q in bing_queries:
        try:
            results = DDGS().text(q, max_results=10, backend='bing')
            for r in results:
                url = r.get('href', '')
                title = r.get('title', '')
                if 'github.com' in url:
                    github_urls.add(url)
                    print(f"    {title[:60]}")
                    print(f"      {url}")
            print(f"    '{q[:55]}' -> {len(results)} results")
        except Exception as e:
            print(f"    Error: {e}")
        time.sleep(1.5)

    # ──────────────────────────────────────────────────────────────
    # Part 4: Check top repos for actual data files
    # ──────────────────────────────────────────────────────────────
    print(f"\n  {'─' * 55}")
    print(f"  PART 4: Inspecting Most Promising Repos")
    print(f"  {'─' * 55}\n")

    # Check the most starred/promising repos for actual data
    promising_repos = [r for r in unique_repos if r['stars'] >= 5][:10]

    for repo in promising_repos:
        print(f"\n    Checking {repo['name']} ({repo['stars']} stars)...")
        try:
            # Get repo contents (root)
            api_url = f"https://api.github.com/repos/{repo['name']}/contents"
            resp = requests.get(api_url, headers=HEADERS, timeout=10)
            if resp.status_code != 200:
                print(f"      Status {resp.status_code}")
                continue

            contents = resp.json()
            if not isinstance(contents, list):
                continue

            data_files = []
            for item in contents:
                name = item.get('name', '').lower()
                if name.endswith(('.csv', '.json', '.xlsx', '.tsv', '.txt')):
                    data_files.append(item)

            if data_files:
                print(f"      Data files found:")
                for df in data_files:
                    print(f"        {df['name']} ({df.get('size', '?')} bytes)")
                    # Fetch first CSV/JSON for email content
                    if df['name'].endswith(('.csv', '.json')) and (df.get('size', 0) or 0) < 500000:
                        raw_url = df.get('download_url', '')
                        if raw_url:
                            content = fetch_raw_content(raw_url)
                            emails = set(EMAIL_RE.findall(content))
                            if emails:
                                real_emails = [e for e in emails if not any(
                                    j in e.lower() for j in ['example.com', 'test.com', 'github.com'])]
                                if real_emails:
                                    print(f"          Found {len(real_emails)} emails!")
                                    for e in list(real_emails)[:5]:
                                        print(f"            {e}")
                                    if len(real_emails) > 5:
                                        print(f"            ... and {len(real_emails) - 5} more")
            else:
                # Check README for links to data
                readme = None
                for item in contents:
                    if item.get('name', '').lower().startswith('readme'):
                        readme = item
                        break
                if readme and readme.get('download_url'):
                    content = fetch_raw_content(readme['download_url'], 50000)
                    if content:
                        # Look for links to spreadsheets or data
                        for match in re.findall(r'https?://[^\s\)]+(?:csv|json|xlsx|sheet|airtable|notion)[^\s\)]*', content):
                            print(f"      Data link: {match[:80]}")
        except Exception as e:
            print(f"      Error: {str(e)[:60]}")
        time.sleep(1)

    # ──────────────────────────────────────────────────────────────
    # Part 5: Search for public Google Sheets / Airtable investor lists
    # ──────────────────────────────────────────────────────────────
    print(f"\n  {'─' * 55}")
    print(f"  PART 5: Public Investor Spreadsheets/Databases")
    print(f"  {'─' * 55}\n")

    sheet_queries = [
        '"docs.google.com/spreadsheets" angel investors health',
        '"docs.google.com/spreadsheets" angel investors email',
        '"airtable.com" angel investors health',
        '"notion.so" angel investors health list',
        'angel investor database free list 2024 2025 health',
        '"angel investor" free database list emails download',
        'angel investor spreadsheet health wellness fitness',
    ]

    for q in sheet_queries:
        try:
            results = DDGS().text(q, max_results=10, backend='bing')
            for r in results:
                url = r.get('href', '')
                title = r.get('title', '')
                if 'docs.google.com' in url or 'airtable.com' in url or 'notion.so' in url:
                    print(f"    SPREADSHEET: {title[:60]}")
                    print(f"      {url}")
                elif any(w in title.lower() for w in ['database', 'list', 'directory', 'spreadsheet']):
                    print(f"    LIST: {title[:60]}")
                    print(f"      {url}")
            print(f"    '{q[:55]}' -> {len(results)} results")
        except Exception as e:
            print(f"    Error: {e}")
        time.sleep(1.5)

    # Save all results
    output = {
        'top_repos': unique_repos[:25],
        'code_files': interesting_files,
        'github_urls': list(github_urls),
    }
    with open('test_github_results.json', 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n  Results saved to test_github_results.json")
    print(f"  Total repos: {len(unique_repos)}, GitHub URLs: {len(github_urls)}\n")


if __name__ == '__main__':
    main()
