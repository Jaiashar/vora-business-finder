#!/usr/bin/env python3
"""
Second-pass email extraction: uses the IG API (now unblocked) to get full bios
and external URLs for accounts that had no email in the first pass.
Then scrapes external URLs (linktrees, personal sites) for emails.

Very careful rate limiting to avoid getting blocked again.
"""

import os
import re
import csv
import time
import json
import random
import urllib.request
import urllib.error
import ssl
from urllib.parse import urlparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

IG_HEADERS = {
    "User-Agent": UA,
    "X-IG-App-ID": "936619743392459",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

WEB_HEADERS = {"User-Agent": UA, "Accept": "text/html,*/*;q=0.8"}

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
JUNK_DOMAINS = {
    'sentry.io', 'w3.org', 'schema.org', 'example.com', 'wixpress.com',
    'cloudflare.com', 'mailchimp.com', 'googleapis.com', 'facebook.com',
    'instagram.com', 'fbcdn.net', 'cdninstagram.com', 'apple.com',
    'google.com', 'gstatic.com', 'youtube.com', 'twitter.com',
    'x.com', 'tiktok.com', 'spotify.com', 'onetrust.com',
    'pinterest.com', 'linkedin.com', 'imginn.org', 'imginn.com',
    'patreon.com', 'substackinc.com', 'stanwith.me', 'domain.com',
    'playbookapp.io', 'joinplaybook.com', 'movesapp.com', 'vrtoapp.com',
    'boostcamp.app', 'strongstrongfriends.com',
}
JUNK_KW = ['sentry', 'noreply', 'no-reply', 'unsubscribe', 'webpack',
           'placeholder', 'donotreply', 'mailer-daemon']


def extract_emails(text):
    if not text:
        return []
    found = EMAIL_RE.findall(text.lower())
    return list(set(
        e for e in found
        if e.split('@')[1] not in JUNK_DOMAINS
        and not any(k in e for k in JUNK_KW)
        and not e.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js'))
        and len(e.split('@')[1]) >= 5
    ))


def fetch_page(url, timeout=8):
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        req = urllib.request.Request(url, headers=WEB_HEADERS)
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        content = resp.read()
        try:
            return content.decode('utf-8')
        except UnicodeDecodeError:
            return content.decode('latin-1', errors='ignore')
    except:
        return None


def scrape_url_for_emails(url):
    all_emails = set()
    html = fetch_page(url)
    if not html:
        return []
    all_emails.update(extract_emails(html))

    linktree_domains = ['linktr.ee', 'komi.io', 'beacons.ai', 'link.bio',
                        'campsite.bio', 'stan.store', 'hoo.be', 'snipfeed.co',
                        'flow.page', 'solo.to', 'tap.bio', 'withkoji.com',
                        'milkshake.app', 'carrd.co', 'lynx.bio', 'bio.link']
    if any(d in url.lower() for d in linktree_domains):
        for link in re.findall(r'href="(https?://[^"]+)"', html)[:10]:
            if any(k in link.lower() for k in ['contact', 'about', 'email', 'book', 'work',
                                                'collab', 'inquiry', 'mailto', 'coach',
                                                'train', 'hire', 'business']):
                sub = fetch_page(link, 5)
                if sub:
                    all_emails.update(extract_emails(sub))
                time.sleep(0.5)
    else:
        parsed = urlparse(url if url.startswith('http') else 'https://' + url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        for path in ['/contact', '/contact-us', '/about', '/about-us', '/work-with-me']:
            if len(all_emails) >= 3:
                break
            sub = fetch_page(base + path, 5)
            if sub:
                all_emails.update(extract_emails(sub))
            time.sleep(0.3)

    return list(all_emails)


def fetch_ig_api(username):
    try:
        url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
        req = urllib.request.Request(url, headers=IG_HEADERS)
        resp = urllib.request.urlopen(req, timeout=10, context=SSL_CTX)
        data = json.loads(resp.read().decode('utf-8'))
        user = data.get("data", {}).get("user", {})
        if not user:
            return None
        return {
            "username": username,
            "full_name": user.get("full_name", ""),
            "bio": user.get("biography", ""),
            "external_url": user.get("external_url", ""),
            "followers": user.get("edge_followed_by", {}).get("count", 0),
            "is_business": user.get("is_business_account", False),
            "business_category": user.get("category_name", ""),
            "business_email": user.get("business_email", ""),
        }
    except urllib.error.HTTPError as e:
        if e.code in (401, 429):
            return "rate_limited"
        return None
    except:
        return None


def main():
    progress_file = os.path.join(BASE_DIR, "fitness_influencer_progress.json")
    csv_path = os.path.join(BASE_DIR, "fitness_influencer_leads.csv")
    json_path = os.path.join(BASE_DIR, "fitness_influencer_leads.json")

    if not os.path.exists(progress_file):
        print("No progress file found. Run fitness_influencer_scraper.py first.")
        return

    with open(progress_file) as f:
        existing = json.load(f)

    # Find accounts with no email yet
    no_email = []
    has_email = []
    for username, data in existing.items():
        if data.get("error"):
            continue
        all_emails = set(data.get("emails_from_bio", []) + data.get("url_emails", []))
        if data.get("business_email"):
            all_emails.add(data["business_email"])
        if all_emails:
            has_email.append(username)
        else:
            no_email.append(username)

    print(f"Progress file: {len(existing)} total, {len(has_email)} with email, {len(no_email)} without email")
    print(f"Running second pass on {len(no_email)} accounts using IG API + URL scraping\n")

    new_emails_found = 0
    api_requests = 0
    rate_limited = False

    for i, username in enumerate(no_email):
        if rate_limited:
            break

        data = existing[username]
        print(f"  [{i+1}/{len(no_email)}] @{username}...", end=" ", flush=True)

        # Step 1: Get full profile from IG API
        api_result = fetch_ig_api(username)
        api_requests += 1

        if api_result == "rate_limited":
            print("RATE LIMITED! Saving and stopping.")
            rate_limited = True
            break

        if api_result is None:
            print("API error, skip")
            time.sleep(random.uniform(10, 15))
            continue

        # Update profile with API data
        data["full_name"] = api_result["full_name"]
        data["bio"] = api_result["bio"]
        data["followers"] = api_result["followers"]
        data["is_business"] = api_result["is_business"]
        data["business_category"] = api_result["business_category"]
        data["business_email"] = api_result.get("business_email", "")

        # Step 2: Extract emails from full bio
        bio_emails = extract_emails(api_result["bio"])
        data["emails_from_bio"] = bio_emails

        # Step 3: Business email
        all_emails = set(bio_emails)
        if api_result.get("business_email"):
            all_emails.add(api_result["business_email"].lower())

        # Step 4: Scrape external URL if still no email
        ext_url = api_result.get("external_url", "")
        if not all_emails and ext_url:
            print(f"scraping {ext_url[:40]}...", end=" ", flush=True)
            try:
                url_emails = scrape_url_for_emails(ext_url)
                data["url_emails"] = url_emails
                all_emails.update(url_emails)
            except:
                pass

        data["external_urls"] = [ext_url] if ext_url else []
        existing[username] = data

        if all_emails:
            new_emails_found += 1
            print(f"EMAIL {api_result['followers']:>7,} flw | {list(all_emails)[:2]}")
        else:
            print(f"  ~   {api_result['followers']:>7,} flw | still no email")

        # Save every 10
        if (i + 1) % 10 == 0:
            with open(progress_file, 'w') as f:
                json.dump(existing, f, indent=2, default=str)
            print(f"  ── saved | {new_emails_found} new emails from {i+1} accounts ──")

        # Very careful rate limiting: 15-25s between API requests
        time.sleep(random.uniform(15, 25))

    # Final save
    with open(progress_file, 'w') as f:
        json.dump(existing, f, indent=2, default=str)

    # Rebuild CSV/JSON
    all_profiles = [data for data in existing.values() if not data.get("error")]
    rows = []
    for p in all_profiles:
        all_emails = set(p.get("emails_from_bio", []) + p.get("url_emails", []))
        if p.get("business_email"):
            all_emails.add(p["business_email"].lower())
        if not all_emails:
            continue

        sources = []
        if p.get("emails_from_bio"):
            sources.append("bio")
        if p.get("url_emails"):
            sources.append("url")
        if p.get("business_email"):
            sources.append("business_email")

        for email in all_emails:
            rows.append({
                "email": email,
                "name": p.get("full_name", ""),
                "ig_username": p["username"],
                "followers": p.get("followers", 0),
                "platform": "instagram",
                "category": "fitness_influencer",
                "tags": "",
                "bio": (p.get("bio") or "")[:200].replace('\n', ' '),
                "external_url": (p.get("external_urls") or [""])[0] if p.get("external_urls") else "",
                "email_source": "+".join(sources),
                "is_business": p.get("is_business", False),
                "business_category": p.get("business_category", ""),
            })

    fieldnames = ["email", "name", "ig_username", "followers", "platform",
                  "category", "tags", "bio", "external_url", "email_source",
                  "is_business", "business_category"]

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    with open(json_path, 'w') as f:
        json.dump(all_profiles, f, indent=2, default=str)

    unique = len(set(r["email"] for r in rows))
    print(f"\n{'='*60}")
    print(f"  SECOND PASS COMPLETE")
    print(f"  API requests: {api_requests}")
    print(f"  New emails found: {new_emails_found}")
    print(f"  Total unique emails: {unique}")
    print(f"  CSV rows: {len(rows)}")
    if rate_limited:
        print(f"\n  Rate limited after {api_requests} requests. Re-run to continue.")
    print(f"\n  Run: python push_consumer_leads.py fitness")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
