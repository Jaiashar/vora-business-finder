#!/usr/bin/env python3
"""
Wearable Device User Email Scraper (v4 — Rate-Limit Safe, Resumable)
======================================================================
Uses Instagram's public web API to find wearable device users:
  1. Curated seed list of wearable reviewers, Garmin/Apple/Oura accounts
  2. Fetch profiles with proper rate limiting (120s cooldown every 30 reqs)
  3. Detect devices from bios, extract emails
  4. Spider to related profiles (only small accounts)
  5. Resumable — saves progress, re-run to continue

Outputs CSV + JSON, ready to push to Supabase consumer_leads table.
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
from datetime import datetime
from urllib.parse import urlparse

# ─── Config ───────────────────────────────────────────────────────
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

WEB_HEADERS = {"User-Agent": UA, "Accept": "text/html,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.5"}

MAX_FOLLOWERS = 300_000
MIN_FOLLOWERS = 1_000
COOLDOWN_EVERY = 30
COOLDOWN_SECONDS = 120

# ─── Seed accounts ────────────────────────────────────────────────
WEARABLE_SEEDS = [
    # Wearable tech reviewers (micro/mid)
    "dcrainmaker", "desfit", "thequantifiedscientist",
    "chasethesummit", "roadtrailrun", "the5krunner",
    "fellrnr", "runwithhal", "jeffgalloway",

    # Garmin community
    "garmin", "garminrunning", "garminoutdoor",

    # Apple Watch / Apple Fitness
    "applewatch", "applefitness",

    # Oura
    "ouraring",

    # Whoop
    "whoop",

    # Running coaches
    "runnersworld", "womensrunning", "trailrunnermag",
    "runwithryan", "the.running.channel",

    # Biohacking / quantified self
    "foundmyfitness", "drpeterattia",

    # Fitness tech
    "peloton", "zwift", "wahoo_fitness", "polarusa", "corosrun",

    # Ultra / marathon micro-influencers
    "courtney_dauwalter",

    # Cycling
    "gcn",
]

# ─── Device detection ─────────────────────────────────────────────
DEVICE_PATTERNS = {
    "apple_watch": re.compile(r'apple\s*watch|close\s*(?:my|your)\s*rings|apple\s*fitness\+?', re.I),
    "garmin": re.compile(r'garmin|forerunner|fenix|venu|instinct|enduro', re.I),
    "oura": re.compile(r'oura\s*ring|#oura\b|\boura\b', re.I),
    "whoop": re.compile(r'\bwhoop\b', re.I),
    "fitbit": re.compile(r'fitbit', re.I),
    "coros": re.compile(r'coros\s*(?:pace|vertix|apex)?', re.I),
    "polar": re.compile(r'polar\s*(?:vantage|grit|ignite|pacer)?', re.I),
    "samsung_watch": re.compile(r'galaxy\s*watch|samsung\s*(?:watch|health)', re.I),
    "suunto": re.compile(r'suunto', re.I),
    "wahoo": re.compile(r'wahoo\s*(?:elemnt|rival|tickr)?', re.I),
}


def detect_devices(text):
    if not text:
        return []
    return [device for device, pattern in DEVICE_PATTERNS.items() if pattern.search(text)]


# ─── Email extraction ─────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
JUNK_DOMAINS = {
    'sentry.io', 'w3.org', 'schema.org', 'example.com', 'domain.com',
    'wixpress.com', 'cloudflare.com', 'mailchimp.com', 'googleapis.com',
    'facebook.com', 'instagram.com', 'fbcdn.net', 'cdninstagram.com',
    'apple.com', 'google.com', 'gstatic.com', 'youtube.com',
    'twitter.com', 'x.com', 'tiktok.com', 'spotify.com',
    'onetrust.com', 'pinterest.com', 'linkedin.com',
    'garmin.com', 'strava.com', 'fitbit.com', 'whoop.com',
    'ouraring.com', 'polar.com', 'coros.com', 'samsung.com',
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


# ─── Web helpers ──────────────────────────────────────────────────
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
                        'campsite.bio', 'stan.store', 'hoo.be', 'snipfeed.co']
    if any(d in url.lower() for d in linktree_domains):
        for link in re.findall(r'href="(https?://[^"]+)"', html)[:5]:
            if any(k in link.lower() for k in ['contact', 'about', 'email', 'book', 'work', 'collab']):
                sub = fetch_page(link, 5)
                if sub:
                    all_emails.update(extract_emails(sub))
                time.sleep(0.5)
    else:
        parsed = urlparse(url if url.startswith('http') else 'https://' + url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        for path in ['/contact', '/contact-us', '/about']:
            if len(all_emails) >= 3:
                break
            sub = fetch_page(base + path, 5)
            if sub:
                all_emails.update(extract_emails(sub))
            time.sleep(0.3)

    return list(all_emails)


# ═══════════════════════════════════════════════════════════════════
# IG PUBLIC API with proper rate limiting
# ═══════════════════════════════════════════════════════════════════
request_count = 0


def fetch_ig_profile(username):
    global request_count
    request_count += 1

    if request_count > 1 and request_count % COOLDOWN_EVERY == 0:
        print(f"\n  --- Cooldown: {COOLDOWN_SECONDS}s after {request_count} requests ---")
        time.sleep(COOLDOWN_SECONDS)

    try:
        url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
        req = urllib.request.Request(url, headers=IG_HEADERS)
        resp = urllib.request.urlopen(req, timeout=10, context=SSL_CTX)
        data = json.loads(resp.read().decode('utf-8'))
        user = data.get("data", {}).get("user", {})
        if not user:
            return None

        related = []
        for edge in user.get("edge_related_profiles", {}).get("edges", []):
            node = edge.get("node", {})
            if node.get("username"):
                related.append(node["username"])

        return {
            "username": username,
            "full_name": user.get("full_name", ""),
            "bio": user.get("biography", ""),
            "external_url": user.get("external_url", ""),
            "followers": user.get("edge_followed_by", {}).get("count", 0),
            "following": user.get("edge_follow", {}).get("count", 0),
            "is_business": user.get("is_business_account", False),
            "is_verified": user.get("is_verified", False),
            "business_category": user.get("category_name", ""),
            "business_email": user.get("business_email", ""),
            "related_profiles": related,
        }
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return "not_found"
        elif e.code in (429, 401):
            return "rate_limited"
        return f"http_{e.code}"
    except Exception as e:
        return f"error: {str(e)[:60]}"


# ═══════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════
def run_pipeline(seeds, progress_file, max_profiles=300):
    existing = {}
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            existing = json.load(f)
        print(f"  Loaded {len(existing)} from progress file")

    results = []
    visited = set(existing.keys())
    to_visit = [s for s in seeds if s not in visited]
    total_fetched = 0
    total_with_email = 0
    rate_limit_count = 0

    wave = 0
    while to_visit and total_fetched < max_profiles:
        wave += 1
        next_wave = []
        batch = to_visit[:max_profiles - total_fetched]

        print(f"\n  ── Wave {wave}: {len(batch)} accounts to process ──\n")

        for i, username in enumerate(batch):
            if username in visited:
                continue
            visited.add(username)

            print(f"  [{total_fetched + 1}] @{username}...", end=" ", flush=True)

            result = fetch_ig_profile(username)

            if result == "rate_limited":
                rate_limit_count += 1
                wait = 60 * rate_limit_count
                print(f"RATE LIMITED (#{rate_limit_count}). Waiting {wait}s...")
                time.sleep(wait)
                result = fetch_ig_profile(username)
                if result == "rate_limited":
                    print(f"  Still rate limited. Saving progress and stopping.")
                    with open(progress_file, 'w') as f:
                        json.dump(existing, f, indent=2, default=str)
                    print(f"  Re-run the script to resume from where we left off.")
                    break

            if isinstance(result, str):
                print(f"skip ({result})")
                existing[username] = {"username": username, "error": result}
                time.sleep(1)
                continue

            if result is None:
                print("empty")
                existing[username] = {"username": username, "error": "empty"}
                time.sleep(1)
                continue

            profile = result
            related = profile.pop("related_profiles", [])
            followers = profile["followers"]

            if followers > MAX_FOLLOWERS:
                print(f"skip (too big: {followers:,})")
                existing[username] = {"username": username, "error": "too_big", "followers": followers}
                time.sleep(random.uniform(5, 10))
                continue

            if followers < MIN_FOLLOWERS:
                print(f"skip (too small: {followers:,})")
                existing[username] = {"username": username, "error": "too_small", "followers": followers}
                time.sleep(random.uniform(5, 10))
                continue

            # Extract emails from bio
            bio = profile.get("bio", "")
            bio_emails = extract_emails(bio)
            profile["emails_from_bio"] = bio_emails
            profile["url_emails"] = []

            # Detect wearable devices from bio
            profile["detected_devices"] = detect_devices(bio)

            # If no bio email but has external URL, scrape it
            if not bio_emails and profile.get("external_url"):
                try:
                    url_emails = scrape_url_for_emails(profile["external_url"])
                    profile["url_emails"] = url_emails
                    # Also check URL page text for device mentions
                    page_text = fetch_page(profile["external_url"])
                    if page_text:
                        extra_devices = detect_devices(page_text)
                        profile["detected_devices"] = list(set(profile["detected_devices"] + extra_devices))
                except:
                    pass

            all_emails = set(bio_emails + profile["url_emails"])
            biz_email = profile.get("business_email", "")
            if biz_email:
                all_emails.add(biz_email.lower())

            profile["error"] = None
            existing[username] = profile
            results.append(profile)
            total_fetched += 1

            devices = profile.get("detected_devices", [])
            device_str = f" [{','.join(devices)}]" if devices else ""

            if all_emails:
                total_with_email += 1
                print(f"EMAIL {followers:>7,} flw{device_str} | {list(all_emails)[:2]}")
            else:
                print(f"  ~   {followers:>7,} flw{device_str} | no email")

            if followers < MAX_FOLLOWERS:
                new_related = [u for u in related if u not in visited]
                next_wave.extend(new_related)

            if total_fetched % 20 == 0:
                with open(progress_file, 'w') as f:
                    json.dump(existing, f, indent=2, default=str)
                hit_rate = total_with_email * 100 // max(total_fetched, 1)
                print(f"  ── saved | {total_fetched} fetched | {total_with_email} emails ({hit_rate}%) ──")

            time.sleep(random.uniform(8, 15))

        to_visit = list(dict.fromkeys(u for u in next_wave if u not in visited))

    with open(progress_file, 'w') as f:
        json.dump(existing, f, indent=2, default=str)

    for username, data in existing.items():
        if data not in results and not data.get("error"):
            results.append(data)

    return results, total_fetched, total_with_email


# ═══════════════════════════════════════════════════════════════════
# OUTPUT
# ═══════════════════════════════════════════════════════════════════
def output_results(profiles, csv_path, json_path):
    rows = []
    device_counts = {}

    for p in profiles:
        if p.get("error"):
            continue

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

        devices = p.get("detected_devices", [])
        for d in devices:
            device_counts[d] = device_counts.get(d, 0) + 1

        category = "fitness_wearable" if devices else "wearable_user"

        for email in all_emails:
            rows.append({
                "email": email,
                "name": p.get("full_name", ""),
                "ig_username": p["username"],
                "followers": p.get("followers", 0),
                "platform": "instagram",
                "category": category,
                "tags": ",".join(devices),
                "bio": (p.get("bio") or "")[:200].replace('\n', ' '),
                "external_url": p.get("external_url", ""),
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
        json.dump(profiles, f, indent=2, default=str)

    return len(rows), len(set(r["email"] for r in rows)), device_counts


# ═══════════════════════════════════════════════════════════════════
def main():
    print("+" + "=" * 60 + "+")
    print("|  WEARABLE DEVICE USER EMAIL SCRAPER v4                   |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+")
    print(f"  Target: {MIN_FOLLOWERS:,}-{MAX_FOLLOWERS:,} followers")
    print(f"  Seeds: {len(WEARABLE_SEEDS)} accounts")
    print(f"  Rate limiting: {COOLDOWN_SECONDS}s cooldown every {COOLDOWN_EVERY} requests")

    progress_file = os.path.join(BASE_DIR, "wearable_user_progress.json")
    csv_path = os.path.join(BASE_DIR, "wearable_user_leads.csv")
    json_path = os.path.join(BASE_DIR, "wearable_user_leads.json")

    profiles, total_fetched, total_with_email = run_pipeline(
        WEARABLE_SEEDS, progress_file, max_profiles=300,
    )

    print("\n" + "=" * 60)
    print("  FINAL RESULTS")
    print("=" * 60)

    csv_rows, unique_emails, device_counts = output_results(profiles, csv_path, json_path)

    valid = [p for p in profiles if not p.get("error")]
    device_str = "\n".join(
        f"    {d}: {c}" for d, c in sorted(device_counts.items(), key=lambda x: -x[1])
    ) if device_counts else "    (none detected yet)"

    print(f"""
  Profiles in range:    {len(valid)}
  With email:           {total_with_email} ({total_with_email * 100 // max(len(valid), 1)}%)
  Unique emails:        {unique_emails}
  CSV rows:             {csv_rows}

  Device breakdown:
{device_str}

  Files:
    CSV:  {csv_path}
    JSON: {json_path}

  TIP: Run again to resume — progress is saved automatically.
  TIP: Then run: python push_consumer_leads.py wearable
""")


if __name__ == "__main__":
    main()
