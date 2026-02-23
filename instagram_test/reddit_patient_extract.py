#!/usr/bin/env python3
"""
Patient IG extractor — works in tiny batches with long cooldowns.
Designed to slowly extract emails from Reddit-sourced IG handles
without triggering rate limits.

Strategy:
  - Try 10 profiles
  - If rate-limited, wait 30 min and retry
  - After each successful batch, wait 10 min
  - Continue until all handles processed or manually stopped

Usage:
  python reddit_patient_extract.py
"""

import os, re, sys, time, json, random, ssl
import urllib.request, urllib.error
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
IG_HEADERS = {
    "User-Agent": UA,
    "X-IG-App-ID": "936619743392459",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

JUNK_DOMAINS = {
    'sentry.io','w3.org','schema.org','example.com','domain.com',
    'wixpress.com','cloudflare.com','mailchimp.com','googleapis.com',
    'facebook.com','instagram.com','fbcdn.net','youtube.com',
    'twitter.com','x.com','tiktok.com','spotify.com','pinterest.com',
    'linkedin.com','apple.com','google.com','gstatic.com',
    'onetrust.com','feedspot.com','imginn.org',
    'patreon.com','substackinc.com','beacons.ai','stanwith.me',
    'mailgun.com','sendgrid.com','hubspot.com',
    'shopify.com','squarespace.com','wix.com','wordpress.com',
    'gravatar.com','amazonaws.com','stripe.com',
}

PERSONAL_PROVIDERS = {
    'gmail.com','yahoo.com','hotmail.com','outlook.com','aol.com',
    'icloud.com','me.com','mac.com','protonmail.com','proton.me',
    'live.com','msn.com','ymail.com','rocketmail.com',
    'mail.com','zoho.com','gmx.com','gmx.net','fastmail.com',
    'hey.com','tutanota.com','pm.me','comcast.net','att.net',
    'verizon.net','cox.net','sbcglobal.net','charter.net',
    'bellsouth.net','earthlink.net','googlemail.com',
}

BIZ_PREFIXES = {
    'hello','info','contact','press','media','pr','booking','inquiries',
    'team','sales','admin','general','office','talent','management',
    'collab','partnerships','sponsor','business','support','help',
    'careers','marketing','advertising','events','membership',
    'editorial','editor','studio','submissions','casting','news',
    'promo','merch','shop','store','wholesale','customerservice',
}

BIZ_LOCAL_WORDS = {
    'gym','studio','crossfit','llc','inc','brand','academy','team',
    'solutions','services','group','fitnesscenter','supplement',
    'apparel','coaching','consulting','foundation','association',
    'league','network','collective','partners','agency','mgmt',
    'magazine','official','headquarters','corporate','enterprise',
}

BIZ_KEYWORDS = re.compile(
    r'\b(?:gym|studio|fitness\s*center|crossfit\s*box|'
    r'supplement|apparel|brand|clothing|shop|store|'
    r'inc\.|llc|ltd|corp|co\.|®|™|'
    r'official\s*account|we\s*are|our\s*team|'
    r'book\s*(?:a|your)\s*class|class\s*schedule|'
    r'managed\s*by|represented\s*by|'
    r'pro\s*athlete|professional\s*athlete|olympian|'
    r'world\s*champion|olympic\s*medalist|'
    r'ufc\s*fighter|nfl|nba|mlb|nhl|mls|wwe|pga|'
    r'national\s*team|team\s*(?:usa|gb|canada)|'
    r'order\s*(?:now|here|today)|shop\s*(?:now|here)|'
    r'use\s*code|discount|promo|coupon|'
    r'free\s*shipping|limited\s*(?:time|edition))\b',
    re.I
)

PROGRESS_FILE = os.path.join(BASE_DIR, "reddit_pipeline_progress.json")
HANDLES_FILE = os.path.join(BASE_DIR, "reddit_ig_handles.json")

BATCH_SIZE = 10
BETWEEN_REQUESTS = (20, 30)
BETWEEN_BATCHES = 600  # 10 min
RATE_LIMIT_WAIT = 1800  # 30 min


def extract_emails(text):
    if not text:
        return []
    found = EMAIL_RE.findall(text.lower())
    return list(set(
        e for e in found
        if e.split("@")[1] not in JUNK_DOMAINS
        and not any(k in e for k in [
            "sentry", "noreply", "no-reply", "unsubscribe",
            "webpack", "placeholder", "donotreply",
            "mailer-daemon", "u003e", "test@", "user@",
        ])
        and not e.endswith((".png", ".jpg", ".gif", ".svg", ".css", ".js",
                           ".webp", ".woff", ".woff2", ".ttf", ".eot", ".ico"))
        and len(e.split("@")[1]) >= 4
        and len(e) < 60
    ))


def is_consumer_email(email):
    local, domain = email.lower().split("@", 1)
    if domain.endswith((".edu", ".gov", ".org")):
        return False
    if ".k12." in domain:
        return False
    if not re.match(r"^[a-z0-9.-]+\.[a-z]{2,6}$", domain):
        return False
    if local in BIZ_PREFIXES:
        return False
    for w in BIZ_LOCAL_WORDS:
        if w in local:
            return False
    if domain in PERSONAL_PROVIDERS:
        if len(local) > 24:
            return False
        return True
    return True


def is_business(name, bio, username, followers):
    if BIZ_KEYWORDS.search(f"{name} {bio} {username}"):
        return True
    for sfx in ["_official", "_brand", "_hq", "_inc", "_co", "_llc", "_team", "_org"]:
        if username.lower().endswith(sfx):
            return True
    if followers > 500000:
        return True
    return False


def fetch_ig(username):
    url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
    req = urllib.request.Request(url, headers=IG_HEADERS)
    resp = urllib.request.urlopen(req, timeout=10, context=SSL_CTX)
    data = json.loads(resp.read().decode("utf-8"))
    user = data.get("data", {}).get("user", {})
    if not user:
        return None
    return {
        "full_name": user.get("full_name", ""),
        "bio": user.get("biography", ""),
        "external_url": user.get("external_url", ""),
        "followers": user.get("edge_followed_by", {}).get("count", 0),
        "is_business": user.get("is_business_account", False),
        "business_category": user.get("category_name", ""),
        "business_email": user.get("business_email", ""),
    }


def fetch_page(url, timeout=8):
    try:
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        req = urllib.request.Request(url, headers={
            "User-Agent": UA, "Accept": "text/html,*/*;q=0.8",
        })
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        content = resp.read()[:300000]
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError:
            return content.decode("latin-1", errors="ignore")
    except Exception:
        return None


def scrape_ext_url(url):
    all_emails = set()
    html = fetch_page(url)
    if not html:
        return []
    for e in extract_emails(html):
        if is_consumer_email(e):
            all_emails.add(e)

    linktree_domains = [
        "linktr.ee", "komi.io", "beacons.ai", "link.bio",
        "campsite.bio", "stan.store", "flow.page", "solo.to",
        "bio.link", "milkshake.app", "carrd.co", "bio.site",
    ]
    if any(d in url.lower() for d in linktree_domains):
        for link in re.findall(r'href="(https?://[^"]+)"', html)[:6]:
            if any(k in link.lower() for k in ["contact", "about", "email"]):
                sub = fetch_page(link, 5)
                if sub:
                    for e in extract_emails(sub):
                        if is_consumer_email(e):
                            all_emails.add(e)
                time.sleep(0.3)
    return list(all_emails)


def load_progress():
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_progress(dp):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(dp, f, indent=1, default=str)


def main():
    print("+" + "=" * 60 + "+")
    print("|  PATIENT IG EXTRACTOR (small batches, long waits)        |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    with open(HANDLES_FILE) as f:
        handles = json.load(f)

    dp = load_progress()
    extraction = dp.get("extraction", {})

    existing_checked = set(extraction.keys())
    for pf in ["ig_api_progress.json", "consumer_progress.json", "mass_progress.json"]:
        fp = os.path.join(BASE_DIR, pf)
        if os.path.exists(fp):
            try:
                with open(fp) as f:
                    existing_checked.update(json.load(f).keys())
            except Exception:
                pass

    to_process = [h for h in handles if h not in existing_checked]
    random.shuffle(to_process)

    total = len(to_process)
    total_emails = sum(1 for p in extraction.values()
                       if isinstance(p, dict) and p.get("emails"))
    print(f"  {len(existing_checked)} already checked, {total} remaining")
    print(f"  Existing emails: {total_emails}")
    print(f"  Batch size: {BATCH_SIZE}, between: {BETWEEN_REQUESTS}s, cooldown: {BETWEEN_BATCHES}s\n")

    global_idx = 0
    batch_num = 0

    while global_idx < total:
        batch_num += 1
        batch = to_process[global_idx:global_idx + BATCH_SIZE]
        print(f"\n  === BATCH {batch_num} ({len(batch)} handles, starting at #{global_idx+1}/{total}) ===")
        print(f"  Time: {datetime.now().strftime('%H:%M:%S')}")

        rate_limited = False
        batch_emails = 0

        for username in batch:
            global_idx += 1
            print(f"  [{global_idx}/{total}] @{username}...", end=" ", flush=True)

            try:
                profile = fetch_ig(username)
            except urllib.error.HTTPError as e:
                if e.code in (401, 429):
                    print(f"\n  RATE LIMITED ({e.code})")
                    rate_limited = True
                    break
                extraction[username] = {"error": str(e.code)}
                print(f"http {e.code}")
                time.sleep(3)
                continue
            except Exception as e:
                extraction[username] = {"error": str(e)[:50]}
                print("error")
                time.sleep(3)
                continue

            if not profile:
                extraction[username] = {"error": "not_found"}
                print("not found")
                time.sleep(random.uniform(*BETWEEN_REQUESTS))
                continue

            name = profile["full_name"]
            bio = profile["bio"]
            followers = profile["followers"]

            if followers < 50:
                extraction[username] = {"error": "too_small", "followers": followers}
                print(f"tiny ({followers} flw)")
                time.sleep(random.uniform(*BETWEEN_REQUESTS))
                continue

            if is_business(name, bio, username, followers):
                extraction[username] = {"error": "business", "full_name": name}
                print(f"SKIP biz ({name[:25]})")
                time.sleep(random.uniform(*BETWEEN_REQUESTS))
                continue

            all_emails = set()

            for e in extract_emails(bio):
                if is_consumer_email(e):
                    all_emails.add(e)

            biz_email = profile.get("business_email", "")
            if biz_email and is_consumer_email(biz_email):
                all_emails.add(biz_email.lower())

            ext_url = profile.get("external_url", "")
            url_emails = []
            if not all_emails and ext_url:
                try:
                    url_emails = scrape_ext_url(ext_url)
                    all_emails.update(url_emails)
                except Exception:
                    pass

            extraction[username] = {
                "full_name": name,
                "bio": (bio or "")[:200],
                "followers": followers,
                "external_url": ext_url,
                "business_category": profile.get("business_category", ""),
                "emails": list(all_emails),
                "url_emails": url_emails,
                "error": None,
                "source": handles.get(username, {}).get("sub", "reddit"),
            }

            if all_emails:
                batch_emails += 1
                total_emails += 1
                print(f"EMAIL {followers:>7,} flw | {list(all_emails)[:2]}")
            else:
                print(f"  ~   {followers:>7,} flw | no email")

            time.sleep(random.uniform(*BETWEEN_REQUESTS))

        dp["extraction"] = extraction
        save_progress(dp)
        print(f"  Batch done: {batch_emails} emails this batch, {total_emails} total")

        if rate_limited:
            print(f"  Waiting {RATE_LIMIT_WAIT//60} min for rate limit reset...")
            time.sleep(RATE_LIMIT_WAIT)
        elif global_idx < total:
            print(f"  Waiting {BETWEEN_BATCHES//60} min before next batch...")
            time.sleep(BETWEEN_BATCHES)

    dp["extraction"] = extraction
    save_progress(dp)

    real_emails = set()
    for p in extraction.values():
        if isinstance(p, dict) and p.get("emails"):
            real_emails.update(p["emails"])

    print(f"\n  DONE: {len(extraction)} profiles checked, {len(real_emails)} unique emails")
    for e in sorted(real_emails):
        print(f"    {e}")


if __name__ == "__main__":
    main()
