#!/usr/bin/env python3
"""
Reddit → Instagram Email Pipeline
===================================
1. Crawl fitness/wearable subreddits for IG handles shared in comments
2. Filter out obvious brands/celebrities
3. Check each handle via IG Public API for email/bio info
4. Push consumer emails to Supabase

Targets "accountability buddy" and "share your IG" threads where
average everyday users share their personal Instagram handles.

Usage:
  python reddit_ig_scraper.py              # Full pipeline: discover + extract
  python reddit_ig_scraper.py discover     # Just discover IG handles from Reddit
  python reddit_ig_scraper.py extract      # Just extract emails from saved handles
  python reddit_ig_scraper.py stats        # Show stats
"""

import os, re, csv, sys, time, json, random, ssl
import urllib.request, urllib.error
from datetime import datetime
from urllib.parse import quote_plus

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

UAS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

IG_HEADERS = {
    "User-Agent": UAS[0],
    "X-IG-App-ID": "936619743392459",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
IG_HANDLE_RE = re.compile(
    r'(?:@|instagram\.com/|ig:\s*|insta:\s*)([a-zA-Z0-9_.]{3,30})',
    re.I
)

JUNK_DOMAINS = {
    'sentry.io','w3.org','schema.org','example.com','domain.com',
    'wixpress.com','cloudflare.com','mailchimp.com','googleapis.com',
    'facebook.com','instagram.com','fbcdn.net','youtube.com',
    'twitter.com','x.com','tiktok.com','spotify.com','pinterest.com',
    'linkedin.com','apple.com','google.com','gstatic.com',
    'onetrust.com','feedspot.com','imginn.org',
    'patreon.com','substackinc.com','beacons.ai',
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
    'yahoo.co.uk','hotmail.co.uk','btinternet.com',
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
    'solutions','services','group','fitness center','supplement',
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
    r'world\s*champion|olympic\s*(?:gold|silver|bronze|medalist)|'
    r'ufc\s*fighter|nfl|nba|mlb|nhl|mls|wwe|pga|'
    r'national\s*team|team\s*(?:usa|gb|canada)|'
    r'order\s*(?:now|here|today)|shop\s*(?:now|here)|'
    r'use\s*code|discount|promo|coupon|'
    r'free\s*shipping|limited\s*(?:time|edition))\b',
    re.I
)

SKIP_HANDLES = {
    'p','reel','explore','stories','tv','accounts','direct','reels',
    'instagram','facebook','twitter','youtube','tiktok','snapchat',
    'nikerunning','nike','adidas','garmin','fitbit','whoop','oura',
    'peloton','lululemon','gymshark','underarmour','reebok','asics',
    'brooks','newbalance','saucony','hoka','on_running',
    'runnersworld','runnersworldmag','menshealth','womenshealth',
    'shape','self','prevention','fitness','crossfit','orangetheory',
}

SUBREDDIT_QUERIES = [
    ("running", "share your instagram"),
    ("running", "instagram accountability"),
    ("running", "follow each other instagram"),
    ("loseit", "share your instagram"),
    ("loseit", "accountability buddy instagram"),
    ("loseit", "follow each other"),
    ("loseit", "instagram accountability"),
    ("progresspics", "share your instagram"),
    ("progresspics", "follow each other"),
    ("progresspics", "instagram accountability"),
    ("xxfitness", "share your instagram"),
    ("xxfitness", "accountability buddy"),
    ("xxfitness", "instagram follow"),
    ("bodyweightfitness", "share your instagram"),
    ("bodyweightfitness", "follow me instagram"),
    ("fitness", "share your instagram"),
    ("fitness", "follow each other instagram"),
    ("fitness", "accountability buddy instagram"),
    ("yoga", "share your instagram"),
    ("yoga", "instagram follow"),
    ("Peloton", "share your instagram"),
    ("Peloton", "instagram follow each other"),
    ("crossfit", "share your instagram"),
    ("crossfit", "follow each other"),
    ("homegym", "share your instagram"),
    ("homegym", "follow my gym"),
    ("C25K", "share instagram"),
    ("C25K", "accountability partner"),
    ("orangetheory", "instagram follow"),
    ("orangetheory", "share your instagram"),
    ("GarminWatches", "instagram"),
    ("fitbit", "share your instagram"),
    ("ouraring", "instagram"),
    ("cycling", "share your instagram"),
    ("triathlon", "share your instagram"),
    ("Swimming", "share your instagram"),
    ("weightlifting", "share your instagram"),
    ("powerlifting", "share your instagram"),
    ("naturalbodybuilding", "share your instagram"),
    ("GYM", "share your instagram"),
    ("GYM", "follow each other instagram"),
    ("1200isplenty", "share your instagram"),
    ("CICO", "share your instagram"),
    ("intermittentfasting", "share your instagram"),
    ("keto", "share your instagram"),
    ("veganfitness", "share your instagram"),
    ("flexibility", "share your instagram"),
    ("Barre", "share your instagram"),
    ("pelotoncycle", "share your instagram"),
    ("whoop", "instagram"),
    ("AppleWatchFitness", "instagram"),
    # Wave 2 — additional fitness subs
    ("StrongCurves", "share your instagram"),
    ("StrongCurves", "follow each other"),
    ("kettlebell", "share your instagram"),
    ("kettlebell", "instagram follow"),
    ("Stronglifts5x5", "share your instagram"),
    ("StartingStrength", "share your instagram"),
    ("Rowing", "share your instagram"),
    ("ClimbingCircleJerk", "share your instagram"),
    ("climbing", "share your instagram"),
    ("bouldering", "share your instagram"),
    ("MuayThai", "share your instagram"),
    ("bjj", "share your instagram"),
    ("bjj", "instagram follow"),
    ("MMA", "share your instagram"),
    ("boxing", "share your instagram"),
    ("hiking", "share your instagram"),
    ("hiking", "instagram follow"),
    ("ultrarunning", "share your instagram"),
    ("ultrarunning", "instagram follow"),
    ("trailrunning", "share your instagram"),
    ("trailrunning", "instagram follow"),
    ("AdvancedRunning", "share your instagram"),
    ("AdvancedRunning", "follow each other"),
    ("Gymnastics", "share your instagram"),
    ("Calisthenics", "share your instagram"),
    ("Calisthenics", "instagram follow"),
    ("pilates", "share your instagram"),
    ("zumba", "share your instagram"),
    ("Brogress", "share your instagram"),
    ("Brogress", "follow each other"),
    ("gainit", "share your instagram"),
    ("gainit", "follow each other instagram"),
    ("leangains", "share your instagram"),
    ("EOOD", "share your instagram"),
    ("90daysgoal", "share your instagram"),
    ("90daysgoal", "instagram accountability"),
    ("getdisciplined", "share your instagram"),
    ("getdisciplined", "accountability instagram"),
    ("BulkOrCut", "share your instagram"),
    ("formcheck", "share your instagram"),
    ("gzcl", "share your instagram"),
    ("nSuns", "share your instagram"),
    ("Fitness_India", "share your instagram"),
    ("FitnessOver30", "share your instagram"),
    ("workout", "share your instagram"),
    ("P90X", "share your instagram"),
    ("insanity", "share your instagram"),
    ("spartanrace", "share your instagram"),
    ("spartanrace", "instagram follow"),
    ("obstacle_course_racing", "share your instagram"),
    ("Zwift", "share your instagram"),
    ("Zwift", "instagram"),
    ("MTB", "share your instagram"),
    ("MTB", "instagram follow"),
    ("bicycling", "share your instagram"),
    ("Velo", "share your instagram"),
]

PROGRESS_FILE = os.path.join(BASE_DIR, "reddit_pipeline_progress.json")
HANDLES_FILE = os.path.join(BASE_DIR, "reddit_ig_handles.json")
CSV_FILE = os.path.join(BASE_DIR, "reddit_consumer_leads.csv")

# ─────────── Reddit helpers ───────────

def reddit_get(path, retries=2):
    url = f"https://www.reddit.com{path}"
    for attempt in range(retries + 1):
        ua = random.choice(UAS)
        req = urllib.request.Request(url, headers={
            "User-Agent": ua,
            "Accept": "application/json",
        })
        try:
            resp = urllib.request.urlopen(req, timeout=15, context=SSL_CTX)
            return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 60 * (attempt + 1)
                print(f"    Reddit 429 — waiting {wait}s...")
                time.sleep(wait)
            elif e.code == 403:
                wait = 30 * (attempt + 1)
                print(f"    Reddit 403 — waiting {wait}s...")
                time.sleep(wait)
            else:
                raise
        except Exception:
            if attempt < retries:
                time.sleep(10)
            else:
                raise
    return None


def extract_handles_from_text(text):
    handles = set()
    for match in IG_HANDLE_RE.finditer(text):
        h = match.group(1).lower().rstrip(".")
        if len(h) < 3 or h in SKIP_HANDLES:
            continue
        if h.startswith("_") and h.endswith("_"):
            continue
        handles.add(h)
    return handles


def scan_comments(listing, depth=0, max_depth=5):
    handles = set()
    children = listing.get("data", {}).get("children", [])
    for item in children:
        body = item.get("data", {}).get("body", "") or ""
        handles.update(extract_handles_from_text(body))
        replies = item.get("data", {}).get("replies", {})
        if isinstance(replies, dict) and depth < max_depth:
            handles.update(scan_comments(replies, depth + 1, max_depth))
    return handles


def is_likely_brand_handle(handle):
    biz_words = [
        "fitness","gym","studio","crossfit","yoga","run","running",
        "cycling","coach","training","nutrition","wellness","health",
        "supplement","apparel","brand","official","hq","inc","llc",
        "co","team","org","shop","store","merch","media","magazine",
        "podcast","news","world","global","network","foundation",
    ]
    h = handle.lower().replace("_", "").replace(".", "")
    for w in biz_words:
        if h == w or h.startswith(w + "official") or h.endswith("official"):
            return True
    return False


# ─────────── Discovery phase ───────────

def discover_handles():
    print("+" + "=" * 60 + "+")
    print("|  REDDIT → IG HANDLE DISCOVERY                            |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    handles = {}
    try:
        with open(HANDLES_FILE) as f:
            handles = json.load(f)
        print(f"  Loaded {len(handles)} existing handles\n")
    except FileNotFoundError:
        pass

    discovery_progress = {}
    try:
        with open(PROGRESS_FILE) as f:
            dp = json.load(f)
        discovery_progress = dp.get("discovery", {})
    except FileNotFoundError:
        pass

    total_new = 0
    request_count = 0

    for idx, (sub, query) in enumerate(SUBREDDIT_QUERIES):
        key = f"{sub}|{query}"
        if key in discovery_progress:
            continue

        print(f"  [{idx+1}/{len(SUBREDDIT_QUERIES)}] r/{sub}: \"{query}\"", end=" ", flush=True)

        try:
            data = reddit_get(
                f"/r/{sub}/search.json?q={quote_plus(query)}&restrict_sr=1&limit=100&sort=relevance&t=all"
            )
            request_count += 1
            if not data:
                print("-> no data")
                discovery_progress[key] = {"posts": 0, "handles": 0}
                continue

            posts = data.get("data", {}).get("children", [])
            found = 0

            for pidx, post in enumerate(posts[:8]):
                pd = post.get("data", {})
                text = f"{pd.get('title', '')} {pd.get('selftext', '')}"

                for h in extract_handles_from_text(text):
                    if h not in handles:
                        handles[h] = {"sub": sub, "source": "post_body", "query": query}
                        found += 1

                permalink = pd.get("permalink", "")
                if permalink:
                    time.sleep(random.uniform(3, 5))
                    request_count += 1
                    try:
                        cdata = reddit_get(f"{permalink}.json?limit=500&sort=top")
                        if cdata and len(cdata) >= 2:
                            for h in scan_comments(cdata[1]):
                                if h not in handles:
                                    handles[h] = {"sub": sub, "source": "comment", "query": query}
                                    found += 1
                    except Exception:
                        pass

            total_new += found
            discovery_progress[key] = {"posts": len(posts), "handles": found}
            print(f"-> {len(posts)} posts, +{found} handles (total: {len(handles)})")

            # Save periodically
            with open(HANDLES_FILE, "w") as f:
                json.dump(handles, f, indent=1)
            dp_save = {}
            try:
                with open(PROGRESS_FILE) as f:
                    dp_save = json.load(f)
            except FileNotFoundError:
                pass
            dp_save["discovery"] = discovery_progress
            with open(PROGRESS_FILE, "w") as f:
                json.dump(dp_save, f, indent=1)

        except Exception as e:
            print(f"-> ERROR: {str(e)[:40]}")

        wait = random.uniform(6, 12)
        if request_count > 0 and request_count % 8 == 0:
            wait = random.uniform(30, 45)
            print(f"    (rate limit pause {wait:.0f}s)")
        time.sleep(wait)

    print(f"\n  Discovery complete: {len(handles)} total handles, {total_new} new this run")
    with open(HANDLES_FILE, "w") as f:
        json.dump(handles, f, indent=1)
    return handles


# ─────────── Email extraction phase ───────────

def extract_emails_from_text(text):
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
        and not e.endswith((".png", ".jpg", ".gif", ".svg", ".css", ".js", ".webp", ".woff", ".woff2", ".ttf", ".eot", ".ico"))
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
    from ig_api_harvester import BIZ_EMAIL_DOMAIN_RE
    if BIZ_EMAIL_DOMAIN_RE.search(domain):
        return False
    return True


def is_business_ig(name, bio, username, followers):
    if BIZ_KEYWORDS.search(f"{name} {bio} {username}"):
        return True
    for sfx in ["_official", "_brand", "_hq", "_inc", "_co", "_llc", "_team", "_org"]:
        if username.lower().endswith(sfx):
            return True
    if followers > 500000:
        return True
    return False


def fetch_ig_profile(username):
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
            "User-Agent": random.choice(UAS),
            "Accept": "text/html,*/*;q=0.8",
        })
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        content = resp.read()[:300000]
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError:
            return content.decode("latin-1", errors="ignore")
    except Exception:
        return None


def scrape_url_for_emails(url):
    all_emails = set()
    html = fetch_page(url)
    if not html:
        return []
    for e in extract_emails_from_text(html):
        if is_consumer_email(e):
            all_emails.add(e)

    linktree_domains = [
        "linktr.ee", "komi.io", "beacons.ai", "link.bio",
        "campsite.bio", "stan.store", "hoo.be", "snipfeed.co",
        "flow.page", "solo.to", "tap.bio", "bio.link",
        "milkshake.app", "carrd.co", "lynx.bio", "bio.site",
    ]
    if any(d in url.lower() for d in linktree_domains):
        for link in re.findall(r'href="(https?://[^"]+)"', html)[:8]:
            if any(k in link.lower() for k in ["contact", "about", "email"]):
                sub = fetch_page(link, 5)
                if sub:
                    for e in extract_emails_from_text(sub):
                        if is_consumer_email(e):
                            all_emails.add(e)
                time.sleep(0.3)
    return list(all_emails)


def extract_emails_pipeline(handles):
    print("\n" + "+" + "=" * 60 + "+")
    print("|  IG API EMAIL EXTRACTION (Reddit handles)                |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    progress = {}
    try:
        with open(PROGRESS_FILE) as f:
            dp = json.load(f)
        progress = dp.get("extraction", {})
        print(f"  Loaded {len(progress)} existing extractions")
    except FileNotFoundError:
        pass

    # Also load any existing ig_api_progress to avoid re-checking
    existing_checked = set(progress.keys())
    for pf in ["ig_api_progress.json", "consumer_progress.json", "mass_progress.json"]:
        fp = os.path.join(BASE_DIR, pf)
        if os.path.exists(fp):
            try:
                with open(fp) as f:
                    existing_checked.update(json.load(f).keys())
            except Exception:
                pass

    to_process = [h for h in handles if h not in existing_checked]
    print(f"  {len(existing_checked)} already checked, {len(to_process)} to process\n")

    if not to_process:
        print("  Nothing to process.")
        return progress

    processed = 0
    emails_found = 0
    biz_skipped = 0
    api_requests = 0

    for username in to_process:
        processed += 1

        if api_requests > 0 and api_requests % 35 == 0:
            cooldown = random.uniform(90, 150)
            print(f"\n  --- Cooldown {cooldown:.0f}s after {api_requests} API requests ---\n")
            time.sleep(cooldown)

        print(f"  [{processed}/{len(to_process)}] @{username}...", end=" ", flush=True)

        try:
            profile = fetch_ig_profile(username)
            api_requests += 1
        except urllib.error.HTTPError as e:
            if e.code in (401, 429):
                print(f"\n  IG RATE LIMITED ({e.code}) after {api_requests} requests. Re-run later.")
                break
            progress[username] = {"error": str(e.code)}
            print(f"http {e.code}")
            time.sleep(3)
            continue
        except Exception as e:
            progress[username] = {"error": str(e)[:50]}
            print("error")
            time.sleep(3)
            continue

        if not profile:
            progress[username] = {"error": "not_found"}
            print("not found")
            time.sleep(random.uniform(15, 22))
            continue

        name = profile["full_name"]
        bio = profile["bio"]
        followers = profile["followers"]

        if followers < 50:
            progress[username] = {"error": "too_small", "followers": followers}
            print(f"skip tiny ({followers} flw)")
            time.sleep(random.uniform(15, 22))
            continue

        if is_business_ig(name, bio, username, followers):
            progress[username] = {"error": "business", "full_name": name}
            biz_skipped += 1
            print(f"SKIP biz ({name[:25]})")
            time.sleep(random.uniform(15, 22))
            continue

        all_emails = set()

        for e in extract_emails_from_text(bio):
            if is_consumer_email(e):
                all_emails.add(e)

        biz_email = profile.get("business_email", "")
        if biz_email and is_consumer_email(biz_email):
            all_emails.add(biz_email.lower())

        ext_url = profile.get("external_url", "")
        url_emails = []
        if not all_emails and ext_url:
            try:
                url_emails = scrape_url_for_emails(ext_url)
                all_emails.update(url_emails)
            except Exception:
                pass

        progress[username] = {
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
            emails_found += 1
            print(f"EMAIL {followers:>7,} flw | {list(all_emails)[:2]}")
        else:
            print(f"  ~   {followers:>7,} flw | no email")

        if processed % 15 == 0:
            save_progress(progress)
            total_e = sum(1 for p in progress.values() if isinstance(p, dict) and p.get("emails"))
            print(f"  ── saved | {processed}/{len(to_process)} | {emails_found} new emails | biz={biz_skipped} | total w/email={total_e} ──")

        time.sleep(random.uniform(15, 22))

    save_progress(progress)
    build_csv(progress)
    return progress


def save_progress(extraction_progress):
    dp = {}
    try:
        with open(PROGRESS_FILE) as f:
            dp = json.load(f)
    except FileNotFoundError:
        pass
    dp["extraction"] = extraction_progress
    with open(PROGRESS_FILE, "w") as f:
        json.dump(dp, f, indent=1, default=str)


def build_csv(progress):
    rows = []
    seen = set()

    for username, p in progress.items():
        if not isinstance(p, dict) or p.get("error"):
            continue
        emails = p.get("emails", [])
        if not emails:
            continue

        name = p.get("full_name", "")
        bio = p.get("bio", "")
        followers = p.get("followers", 0)

        for email in emails:
            if email in seen:
                continue
            seen.add(email)
            rows.append({
                "email": email,
                "name": name,
                "ig_username": username,
                "followers": followers,
                "platform": "instagram",
                "source": f"reddit/{p.get('source', 'unknown')}",
                "bio": bio.replace("\n", " ")[:200],
                "external_url": p.get("external_url", ""),
            })

    fieldnames = ["email", "name", "ig_username", "followers", "platform",
                  "source", "bio", "external_url"]

    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  CSV: {CSV_FILE}")
    print(f"  Consumer emails: {len(rows)}")


def show_stats():
    print("+" + "=" * 60 + "+")
    print("|  REDDIT PIPELINE STATS                                   |")
    print("+" + "=" * 60 + "+\n")

    handles = {}
    try:
        with open(HANDLES_FILE) as f:
            handles = json.load(f)
    except FileNotFoundError:
        pass

    progress = {}
    try:
        with open(PROGRESS_FILE) as f:
            dp = json.load(f)
        progress = dp.get("extraction", {})
    except FileNotFoundError:
        pass

    print(f"  Total IG handles discovered: {len(handles)}")

    by_sub = {}
    by_source = {}
    for h, info in handles.items():
        s = info.get("sub", "?")
        by_sub[s] = by_sub.get(s, 0) + 1
        src = info.get("source", "?")
        by_source[src] = by_source.get(src, 0) + 1

    print(f"\n  By subreddit:")
    for s, c in sorted(by_sub.items(), key=lambda x: -x[1]):
        print(f"    r/{s}: {c}")
    print(f"\n  By source: {by_source}")

    checked = len(progress)
    with_email = sum(1 for p in progress.values() if isinstance(p, dict) and p.get("emails"))
    biz = sum(1 for p in progress.values() if isinstance(p, dict) and p.get("error") == "business")
    errors = sum(1 for p in progress.values() if isinstance(p, dict) and p.get("error"))

    all_emails = set()
    for p in progress.values():
        if isinstance(p, dict) and p.get("emails"):
            all_emails.update(p["emails"])

    print(f"\n  Extraction: {checked} checked, {with_email} with email, {biz} businesses")
    print(f"  Unique consumer emails: {len(all_emails)}")
    print(f"  Remaining: {len(handles) - checked}")

    if all_emails:
        print(f"\n  Sample emails:")
        for e in list(all_emails)[:10]:
            print(f"    {e}")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "full"

    if mode == "stats":
        show_stats()
        return

    if mode in ("full", "discover"):
        handles = discover_handles()
    else:
        try:
            with open(HANDLES_FILE) as f:
                handles = json.load(f)
        except FileNotFoundError:
            print("No handles file. Run 'discover' first.")
            return

    if mode in ("full", "extract"):
        extract_emails_pipeline(handles)


if __name__ == "__main__":
    main()
