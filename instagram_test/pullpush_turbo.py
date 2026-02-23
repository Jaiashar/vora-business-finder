#!/usr/bin/env python3
"""
PullPush TURBO — Cross-Reddit Email Harvester
===============================================
Instead of iterating 170 subs one by one, this searches across ALL of
Reddit with targeted queries like "gmail.com" + fitness keywords.
Each query pages deeply (up to 5000 results) to maximize yield.

Runs WAY faster than the per-subreddit approach.
"""

import os, re, csv, sys, time, json, ssl
import urllib.request, urllib.error
from datetime import datetime
from urllib.parse import quote_plus

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

IG_RE = re.compile(r'(?:@|instagram\.com/)([a-zA-Z0-9_.]{3,30})', re.I)
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

SKIP_HANDLES = {
    'p', 'reel', 'explore', 'stories', 'tv', 'accounts', 'direct', 'reels',
    'instagram', 'facebook', 'twitter', 'youtube', 'tiktok', 'snapchat',
    'nikerunning', 'nike', 'adidas', 'garmin', 'fitbit', 'whoop', 'oura',
    'peloton', 'lululemon', 'gymshark', 'underarmour', 'reebok',
}

PERSONAL_PROVIDERS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
    'icloud.com', 'me.com', 'mac.com', 'protonmail.com', 'proton.me',
    'live.com', 'msn.com', 'ymail.com', 'rocketmail.com',
    'mail.com', 'zoho.com', 'gmx.com', 'comcast.net', 'att.net',
    'verizon.net', 'cox.net', 'sbcglobal.net', 'bellsouth.net',
    'earthlink.net', 'googlemail.com', 'fastmail.com',
    'yahoo.co.uk', 'hotmail.co.uk', 'btinternet.com',
}

BIZ_LOCAL_WORDS = {
    'gym', 'studio', 'crossfit', 'llc', 'inc', 'brand', 'academy',
    'solutions', 'services', 'group', 'supplement',
    'apparel', 'consulting', 'foundation', 'association',
    'league', 'network', 'collective', 'partners', 'agency', 'mgmt',
    'magazine', 'official', 'headquarters', 'corporate', 'enterprise',
    'company', 'clinic', 'hospital', 'therapy', 'school', 'university',
    'shop', 'store', 'sell', 'promo', 'wholesale', 'retail',
    'seo', 'marketing', 'smm', 'distro', 'vendor',
    'coach', 'trainer', 'instruct', 'expert', 'consult',
    'crypto', 'bitcoin', 'forex', 'trading', 'casino', 'gambl',
    'pharma', 'steroid', 'dating', 'escort', 'hack', 'spam',
    'team', 'squad', 'crew',
}

BIZ_PREFIXES = {
    'hello', 'info', 'contact', 'press', 'media', 'pr', 'booking',
    'team', 'sales', 'admin', 'office', 'management', 'support',
    'careers', 'marketing', 'events', 'membership', 'studio',
    'news', 'promo', 'merch', 'shop', 'store', 'customerservice',
    'hr', 'ceo', 'cfo', 'billing', 'legal', 'noreply', 'webmaster',
}

QUERIES = [
    # Email provider + fitness keywords — each yields thousands of results
    'gmail.com fitness', 'gmail.com workout', 'gmail.com running',
    'gmail.com yoga', 'gmail.com weight loss', 'gmail.com crossfit',
    'gmail.com gym', 'gmail.com marathon', 'gmail.com cycling',
    'gmail.com hiking', 'gmail.com swimming', 'gmail.com diet',
    'gmail.com keto', 'gmail.com fasting', 'gmail.com bodybuilding',
    'gmail.com lifting', 'gmail.com health', 'gmail.com wellness',
    'gmail.com meditation', 'gmail.com apple watch',
    'gmail.com garmin', 'gmail.com fitbit', 'gmail.com oura',
    'gmail.com whoop', 'gmail.com strava',
    'gmail.com climbing', 'gmail.com mma', 'gmail.com bjj',
    'gmail.com boxing', 'gmail.com pilates', 'gmail.com peloton',
    'gmail.com accountability partner',
    'gmail.com accountability buddy', 'gmail.com workout buddy',
    'gmail.com running partner', 'gmail.com running buddy',

    'yahoo.com fitness', 'yahoo.com workout', 'yahoo.com running',
    'yahoo.com weight loss', 'yahoo.com gym', 'yahoo.com health',
    'yahoo.com diet', 'yahoo.com yoga',

    'hotmail.com fitness', 'hotmail.com workout', 'hotmail.com running',
    'hotmail.com weight loss', 'hotmail.com health',
    'hotmail.com diet', 'hotmail.com gym',

    'outlook.com fitness', 'outlook.com workout', 'outlook.com running',
    'outlook.com weight loss', 'outlook.com health',

    'my email fitness', 'my email workout', 'my email running',
    'email me fitness', 'email me workout', 'email me running',
    'reach me at gmail', 'contact me gmail fitness',

    # IG handles in fitness contexts
    'instagram fitness accountability',
    'share your instagram fitness',
    'follow me instagram running',
    'my ig fitness',
    'instagram challenge fitness',
    'add me on instagram gym',
]

PROGRESS_FILE = os.path.join(BASE_DIR, "pullpush_turbo_progress.json")
CSV_FILE = os.path.join(BASE_DIR, "pullpush_turbo_leads.csv")

MAX_PAGES = 50  # up to 5000 results per query


def pullpush_get(endpoint, params, retries=3):
    base = f"https://api.pullpush.io/reddit/search/{endpoint}/?"
    qs = "&".join(f"{k}={quote_plus(str(v))}" for k, v in params.items())
    url = base + qs
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            resp = urllib.request.urlopen(req, timeout=30, context=SSL_CTX)
            return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 8 * (attempt + 1)
                time.sleep(wait)
                continue
            raise
        except Exception:
            time.sleep(5)
    return {"data": []}


def is_consumer_email(email):
    email = email.lower().strip()
    if '@' not in email:
        return False
    local, domain = email.split("@", 1)
    if domain.endswith((".edu", ".gov", ".org", ".mil")):
        return False
    if ".k12." in domain:
        return False
    if not re.match(r"^[a-z0-9.-]+\.[a-z]{2,6}$", domain):
        return False
    if local in BIZ_PREFIXES:
        return False
    clean = local.replace('.', '').replace('_', '').replace('-', '')
    for w in BIZ_LOCAL_WORDS:
        if w in clean:
            return False
    if domain in PERSONAL_PROVIDERS:
        if len(local) > 25 or len(local) < 3:
            return False
        return True
    return False


def extract_handles(text):
    handles = set()
    for m in IG_RE.finditer(text):
        h = m.group(1).lower().strip(".")
        if h not in SKIP_HANDLES and 3 <= len(h) <= 28:
            if not h.startswith("_") or not h.endswith("_"):
                handles.add(h)
    return handles


def extract_emails_from_text(text):
    found = EMAIL_RE.findall(text.lower())
    return [
        e for e in set(found)
        if is_consumer_email(e)
        and not any(k in e for k in [
            'noreply', 'no-reply', 'test@', 'example',
            'user@', 'admin@', 'donotreply', 'placeholder',
        ])
        and not e.endswith(('.png', '.jpg', '.gif', '.svg', '.webp'))
    ]


def main():
    print("+" + "=" * 60 + "+")
    print("|  PULLPUSH TURBO — CROSS-REDDIT EMAIL HARVESTER           |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    progress = {}
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)

    all_handles = progress.get("handles", {})
    all_emails = progress.get("emails", {})
    done_queries = set(progress.get("done_queries", []))

    # Also load wave 1 emails to dedup
    w1_file = os.path.join(BASE_DIR, "pullpush_progress.json")
    existing_emails = set()
    if os.path.exists(w1_file):
        with open(w1_file) as f:
            w1 = json.load(f)
        existing_emails = set(w1.get("emails", {}).keys())

    print(f"  Existing turbo: {len(all_handles)} handles, {len(all_emails)} emails")
    print(f"  Wave 1 emails to dedup: {len(existing_emails)}")
    print(f"  Done queries: {len(done_queries)}/{len(QUERIES)}\n")

    new_queries = [q for q in QUERIES if q not in done_queries]
    print(f"  New queries: {len(new_queries)}\n")

    for qi, query in enumerate(new_queries):
        print(f"\n  [{qi+1}/{len(new_queries)}] \"{query}\"")

        q_handles = 0
        q_emails = 0
        total_items = 0

        # Search comments
        before = None
        for page in range(MAX_PAGES):
            try:
                params = {"q": query, "size": 100}
                if before:
                    params["before"] = before

                data = pullpush_get("comment", params)
                comments = data.get("data", [])

                if not comments:
                    break

                total_items += len(comments)

                for c in comments:
                    body = c.get("body", "") or ""
                    author = c.get("author", "") or ""
                    sub = c.get("subreddit", "") or ""

                    for h in extract_handles(body):
                        if h not in all_handles:
                            all_handles[h] = {"sub": sub, "author": author, "query": query}
                            q_handles += 1

                    for e in extract_emails_from_text(body):
                        if e not in all_emails and e not in existing_emails:
                            all_emails[e] = {
                                "sub": sub,
                                "author": author,
                                "source": "comment",
                                "query": query,
                            }
                            q_emails += 1

                timestamps = [c.get("created_utc", 0) for c in comments
                              if isinstance(c.get("created_utc"), (int, float))]
                if timestamps:
                    before = min(timestamps)
                else:
                    break

                if len(comments) < 100:
                    break

                time.sleep(1.5)

            except Exception as e:
                print(f"    p{page} ERR: {str(e)[:40]}")
                time.sleep(5)
                break

        # Search submissions too
        try:
            params = {"q": query, "size": 100}
            data = pullpush_get("submission", params)
            posts = data.get("data", [])
            total_items += len(posts)

            for p in posts:
                text = f"{p.get('title', '')} {p.get('selftext', '')}"
                sub = p.get("subreddit", "") or ""

                for h in extract_handles(text):
                    if h not in all_handles:
                        all_handles[h] = {"sub": sub, "author": p.get("author", ""), "query": query}
                        q_handles += 1

                for e in extract_emails_from_text(text):
                    if e not in all_emails and e not in existing_emails:
                        all_emails[e] = {
                            "sub": sub,
                            "author": p.get("author", ""),
                            "source": "post",
                            "query": query,
                        }
                        q_emails += 1

            time.sleep(1)
        except Exception:
            pass

        done_queries.add(query)
        print(f"    {total_items} items | +{q_handles} handles | +{q_emails} emails | "
              f"totals: {len(all_handles)} handles, {len(all_emails)} emails")

        # Save after each query
        progress["handles"] = all_handles
        progress["emails"] = all_emails
        progress["done_queries"] = list(done_queries)
        with open(PROGRESS_FILE, "w") as f:
            json.dump(progress, f, indent=1)

    # Final save
    progress["handles"] = all_handles
    progress["emails"] = all_emails
    progress["done_queries"] = list(done_queries)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=1)

    # Build CSV
    rows = []
    for email, info in all_emails.items():
        rows.append({
            "email": email,
            "source": f"reddit/{info.get('sub', '?')}",
            "reddit_author": info.get("author", ""),
            "query": info.get("query", ""),
            "type": info.get("source", "comment"),
        })

    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["email", "source", "reddit_author", "query", "type"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  CSV: {CSV_FILE} ({len(rows)} emails)")
    print(f"\n  TURBO COMPLETE: {len(all_handles)} IG handles, {len(all_emails)} direct emails")


if __name__ == "__main__":
    main()
