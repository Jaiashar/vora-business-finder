#!/usr/bin/env python3
"""
PullPush Wave 3 — Lifestyle & General Consumer
================================================
Targets lifestyle, self-improvement, daily life subreddits where
average joes share emails and Instagram. Separate progress file.
"""

import os, re, csv, sys, time, json, random, ssl
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

WAVE3_SUBREDDITS = [
    # Accountability / Self-Improvement
    'GetMotivated', 'productivity', 'getdisciplined', 'selfimprovement',
    'DecidingToBeBetter', 'Habits', 'NonZeroDay', 'theXeffect',
    'ZenHabits', 'LifeProTips', 'socialskills', 'confidence',
    'Journaling', 'bulletjournal', 'morningroutines',
    'datingoverthirty', 'datingoverforty',

    # Lifestyle / Hobbies with health overlap
    'cooking', 'Baking', 'gardening', 'homestead',
    'dogs', 'dogtraining', 'RunningWithDogs',
    'bicycletouring', 'bikecommuting', 'ebikes',
    'outdoors', 'camping', 'fishing', 'hunting',
    'photography', 'travel', 'solotravel', 'digitalnomad',

    # Specific health conditions (people share personal emails)
    'keto', 'Psoriasis', 'eczema', 'Asthma',
    'Allergies', 'CeliacDisease', 'foodallergy',
    'mentalhealth', 'Anxiety', 'depression', 'bipolar',
    'PTSD', 'OCD', 'EDrecovery', 'StopDrinking',
    'leaves', 'stopdrinking', 'sober',

    # Parenting + Fitness
    'Parenting', 'beyondthebump', 'Mommit', 'daddit',
    'fitpregnancy', 'BabyBumps', 'toddlers',

    # Wearable communities (deep cuts)
    'garmin', 'GarminVenu', 'vivosmart', 'forerunner',
    'amazfit', 'samsunghealth', 'galaxywatchactive',
    'miband', 'HuaweiWatch',

    # General health communities
    'AskDocs', 'medical', 'DiagnoseMe', 'HealthAnxiety',
    'Dentistry', 'optometry', 'hearing',
    'birthcontrol', 'WomensHealth', 'MensHealth',

    # Sports fans (they often are active themselves)
    'MMA', 'UFC', 'Boxing_', 'kickboxing',
    'rugbyunion', 'AFL', 'cricket',
    'sportsscience', 'sportsmedicine',

    # College fitness
    'college', 'GradSchool', 'lawschool', 'medicalschool',
    'premed', 'predental', 'prenursing',
]

SEARCH_TERMS = [
    'gmail.com', 'yahoo.com', 'hotmail.com',
    'my email', 'email me', 'accountability',
    'instagram',
]

PROGRESS_FILE = os.path.join(BASE_DIR, "pullpush_wave3_progress.json")
CSV_FILE = os.path.join(BASE_DIR, "pullpush_wave3_leads.csv")


def pullpush_get(endpoint, params, retries=3):
    base = f"https://api.pullpush.io/reddit/search/{endpoint}/?"
    qs = "&".join(f"{k}={quote_plus(str(v))}" for k, v in params.items())
    url = base + qs
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            resp = urllib.request.urlopen(req, timeout=20, context=SSL_CTX)
            return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 10 * (attempt + 1)
                time.sleep(wait)
                continue
            raise
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
        if len(local) > 25:
            return False
        if len(local) < 3:
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
    print("|  PULLPUSH WAVE 3 — LIFESTYLE & GENERAL CONSUMER          |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    # Load wave 1 emails to deduplicate
    wave1_emails = set()
    w1_file = os.path.join(BASE_DIR, "pullpush_progress.json")
    if os.path.exists(w1_file):
        with open(w1_file) as f:
            w1 = json.load(f)
        wave1_emails = set(w1.get("emails", {}).keys())
        print(f"  Wave 1 emails to skip: {len(wave1_emails)}")

    progress = {}
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)

    all_handles = progress.get("handles", {})
    all_emails = progress.get("emails", {})
    done_subs = set(progress.get("done_subs", []))

    print(f"  Wave 3 existing: {len(all_handles)} handles, {len(all_emails)} emails")
    print(f"  Done subs: {len(done_subs)}\n")

    new_subs = [s for s in WAVE3_SUBREDDITS if s not in done_subs]
    print(f"  New subs to process: {len(new_subs)}\n")

    for si, sub in enumerate(new_subs):
        print(f"\n  [{si+1}/{len(new_subs)}] r/{sub}")

        sub_handles = 0
        sub_emails = 0
        total_items = 0

        for term in SEARCH_TERMS:
            before = None
            pages = 0
            max_pages = 8

            while pages < max_pages:
                pages += 1
                try:
                    params = {"subreddit": sub, "q": term, "size": 100}
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

                        for h in extract_handles(body):
                            if h not in all_handles:
                                all_handles[h] = {"sub": sub, "author": author}
                                sub_handles += 1

                        for e in extract_emails_from_text(body):
                            if e not in all_emails and e not in wave1_emails:
                                all_emails[e] = {
                                    "sub": sub,
                                    "author": author,
                                    "source": "comment",
                                }
                                sub_emails += 1

                    timestamps = [c.get("created_utc", 0) for c in comments
                                  if isinstance(c.get("created_utc"), (int, float))]
                    if timestamps:
                        before = min(timestamps)
                    else:
                        break

                    if len(comments) < 100:
                        break

                    time.sleep(1.2)

                except Exception as e:
                    print(f"    [{term}] p{pages} ERROR: {str(e)[:40]}")
                    time.sleep(3)
                    break

            try:
                params = {"subreddit": sub, "q": term, "size": 100}
                data = pullpush_get("submission", params)
                posts = data.get("data", [])
                total_items += len(posts)

                for p in posts:
                    text = f"{p.get('title', '')} {p.get('selftext', '')}"

                    for h in extract_handles(text):
                        if h not in all_handles:
                            all_handles[h] = {"sub": sub, "author": p.get("author", "")}
                            sub_handles += 1

                    for e in extract_emails_from_text(text):
                        if e not in all_emails and e not in wave1_emails:
                            all_emails[e] = {
                                "sub": sub,
                                "author": p.get("author", ""),
                                "source": "post",
                            }
                            sub_emails += 1

                time.sleep(0.8)
            except Exception:
                pass

        done_subs.add(sub)
        print(f"    {total_items} items | +{sub_handles} handles | +{sub_emails} emails | "
              f"totals: {len(all_handles)} handles, {len(all_emails)} emails")

        progress["handles"] = all_handles
        progress["emails"] = all_emails
        progress["done_subs"] = list(done_subs)
        with open(PROGRESS_FILE, "w") as f:
            json.dump(progress, f, indent=1)

    progress["handles"] = all_handles
    progress["emails"] = all_emails
    progress["done_subs"] = list(done_subs)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=1)

    rows = []
    for email, info in all_emails.items():
        rows.append({
            "email": email,
            "source": f"reddit/{info.get('sub', '?')}",
            "reddit_author": info.get("author", ""),
            "type": info.get("source", "comment"),
        })

    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["email", "source", "reddit_author", "type"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  CSV: {CSV_FILE} ({len(rows)} emails)")
    print(f"\n  WAVE 3 COMPLETE: {len(all_handles)} IG handles, {len(all_emails)} direct emails")


if __name__ == "__main__":
    main()
