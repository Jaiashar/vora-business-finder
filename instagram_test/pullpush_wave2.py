#!/usr/bin/env python3
"""
PullPush Wave 2 — Massive expansion
=====================================
120+ new subreddits, more search terms, focused on email extraction.
Reuses wave 1 progress file so emails/handles are deduplicated.
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
    'peloton', 'lululemon', 'gymshark', 'underarmour', 'reebok', 'asics',
    'brooks', 'hoka', 'newbalance', 'saucony', 'on_running',
    'runnersworld', 'runnersworldmag', 'menshealth', 'womenshealth',
    'shape', 'self', 'crossfit', 'orangetheory', 'planetfitness',
    'equinox', 'soulcycle', 'barrys', 'f45_training',
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
    'solutions', 'services', 'group', 'fitnesscenter', 'supplement',
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

WAVE2_SUBREDDITS = [
    # Health & Wellness
    'HealthyFood', 'nutrition', 'Supplements', 'SkincareAddiction',
    'sleep', 'Meditation', 'mindfulness', 'Nootropics',
    'PlantBasedDiet', 'WholeFoodsPlantBased', 'EatCheapAndHealthy',
    'MealPrepSunday', '1500isplenty', '1200isplenty', 'Volumeeating',
    'CICO', 'fatlogic', 'SuperMorbidlyObese', 'PlusSize',
    'fasting', 'OmadDiet', 'zerocarb', 'carnivore', 'Paleo',
    'whole30', 'glutenfree', 'AntiInflammatoryDiet',

    # Weight Loss / Body Transformation
    'BulkOrCut', 'leangains', 'cut', 'recomp', 'Fitness30Plus',
    'FitnessOver50', 'xxloseit', 'WeightLossAdvice', 'WeightLoss',
    'ObesityActionCoalition', 'Myfitnesspal', 'BTFC',
    'Brogress', 'brogress',

    # Running & Endurance
    'Marathon', 'halfmarathon', 'firstmarathon', 'ultrarunning',
    'RunnersOfTheUS', 'RunNYC', 'SFrunning', 'RunChicago',
    'trailrunning', 'skyrunning', 'Couch25K', 'B210K',
    'Strava', 'nikerunclub', 'runninglifestyle',

    # Outdoor/Adventure Fitness
    'CampingandHiking', 'Ultralight', 'backpacking', 'trailrunning',
    'surfing', 'snowboarding', 'skiing', 'skateboarding',
    'longboarding', 'ParkourTeachers', 'Parkour',
    'rockclimbing', 'alpinism', 'mountaineering',
    'kayaking', 'canoeing', 'paddleboarding',
    'scuba', 'freediving', 'spearfishing',

    # Combat Sports / Martial Arts
    'martialarts', 'Judo', 'wrestling', 'Taekwondo',
    'WingChun', 'kungfu', 'karate', 'fencing',
    'amateur_boxing', 'Kickboxing',

    # Team Sports / Recreational
    'volleyball', 'basketball', 'tennis', 'golf',
    'soccer', 'football', 'rugbyunion', 'lacrosse',
    'ultimate', 'pickleball', 'badminton', 'tabletennis',
    'SquashFit', 'racquetball', 'softball', 'baseball',
    'FieldHockey', 'waterpolo',

    # Wearables & Tech
    'GarminFenix', 'Garmin', 'coros', 'amazfit',
    'galaxywatch', 'wearables', 'QuantifiedSelf',
    'AppleFitnessPlus', 'AppleWatchUltra',
    'fitbitversa', 'withings', 'polarfitness',

    # Gym / Lifting
    'GripTraining', 'homegym', 'weightroom', 'Strongman',
    'strength_training', 'StartingStrength', 'StrongLifts5x5',
    'formcheck', 'GYM', 'kettleballs', 'resistance_band',

    # Mind-Body / Recovery
    'Stretching', 'mobility', 'posture', 'physicaltherapy',
    'ChronicPain', 'Fibromyalgia', 'backpain',
    'yoga', 'Ashtanga', 'Bikram', 'hotyoga',

    # Women-focused
    'XXRunning', 'xxketo', 'PeriodGym', 'PCOSloseit',
    'PCOS', 'prenatalfitness', 'fitpregnancy', 'postpartumfitness',
    'MomFit', 'StrongMoms',

    # Mental Health + Fitness
    'DecidingToBeBetter', 'getdisciplined', 'selfimprovement',
    'NonZeroDay', 'thexeffect', 'Habits',

    # Specific Programs
    'p90x', 'BeachBodyWorkouts', 'T25', 'LesMillsOnDemand',
    'F45', 'BarryBootcamp', 'SolidCore',
    'obstaclecourserace', 'Hyrox', 'ToughMudder', 'GoRuck',

    # General Health & Medical Fitness
    'diabetes', 'diabetes_t2', 'prediabetes',
    'Hypothyroidism', 'Hashimotos',
    'ibs', 'CrohnsDisease', 'UlcerativeColitis',
    'Migraine', 'ADHD', 'adhdwomen',
]

SEARCH_TERMS = [
    'gmail.com', 'instagram', 'my email', 'follow me',
    'yahoo.com', 'hotmail.com', 'accountability',
]

PROGRESS_FILE = os.path.join(BASE_DIR, "pullpush_progress.json")
HANDLES_FILE = os.path.join(BASE_DIR, "pullpush_ig_handles.json")
CSV_FILE = os.path.join(BASE_DIR, "pullpush_leads.csv")


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
    print("|  PULLPUSH WAVE 2 — MASSIVE EXPANSION                     |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    progress = {}
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)

    all_handles = progress.get("handles", {})
    all_emails = progress.get("emails", {})
    done_subs = set(progress.get("done_subs", []))

    print(f"  Existing: {len(all_handles)} handles, {len(all_emails)} emails")
    print(f"  Done subs: {len(done_subs)}")
    print(f"  Wave 2 subs: {len(WAVE2_SUBREDDITS)}\n")

    new_subs = [s for s in WAVE2_SUBREDDITS if s not in done_subs]
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
                                all_handles[h] = {
                                    "sub": sub,
                                    "author": author,
                                    "term": term,
                                }
                                sub_handles += 1

                        for e in extract_emails_from_text(body):
                            if e not in all_emails:
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

                    time.sleep(2)

                except Exception as e:
                    print(f"    [{term}] p{pages} ERROR: {str(e)[:40]}")
                    time.sleep(5)
                    break

            # Also search submissions
            try:
                params = {"subreddit": sub, "q": term, "size": 100}
                data = pullpush_get("submission", params)
                posts = data.get("data", [])
                total_items += len(posts)

                for p in posts:
                    text = f"{p.get('title', '')} {p.get('selftext', '')}"

                    for h in extract_handles(text):
                        if h not in all_handles:
                            all_handles[h] = {
                                "sub": sub,
                                "author": p.get("author", ""),
                                "term": term,
                            }
                            sub_handles += 1

                    for e in extract_emails_from_text(text):
                        if e not in all_emails:
                            all_emails[e] = {
                                "sub": sub,
                                "author": p.get("author", ""),
                                "source": "post",
                            }
                            sub_emails += 1

                time.sleep(1.5)
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

    with open(HANDLES_FILE, "w") as f:
        json.dump(all_handles, f, indent=1)

    build_csv(all_handles, all_emails)
    print(f"\n  WAVE 2 COMPLETE: {len(all_handles)} IG handles, {len(all_emails)} direct emails")


def build_csv(handles, emails):
    rows = []
    for email, info in emails.items():
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


if __name__ == "__main__":
    main()
