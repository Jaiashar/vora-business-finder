#!/usr/bin/env python3
"""
PullPush MEGA Harvester — Road to 10k
=======================================
Two-phase approach:
  Phase 1: Per-subreddit scraping (300+ subs)
  Phase 2: Cross-Reddit keyword queries

Reuses existing progress file, auto-pushes to Supabase every 500 new emails.
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
    'p','reel','explore','stories','tv','accounts','direct','reels',
    'instagram','facebook','twitter','youtube','tiktok','snapchat',
    'nikerunning','nike','adidas','garmin','fitbit','whoop','oura',
    'peloton','lululemon','gymshark','underarmour','reebok','asics',
    'brooks','hoka','newbalance','saucony','on_running',
    'runnersworld','menshealth','womenshealth','crossfit',
    'orangetheory','planetfitness','equinox','soulcycle','barrys',
}

PERSONAL_PROVIDERS = {
    'gmail.com','yahoo.com','hotmail.com','outlook.com','aol.com',
    'icloud.com','me.com','mac.com','protonmail.com','proton.me',
    'live.com','msn.com','ymail.com','rocketmail.com',
    'mail.com','zoho.com','gmx.com','comcast.net','att.net',
    'verizon.net','cox.net','sbcglobal.net','bellsouth.net',
    'earthlink.net','googlemail.com','fastmail.com',
    'yahoo.co.uk','hotmail.co.uk','btinternet.com',
}

BIZ_WORDS = {
    'gym','studio','crossfit','llc','inc','brand','academy',
    'solutions','services','group','supplement','apparel','consulting',
    'foundation','association','league','network','collective','partners',
    'agency','mgmt','magazine','official','headquarters','corporate',
    'enterprise','company','clinic','hospital','therapy','school',
    'university','shop','store','sell','promo','wholesale','retail',
    'seo','marketing','smm','distro','vendor','coach','trainer',
    'instruct','expert','consult','crypto','bitcoin','forex','trading',
    'casino','gambl','pharma','steroid','dating','escort','hack','spam',
    'team','squad','crew','club','association','federation',
    'collab','booking','talent','management','record','label',
    'healing','suites','rental','properties','dance','equipment',
    'optimize','culture','connect','supply','depot','warehouse',
    'center','centre','institute','movement','photography',
    'videography','production','tattoo','grooming','daycare',
    'webdev','software','graphic','printing','repair','handyman',
    'pest','trucking','freight','shipping',
}

BIZ_PREFIXES = {
    'hello','info','contact','press','media','pr','booking',
    'team','sales','admin','office','management','support',
    'careers','marketing','events','membership','studio',
    'news','promo','merch','shop','store','customerservice',
    'hr','ceo','cfo','billing','legal','noreply','webmaster',
}

DUMMY_EMAILS = {
    'email@gmail.com','first.last@gmail.com','firstlast@gmail.com',
    'primaryemail@gmail.com','youremail@gmail.com','name@gmail.com',
    'test@gmail.com','example@gmail.com','user@gmail.com',
    'john.doe@gmail.com','janedoe@gmail.com','sample@gmail.com',
    'username@gmail.com','myemail@gmail.com',
}

ALL_SUBREDDITS = [
    # === FITNESS CORE ===
    'xxfitness','running','loseit','fitness','yoga','crossfit',
    'bodyweightfitness','Peloton','homegym','progresspics',
    'weightlifting','cycling','triathlon','C25K','naturalbodybuilding',
    'powerlifting','kickboxing','bjj','hiking','ultrarunning',
    'trailrunning','Swimming','kettlebell','MuayThai','boxing',
    'Brogress','gainit','StrongCurves','flexibility','Calisthenics',
    'orangetheory','climbing','bouldering','pilates','GYM',
    'spartanrace','MTB','Zwift','AdvancedRunning','EOOD',
    '90daysgoal','veganfitness','intermittentfasting','keto',
    'workout','Rowing','insanity','pelotoncycle',
    'GarminWatches','AppleWatch','fitbit','ouraring','whoop',
    'AppleWatchFitness',

    # === HEALTH & WELLNESS ===
    'HealthyFood','nutrition','Supplements','SkincareAddiction',
    'sleep','Meditation','mindfulness','Nootropics',
    'PlantBasedDiet','WholeFoodsPlantBased','EatCheapAndHealthy',
    'MealPrepSunday','1500isplenty','1200isplenty','Volumeeating',
    'CICO','fatlogic','SuperMorbidlyObese','PlusSize',
    'fasting','OmadDiet','zerocarb','carnivore','Paleo',
    'whole30','glutenfree','AntiInflammatoryDiet',

    # === WEIGHT LOSS / TRANSFORM ===
    'BulkOrCut','leangains','cut','recomp','Fitness30Plus',
    'FitnessOver50','xxloseit','WeightLossAdvice','WeightLoss',
    'ObesityActionCoalition','Myfitnesspal','BTFC','brogress',

    # === RUNNING & ENDURANCE ===
    'Marathon','halfmarathon','firstmarathon',
    'RunnersOfTheUS','RunNYC','SFrunning','RunChicago',
    'skyrunning','Couch25K','B210K','Strava','nikerunclub',

    # === OUTDOOR / ADVENTURE ===
    'CampingandHiking','Ultralight','backpacking',
    'surfing','snowboarding','skiing','skateboarding',
    'rockclimbing','alpinism','mountaineering',
    'kayaking','canoeing','paddleboarding',
    'scuba','freediving',

    # === COMBAT SPORTS ===
    'martialarts','Judo','wrestling','Taekwondo',
    'WingChun','kungfu','karate','fencing','amateur_boxing',

    # === TEAM / RECREATIONAL SPORTS ===
    'volleyball','basketball','tennis','golf',
    'soccer','rugbyunion','lacrosse','ultimate',
    'pickleball','badminton','tabletennis','racquetball',
    'softball','baseball','waterpolo',

    # === WEARABLES / TECH ===
    'GarminFenix','Garmin','coros','amazfit',
    'galaxywatch','wearables','QuantifiedSelf',
    'AppleFitnessPlus','fitbitversa','withings','polarfitness',

    # === GYM / LIFTING ===
    'GripTraining','weightroom','Strongman',
    'StartingStrength','StrongLifts5x5','formcheck',

    # === MIND-BODY / RECOVERY ===
    'Stretching','mobility','posture',
    'ChronicPain','Fibromyalgia','backpain',
    'Ashtanga','Bikram','hotyoga',

    # === WOMEN-FOCUSED ===
    'XXRunning','xxketo','PCOSloseit','PCOS',
    'fitpregnancy','postpartumfitness',

    # === SELF-IMPROVEMENT ===
    'GetMotivated','productivity','getdisciplined','selfimprovement',
    'DecidingToBeBetter','Habits','NonZeroDay','theXeffect',

    # === SPECIFIC PROGRAMS ===
    'p90x','F45','obstaclecourserace','Hyrox','ToughMudder','GoRuck',

    # === HEALTH CONDITIONS ===
    'diabetes','diabetes_t2','prediabetes',
    'Hypothyroidism','Hashimotos',
    'ibs','CrohnsDisease','UlcerativeColitis',
    'ADHD','adhdwomen',

    # === LIFESTYLE + ACCOUNTABILITY ===
    'Journaling','bulletjournal',
    'StopDrinking','sober','leaves',
    'EDrecovery','BingeEatingDisorder',
    'Parenting','beyondthebump','Mommit','daddit',
    'bicycletouring','bikecommuting','ebikes',
    'dogs','RunningWithDogs',

    # === MORE WEARABLE DEEP CUTS ===
    'GarminVenu','vivosmart','forerunner',
    'samsunghealth','miband','HuaweiWatch',

    # === SPORTS SCIENCE ===
    'sportsscience','sportsmedicine',
    'AdvancedFitness','overcominggravity',

    # === NICHE FITNESS ===
    'kettlebells','sandbagtraining','bodyweightfitness',
    'AnarchyFitness','functionalfitness','MovementCulture',
    'Parkour','tricking','poledancing','aerialyoga',
    'aerialhoop','silks','trapeze',

    # === DIET / FOOD DEEP CUTS ===
    'fitmeals','1200isjerky','MeatlessMealPrep',
    'fermentation','Kombucha','juicing','Smoothies',
    'macros','flexibledieting','IIFYM',

    # === RECOVERY / BIOHACKING ===
    'coldstorage','coldshowers','BecomingTheIceman',
    'Sauna','RedLightTherapy','biohackers',
    'Breathwork','WimHof',

    # === MORE RUNNING ===
    'HalfMarathonTraining','CouchTo5K','running_training',
    'RunLA','RunATL','RunDenver','RunPortland',
    'RunSeattle','RunDC',

    # === CYCLING DEEP CUTS ===
    'Velo','peloton','CyclingFashion','gravelcycling',
    'bikepacking','fixedgearbicycle','BMX',
    'IndoorCycling','SpinClass',

    # === SWIMMING ===
    'triathlon','openwater','masterswimming','SwimmingLifestyle',

    # === GENERAL ===
    'AskReddit', 'CasualConversation', 'TwoXChromosomes',
    'AskWomen', 'AskMen', 'Frugal',
]

SEARCH_TERMS = [
    'gmail.com', 'instagram', 'my email',
    'yahoo.com', 'hotmail.com', 'accountability',
    'follow me',
]

CROSS_REDDIT_QUERIES = [
    'gmail.com accountability fitness',
    'gmail.com workout buddy',
    'gmail.com running partner',
    'gmail.com weight loss accountability',
    'my email address fitness',
    'email me workout',
    'reach me gmail running',
    'gmail.com apple watch',
    'gmail.com garmin strava',
    'gmail.com marathon training',
    'gmail.com yoga meditation',
    'gmail.com cycling',
    'gmail.com hiking buddy',
    'gmail.com climbing partner',
    'yahoo.com fitness accountability',
    'hotmail.com fitness workout',
    'outlook.com fitness running',
    'gmail.com diet accountability',
    'gmail.com keto buddy',
    'gmail.com fasting partner',
    'my email crossfit',
    'my email peloton',
    'my email orangetheory',
    'gmail.com bjj',
    'gmail.com swimming',
    'gmail.com home gym',
]

PROGRESS_FILE = os.path.join(BASE_DIR, "pullpush_progress.json")
CSV_FILE = os.path.join(BASE_DIR, "pullpush_mega_leads.csv")


def pullpush_get(endpoint, params, retries=4):
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
                wait = 15 * (attempt + 1)
                print(f"      429 rate limit, waiting {wait}s...")
                time.sleep(wait)
                continue
            raise
        except Exception:
            time.sleep(5)
    return {"data": []}


def is_consumer_email(email):
    email = email.lower().strip()
    if email in DUMMY_EMAILS:
        return False
    if '@' not in email:
        return False
    local, domain = email.split("@", 1)
    if domain.endswith((".edu", ".gov", ".org", ".mil")):
        return False
    if ".k12." in domain:
        return False
    if not re.match(r"^[a-z0-9.-]+\.[a-z]{2,6}$", domain):
        return False
    if domain not in PERSONAL_PROVIDERS:
        return False
    if local in BIZ_PREFIXES:
        return False
    if len(local) > 25 or len(local) < 3:
        return False
    clean = local.replace('.', '').replace('_', '').replace('-', '')
    for w in BIZ_WORDS:
        if w in clean:
            return False
    if re.match(r'^[a-z]{20,}$', clean):
        return False
    return True


def extract_handles(text):
    handles = set()
    for m in IG_RE.finditer(text):
        h = m.group(1).lower().strip(".")
        if h not in SKIP_HANDLES and 3 <= len(h) <= 28:
            if not h.startswith("_") or not h.endswith("_"):
                handles.add(h)
    return handles


def extract_emails(text):
    found = EMAIL_RE.findall(text.lower())
    return [
        e for e in set(found)
        if is_consumer_email(e)
        and not any(k in e for k in [
            'noreply','no-reply','test@','example',
            'user@','admin@','donotreply','placeholder',
        ])
        and not e.endswith(('.png','.jpg','.gif','.svg','.webp'))
    ]


def process_items(items, all_handles, all_emails, sub, is_post=False):
    new_h = 0
    new_e = 0
    for item in items:
        if is_post:
            text = f"{item.get('title', '')} {item.get('selftext', '')}"
        else:
            text = item.get("body", "") or ""
        author = item.get("author", "") or ""

        for h in extract_handles(text):
            if h not in all_handles:
                all_handles[h] = {"sub": sub, "author": author}
                new_h += 1

        for e in extract_emails(text):
            if e not in all_emails:
                all_emails[e] = {"sub": sub, "author": author, "source": "post" if is_post else "comment"}
                new_e += 1

    return new_h, new_e


def save_progress(progress, all_handles, all_emails, done_subs, done_queries):
    progress["handles"] = all_handles
    progress["emails"] = all_emails
    progress["done_subs"] = list(done_subs)
    progress["done_queries"] = list(done_queries)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=1)


def push_to_supabase(emails_dict):
    """Push new emails to Supabase, returns count pushed."""
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(BASE_DIR), '.env'))

    SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
    SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_KEY", "")
    if not SUPABASE_URL or not SUPABASE_KEY:
        return 0

    WEARABLE_SUBS = {'AppleWatch','AppleWatchFitness','fitbit','ouraring','whoop',
                     'GarminWatches','Garmin','GarminFenix','GarminVenu','coros',
                     'amazfit','galaxywatch','wearables','QuantifiedSelf',
                     'AppleFitnessPlus','fitbitversa','withings','polarfitness',
                     'vivosmart','forerunner','samsunghealth','miband','HuaweiWatch'}
    WELLNESS_SUBS = {'keto','intermittentfasting','loseit','EOOD','veganfitness',
                     'fasting','OmadDiet','Meditation','mindfulness','sleep',
                     'PlantBasedDiet','WholeFoodsPlantBased','EatCheapAndHealthy',
                     '1200isplenty','1500isplenty','Volumeeating','CICO',
                     'whole30','carnivore','Paleo','zerocarb','glutenfree',
                     'AntiInflammatoryDiet','nutrition','HealthyFood',
                     'StopDrinking','sober','EDrecovery','BingeEatingDisorder'}

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates",
    }

    rows = []
    for email, info in emails_dict.items():
        sub = info.get("sub", "")
        if sub in WEARABLE_SUBS:
            cat = "wearable_user"
        elif sub in WELLNESS_SUBS:
            cat = "wellness"
        else:
            cat = "fitness_consumer"
        rows.append({
            "email": email,
            "platform": "reddit",
            "category": cat,
            "tags": "{reddit,consumer,fitness}",
        })

    pushed = 0
    BATCH = 200
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i+BATCH]
        body = json.dumps(batch).encode("utf-8")
        url = f"{SUPABASE_URL}/rest/v1/consumer_leads?on_conflict=email"
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            resp = urllib.request.urlopen(req, timeout=30, context=SSL_CTX)
            pushed += len(batch)
        except Exception:
            pass

    return pushed


def main():
    print("+" + "=" * 60 + "+")
    print("|  PULLPUSH MEGA — ROAD TO 10K                             |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    progress = {}
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)

    all_handles = progress.get("handles", {})
    all_emails = progress.get("emails", {})
    done_subs = set(progress.get("done_subs", []))
    done_queries = set(progress.get("done_queries", []))

    print(f"  Existing: {len(all_handles)} handles, {len(all_emails)} emails")
    print(f"  Done subs: {len(done_subs)}, Done queries: {len(done_queries)}")

    # Unique subs only
    unique_subs = list(dict.fromkeys(ALL_SUBREDDITS))
    new_subs = [s for s in unique_subs if s not in done_subs]
    new_queries = [q for q in CROSS_REDDIT_QUERIES if q not in done_queries]

    print(f"  Phase 1: {len(new_subs)} new subs to process")
    print(f"  Phase 2: {len(new_queries)} cross-Reddit queries\n")

    last_push_count = len(all_emails)
    emails_since_push = 0

    # ============ PHASE 1: Per-subreddit ============
    for si, sub in enumerate(new_subs):
        print(f"\n  P1 [{si+1}/{len(new_subs)}] r/{sub}")

        sub_h = 0
        sub_e = 0
        total = 0

        for term in SEARCH_TERMS:
            before = None
            for page in range(8):
                try:
                    params = {"subreddit": sub, "q": term, "size": 100}
                    if before:
                        params["before"] = before

                    data = pullpush_get("comment", params)
                    comments = data.get("data", [])
                    if not comments:
                        break

                    total += len(comments)
                    h, e = process_items(comments, all_handles, all_emails, sub)
                    sub_h += h
                    sub_e += e

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
                    time.sleep(5)
                    break

            try:
                data = pullpush_get("submission", {"subreddit": sub, "q": term, "size": 100})
                posts = data.get("data", [])
                total += len(posts)
                h, e = process_items(posts, all_handles, all_emails, sub, is_post=True)
                sub_h += h
                sub_e += e
                time.sleep(1)
            except Exception:
                pass

        done_subs.add(sub)
        emails_since_push += sub_e
        print(f"    {total} items | +{sub_h} handles | +{sub_e} emails | "
              f"totals: {len(all_handles)} handles, {len(all_emails)} emails")

        save_progress(progress, all_handles, all_emails, done_subs, done_queries)

        if emails_since_push >= 500:
            new_emails = {k: v for k, v in all_emails.items()
                         if k not in progress.get("_pushed", set())}
            n = push_to_supabase(new_emails)
            print(f"    >> AUTO-PUSH: {n} emails to Supabase")
            progress["_pushed_count"] = progress.get("_pushed_count", 0) + n
            emails_since_push = 0

    # ============ PHASE 2: Cross-Reddit queries ============
    print(f"\n{'='*60}")
    print(f"  PHASE 2: Cross-Reddit queries ({len(new_queries)} remaining)")
    print(f"{'='*60}\n")

    for qi, query in enumerate(new_queries):
        print(f"\n  P2 [{qi+1}/{len(new_queries)}] \"{query}\"")

        q_h = 0
        q_e = 0
        total = 0

        before = None
        for page in range(30):
            try:
                params = {"q": query, "size": 100}
                if before:
                    params["before"] = before

                data = pullpush_get("comment", params)
                comments = data.get("data", [])
                if not comments:
                    break

                total += len(comments)
                for c in comments:
                    body = c.get("body", "") or ""
                    author = c.get("author", "") or ""
                    sub = c.get("subreddit", "") or ""

                    for h in extract_handles(body):
                        if h not in all_handles:
                            all_handles[h] = {"sub": sub, "author": author}
                            q_h += 1
                    for e in extract_emails(body):
                        if e not in all_emails:
                            all_emails[e] = {"sub": sub, "author": author, "source": "comment", "query": query}
                            q_e += 1

                timestamps = [c.get("created_utc", 0) for c in comments
                              if isinstance(c.get("created_utc"), (int, float))]
                if timestamps:
                    before = min(timestamps)
                else:
                    break
                if len(comments) < 100:
                    break
                time.sleep(1.5)
            except Exception:
                time.sleep(5)
                break

        try:
            data = pullpush_get("submission", {"q": query, "size": 100})
            posts = data.get("data", [])
            total += len(posts)
            for p in posts:
                text = f"{p.get('title', '')} {p.get('selftext', '')}"
                sub = p.get("subreddit", "") or ""
                for h in extract_handles(text):
                    if h not in all_handles:
                        all_handles[h] = {"sub": sub, "author": p.get("author", "")}
                        q_h += 1
                for e in extract_emails(text):
                    if e not in all_emails:
                        all_emails[e] = {"sub": sub, "author": p.get("author", ""), "source": "post", "query": query}
                        q_e += 1
            time.sleep(1)
        except Exception:
            pass

        done_queries.add(query)
        emails_since_push += q_e
        print(f"    {total} items | +{q_h} handles | +{q_e} emails | "
              f"totals: {len(all_handles)} handles, {len(all_emails)} emails")

        save_progress(progress, all_handles, all_emails, done_subs, done_queries)

        if emails_since_push >= 500:
            n = push_to_supabase(all_emails)
            print(f"    >> AUTO-PUSH: {n} emails to Supabase")
            emails_since_push = 0

    # Final push
    save_progress(progress, all_handles, all_emails, done_subs, done_queries)
    n = push_to_supabase(all_emails)
    print(f"\n  FINAL PUSH: {n} emails to Supabase")

    # Build CSV
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

    print(f"\n  MEGA COMPLETE: {len(all_handles)} IG handles, {len(all_emails)} direct emails")


if __name__ == "__main__":
    main()
