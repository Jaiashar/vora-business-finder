#!/usr/bin/env python3
"""
Ecosia Consumer Search Harvester v2
=====================================
Targets "average joe" consumers — runners, gym members, wearable users
rather than fitness professionals. Uses search queries designed to find
individual people's emails rather than business emails.

Strategy: Search for fitness challenge participants, running club members,
gym community members, race signup contacts, etc.
"""

import os, re, csv, sys, time, json, random, ssl
import urllib.request, urllib.error
from datetime import datetime
from urllib.parse import quote_plus

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

UA_LIST = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

PERSONAL_PROVIDERS = {
    'gmail.com','yahoo.com','hotmail.com','outlook.com','aol.com',
    'icloud.com','me.com','mac.com','protonmail.com','proton.me',
    'live.com','msn.com','ymail.com','rocketmail.com',
    'mail.com','zoho.com','gmx.com','gmx.net','fastmail.com',
    'hey.com','tutanota.com','pm.me','comcast.net','att.net',
    'verizon.net','cox.net','sbcglobal.net','charter.net',
    'bellsouth.net','earthlink.net','googlemail.com',
}

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
    'ecosia.org','brave.com','bing.com','duckduckgo.com',
    'microsoft.com','yahoo.co.jp','yandex.ru',
}

BIZ_PREFIXES = {
    'hello','info','contact','press','media','pr','booking','inquiries',
    'team','sales','admin','general','office','talent','management',
    'collab','partnerships','sponsor','business','support','help',
    'careers','marketing','advertising','events','membership',
    'editorial','editor','studio','submissions','casting','news',
    'promo','merch','shop','store','wholesale','customerservice',
    'hr','ceo','cfo','cto','coo','vp','director','webmaster',
    'accounts','billing','finance','legal','compliance','noreply',
    'donotreply','no-reply','mailer','postmaster','abuse','security',
}

BIZ_LOCAL_WORDS = {
    'gym','studio','crossfit','llc','inc','brand','academy',
    'solutions','services','group','fitnesscenter','supplement',
    'apparel','consulting','foundation','association',
    'league','network','collective','partners','agency','mgmt',
    'magazine','official','headquarters','corporate','enterprise',
    'company','medical','clinic','hospital','therapy','chiropractic',
    'physical therapy','acupuncture','massage therapy','rehab',
    'church','ministry','school','university','college',
    'government','municipal','county','city of','state of',
    'police','fire dept','sheriff','military',
    'real estate','realty','mortgage','insurance','law firm',
    'attorney','plumbing','electric','construction','roofing',
    'landscaping','cleaning','auto','dealership','restaurant',
    'catering','hotel','resort','salon','spa','barbershop',
    'photography','videography','media group','production',
    'radio','television','newspaper','publishing',
}

KNOWN_COMPANY_DOMAINS = {
    'precisionnutrition.com','fleetfeet.com','lululemon.com',
    'nike.com','adidas.com','underarmour.com','reebok.com',
    'garmin.com','fitbit.com','whoop.com','oura.com',
    'peloton.com','soulcycle.com','equinox.com','orangetheory.com',
    'planetfitness.com','goldsgym.com','24hourfitness.com',
    'anytimefitness.com','lafitness.com','lifetimefitness.com',
    'crossfit.com','beachbody.com','noom.com','weightwatchers.com',
    'myfitnesspal.com','strava.com','alltrails.com',
}

CITY_WORDS = {
    'houston','dallas','austin','chicago','miami','atlanta',
    'denver','phoenix','seattle','portland','nashville',
    'charlotte','raleigh','orlando','tampa','jacksonville',
    'losangeles','sandiego','sanfrancisco','newyork','brooklyn',
    'manhattan','boston','philadelphia','detroit','cleveland',
    'minneapolis','stlouis','kansascity','indianapolis','columbus',
    'milwaukee','memphis','louisville','saltlakecity','lasvegas',
}

ACTIVITY_WORDS = {
    'fitness','yoga','pilates','crossfit','running','cycling',
    'swimming','boxing','mma','karate','judo','taekwondo',
    'martialarts','kickboxing','weightlifting','powerlifting',
    'bodybuilding','gymnastics','calisthenics','bootcamp',
    'personaltraining','massage','wellness','nutrition',
    'dance','zumba','barre','spin','rowing','climbing',
}


def is_consumer_email(email):
    email = email.lower().strip()
    local, domain = email.split("@", 1)

    if domain in JUNK_DOMAINS or domain in KNOWN_COMPANY_DOMAINS:
        return False

    if domain.endswith((".edu", ".gov", ".org", ".mil")):
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

    clean_local = local.replace(".", "").replace("_", "").replace("-", "")

    for c in CITY_WORDS:
        for a in ACTIVITY_WORDS:
            if c + a in clean_local or a + c in clean_local:
                return False

    if domain in PERSONAL_PROVIDERS:
        if len(local) > 22:
            return False
        if re.match(r'^[a-z]{15,}$', clean_local):
            return False
        return True

    return False


def search_ecosia(query):
    url = f"https://www.ecosia.org/search?q={quote_plus(query)}"
    req = urllib.request.Request(url, headers={
        "User-Agent": random.choice(UA_LIST),
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    resp = urllib.request.urlopen(req, timeout=12, context=SSL_CTX)
    return resp.read().decode("utf-8", errors="ignore")


def search_brave(query):
    url = f"https://search.brave.com/search?q={quote_plus(query)}"
    req = urllib.request.Request(url, headers={
        "User-Agent": random.choice(UA_LIST),
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    resp = urllib.request.urlopen(req, timeout=12, context=SSL_CTX)
    return resp.read().decode("utf-8", errors="ignore")


def extract_emails(raw_html):
    found = EMAIL_RE.findall(raw_html.lower())
    return [e for e in set(found) if is_consumer_email(e)]


def generate_queries():
    queries = []

    # Average consumer queries — NOT professional trainers
    consumer_activities = [
        "runner", "jogger", "gym member", "yoga practitioner",
        "cyclist", "swimmer", "hiker", "rock climber",
        "weightlifter", "CrossFit member", "Peloton rider",
        "spin class member", "barre class", "pilates student",
        "marathon finisher", "triathlete", "obstacle racer",
        "Spartan racer", "5K runner", "10K runner",
        "trail runner", "ultra runner", "half marathon",
        "rowing enthusiast", "kayaker fitness",
        "boxing fitness", "kickboxing class",
        "dance fitness", "Zumba class",
    ]

    wearable_queries = [
        "apple watch runner email @gmail.com",
        "garmin forerunner user email @gmail.com",
        "garmin fenix user email @gmail.com",
        "fitbit user email @gmail.com",
        "oura ring user email @gmail.com",
        "whoop band user email @gmail.com",
        "strava runner email @gmail.com",
        "strava cyclist email @gmail.com",
        "apple watch workout email @gmail.com",
        "garmin watch runner email @gmail.com",
        "fitbit versa user email @gmail.com",
        "apple watch fitness goals email",
        "garmin running watch email contact",
        "oura ring sleep tracking email @gmail.com",
        "whoop recovery athlete email @gmail.com",
        "coros watch runner email @gmail.com",
        "polar watch runner email @gmail.com",
        "suunto watch user email @gmail.com",
    ]

    race_queries = [
        "5K race participant email @gmail.com",
        "10K race participant email @gmail.com",
        "half marathon participant email @gmail.com",
        "marathon runner email @gmail.com",
        "Spartan race participant email @gmail.com",
        "Tough Mudder participant email @gmail.com",
        "color run participant email @gmail.com",
        "turkey trot participant email @gmail.com",
        "fun run participant email @gmail.com",
        "trail race participant email @gmail.com",
        "triathlon participant email @gmail.com",
        "swimrun participant email @gmail.com",
        "cycling race participant email @gmail.com",
        "criterium rider email @gmail.com",
        "duathlon participant email @gmail.com",
    ]

    community_queries = [
        "running club member email @gmail.com",
        "cycling club member email @gmail.com",
        "CrossFit member email @gmail.com",
        "gym buddy email @gmail.com",
        "fitness accountability partner email @gmail.com",
        "workout buddy email @gmail.com",
        "running partner email @gmail.com",
        "yoga community member email @gmail.com",
        "fitness challenge participant email @gmail.com",
        "weight loss challenge participant email @gmail.com",
        "fitness journey email @gmail.com",
        "body transformation email @gmail.com",
        "weight loss journey email @gmail.com",
        "fitness goals email @gmail.com",
        "new year fitness resolution email @gmail.com",
    ]

    cities = [
        "Los Angeles", "New York", "Chicago", "Houston", "Phoenix",
        "San Diego", "Dallas", "Austin", "San Francisco", "Denver",
        "Nashville", "Portland", "Seattle", "Atlanta", "Miami",
        "Minneapolis", "Tampa", "Charlotte", "Raleigh", "Orlando",
        "Salt Lake City", "Boulder", "Scottsdale", "Santa Monica",
        "San Jose", "Sacramento", "Las Vegas", "Brooklyn",
        "Charleston", "Madison", "Ann Arbor", "Asheville",
        "Pittsburgh", "Columbus", "Indianapolis", "Kansas City",
        "Fort Collins", "Bend OR", "Boise", "Tucson",
    ]

    for activity in consumer_activities:
        sampled_cities = random.sample(cities, min(6, len(cities)))
        for city in sampled_cities:
            queries.append(f'{activity} {city} email @gmail.com')

    queries.extend(wearable_queries)
    queries.extend(race_queries)
    queries.extend(community_queries)

    for activity in random.sample(consumer_activities, 10):
        queries.append(f'{activity} email @yahoo.com')
        queries.append(f'{activity} email @hotmail.com')

    random.shuffle(queries)
    return queries


def main():
    print("+" + "=" * 60 + "+")
    print("|  ECOSIA CONSUMER HARVESTER v2 — AVERAGE JOE              |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    progress_file = os.path.join(BASE_DIR, "search_harvest_v2_progress.json")
    csv_path = os.path.join(BASE_DIR, "search_harvest_v2_leads.csv")

    progress = {}
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            progress = json.load(f)

    done_queries = set(progress.get("_done_queries", []))
    email_map = progress.get("_email_map", {})

    all_queries = generate_queries()
    new_queries = [q for q in all_queries if q not in done_queries]

    print(f"  {len(email_map)} emails already found")
    print(f"  {len(done_queries)}/{len(all_queries)} queries done")
    print(f"  {len(new_queries)} queries remaining\n")

    if not new_queries:
        print("  All queries done!")
        build_csv(email_map, csv_path)
        return

    ecosia_blocked = False
    brave_blocked = False
    consecutive_zeros = 0
    queries_this_run = 0

    for qi, query in enumerate(new_queries):
        queries_this_run += 1
        tag = f"[{qi+1}/{len(new_queries)}]"

        raw = None
        engine = None

        if not ecosia_blocked:
            try:
                raw = search_ecosia(query)
                engine = "ecosia"
            except urllib.error.HTTPError as e:
                if e.code in (403, 429):
                    ecosia_blocked = True
                    print(f"  {tag} Ecosia blocked ({e.code})")
            except Exception:
                pass

        if raw is None and not brave_blocked:
            try:
                raw = search_brave(query)
                engine = "brave"
            except urllib.error.HTTPError as e:
                if e.code in (403, 429):
                    brave_blocked = True
                    print(f"  {tag} Brave blocked ({e.code})")
            except Exception:
                pass

        if ecosia_blocked and brave_blocked:
            print(f"\n  Both engines blocked. Waiting 15 min...")
            time.sleep(900)
            ecosia_blocked = False
            brave_blocked = False
            continue

        if raw is None:
            done_queries.add(query)
            consecutive_zeros += 1
            print(f"  {tag} no results | {query[:50]}")
        else:
            emails = extract_emails(raw)
            new_count = 0
            for e in emails:
                if e not in email_map:
                    email_map[e] = {
                        "query": query,
                        "engine": engine,
                        "found_at": datetime.now().isoformat(),
                    }
                    new_count += 1

            done_queries.add(query)

            if new_count > 0:
                consecutive_zeros = 0
                print(f"  {tag} +{new_count:2} ({engine}) | total: {len(email_map):>5} | {query[:45]}")
            else:
                consecutive_zeros += 1
                print(f"  {tag}  0  ({engine}) | {query[:50]}")

        if queries_this_run % 10 == 0:
            progress["_done_queries"] = list(done_queries)
            progress["_email_map"] = email_map
            with open(progress_file, "w") as f:
                json.dump(progress, f, indent=1)
            print(f"  --- saved: {len(email_map)} emails, {len(done_queries)} queries done ---")

        if consecutive_zeros >= 5:
            delay = random.uniform(45, 75)
        else:
            delay = random.uniform(25, 45)

        time.sleep(delay)

    progress["_done_queries"] = list(done_queries)
    progress["_email_map"] = email_map
    with open(progress_file, "w") as f:
        json.dump(progress, f, indent=1)

    build_csv(email_map, csv_path)


def build_csv(email_map, csv_path):
    rows = []
    for email, info in email_map.items():
        rows.append({
            "email": email,
            "query": info.get("query", ""),
            "engine": info.get("engine", ""),
            "found_at": info.get("found_at", ""),
        })

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["email", "query", "engine", "found_at"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  CSV: {csv_path}")
    print(f"  Total consumer emails: {len(rows)}")


if __name__ == "__main__":
    main()
