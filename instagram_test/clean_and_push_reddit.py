#!/usr/bin/env python3
"""
Clean Reddit-sourced emails and push to Supabase consumer_leads.
Applies aggressive business filtering before push.
"""

import os, re, json, sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PERSONAL_PROVIDERS = {
    'gmail.com','yahoo.com','hotmail.com','outlook.com','aol.com',
    'icloud.com','me.com','mac.com','protonmail.com','proton.me',
    'live.com','msn.com','ymail.com','rocketmail.com',
    'mail.com','zoho.com','gmx.com','comcast.net','att.net',
    'verizon.net','cox.net','sbcglobal.net','bellsouth.net',
    'earthlink.net','googlemail.com','fastmail.com',
    'yahoo.co.uk','hotmail.co.uk','btinternet.com',
}

DUMMY_EMAILS = {
    'email@gmail.com','first.last@gmail.com','firstlast@gmail.com',
    'primaryemail@gmail.com','youremail@gmail.com','your.email@gmail.com',
    'name@gmail.com','test@gmail.com','example@gmail.com',
    'user@gmail.com','john.doe@gmail.com','janedoe@gmail.com',
    'sample@gmail.com','username@gmail.com','myemail@gmail.com',
    'firstname.lastname@gmail.com','deadname@gmail.com',
    'mytestaccount@gmail.com','chooseyourdeath@gmail.com',
    'youremail+save@gmail.com','john.doe+amazon@gmail.com',
    'mytestaccount+hulu@gmail.com','mytestaccount+netflix@gmail.com',
    'lula@gmail.com','bobbyroger469@gmail.com',
    '__@mail.com',
}

BIZ_LOCAL_WORDS = [
    'gym','studio','crossfit','llc','inc','brand','academy',
    'solutions','services','group','fitnesscenter','supplement',
    'apparel','consulting','foundation','association','federation',
    'league','network','collective','partners','agency','mgmt',
    'magazine','official','headquarters','corporate','enterprise',
    'company','clinic','hospital','therapy','chiropractic',
    'school','university','college','church','ministry',
    'government','municipal','county','police','sheriff',
    'realestate','realty','mortgage','insurance','lawfirm',
    'attorney','plumbing','electric','construction','roofing',
    'landscaping','cleaning','auto','dealership','restaurant',
    'catering','hotel','resort','salon','barbershop',
    'photography','videography','mediagroup','production',
    'radio','television','newspaper','publishing',
    'martial arts','martialart','karate','taekwondo','judo',
    'bjjacademy','bjjhq','bjjconnect','bjjbrick',
    'kimonos','rashguard','mma gear','fightgear',
    'nutrition coach','wellness center','healthcenter',
    'fitnessstudio','yogastudio','pilatestudio',
    'bootcamp','personaltraining','strengthtraining',
    'physical therapy','physicaltherapy','massage therapy',
    'acupuncture','chiropractor','rehab','recovery center',
    'weightloss clinic','diet center','dietcenter',
    'planetbeirut','10thplanet','alliancebjj',
    'achievefitness','bellarosechemical',
    'shop','store','sell','buy','deal','promo','offer',
    'discount','coupon','wholesale','retail','ecommerce',
    'seo','marketing','social media','smm','smmit',
    'distro','distributor','supply','supplier','vendor',
    'coach','trainer','training','instruct',
    'onlinesell','getusuk','usukshop','biz',
    'expert','consult','freelanc','outsourc',
    'crypto','bitcoin','forex','invest','trading',
    'loan','credit','debt','payday',
    'casino','bet','gambl','poker',
    'hack','crack','cheat','bot','spam',
    'pharma','drug','pill','suppl','steroid',
    'dating','hookup','escort','adult',
    'farm','ranch','agricult',
    'trucking','freight','shipping','cargo','logistic',
    'pest','exterminator','termit',
    'tattoo','piercing',
    'tutor','lesson','class',
    'daycare','childcare','nanny','babysit',
    'pet','veterinar','grooming','kennel',
    'moving','storage','haul',
    'print','design','graphic','logo',
    'webdev','appdev','software','tech',
    'repair','maint','install','handyman','plumber',
    'team','crew','squad','tribe','clan',
    'league','tournament','championship','competition',
]

BIZ_PREFIXES = [
    'hello','info','contact','press','media','pr','booking',
    'inquiries','team','sales','admin','general','office',
    'talent','management','collab','partnerships','sponsor',
    'business','support','help','careers','marketing',
    'advertising','events','membership','editorial','editor',
    'studio','submissions','casting','news','promo','merch',
    'shop','store','wholesale','customerservice','hr','ceo',
    'cfo','cto','coo','vp','director','webmaster','accounts',
    'billing','finance','legal','compliance','noreply',
    'donotreply','postmaster','abuse','security','sysadmin',
]

KNOWN_BIZ_DOMAINS = {
    'precisionnutrition.com','fleetfeet.com','lululemon.com',
    'nike.com','adidas.com','underarmour.com','reebok.com',
    'garmin.com','fitbit.com','whoop.com','oura.com',
    'peloton.com','soulcycle.com','equinox.com','orangetheory.com',
    'planetfitness.com','goldsgym.com','24hourfitness.com',
    'anytimefitness.com','lafitness.com','lifetimefitness.com',
    'crossfit.com','beachbody.com','noom.com','weightwatchers.com',
}

CITY_WORDS = [
    'houston','dallas','austin','chicago','miami','atlanta',
    'denver','phoenix','seattle','portland','nashville',
    'charlotte','raleigh','orlando','tampa','jacksonville',
    'losangeles','sandiego','sanfrancisco','newyork','brooklyn',
    'boston','philadelphia','detroit','cleveland',
    'minneapolis','stlouis','kansascity','indianapolis','columbus',
    'milwaukee','memphis','louisville','saltlake','lasvegas',
    'boise','tucson','sacramento','fresno','bakersfield',
    'beirut','london','toronto','sydney','melbourne',
    'fredericton','basingstoke',
]

ACTIVITY_WORDS = [
    'fitness','yoga','pilates','crossfit','running','cycling',
    'swimming','boxing','mma','karate','judo','taekwondo',
    'martialarts','kickboxing','weightlifting','powerlifting',
    'bodybuilding','gymnastics','calisthenics','bootcamp',
    'personaltraining','massage','wellness','nutrition',
    'dance','zumba','barre','spin','rowing','climbing',
    'bjj','jiujitsu','jujitsu','grappling','wrestling',
    'muaythai','kravmaga',
]


def is_clean_consumer(email):
    email = email.lower().strip()

    if email in DUMMY_EMAILS:
        return False

    if not re.match(r'^[a-z0-9][a-z0-9._%+-]*@[a-z0-9.-]+\.[a-z]{2,6}$', email):
        return False

    local, domain = email.split('@', 1)

    if domain.endswith(('.edu', '.gov', '.org', '.mil')):
        return False
    if '.k12.' in domain:
        return False
    if domain in KNOWN_BIZ_DOMAINS:
        return False
    if not re.match(r'^[a-z0-9.-]+\.[a-z]{2,6}$', domain):
        return False

    if domain not in PERSONAL_PROVIDERS:
        return False

    if local in BIZ_PREFIXES:
        return False

    clean = local.replace('.', '').replace('_', '').replace('-', '')

    for w in BIZ_LOCAL_WORDS:
        w_clean = w.replace(' ', '')
        if w_clean in clean:
            return False

    for c in CITY_WORDS:
        for a in ACTIVITY_WORDS:
            if c + a in clean or a + c in clean:
                return False

    if len(local) > 28:
        return False
    if len(local) < 3:
        return False

    if re.match(r'^[a-z]{20,}$', clean):
        return False

    return True


def main():
    print("+" + "=" * 60 + "+")
    print("|  CLEAN & PUSH REDDIT EMAILS TO SUPABASE                 |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    progress_file = os.path.join(BASE_DIR, "pullpush_progress.json")
    if not os.path.exists(progress_file):
        print("  No pullpush_progress.json found.")
        return

    with open(progress_file) as f:
        data = json.load(f)

    raw_emails = data.get("emails", {})
    print(f"  Raw emails from PullPush: {len(raw_emails)}")

    # Also load ecosia v2 emails
    v2_file = os.path.join(BASE_DIR, "search_harvest_v2_progress.json")
    v2_emails = {}
    if os.path.exists(v2_file):
        with open(v2_file) as f:
            v2 = json.load(f)
        v2_emails = v2.get("_email_map", {})
        print(f"  Raw emails from Ecosia v2: {len(v2_emails)}")

    # Clean
    clean = {}
    rejected = {"dummy": 0, "business": 0, "non_personal": 0, "format": 0}

    for email, info in {**raw_emails, **v2_emails}.items():
        email = email.lower().strip()
        if email in DUMMY_EMAILS:
            rejected["dummy"] += 1
            continue
        if not re.match(r'^[a-z0-9][a-z0-9._%+-]*@[a-z0-9.-]+\.[a-z]{2,6}$', email):
            rejected["format"] += 1
            continue

        local, domain = email.split('@', 1)
        if domain not in PERSONAL_PROVIDERS:
            rejected["non_personal"] += 1
            continue

        if is_clean_consumer(email):
            source = "reddit"
            sub = ""
            if isinstance(info, dict):
                sub = info.get("sub", info.get("query", ""))
                if "query" in info:
                    source = "ecosia_v2"
            clean[email] = {"source": source, "sub": sub}
        else:
            rejected["business"] += 1

    print(f"\n  After cleaning: {len(clean)} consumer emails")
    print(f"  Rejected: {rejected}")

    # Show samples by source subreddit
    by_sub = {}
    for e, info in clean.items():
        s = info.get("sub", "?")
        by_sub[s] = by_sub.get(s, 0) + 1

    print(f"\n  By subreddit (top 15):")
    for s, c in sorted(by_sub.items(), key=lambda x: -x[1])[:15]:
        print(f"    r/{s}: {c}")

    # Push to Supabase
    if "--push" not in sys.argv:
        print(f"\n  Dry run. Use --push to actually push to Supabase.")
        print(f"  Sample clean emails:")
        for e in list(clean.keys())[:20]:
            print(f"    {e}")
        return

    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(BASE_DIR), '.env'))

    SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
    SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_KEY", "")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("  ERROR: Missing SUPABASE_URL or SUPABASE_KEY in .env")
        return

    import urllib.request
    import ssl

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates",
    }

    WEARABLE_SUBS = {'AppleWatch','AppleWatchFitness','fitbit','ouraring','whoop','GarminWatches','Garmin'}
    WELLNESS_SUBS = {'keto','intermittentfasting','loseit','EOOD','veganfitness','1200isplenty','fasting','mealprep'}

    rows = []
    for email, info in clean.items():
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

    BATCH = 200
    pushed = 0
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i+BATCH]
        body = json.dumps(batch).encode("utf-8")
        url = f"{SUPABASE_URL}/rest/v1/consumer_leads?on_conflict=email"
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            resp = urllib.request.urlopen(req, timeout=30, context=ctx)
            pushed += len(batch)
            print(f"  Pushed {pushed}/{len(rows)}")
        except Exception as e:
            print(f"  ERROR pushing batch {i}: {str(e)[:60]}")
            try:
                err = e.read().decode() if hasattr(e, 'read') else ''
                print(f"    {err[:200]}")
            except:
                pass

    print(f"\n  Done: {pushed} emails pushed to Supabase")


if __name__ == "__main__":
    main()
