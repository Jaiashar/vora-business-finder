#!/usr/bin/env python3
"""
Consumer Email Pipeline v2
============================
Focused on INDIVIDUAL consumers, NOT businesses.
Discovers IG usernames from 80+ Feedspot categories, then extracts emails
via imginn.org bios + scraping external URLs found in bios.

Filters out: gyms, studios, brands, supplement companies, fitness centers.

Usage:
  python consumer_pipeline.py discover     # Phase 1: get usernames
  python consumer_pipeline.py extract      # Phase 2: get emails
  python consumer_pipeline.py resume       # Resume extraction from last save
  python consumer_pipeline.py stats        # Show current stats
  python consumer_pipeline.py csv          # Rebuild CSV from progress
"""

import os
import re
import csv
import sys
import time
import json
import random
import urllib.request
import urllib.error
import ssl
import html as html_lib
from datetime import datetime
from urllib.parse import urlparse, quote_plus

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept": "text/html,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.5"}
IG_HEADERS = {"User-Agent": UA, "X-IG-App-ID": "936619743392459", "Accept": "*/*"}

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
URL_RE = re.compile(r'https?://[^\s"\'<>,;)\]]+|(?:linktr\.ee|beacons\.ai|stan\.store|link\.bio|bio\.link|bio\.site|campsite\.bio|hoo\.be|lynx\.bio|tap\.bio|flow\.page|solo\.to|snipfeed\.co|carrd\.co)/[^\s"\'<>,;)\]]+')

JUNK_DOMAINS = {
    'sentry.io', 'w3.org', 'schema.org', 'example.com', 'domain.com',
    'wixpress.com', 'cloudflare.com', 'mailchimp.com', 'googleapis.com',
    'facebook.com', 'instagram.com', 'fbcdn.net', 'cdninstagram.com',
    'apple.com', 'google.com', 'gstatic.com', 'youtube.com',
    'twitter.com', 'x.com', 'tiktok.com', 'spotify.com',
    'onetrust.com', 'pinterest.com', 'linkedin.com',
    'imginn.org', 'imginn.com', 'feedspot.com',
    'patreon.com', 'substackinc.com', 'stanwith.me',
    'playbookapp.io', 'joinplaybook.com', 'movesapp.com', 'vrtoapp.com',
    'boostcamp.app', 'strongstrongfriends.com', 'beacons.ai',
    'mailgun.com', 'sendgrid.com', 'hubspot.com', 'intercom.io',
    'zendesk.com', 'crisp.chat', 'hotjar.com', 'stripe.com',
    'shopify.com', 'squarespace.com', 'wix.com', 'wordpress.com',
    'gravatar.com', 'wp.com', 'amazonaws.com', 'cloudinary.com',
    'googletagmanager.com', 'google-analytics.com',
}
JUNK_KW = ['sentry', 'noreply', 'no-reply', 'unsubscribe', 'webpack',
           'placeholder', 'donotreply', 'mailer-daemon', 'u003e',
           'anuj@feedspot', 'test@', 'abuse@', 'postmaster@', 'webmaster@',
           'info@imginn']

# Email domain patterns that indicate business/agency/management — NOT a consumer
BIZ_EMAIL_DOMAIN_RE = re.compile(
    r'(?:management|agency|agencies|talent|sports|media|'
    r'entertainment|creative|marketing|digital|consulting|'
    r'production|records|music|mgmt|mgt|'
    r'pr\b|prfirm|publicrelations|publicity|'
    r'represent|brand|studio|fitness|gym|'
    r'supplement|nutrition(?:company|brand)|'
    r'apparel|clothing|wear|merch|'
    r'capital|ventures|holdings|group|inc|'
    r'foundation|association|federation|league|'
    r'network|solutions|services|global|'
    r'collective|partners|team)',
    re.I
)

# Personal email providers — these are almost certainly consumers
PERSONAL_PROVIDERS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
    'icloud.com', 'me.com', 'mac.com', 'protonmail.com', 'proton.me',
    'live.com', 'msn.com', 'ymail.com', 'rocketmail.com',
    'mail.com', 'zoho.com', 'gmx.com', 'gmx.net', 'fastmail.com',
    'hey.com', 'tutanota.com', 'pm.me', 'comcast.net', 'att.net',
    'verizon.net', 'cox.net', 'sbcglobal.net', 'charter.net',
    'bellsouth.net', 'earthlink.net', 'optonline.net',
    'yahoo.co.uk', 'hotmail.co.uk', 'btinternet.com',
    'googlemail.com', 'inbox.com',
}

# Business keywords — skip accounts matching these in name/bio
BIZ_KEYWORDS = re.compile(
    r'\b(?:gym|studio|fitness\s*center|crossfit\s*box|'
    r'supplement|apparel|brand|clothing|wear|shop|store|'
    r'inc\.|llc|ltd|corp|co\.|®|™|'
    r'franchise|chain|outlet|warehouse|'
    r'official\s*account|we\s*are|our\s*team|our\s*mission|'
    r'book\s*(?:a|your)\s*class|class\s*schedule|'
    r'visit\s*(?:us|our)|come\s*(?:train|visit)|'
    r'locations?\s*(?:in|near)|open\s*7\s*days|hours?\s*of\s*operation|'
    r'managed\s*by|represented\s*by|signed\s*(?:with|to|by)|'
    r'talent\s*agency|sports\s*(?:management|agency)|'
    r'pro\s*athlete|professional\s*athlete|olympian|'
    r'world\s*champion|olympic\s*(?:gold|silver|bronze|medalist)|'
    r'ufc\s*fighter|nfl|nba|mlb|nhl|mls|wwe|pga|'
    r'national\s*team|team\s*(?:usa|gb|canada)|'
    r'x\s*games|world\s*record|espn|fox\s*sports|'
    r'order\s*(?:now|here|today)|shop\s*(?:now|here)|'
    r'use\s*code|discount|promo|coupon|'
    r'free\s*shipping|limited\s*(?:time|edition)|'
    r'subscribe|membership|join\s*(?:us|the|our))\b',
    re.I
)


def extract_emails(text):
    if not text:
        return []
    found = EMAIL_RE.findall(text.lower())
    return list(set(
        e for e in found
        if e.split('@')[1] not in JUNK_DOMAINS
        and not any(k in e for k in JUNK_KW)
        and not e.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js'))
        and len(e.split('@')[1]) >= 4
        and len(e) < 60
    ))


def is_consumer_email(email):
    """Returns True only if this looks like an individual consumer's email."""
    email = email.lower()
    local, domain = email.split('@', 1)

    # Personal email providers are always consumer
    if domain in PERSONAL_PROVIDERS:
        return True

    # Check if domain looks like a business/agency
    if BIZ_EMAIL_DOMAIN_RE.search(domain):
        return False

    # Generic business prefixes on non-personal domains = likely business
    biz_prefixes = ['hello', 'info', 'contact', 'press', 'media', 'pr',
                    'booking', 'inquiries', 'enquiries', 'enquiry',
                    'team', 'sales', 'admin', 'general',
                    'office', 'talent', 'management', 'agents', 'collab',
                    'partnerships', 'sponsor', 'business', 'support',
                    'help', 'hey', 'careers', 'jobs', 'hire',
                    'reception', 'frontdesk', 'orders', 'billing',
                    'marketing', 'advertising', 'events', 'membership',
                    'magazin', 'editorial', 'editor', 'studio',
                    'submissions', 'casting', 'news', 'promo',
                    'merch', 'shop', 'store', 'wholesale']
    if local in biz_prefixes:
        return False

    # If it's name@personaldomain.com, it's probably fine (personal website)
    return True


def extract_urls_from_text(text):
    """Extract external URLs from bio text."""
    if not text:
        return []
    urls = URL_RE.findall(text)
    skip_domains = ['instagram.com', 'facebook.com', 'twitter.com', 'x.com',
                    'tiktok.com', 'youtube.com', 'imginn.org', 'imginn.com',
                    'cdninstagram.com', 'fbcdn.net']
    clean = []
    for u in urls:
        if not u.startswith('http'):
            u = 'https://' + u
        parsed = urlparse(u)
        if not any(d in parsed.netloc for d in skip_domains):
            clean.append(u.rstrip('.,;:)'))
    return list(dict.fromkeys(clean))


def is_business_account(name, bio, username):
    """Check if account looks like a business, pro athlete, or celebrity — not an average consumer."""
    text = f"{name} {bio} {username}".lower()

    # Business keywords in bio/name
    if BIZ_KEYWORDS.search(text):
        return True

    # Business-sounding names
    biz_name_patterns = [
        r'(?:^|\s)(?:the\s+)?(?:\w+\s+)?(?:gym|studio|box|center|club)\b',
        r'(?:^|\s)(?:\w+\s+)?(?:fitness|athletics|performance)\s+(?:llc|inc|co)\b',
    ]
    for pat in biz_name_patterns:
        if re.search(pat, name.lower()):
            return True

    # Usernames ending with _official, _brand, _hq etc
    biz_username_suffixes = ['_official', '_brand', '_hq', '_inc', '_co',
                             '_llc', '_team', '_org', '_global']
    for suffix in biz_username_suffixes:
        if username.lower().endswith(suffix):
            return True

    return False


def fetch_page(url, timeout=10):
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        req = urllib.request.Request(url, headers=HEADERS)
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        content = resp.read()
        try:
            return content.decode('utf-8')
        except UnicodeDecodeError:
            return content.decode('latin-1', errors='ignore')
    except:
        return None


def scrape_url_for_emails(url):
    """Follow a URL and extract emails from it and its sub-pages."""
    all_emails = set()
    html = fetch_page(url)
    if not html:
        return []
    all_emails.update(extract_emails(html))

    linktree_domains = ['linktr.ee', 'komi.io', 'beacons.ai', 'link.bio',
                        'campsite.bio', 'stan.store', 'hoo.be', 'snipfeed.co',
                        'flow.page', 'solo.to', 'tap.bio', 'bio.link',
                        'milkshake.app', 'carrd.co', 'lynx.bio', 'bio.site',
                        'konect.to', 'link.me']
    if any(d in url.lower() for d in linktree_domains):
        for link in re.findall(r'href="(https?://[^"]+)"', html)[:8]:
            if any(k in link.lower() for k in ['contact', 'about', 'email', 'book',
                                                'work', 'collab', 'inquiry', 'coach', 'hire']):
                sub = fetch_page(link, 5)
                if sub:
                    all_emails.update(extract_emails(sub))
                time.sleep(0.3)
    else:
        parsed = urlparse(url if url.startswith('http') else 'https://' + url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        for path in ['/contact', '/about', '/work-with-me']:
            if len(all_emails) >= 2:
                break
            sub = fetch_page(base + path, 5)
            if sub:
                all_emails.update(extract_emails(sub))
            time.sleep(0.2)
    return list(all_emails)


# ═══════════════════════════════════════════════════════════════════
# DISCOVERY — 80+ Feedspot categories focused on individuals
# ═══════════════════════════════════════════════════════════════════
FEEDSPOT_CATEGORIES = [
    # Micro/nano (average people)
    ("micro_fitness", "https://influencers.feedspot.com/micro_fitness_instagram_influencers/"),
    ("micro_influencers", "https://influencers.feedspot.com/micro_instagram_influencers/"),
    ("nano_influencers", "https://influencers.feedspot.com/nano_instagram_influencers/"),
    # Fitness
    ("fitness", "https://influencers.feedspot.com/fitness_instagram_influencers/"),
    ("female_fitness", "https://influencers.feedspot.com/female_fitness_instagram_influencers/"),
    ("fitness_us", "https://influencers.feedspot.com/fitness_us_instagram_influencers/"),
    ("strength_training", "https://influencers.feedspot.com/strength_training_instagram_influencers/"),
    ("home_workout", "https://influencers.feedspot.com/home_workout_instagram_influencers/"),
    ("powerlifting", "https://influencers.feedspot.com/powerlifting_instagram_influencers/"),
    ("weightlifting", "https://influencers.feedspot.com/weightlifting_instagram_influencers/"),
    ("functional_training", "https://influencers.feedspot.com/functional_training_instagram_influencers/"),
    ("calisthenics", "https://influencers.feedspot.com/calisthenics_instagram_influencers/"),
    ("hiit", "https://influencers.feedspot.com/hiit_instagram_influencers/"),
    # Health & wellness
    ("health", "https://influencers.feedspot.com/health_instagram_influencers/"),
    ("health_coach", "https://influencers.feedspot.com/health_coach_instagram_influencers/"),
    ("nutrition", "https://influencers.feedspot.com/nutrition_instagram_influencers/"),
    ("wellness", "https://influencers.feedspot.com/wellness_instagram_influencers/"),
    ("mental_health", "https://influencers.feedspot.com/mental_health_instagram_influencers/"),
    ("self_care", "https://influencers.feedspot.com/self_care_instagram_influencers/"),
    ("spiritual", "https://influencers.feedspot.com/spiritual_instagram_influencers/"),
    ("holistic", "https://influencers.feedspot.com/holistic_health_instagram_influencers/"),
    # Yoga & Pilates
    ("yoga", "https://influencers.feedspot.com/yoga_instagram_influencers/"),
    ("pilates", "https://influencers.feedspot.com/pilates_instagram_influencers/"),
    # Running & endurance
    ("running", "https://influencers.feedspot.com/running_instagram_influencers/"),
    ("marathon", "https://influencers.feedspot.com/marathon_instagram_influencers/"),
    ("triathlon", "https://influencers.feedspot.com/triathlon_instagram_influencers/"),
    ("trail_running", "https://influencers.feedspot.com/trail_running_instagram_influencers/"),
    # Sports
    ("crossfit", "https://influencers.feedspot.com/crossfit_instagram_influencers/"),
    ("bodybuilding", "https://influencers.feedspot.com/bodybuilding_instagram_influencers/"),
    ("cycling", "https://influencers.feedspot.com/cycling_instagram_influencers/"),
    ("swimming", "https://influencers.feedspot.com/swimming_instagram_influencers/"),
    ("golf", "https://influencers.feedspot.com/golf_instagram_influencers/"),
    ("tennis", "https://influencers.feedspot.com/tennis_instagram_influencers/"),
    ("soccer", "https://influencers.feedspot.com/soccer_instagram_influencers/"),
    ("basketball", "https://influencers.feedspot.com/basketball_instagram_influencers/"),
    ("boxing", "https://influencers.feedspot.com/boxing_instagram_influencers/"),
    ("mma", "https://influencers.feedspot.com/mma_instagram_influencers/"),
    ("martial_arts", "https://influencers.feedspot.com/martial_arts_instagram_influencers/"),
    ("surfing", "https://influencers.feedspot.com/surfing_instagram_influencers/"),
    ("hiking", "https://influencers.feedspot.com/hiking_instagram_influencers/"),
    ("climbing", "https://influencers.feedspot.com/climbing_instagram_influencers/"),
    ("skiing", "https://influencers.feedspot.com/skiing_instagram_influencers/"),
    ("skateboarding", "https://influencers.feedspot.com/skateboarding_instagram_influencers/"),
    # Diet & lifestyle
    ("vegan", "https://influencers.feedspot.com/vegan_instagram_influencers/"),
    ("keto", "https://influencers.feedspot.com/keto_instagram_influencers/"),
    ("weight_loss", "https://influencers.feedspot.com/weight_loss_instagram_influencers/"),
    ("clean_eating", "https://influencers.feedspot.com/clean_eating_instagram_influencers/"),
    ("meal_prep", "https://influencers.feedspot.com/meal_prep_instagram_influencers/"),
    ("plant_based", "https://influencers.feedspot.com/plant_based_instagram_influencers/"),
    ("gluten_free", "https://influencers.feedspot.com/gluten_free_instagram_influencers/"),
    ("paleo", "https://influencers.feedspot.com/paleo_instagram_influencers/"),
    # Lifestyle/wellness adjacent
    ("mom", "https://influencers.feedspot.com/mom_instagram_influencers/"),
    ("parenting", "https://influencers.feedspot.com/parenting_instagram_influencers/"),
    ("over_50", "https://influencers.feedspot.com/over_50_instagram_influencers/"),
    ("lifestyle", "https://influencers.feedspot.com/lifestyle_instagram_influencers/"),
    ("motivation", "https://influencers.feedspot.com/motivation_instagram_influencers/"),
    ("mindfulness", "https://influencers.feedspot.com/mindfulness_instagram_influencers/"),
    ("meditation", "https://influencers.feedspot.com/meditation_instagram_influencers/"),
    # Outdoor/adventure
    ("outdoor", "https://influencers.feedspot.com/outdoor_instagram_influencers/"),
    ("adventure", "https://influencers.feedspot.com/adventure_instagram_influencers/"),
    ("camping", "https://influencers.feedspot.com/camping_instagram_influencers/"),
    ("travel_fitness", "https://influencers.feedspot.com/travel_instagram_influencers/"),
    # Regional (regular local fitness people)
    ("la_fitness", "https://influencers.feedspot.com/los_angeles_fitness_instagram_influencers/"),
    ("nyc_fitness", "https://influencers.feedspot.com/nyc_fitness_instagram_influencers/"),
    ("uk_fitness", "https://influencers.feedspot.com/uk_fitness_instagram_influencers/"),
    ("australian_fitness", "https://influencers.feedspot.com/australian_fitness_instagram_influencers/"),
    ("canadian_fitness", "https://influencers.feedspot.com/canadian_fitness_instagram_influencers/"),
    ("norwegian_fitness", "https://influencers.feedspot.com/norwegian_fitness_instagram_influencers/"),
    ("indian_fitness", "https://influencers.feedspot.com/indian_fitness_instagram_influencers/"),
    ("german_fitness", "https://influencers.feedspot.com/german_fitness_instagram_influencers/"),
    ("brazilian_fitness", "https://influencers.feedspot.com/brazilian_fitness_instagram_influencers/"),
    # Wearable/tech adjacent
    ("tech", "https://influencers.feedspot.com/tech_instagram_influencers/"),
    ("gadget", "https://influencers.feedspot.com/gadget_instagram_influencers/"),
    # Food (people who track macros etc)
    ("food", "https://influencers.feedspot.com/food_instagram_influencers/"),
    ("healthy_food", "https://influencers.feedspot.com/healthy_food_instagram_influencers/"),
    ("recipe", "https://influencers.feedspot.com/recipe_instagram_influencers/"),
    # Dance/movement
    ("dance", "https://influencers.feedspot.com/dance_instagram_influencers/"),
    ("barre", "https://influencers.feedspot.com/barre_instagram_influencers/"),
    # Recovery
    ("physical_therapy", "https://influencers.feedspot.com/physical_therapy_instagram_influencers/"),
    ("chiropractic", "https://influencers.feedspot.com/chiropractic_instagram_influencers/"),
]


def scrape_feedspot_page(url):
    """Extract IG usernames from a Feedspot category page."""
    html = fetch_page(url)
    if not html:
        return []
    usernames = []
    for match in re.finditer(r'instagram\.com/([a-zA-Z0-9_.]+)/?["\s<]', html):
        un = match.group(1).rstrip('/')
        if un and len(un) > 1 and un not in ('p', 'explore', 'accounts', 'reel', 'stories'):
            usernames.append(un)
    return list(dict.fromkeys(usernames))


def discover_from_bing(queries):
    """Search Bing for fitness IG usernames."""
    all_usernames = []
    for query in queries:
        url = f'https://www.bing.com/search?q={quote_plus(query)}&count=50'
        html = fetch_page(url)
        if not html:
            continue
        for match in re.finditer(r'instagram\.com/([a-zA-Z0-9_.]{2,30})', html):
            un = match.group(1)
            if un not in ('p', 'explore', 'accounts', 'reel', 'stories', 'about', 'developer'):
                all_usernames.append(un)
        time.sleep(random.uniform(2, 4))
    return list(dict.fromkeys(all_usernames))


def discover_from_web_lists():
    """Search for blog posts listing fitness Instagram accounts."""
    queries = [
        '"fitness accounts to follow" instagram list 2025 OR 2026',
        '"fitness influencers to follow" micro nano list',
        '"best fitness instagram" small accounts follow',
        '"running instagram accounts" follow 2025 OR 2026',
        '"yoga instagram accounts" must follow list',
        '"wellness instagram" "accounts to follow" list',
        '"crossfit athletes" instagram follow list',
        '"personal trainer" "instagram" accounts follow list 2025',
        '"fitness journey" instagram accounts follow micro',
        '"healthy living" instagram accounts list',
        'nano fitness influencers instagram list email',
        '"fitness instagrammers" email contact',
    ]
    all_usernames = []
    for query in queries:
        url = f'https://www.bing.com/search?q={quote_plus(query)}&count=30'
        html = fetch_page(url)
        if not html:
            continue
        # Get result URLs
        result_urls = re.findall(r'<a\s+href="(https?://(?!bing\.com|microsoft\.com)[^"]+)"', html)
        for rurl in result_urls[:5]:
            page = fetch_page(rurl, 8)
            if not page:
                continue
            for match in re.finditer(r'(?:@|instagram\.com/)([a-zA-Z0-9_.]{2,30})', page):
                un = match.group(1)
                if un not in ('p', 'explore', 'accounts', 'reel', 'stories', 'about'):
                    all_usernames.append(un)
            time.sleep(random.uniform(1, 2))
        time.sleep(random.uniform(2, 4))
    return list(dict.fromkeys(all_usernames))


def phase1_discover():
    """Discover IG usernames from directories and web."""
    usernames_file = os.path.join(BASE_DIR, "consumer_usernames.json")

    existing = {}
    if os.path.exists(usernames_file):
        with open(usernames_file) as f:
            existing = json.load(f)

    all_usernames = set(existing.get("usernames", []))
    print(f"  Starting with {len(all_usernames)} existing usernames\n")

    # Also merge in any previously discovered usernames from mass_usernames.json
    mass_file = os.path.join(BASE_DIR, "mass_usernames.json")
    if os.path.exists(mass_file):
        with open(mass_file) as f:
            mass_data = json.load(f)
        old = len(all_usernames)
        all_usernames.update(mass_data.get("usernames", []))
        print(f"  Merged {len(all_usernames) - old} from previous discovery\n")

    # 1. Feedspot categories
    print("  ═══ Scraping Feedspot categories ═══\n")
    scraped = set(existing.get("scraped_categories", []))
    for cat_name, cat_url in FEEDSPOT_CATEGORIES:
        if cat_name in scraped:
            continue
        print(f"  [{cat_name}] ...", end=" ", flush=True)
        usernames = scrape_feedspot_page(cat_url)
        new = [u for u in usernames if u not in all_usernames]
        all_usernames.update(usernames)
        print(f"+{len(new)} new (total: {len(all_usernames)})")
        scraped.add(cat_name)
        existing["scraped_categories"] = list(scraped)
        existing["usernames"] = list(all_usernames)
        with open(usernames_file, 'w') as f:
            json.dump(existing, f)
        time.sleep(random.uniform(2, 4))

    # 2. Web lists (blog posts listing fitness accounts)
    if not existing.get("web_lists_done"):
        print("\n  ═══ Scraping web lists for more usernames ═══\n")
        web_usernames = discover_from_web_lists()
        new = [u for u in web_usernames if u not in all_usernames]
        all_usernames.update(web_usernames)
        print(f"  Web lists: +{len(new)} new (total: {len(all_usernames)})")
        existing["web_lists_done"] = True

    existing["usernames"] = list(all_usernames)
    existing["discovery_timestamp"] = datetime.now().isoformat()
    with open(usernames_file, 'w') as f:
        json.dump(existing, f, indent=2)

    print(f"\n  ═══ Discovery complete: {len(all_usernames)} total usernames ═══")
    return list(all_usernames)


# ═══════════════════════════════════════════════════════════════════
# EXTRACTION — imginn + bio URL scraping + business filtering
# ═══════════════════════════════════════════════════════════════════
def fetch_imginn(username):
    """Fetch profile via imginn.org with retry on 429."""
    url = f'https://imginn.org/{username}/'
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            resp = urllib.request.urlopen(req, timeout=15, context=SSL_CTX)
            raw = resp.read().decode('utf-8', errors='ignore')
            break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                wait = (attempt + 1) * 15
                print(f"(429, wait {wait}s) ", end="", flush=True)
                time.sleep(wait)
                continue
            raise
    else:
        raise Exception("imginn 429 after retries")

    # Full page text for email scanning
    text = re.sub(r'<[^>]+>', ' ', raw)
    text = html_lib.unescape(text)

    # Bio section
    bio = ''
    for pat in [
        r'class="[^"]*bio[^"]*"[^>]*>(.*?)</(?:div|span|p)',
        r'class="[^"]*description[^"]*"[^>]*>(.*?)</(?:div|span|p)',
    ]:
        m = re.search(pat, raw, re.DOTALL | re.I)
        if m:
            bio = re.sub(r'<[^>]+>', ' ', m.group(1)).strip()
            bio = html_lib.unescape(bio)
            break

    # Name
    name = ''
    name_match = re.search(r'<h1[^>]*>(.*?)</h1>', raw, re.DOTALL)
    if name_match:
        name = re.sub(r'<[^>]+>', '', name_match.group(1)).strip()

    # Emails from full page
    page_emails = extract_emails(text)

    # External URLs from bio text
    bio_urls = extract_urls_from_text(bio)
    # Also look for href links in the bio HTML section
    for pat in [r'class="[^"]*bio[^"]*"[^>]*>(.*?)</(?:div|span|p)',
                r'class="[^"]*description[^"]*"[^>]*>(.*?)</(?:div|span|p)']:
        m = re.search(pat, raw, re.DOTALL | re.I)
        if m:
            for href in re.findall(r'href="(https?://[^"]+)"', m.group(1)):
                parsed = urlparse(href)
                if not any(d in parsed.netloc for d in ['instagram.com', 'imginn', 'facebook.com', 'cdninstagram']):
                    bio_urls.append(href)
            break

    # Also look for a dedicated external link on the page
    for link_match in re.finditer(r'<a[^>]*href="(https?://(?!imginn|instagram|facebook|cdninstagram|twitter|x\.com)[^"]+)"[^>]*class="[^"]*(?:external|website|link|url|bio)[^"]*"', raw, re.I):
        bio_urls.append(link_match.group(1))

    return {
        "username": username,
        "full_name": name,
        "bio": bio[:500],
        "emails_from_bio": page_emails,
        "bio_urls": list(dict.fromkeys(bio_urls))[:3],
        "source": "imginn",
    }


def phase2_extract():
    """Extract emails from usernames via imginn + URL scraping."""
    usernames_file = os.path.join(BASE_DIR, "consumer_usernames.json")
    progress_file = os.path.join(BASE_DIR, "consumer_progress.json")
    csv_path = os.path.join(BASE_DIR, "consumer_leads_all.csv")

    if not os.path.exists(usernames_file):
        print("  No usernames file. Run: python consumer_pipeline.py discover")
        return

    with open(usernames_file) as f:
        data = json.load(f)
    all_usernames = data.get("usernames", [])

    # Also merge in progress from mass_progress.json (don't re-process)
    progress = {}
    mass_progress_file = os.path.join(BASE_DIR, "mass_progress.json")
    if os.path.exists(mass_progress_file):
        with open(mass_progress_file) as f:
            progress = json.load(f)
        print(f"  Loaded {len(progress)} from previous mass extraction")

    if os.path.exists(progress_file):
        with open(progress_file) as f:
            saved = json.load(f)
        progress.update(saved)
        print(f"  Loaded {len(saved)} from consumer progress (total: {len(progress)})")

    to_process = [u for u in all_usernames if u not in progress]
    total = len(to_process)
    emails_found = 0
    processed = 0
    businesses_skipped = 0

    print(f"  {len(progress)} already processed, {total} remaining\n")

    for i, username in enumerate(to_process):
        processed += 1
        print(f"  [{processed}/{total}] @{username}...", end=" ", flush=True)

        all_emails = set()

        # Fetch from imginn
        try:
            profile = fetch_imginn(username)
            all_emails.update(profile.get("emails_from_bio", []))
        except urllib.error.HTTPError as e:
            if e.code == 410:
                progress[username] = {"error": "gone"}
                print("gone")
                continue
            if e.code == 404:
                progress[username] = {"error": "not_found"}
                print("not found")
                continue
            progress[username] = {"error": str(e.code)}
            print(f"http {e.code}")
            time.sleep(1)
            continue
        except Exception as e:
            progress[username] = {"error": str(e)[:50]}
            print("error")
            time.sleep(1)
            continue

        # Check if it's a business/brand/pro athlete — skip
        name = profile.get("full_name", "")
        bio = profile.get("bio", "")
        if is_business_account(name, bio, username):
            progress[username] = {"error": "business", "full_name": name}
            businesses_skipped += 1
            print(f"SKIP biz ({name[:30]})")
            time.sleep(0.5)
            continue

        # Filter emails through consumer check
        all_emails = {e for e in all_emails if is_consumer_email(e)}

        # If no email from bio, try scraping URLs found in bio
        bio_urls = profile.get("bio_urls", [])
        if not all_emails and bio_urls:
            for ext_url in bio_urls[:2]:
                try:
                    url_emails = scrape_url_for_emails(ext_url)
                    consumer_url_emails = [e for e in url_emails if is_consumer_email(e)]
                    profile.setdefault("url_emails", []).extend(consumer_url_emails)
                    all_emails.update(consumer_url_emails)
                except:
                    pass
                time.sleep(0.3)

        profile["error"] = None
        progress[username] = profile

        if all_emails:
            emails_found += 1
            print(f"EMAIL | {list(all_emails)[:2]}")
        else:
            print("no email")

        # Save every 50 accounts
        if processed % 50 == 0:
            with open(progress_file, 'w') as f:
                json.dump(progress, f, indent=1, default=str)
            rate = emails_found * 100 // max(processed, 1)
            total_e = count_emails(progress)
            print(f"  ── saved | {processed}/{total} | batch: {emails_found} ({rate}%) | biz skipped: {businesses_skipped} | total emails: {total_e} ──")

        time.sleep(random.uniform(8, 15))

    # Final save
    with open(progress_file, 'w') as f:
        json.dump(progress, f, indent=1, default=str)

    build_consumer_csv(progress, csv_path)


def count_emails(progress):
    """Count total unique CONSUMER emails in progress."""
    emails = set()
    for username, p in progress.items():
        if isinstance(p, dict) and not p.get("error"):
            if is_business_account(p.get("full_name", ""), p.get("bio", ""), username):
                continue
            for e in p.get("emails_from_bio", []):
                if is_consumer_email(e):
                    emails.add(e)
            for e in p.get("url_emails", []):
                if is_consumer_email(e):
                    emails.add(e)
    return len(emails)


def build_consumer_csv(progress, csv_path):
    """Build CSV with ONLY consumer (non-business) leads."""
    rows = []
    seen_emails = set()

    for username, p in progress.items():
        if not isinstance(p, dict) or p.get("error"):
            continue

        all_emails = set(p.get("emails_from_bio", []) + p.get("url_emails", []))
        # Filter to consumer emails only
        all_emails = {e for e in all_emails if is_consumer_email(e)}
        if not all_emails:
            continue

        # Double-check business filter
        name = p.get("full_name", "")
        bio = p.get("bio", "")
        if is_business_account(name, bio, username):
            continue

        sources = []
        if p.get("emails_from_bio"):
            sources.append("bio")
        if p.get("url_emails"):
            sources.append("url")

        for email in all_emails:
            if email in seen_emails:
                continue
            seen_emails.add(email)
            rows.append({
                "email": email,
                "name": name,
                "ig_username": username,
                "followers": p.get("followers", 0),
                "platform": "instagram",
                "category": "fitness_influencer",
                "tags": "",
                "bio": (bio or "")[:200].replace('\n', ' '),
                "external_url": p.get("external_url", ""),
                "email_source": "+".join(sources),
                "is_business": False,
                "business_category": p.get("business_category", ""),
            })

    fieldnames = ["email", "name", "ig_username", "followers", "platform",
                  "category", "tags", "bio", "external_url", "email_source",
                  "is_business", "business_category"]

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  CSV: {csv_path}")
    print(f"  Total unique consumer emails: {len(rows)}")
    return len(rows)


def show_stats():
    """Show current extraction stats."""
    progress_file = os.path.join(BASE_DIR, "consumer_progress.json")
    mass_file = os.path.join(BASE_DIR, "mass_progress.json")

    all_progress = {}
    if os.path.exists(mass_file):
        with open(mass_file) as f:
            all_progress.update(json.load(f))
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            all_progress.update(json.load(f))

    total = len(all_progress)
    errors = sum(1 for p in all_progress.values() if isinstance(p, dict) and p.get("error"))
    businesses = sum(1 for p in all_progress.values() if isinstance(p, dict) and p.get("error") == "business")
    with_email = 0
    unique_emails = set()

    for p in all_progress.values():
        if not isinstance(p, dict) or p.get("error"):
            continue
        emails = set(p.get("emails_from_bio", []) + p.get("url_emails", []))
        if p.get("business_email"):
            emails.add(p["business_email"].lower())
        if emails:
            with_email += 1
            unique_emails.update(emails)

    print(f"  Total processed: {total}")
    print(f"  Errors/gone: {errors - businesses}")
    print(f"  Businesses skipped: {businesses}")
    print(f"  With email: {with_email}")
    print(f"  Unique emails: {len(unique_emails)}")
    print(f"  Hit rate: {with_email * 100 // max(total - errors, 1)}%")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    print("+" + "=" * 60 + "+")
    print("|  CONSUMER EMAIL PIPELINE v2                              |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    if mode == "stats":
        show_stats()
    elif mode == "csv":
        progress = {}
        pf = os.path.join(BASE_DIR, "consumer_progress.json")
        mf = os.path.join(BASE_DIR, "mass_progress.json")
        if os.path.exists(mf):
            with open(mf) as f:
                progress.update(json.load(f))
        if os.path.exists(pf):
            with open(pf) as f:
                progress.update(json.load(f))
        build_consumer_csv(progress, os.path.join(BASE_DIR, "consumer_leads_all.csv"))
    elif mode in ("discover", "all"):
        print("  PHASE 1: DISCOVERY")
        print("  " + "─" * 40)
        phase1_discover()
        print()
        if mode == "all":
            print("  PHASE 2: EXTRACTION")
            print("  " + "─" * 40)
            phase2_extract()
    elif mode in ("extract", "resume"):
        print("  PHASE 2: EXTRACTION")
        print("  " + "─" * 40)
        phase2_extract()


if __name__ == "__main__":
    main()
