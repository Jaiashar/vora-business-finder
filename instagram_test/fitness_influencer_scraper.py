#!/usr/bin/env python3
"""
Fitness Influencer Email Scraper (v5 — imginn.org proxy + URL scraping)
=========================================================================
Bypasses Instagram API rate limits by using imginn.org (third-party IG viewer)
to fetch profile data. Then scrapes external URLs for additional emails.

Strategy:
  1. 90+ curated micro-influencer seeds (10k-100k followers, real trainers)
  2. Fetch bio/name via imginn.org (their servers hit IG, not our IP)
  3. Extract emails from bios
  4. Scrape external URLs (linktree, personal sites) for more emails
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
import html as html_lib
from datetime import datetime
from urllib.parse import urlparse

# ─── Config ───────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

WEB_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

IG_API_HEADERS = {
    "User-Agent": UA,
    "X-IG-App-ID": "936619743392459",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

# ─── Curated micro-influencer seed list ───────────────────────────
SEEDS = [
    # Feedspot top micro fitness (10k-100k followers, emails in bios)
    "_dnzhrz", "train_with_tazmin", "lizztheshizzz", "fitxpay",
    "nicolasgarratt", "tiffanyyysanders", "easymarks", "__bignoah",
    "msquare91", "qedfitness", "joannamariefit", "veeglutez",
    "ohhhjojo", "bodybytrinaa", "kyreefitness", "mr.thomas_makris",
    "bellaahaag", "robinzahra", "fitlikelamb", "niahuntley_",
    "workoutwithroxanne", "fitladybug", "chlo.chlolifts",
    "battles.she.won", "nicole_marsh", "missfit_vi100",
    "jusswaitonitdc", "brady.mcdonald84", "nickbracks",
    "nataduquee", "yonatan_tyk", "dequawn.brown", "jannickehustad",
    "cmbarsky", "maysapreys", "jyhodges", "tharealoutlaw",
    "fitwyasmin", "dajanakovacova_", "coach_kristaveberova",
    "adaletefitness", "najee2smith", "rizovaenn",
    "soyjohncharles", "sainasante", "sweatwithsierra",
    "claudi_wl", "deyaniestaine_", "fit.by_trish",
    "holhealthfitness", "runwithfreedom", "tarasfit91",
    "motionmelissa", "ste_marr", "mayamoise",
    "pilinemer", "bielchristo", "chief_kelechi",
    "tonydiazcervo", "lag360", "jelenafit", "betaniayvr",
    "aishazaza", "mickmovements", "adamsalmanovbiz",
    "jakegroinus", "lalalamarco", "elenichristina_fmgpro",

    # More micro fitness coaches / personal trainers
    "britneychiu04", "fitbytrish", "megsquats",
    "gainsbybrains", "coachmarkcarroll",
    "lisafiitt", "racheljdillon",
    "yogabycandace", "thetravelingyogi",
    "the.running.channel", "runwithryan",

    # Feedspot strength training influencers (micro/nano with emails)
    "halinkahart", "ella.rue_", "charleygouldscc", "allisontenney",
    "wcroz", "peakperformancehq", "mattdomney_pt", "juliesrunninglife",
    "fred_silcock_coaching", "_adamwillis", "lauren_runswild",
    "jasoncoultman", "prevail_performance_",
    "dr.asher.runs", "coachcam_pl", "ajcronin_strength_and_fitness",
    "chrisaydin_", "chris_strong_wong", "lars.linnenbank",
    "womensyogaandstrengthsociety", "coach.clv", "rianmckeever",
    "milesfromherview", "sara_lach", "loisanderton_",
    "wrkhrd.performance", "strengthcoachrob", "pr_papi_k",
    "trainwithfys", "fionajmackay", "alex_strengthsyndicate",

    # Health coaches / wellness (Feedspot)
    "digvijaylifestyle", "splashofgoodness",

    # More micro fitness from various sources
    "massy.arias", "australianstrengthcoach",
    "stephanie_buttermore", "laurensimpson",
    "alexia_clark", "krissycela", "hannaoeberg",
    "senada.greca", "chontelduncan",
    "kelseywells",
]

SEEDS = list(dict.fromkeys(SEEDS))

# ─── Email extraction ─────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
JUNK_DOMAINS = {
    'sentry.io', 'w3.org', 'schema.org', 'example.com', 'domain.com',
    'wixpress.com', 'cloudflare.com', 'mailchimp.com', 'googleapis.com',
    'facebook.com', 'instagram.com', 'fbcdn.net', 'cdninstagram.com',
    'apple.com', 'google.com', 'gstatic.com', 'youtube.com',
    'twitter.com', 'x.com', 'tiktok.com', 'spotify.com',
    'onetrust.com', 'pinterest.com', 'linkedin.com',
    'imginn.org', 'imginn.com',
    'patreon.com', 'substackinc.com', 'stanwith.me',
    'playbookapp.io', 'joinplaybook.com', 'movesapp.com', 'vrtoapp.com',
    'boostcamp.app', 'strongstrongfriends.com',
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
        req = urllib.request.Request(url, headers={
            "User-Agent": UA,
            "Accept": "text/html,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
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
        for link in re.findall(r'href="(https?://[^"]+)"', html)[:8]:
            if any(k in link.lower() for k in ['contact', 'about', 'email', 'book', 'work', 'collab', 'inquiry', 'mailto']):
                sub = fetch_page(link, 5)
                if sub:
                    all_emails.update(extract_emails(sub))
                time.sleep(0.5)
    else:
        parsed = urlparse(url if url.startswith('http') else 'https://' + url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        for path in ['/contact', '/contact-us', '/about', '/about-us']:
            if len(all_emails) >= 3:
                break
            sub = fetch_page(base + path, 5)
            if sub:
                all_emails.update(extract_emails(sub))
            time.sleep(0.3)

    return list(all_emails)


# ═══════════════════════════════════════════════════════════════════
# IMGINN.ORG FETCHER (bypasses IG API rate limits)
# ═══════════════════════════════════════════════════════════════════
def fetch_via_imginn(username):
    """Fetch profile data via imginn.org — their servers hit IG, not ours."""
    url = f'https://imginn.org/{username}/'
    req = urllib.request.Request(url, headers=WEB_HEADERS)
    resp = urllib.request.urlopen(req, timeout=15, context=SSL_CTX)
    raw = resp.read().decode('utf-8', errors='ignore')

    # Clean HTML to text for email extraction
    text = re.sub(r'<[^>]+>', ' ', raw)
    text = html_lib.unescape(text)

    # Extract bio (common imginn patterns)
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

    # Extract name
    name = ''
    name_match = re.search(r'<h1[^>]*>(.*?)</h1>', raw, re.DOTALL)
    if name_match:
        name = re.sub(r'<[^>]+>', '', name_match.group(1)).strip()

    # Extract follower count
    followers = 0
    for fol_pat in [
        r'([\d,]+)\s*(?:Followers|followers)',
        r'(?:Followers|followers)\s*:\s*([\d,]+)',
    ]:
        fol_match = re.search(fol_pat, text)
        if fol_match:
            try:
                followers = int(fol_match.group(1).replace(',', ''))
            except:
                pass
            break

    # Extract external URLs (linktree, personal sites)
    ext_urls = []
    for href in re.findall(r'href="(https?://[^"]+)"', raw):
        parsed = urlparse(href)
        if parsed.netloc and parsed.netloc not in ('imginn.org', 'instagram.com', 'www.instagram.com',
                                                    'cdninstagram.com', 'facebook.com', 'imginn.com'):
            ext_urls.append(href)

    # Extract emails from full page text
    page_emails = extract_emails(text)

    return {
        "username": username,
        "full_name": name,
        "bio": bio[:500],
        "followers": followers,
        "external_urls": ext_urls[:3],
        "emails_from_bio": page_emails,
    }


# ═══════════════════════════════════════════════════════════════════
# IG PUBLIC API (try first, fallback to imginn)
# ═══════════════════════════════════════════════════════════════════
def fetch_ig_api(username):
    """Try the IG public API. Returns None if rate limited."""
    try:
        url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
        req = urllib.request.Request(url, headers=IG_API_HEADERS)
        resp = urllib.request.urlopen(req, timeout=10, context=SSL_CTX)
        data = json.loads(resp.read().decode('utf-8'))
        user = data.get("data", {}).get("user", {})
        if not user:
            return None

        related = [e["node"]["username"] for e in user.get("edge_related_profiles", {}).get("edges", [])
                   if e.get("node", {}).get("username")]

        return {
            "username": username,
            "full_name": user.get("full_name", ""),
            "bio": user.get("biography", ""),
            "external_url": user.get("external_url", ""),
            "followers": user.get("edge_followed_by", {}).get("count", 0),
            "is_business": user.get("is_business_account", False),
            "business_category": user.get("category_name", ""),
            "business_email": user.get("business_email", ""),
            "related_profiles": related,
        }
    except:
        return None


# ═══════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════
def run_pipeline(seeds, progress_file):
    existing = {}
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            existing = json.load(f)
        print(f"  Loaded {len(existing)} from progress file")

    results = []
    total_fetched = 0
    total_with_email = 0
    ig_api_works = None  # None = untested, True/False after first test

    for i, username in enumerate(seeds):
        if username in existing and not existing[username].get("error"):
            results.append(existing[username])
            continue

        print(f"  [{i + 1}/{len(seeds)}] @{username}...", end=" ", flush=True)

        profile = None
        source = "imginn"

        # Try IG API first (faster, richer data with related profiles)
        if ig_api_works is not False:
            api_result = fetch_ig_api(username)
            if api_result:
                ig_api_works = True
                source = "ig_api"
                profile = {
                    "username": username,
                    "full_name": api_result["full_name"],
                    "bio": api_result["bio"],
                    "followers": api_result["followers"],
                    "external_urls": [api_result["external_url"]] if api_result["external_url"] else [],
                    "emails_from_bio": extract_emails(api_result["bio"]),
                    "is_business": api_result.get("is_business", False),
                    "business_category": api_result.get("business_category", ""),
                    "business_email": api_result.get("business_email", ""),
                    "related_profiles": api_result.get("related_profiles", []),
                }
            else:
                ig_api_works = False
                print("(API blocked, using imginn) ", end="", flush=True)

        # Fallback to imginn.org
        if profile is None:
            try:
                profile = fetch_via_imginn(username)
                profile["is_business"] = False
                profile["business_category"] = ""
                profile["business_email"] = ""
                profile["related_profiles"] = []
            except Exception as e:
                print(f"error ({str(e)[:40]})")
                existing[username] = {"username": username, "error": str(e)[:100]}
                time.sleep(2)
                continue

        # Extract emails from external URLs if no bio email
        all_emails = set(profile.get("emails_from_bio", []))
        profile["url_emails"] = []

        ext_urls = profile.get("external_urls", [])
        if not all_emails and ext_urls:
            for ext_url in ext_urls[:2]:
                try:
                    url_emails = scrape_url_for_emails(ext_url)
                    profile["url_emails"].extend(url_emails)
                    all_emails.update(url_emails)
                except:
                    pass

        biz_email = profile.get("business_email", "")
        if biz_email:
            all_emails.add(biz_email.lower())

        profile["error"] = None
        profile["source"] = source
        existing[username] = profile
        results.append(profile)
        total_fetched += 1

        followers = profile.get("followers", 0)
        if all_emails:
            total_with_email += 1
            print(f"EMAIL {followers:>7,} flw | {list(all_emails)[:2]}")
        else:
            print(f"  ~   {followers:>7,} flw | no email")

        # Save periodically
        if total_fetched % 10 == 0:
            with open(progress_file, 'w') as f:
                json.dump(existing, f, indent=2, default=str)
            hit_rate = total_with_email * 100 // max(total_fetched, 1)
            print(f"  ── saved | {total_fetched} fetched | {total_with_email} emails ({hit_rate}%) ──")

        time.sleep(random.uniform(3, 6))

    # Final save
    with open(progress_file, 'w') as f:
        json.dump(existing, f, indent=2, default=str)

    return results, total_fetched, total_with_email


# ═══════════════════════════════════════════════════════════════════
# OUTPUT
# ═══════════════════════════════════════════════════════════════════
def output_results(profiles, csv_path, json_path):
    rows = []

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

        for email in all_emails:
            rows.append({
                "email": email,
                "name": p.get("full_name", ""),
                "ig_username": p["username"],
                "followers": p.get("followers", 0),
                "platform": "instagram",
                "category": "fitness_influencer",
                "tags": "",
                "bio": (p.get("bio") or "")[:200].replace('\n', ' '),
                "external_url": (p.get("external_urls") or [""])[0] if p.get("external_urls") else "",
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

    return len(rows), len(set(r["email"] for r in rows))


# ═══════════════════════════════════════════════════════════════════
def main():
    print("+" + "=" * 60 + "+")
    print("|  FITNESS MICRO-INFLUENCER EMAIL SCRAPER v5               |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+")
    print(f"  Seeds: {len(SEEDS)} accounts")
    print(f"  Primary source: imginn.org (bypasses IG rate limits)")
    print(f"  Fallback: IG public API (if available)")

    progress_file = os.path.join(BASE_DIR, "fitness_influencer_progress.json")
    csv_path = os.path.join(BASE_DIR, "fitness_influencer_leads.csv")
    json_path = os.path.join(BASE_DIR, "fitness_influencer_leads.json")

    profiles, total_fetched, total_with_email = run_pipeline(SEEDS, progress_file)

    print("\n" + "=" * 60)
    print("  FINAL RESULTS")
    print("=" * 60)

    csv_rows, unique_emails = output_results(profiles, csv_path, json_path)

    valid = [p for p in profiles if not p.get("error")]
    print(f"""
  Profiles fetched:     {len(valid)}
  With email:           {total_with_email} ({total_with_email * 100 // max(len(valid), 1)}%)
  Unique emails:        {unique_emails}
  CSV rows:             {csv_rows}

  Files:
    CSV:  {csv_path}
    JSON: {json_path}

  TIP: Run again to resume — progress is saved automatically.
  TIP: Then run: python push_consumer_leads.py fitness
""")


if __name__ == "__main__":
    main()
