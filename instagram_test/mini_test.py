#!/usr/bin/env python3
"""
Instagram Scraper Mini-Test
============================
Tests 3 different approaches for extracting emails from Instagram profiles.

Tool A: Instaloader (Python library)
Tool B: Instagram Web Scraping (requests + public endpoints)
Tool C: Instagram GraphQL public API (no auth)

We test all 3 against the same 10 accounts and compare results.
"""

import re
import time
import json
import urllib.request
import urllib.error
import ssl
from datetime import datetime

# ─── Test Accounts ───────────────────────────────────────────────
# Mix of fitness influencers, university accounts, and student athletes
# that are likely to have emails in their bios or as business contacts.
TEST_ACCOUNTS = [
    "kayla_itsines",        # Fitness influencer, 16M followers, has business email
    "jeff_nippard",         # Fitness/science YouTuber, likely has email
    "whitneyysimmons",      # Fitness influencer, has business email
    "ucaborec",             # UC Irvine Campus Rec (official account)
    "ucloarec",             # UCLA Recreation
    "chloeting",            # Fitness influencer, likely has email in bio
    "bretcontreras1",       # PhD in sports science / glute guy
    "drjohnrusin",          # Strength training, has business email
    "soheefit",             # Fitness influencer / coach
    "mindpumpmedia",        # Fitness podcast / brand
]

# ─── Email Regex ─────────────────────────────────────────────────
EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
)

def extract_emails_from_text(text):
    """Extract email addresses from any text string."""
    if not text:
        return []
    emails = EMAIL_PATTERN.findall(text.lower())
    # Filter out obvious junk
    junk = {'email@example.com', 'your@email.com', 'name@domain.com'}
    return [e for e in emails if e not in junk]


# ═══════════════════════════════════════════════════════════════════
# TOOL A: Instaloader
# ═══════════════════════════════════════════════════════════════════
def test_instaloader(accounts):
    """
    Uses the Instaloader Python library to fetch profile metadata.
    No login required for public profiles.
    """
    print("\n" + "="*60)
    print("  TOOL A: Instaloader")
    print("="*60)
    
    try:
        import instaloader
    except ImportError:
        print("  ERROR: instaloader not installed. Run: pip install instaloader")
        return {"tool": "Instaloader", "error": "not installed", "results": []}
    
    L = instaloader.Instaloader()
    results = []
    start_time = time.time()
    
    for username in accounts:
        entry = {
            "username": username,
            "bio": None,
            "emails": [],
            "external_url": None,
            "followers": None,
            "is_business": None,
            "error": None,
        }
        
        try:
            profile = instaloader.Profile.from_username(L.context, username)
            entry["bio"] = profile.biography
            entry["external_url"] = profile.external_url
            entry["followers"] = profile.followers
            entry["is_business"] = profile.is_business_account
            
            # Extract emails from bio
            bio_emails = extract_emails_from_text(profile.biography)
            entry["emails"] = bio_emails
            
            print(f"  ✓ @{username}: bio={len(profile.biography)}chars, "
                  f"emails={bio_emails or 'none'}, "
                  f"url={profile.external_url or 'none'}, "
                  f"followers={profile.followers:,}, "
                  f"business={profile.is_business_account}")
            
        except instaloader.exceptions.ProfileNotExistsException:
            entry["error"] = "profile not found"
            print(f"  ✗ @{username}: Profile not found")
        except instaloader.exceptions.ConnectionException as e:
            entry["error"] = f"connection error: {str(e)[:80]}"
            print(f"  ✗ @{username}: Connection error - {str(e)[:80]}")
        except Exception as e:
            entry["error"] = str(e)[:100]
            print(f"  ✗ @{username}: {str(e)[:80]}")
        
        results.append(entry)
        time.sleep(1.5)  # Rate limit: be gentle
    
    elapsed = time.time() - start_time
    emails_found = sum(1 for r in results if r["emails"])
    errors = sum(1 for r in results if r["error"])
    
    print(f"\n  Summary: {emails_found}/{len(accounts)} emails found, "
          f"{errors} errors, {elapsed:.1f}s elapsed")
    
    return {
        "tool": "Instaloader",
        "emails_found": emails_found,
        "errors": errors,
        "elapsed_seconds": round(elapsed, 1),
        "results": results,
    }


# ═══════════════════════════════════════════════════════════════════
# TOOL B: Instagram Web Scraping (Public Endpoints)
# ═══════════════════════════════════════════════════════════════════
def test_web_scraping(accounts):
    """
    Scrapes Instagram profile pages directly via web requests.
    No login required. Uses the public web endpoint that returns
    profile data embedded in the page HTML/JSON.
    """
    print("\n" + "="*60)
    print("  TOOL B: Instagram Web Scraping (requests)")
    print("="*60)
    
    results = []
    start_time = time.time()
    
    # Create SSL context that doesn't verify (some systems have cert issues)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",
        "Connection": "keep-alive",
    }
    
    for username in accounts:
        entry = {
            "username": username,
            "bio": None,
            "emails": [],
            "external_url": None,
            "followers": None,
            "error": None,
        }
        
        try:
            url = f"https://www.instagram.com/{username}/"
            req = urllib.request.Request(url, headers=HEADERS)
            resp = urllib.request.urlopen(req, timeout=10, context=ctx)
            html = resp.read().decode('utf-8', errors='ignore')
            
            # Method 1: Try to find JSON data in the page
            # Instagram embeds profile data in a script tag
            json_patterns = [
                r'"biography"\s*:\s*"([^"]*)"',
                r'"external_url"\s*:\s*"([^"]*)"',
                r'"edge_followed_by"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
                r'"is_business_account"\s*:\s*(true|false)',
            ]
            
            bio_match = re.search(json_patterns[0], html)
            url_match = re.search(json_patterns[1], html)
            followers_match = re.search(json_patterns[2], html)
            
            if bio_match:
                # Decode unicode escapes in bio
                bio = bio_match.group(1).encode().decode('unicode_escape', errors='ignore')
                entry["bio"] = bio
                entry["emails"] = extract_emails_from_text(bio)
            
            if url_match:
                entry["external_url"] = url_match.group(1)
            
            if followers_match:
                entry["followers"] = int(followers_match.group(1))
            
            # Method 2: Also check meta description for email
            meta_match = re.search(r'<meta[^>]*content="([^"]*)"[^>]*name="description"', html)
            if meta_match:
                meta_emails = extract_emails_from_text(meta_match.group(1))
                for e in meta_emails:
                    if e not in entry["emails"]:
                        entry["emails"].append(e)
            
            # Method 3: Brute force - search entire page for emails
            all_page_emails = extract_emails_from_text(html)
            # Filter to only likely real contact emails (not Instagram internal)
            for e in all_page_emails:
                if 'instagram.com' not in e and 'fbcdn' not in e and e not in entry["emails"]:
                    entry["emails"].append(e)
            
            found_bio = f"bio={len(entry['bio'])}chars" if entry['bio'] else "no bio parsed"
            print(f"  ✓ @{username}: {found_bio}, "
                  f"emails={entry['emails'] or 'none'}, "
                  f"url={entry['external_url'] or 'none'}")
            
        except urllib.error.HTTPError as e:
            entry["error"] = f"HTTP {e.code}"
            print(f"  ✗ @{username}: HTTP {e.code}")
        except Exception as e:
            entry["error"] = str(e)[:100]
            print(f"  ✗ @{username}: {str(e)[:80]}")
        
        results.append(entry)
        time.sleep(2)  # Be gentle with web requests
    
    elapsed = time.time() - start_time
    emails_found = sum(1 for r in results if r["emails"])
    errors = sum(1 for r in results if r["error"])
    
    print(f"\n  Summary: {emails_found}/{len(accounts)} emails found, "
          f"{errors} errors, {elapsed:.1f}s elapsed")
    
    return {
        "tool": "Web Scraping",
        "emails_found": emails_found,
        "errors": errors,
        "elapsed_seconds": round(elapsed, 1),
        "results": results,
    }


# ═══════════════════════════════════════════════════════════════════
# TOOL C: Instagram GraphQL Public API
# ═══════════════════════════════════════════════════════════════════
def test_graphql_api(accounts):
    """
    Uses Instagram's public GraphQL endpoint to fetch profile data.
    This is the same endpoint the web app uses internally.
    No login required but rate-limited.
    """
    print("\n" + "="*60)
    print("  TOOL C: Instagram GraphQL API (public endpoint)")
    print("="*60)
    
    results = []
    start_time = time.time()
    
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
        "X-IG-App-ID": "936619743392459",  # Instagram web app ID (public)
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    
    for username in accounts:
        entry = {
            "username": username,
            "bio": None,
            "emails": [],
            "external_url": None,
            "followers": None,
            "is_business": None,
            "business_email": None,
            "business_phone": None,
            "category": None,
            "error": None,
        }
        
        try:
            # Instagram's public web API endpoint
            url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
            req = urllib.request.Request(url, headers=HEADERS)
            resp = urllib.request.urlopen(req, timeout=10, context=ctx)
            data = json.loads(resp.read().decode('utf-8'))
            
            user = data.get("data", {}).get("user", {})
            
            if user:
                entry["bio"] = user.get("biography", "")
                entry["external_url"] = user.get("external_url", "")
                entry["followers"] = user.get("edge_followed_by", {}).get("count", 0)
                entry["is_business"] = user.get("is_business_account", False)
                entry["business_email"] = user.get("business_email", "")
                entry["business_phone"] = user.get("business_phone_number", "")
                entry["category"] = user.get("category_name", "")
                
                # Collect emails from multiple sources
                all_emails = []
                
                # Source 1: business_email field (best quality)
                if entry["business_email"]:
                    all_emails.append(entry["business_email"].lower())
                
                # Source 2: bio text
                bio_emails = extract_emails_from_text(entry["bio"])
                all_emails.extend(bio_emails)
                
                # Source 3: external URL (we'd need to scrape it - just note it)
                entry["emails"] = list(set(all_emails))
                
                print(f"  ✓ @{username}: "
                      f"bio={len(entry['bio'])}chars, "
                      f"emails={entry['emails'] or 'none'}, "
                      f"biz_email={entry['business_email'] or 'none'}, "
                      f"url={entry['external_url'] or 'none'}, "
                      f"followers={entry['followers']:,}, "
                      f"business={entry['is_business']}, "
                      f"category={entry['category'] or 'none'}")
            else:
                entry["error"] = "user object empty"
                print(f"  ✗ @{username}: Empty user data")
            
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode('utf-8', errors='ignore')[:200]
            except:
                pass
            entry["error"] = f"HTTP {e.code}: {body[:100]}"
            print(f"  ✗ @{username}: HTTP {e.code} - {body[:80]}")
        except Exception as e:
            entry["error"] = str(e)[:100]
            print(f"  ✗ @{username}: {str(e)[:80]}")
        
        results.append(entry)
        time.sleep(2)  # Rate limit
    
    elapsed = time.time() - start_time
    emails_found = sum(1 for r in results if r["emails"])
    errors = sum(1 for r in results if r["error"])
    
    print(f"\n  Summary: {emails_found}/{len(accounts)} emails found, "
          f"{errors} errors, {elapsed:.1f}s elapsed")
    
    return {
        "tool": "GraphQL API",
        "emails_found": emails_found,
        "errors": errors,
        "elapsed_seconds": round(elapsed, 1),
        "results": results,
    }


# ═══════════════════════════════════════════════════════════════════
# MAIN: Run all tests and compare
# ═══════════════════════════════════════════════════════════════════
def print_comparison(results_a, results_b, results_c):
    """Print a side-by-side comparison table."""
    print("\n" + "="*60)
    print("  COMPARISON TABLE")
    print("="*60)
    
    tools = [results_a, results_b, results_c]
    
    # Header
    print(f"\n  {'Metric':<25} {'Instaloader':<18} {'Web Scrape':<18} {'GraphQL API':<18}")
    print(f"  {'-'*25} {'-'*18} {'-'*18} {'-'*18}")
    
    for metric in ["emails_found", "errors", "elapsed_seconds"]:
        label = metric.replace("_", " ").title()
        vals = []
        for t in tools:
            v = t.get(metric, "N/A")
            if metric == "emails_found":
                vals.append(f"{v}/10")
            elif metric == "elapsed_seconds":
                vals.append(f"{v}s")
            else:
                vals.append(str(v))
        print(f"  {label:<25} {vals[0]:<18} {vals[1]:<18} {vals[2]:<18}")
    
    # Per-account breakdown
    print(f"\n  {'Account':<22} {'Insta':<18} {'Web':<18} {'GraphQL':<18}")
    print(f"  {'-'*22} {'-'*18} {'-'*18} {'-'*18}")
    
    for i, username in enumerate(TEST_ACCOUNTS):
        vals = []
        for t in tools:
            if i < len(t.get("results", [])):
                r = t["results"][i]
                if r.get("error"):
                    vals.append("ERROR")
                elif r.get("emails"):
                    vals.append(", ".join(r["emails"][:2])[:16])
                else:
                    vals.append("no email")
            else:
                vals.append("N/A")
        print(f"  @{username:<20} {vals[0]:<18} {vals[1]:<18} {vals[2]:<18}")
    
    # Winner
    print(f"\n  {'='*60}")
    scores = [(t["tool"], t.get("emails_found", 0), t.get("errors", 10)) for t in tools]
    scores.sort(key=lambda x: (-x[1], x[2]))  # Most emails, fewest errors
    print(f"  WINNER: {scores[0][0]} ({scores[0][1]} emails, {scores[0][2]} errors)")
    print(f"  {'='*60}")


def main():
    print("╔════════════════════════════════════════════════════════════╗")
    print("║        INSTAGRAM SCRAPER MINI-TEST                        ║")
    print("║        Testing 3 tools against 10 accounts                ║")
    print(f"║        {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<49}║")
    print("╚════════════════════════════════════════════════════════════╝")
    
    print(f"\n  Test accounts: {', '.join(TEST_ACCOUNTS)}")
    
    # Run Tool A: Instaloader
    results_a = test_instaloader(TEST_ACCOUNTS)
    
    # Run Tool B: Web Scraping
    results_b = test_web_scraping(TEST_ACCOUNTS)
    
    # Run Tool C: GraphQL API
    results_c = test_graphql_api(TEST_ACCOUNTS)
    
    # Compare
    print_comparison(results_a, results_b, results_c)
    
    # Save full results to JSON
    output = {
        "test_date": datetime.now().isoformat(),
        "accounts_tested": TEST_ACCOUNTS,
        "tool_a_instaloader": results_a,
        "tool_b_web_scraping": results_b,
        "tool_c_graphql_api": results_c,
    }
    
    with open("instagram_test/test_results.json", "w") as f:
        json.dump(output, f, indent=2, default=str)
    
    print(f"\n  Full results saved to instagram_test/test_results.json")


if __name__ == "__main__":
    main()
