#!/usr/bin/env python3
"""Get Instagram session cookie via Playwright browser login."""
import os, sys, time, json
from playwright.sync_api import sync_playwright

def load_env():
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    env = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                env[k.strip()] = v.strip()
    return env

ENV = load_env()

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 800},
    )
    page = ctx.new_page()

    print("Navigating to Instagram login...")
    page.goto("https://www.instagram.com/accounts/login/", timeout=20000)
    time.sleep(4)

    # Accept cookies if prompted
    for text in ["Allow all cookies", "Allow essential and optional cookies", "Accept"]:
        try:
            page.click(f"text={text}", timeout=2000)
            time.sleep(1)
            break
        except: pass

    print(f"Logging in as @{ENV['IG_USERNAME']}...")
    page.fill('input[name="username"]', ENV['IG_USERNAME'])
    time.sleep(0.5)
    page.fill('input[name="password"]', ENV['IG_PASSWORD'])
    time.sleep(0.5)
    page.click('button[type="submit"]')
    time.sleep(8)

    # Dismiss popups
    for text in ["Not Now", "Not now", "Skip"]:
        try:
            page.click(f"text={text}", timeout=3000)
            time.sleep(1)
        except: pass

    # Get cookies
    cookies = ctx.cookies()
    session_id = ""
    csrf = ""
    for c in cookies:
        if c["name"] == "sessionid":
            session_id = c["value"]
        if c["name"] == "csrftoken":
            csrf = c["value"]

    browser.close()

    if session_id:
        print(f"\n✅ Session ID: {session_id}")
        print(f"   CSRF Token: {csrf}")
        # Save to file
        with open(os.path.join(os.path.dirname(__file__), "session_cookie.txt"), "w") as f:
            f.write(session_id)
        print("   Saved to session_cookie.txt")
    else:
        print("\n❌ No session cookie found. Login may have failed.")
        print(f"   Current URL: {page.url if not browser.is_connected() else 'browser closed'}")
        print(f"   All cookies: {[c['name'] for c in cookies]}")
