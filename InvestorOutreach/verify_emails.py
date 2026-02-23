#!/usr/bin/env python3
"""
Stage 3: SMTP Email Verification

Verifies candidate email patterns against mail servers.
For each lead, tests each candidate email via SMTP RCPT TO.
Detects catch-all domains and handles rate limiting.

Usage:
    python verify_emails.py --test-port               # Check if port 25 works
    python verify_emails.py                            # Verify all pending leads
    python verify_emails.py --company "Accel"          # Single company
    python verify_emails.py --batch-size 50            # Limit per run
    python verify_emails.py --dry-run                  # Preview without saving
"""

import argparse
import dns.resolver
import json
import os
import random
import smtplib
import socket
import string
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from supabase import create_client

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# SMTP config
SMTP_TIMEOUT = 8
SMTP_FROM = 'verify@askvora.com'
SMTP_HELO = 'askvora.com'
MAX_PER_DOMAIN_PER_MIN = 15
DELAY_BETWEEN_CHECKS = 0.5


def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in ../.env")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# Email providers / security gateways that block SMTP RCPT TO verification
SECURITY_GATEWAYS = [
    # Security gateways
    'mimecast', 'proofpoint', 'barracuda', 'pphosted', 'messagelabs',
    'postini', 'symanteccloud', 'forcepoint', 'mxlogic', 'reflexion',
    'trendmicro', 'sophos', 'fireeye', 'agari', 'ironport', 'cisco',
    # Major email providers that reject RCPT TO verification
    'google.com', 'googlemail', 'aspmx', 'outlook.com', 'microsoft.com',
    'protection.outlook', 'mail.protection',
    # Proofpoint variants
    'ppe-hosted', 'pphosted',
]

# ── DNS/MX Lookup ────────────────────────────────────────────────────────────

_mx_cache = {}

def get_mx_host(domain):
    """Get the primary MX server for a domain. Caches results."""
    if domain in _mx_cache:
        return _mx_cache[domain]

    # Validate domain format
    if not domain or ',' in domain or '..' in domain or ' ' in domain:
        _mx_cache[domain] = None
        return None

    try:
        answers = dns.resolver.resolve(domain, 'MX')
        mx_records = sorted(answers, key=lambda r: r.preference)
        mx_host = str(mx_records[0].exchange).rstrip('.')
        _mx_cache[domain] = mx_host
        return mx_host
    except Exception:
        _mx_cache[domain] = None
        return None


def is_security_gateway(mx_host):
    """Check if MX host is a known email security gateway that blocks verification."""
    mx_lower = mx_host.lower()
    return any(gw in mx_lower for gw in SECURITY_GATEWAYS)


# ── SMTP Verification ────────────────────────────────────────────────────────

def verify_email_smtp(email, mx_host):
    """
    Verify an email address via SMTP RCPT TO.
    Returns: ('valid', 'invalid', 'error', 'timeout', 'blocked')
    """
    try:
        smtp = smtplib.SMTP(timeout=SMTP_TIMEOUT)
        smtp.connect(mx_host, 25)
        smtp.ehlo(SMTP_HELO)

        # Some servers require STARTTLS
        try:
            smtp.starttls()
            smtp.ehlo(SMTP_HELO)
        except (smtplib.SMTPException, OSError):
            pass

        smtp.mail(SMTP_FROM)
        code, msg = smtp.rcpt(email)
        smtp.quit()

        if code == 250:
            return 'valid'
        elif code in (550, 551, 552, 553, 554):
            return 'invalid'
        elif code in (450, 451, 452):
            return 'error'  # greylisting or temp issue
        else:
            return 'error'

    except smtplib.SMTPConnectError:
        return 'blocked'
    except smtplib.SMTPServerDisconnected:
        return 'error'
    except socket.timeout:
        return 'timeout'
    except ConnectionRefusedError:
        return 'blocked'
    except OSError as e:
        if 'Network is unreachable' in str(e) or 'Connection refused' in str(e):
            return 'blocked'
        return 'error'
    except Exception:
        return 'error'


def check_catch_all(domain, mx_host):
    """
    Check if a domain is a catch-all (accepts all addresses).
    Send RCPT TO a random nonsense address. If 250, it is catch-all.
    """
    random_user = ''.join(random.choices(string.ascii_lowercase, k=12)) + '9999'
    fake_email = f"{random_user}@{domain}"
    result = verify_email_smtp(fake_email, mx_host)
    return result == 'valid'


def test_port_25():
    """Test if outbound port 25 is accessible."""
    print("\n  Testing outbound port 25 access...")

    test_targets = [
        ('gmail-smtp-in.l.google.com', 'Gmail'),
        ('mx1.hotmail.com', 'Hotmail/Outlook'),
    ]

    for mx, name in test_targets:
        try:
            sock = socket.create_connection((mx, 25), timeout=10)
            banner = sock.recv(1024).decode('utf-8', errors='ignore')
            sock.close()
            print(f"    {name} ({mx}): OPEN")
            print(f"      Banner: {banner.strip()[:80]}")
            return True
        except socket.timeout:
            print(f"    {name} ({mx}): TIMEOUT (likely blocked)")
        except ConnectionRefusedError:
            print(f"    {name} ({mx}): REFUSED")
        except OSError as e:
            print(f"    {name} ({mx}): {str(e)[:50]}")

    print("\n  Port 25 appears BLOCKED on this network.")
    print("  Options:")
    print("    1. Run from a VPS/server that allows port 25")
    print("    2. Use the pipeline without verification (best-guess patterns)")
    print("    3. Use a third-party email verification API\n")
    return False


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Stage 3: SMTP Email Verification')
    parser.add_argument('--test-port', action='store_true',
                        help='Just test if port 25 is accessible')
    parser.add_argument('--company', type=str, help='Verify for a specific company only')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Max leads to verify per run')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview what would be verified')
    parser.add_argument('--skip-catch-all', action='store_true',
                        help='Skip catch-all detection (faster)')
    args = parser.parse_args()

    if args.test_port:
        test_port_25()
        return

    supabase = get_supabase()

    # First test port 25
    print("\n  Pre-flight: checking port 25...")
    port_open = test_port_25()
    if not port_open:
        print("  Continuing anyway (will mark unverifiable leads)...\n")

    # Fetch leads needing verification
    query = (
        supabase.table('investor_leads')
        .select('*')
        .eq('verification_status', 'pending')
        .neq('candidate_emails', '[]')
    )

    if args.company:
        query = query.ilike('company', f'%{args.company}%')

    # Paginate
    all_leads = []
    page_size = 1000
    offset = 0
    while True:
        response = query.range(offset, offset + page_size - 1).execute()
        all_leads.extend(response.data)
        if len(response.data) < page_size:
            break
        offset += page_size

    # Apply batch size
    leads = all_leads[:args.batch_size]

    print(f"\n{'=' * 65}")
    print(f"  SMTP EMAIL VERIFICATION - Stage 3")
    print(f"  Leads to verify: {len(leads)} (of {len(all_leads)} pending)")
    print(f"  Port 25: {'OPEN' if port_open else 'BLOCKED'}")
    print(f"  {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"{'=' * 65}\n")

    if not leads:
        print("  No leads to verify. Run generate_emails.py first.\n")
        return

    # Group by domain to handle catch-all detection and rate limiting
    domain_groups = {}
    for lead in leads:
        domain = lead.get('domain', '')
        if domain not in domain_groups:
            domain_groups[domain] = []
        domain_groups[domain].append(lead)

    # Track catch-all domains
    catch_all_domains = set()
    domain_check_times = {}

    verified_count = 0
    failed_count = 0
    catch_all_count = 0
    unverifiable_count = 0

    for domain, domain_leads in domain_groups.items():
        if not domain:
            continue

        print(f"\n  Domain: {domain} ({len(domain_leads)} leads)")

        # Get MX server
        mx = get_mx_host(domain)
        if not mx:
            print(f"    No MX record found. Marking as unverifiable.")
            if not args.dry_run:
                for lead in domain_leads:
                    supabase.table('investor_leads').update({
                        'verification_status': 'unverifiable',
                    }).eq('id', lead['id']).execute()
            unverifiable_count += len(domain_leads)
            continue

        print(f"    MX: {mx}")

        # Check for email security gateways that block SMTP verification
        if is_security_gateway(mx):
            print(f"    Security gateway detected. Using best-guess pattern.")
            for lead in domain_leads:
                candidates = lead.get('candidate_emails', [])
                # first.last@domain is the most common corporate pattern (index 1)
                best_guess = candidates[1] if len(candidates) > 1 else (candidates[0] if candidates else None)
                if not args.dry_run:
                    supabase.table('investor_leads').update({
                        'verification_status': 'unverifiable',
                        'verified_email': best_guess,
                        'verified_at': datetime.now(timezone.utc).isoformat(),
                    }).eq('id', lead['id']).execute()
                else:
                    print(f"    [DRY] {lead['full_name']}: best guess {best_guess}")
                unverifiable_count += 1
            continue

        if not port_open:
            print(f"    Port 25 blocked. Marking as unverifiable.")
            if not args.dry_run:
                for lead in domain_leads:
                    candidates = lead.get('candidate_emails', [])
                    best_guess = candidates[1] if len(candidates) > 1 else (candidates[0] if candidates else None)
                    supabase.table('investor_leads').update({
                        'verification_status': 'unverifiable',
                        'verified_email': best_guess,
                    }).eq('id', lead['id']).execute()
            unverifiable_count += len(domain_leads)
            continue

        # Check catch-all (unless skipped)
        if not args.skip_catch_all and domain not in catch_all_domains:
            if args.dry_run:
                print(f"    [DRY] Would check catch-all")
            else:
                print(f"    Checking catch-all...", end=' ')
                is_catch_all = check_catch_all(domain, mx)
                if is_catch_all:
                    catch_all_domains.add(domain)
                    print("YES (accepts all addresses)")
                else:
                    print("No")
                time.sleep(DELAY_BETWEEN_CHECKS)

        if domain in catch_all_domains:
            # For catch-all domains, use the most common pattern as best guess
            for lead in domain_leads:
                candidates = lead.get('candidate_emails', [])
                # first.last@domain is typically the best guess (index 1)
                best_guess = candidates[1] if len(candidates) > 1 else (candidates[0] if candidates else None)
                if not args.dry_run:
                    supabase.table('investor_leads').update({
                        'verification_status': 'catch_all',
                        'verified_email': best_guess,
                        'verified_at': datetime.now(timezone.utc).isoformat(),
                    }).eq('id', lead['id']).execute()
                else:
                    print(f"    [DRY] {lead['full_name']}: catch-all, best guess: {best_guess}")
                catch_all_count += 1
            continue

        # Verify each lead's candidate emails
        for lead in domain_leads:
            candidates = lead.get('candidate_emails', [])
            full_name = lead.get('full_name', '?')

            if args.dry_run:
                print(f"    [DRY] {full_name}: would test {len(candidates)} patterns")
                continue

            print(f"    {full_name}: testing {len(candidates)} patterns...", end=' ')

            found_email = None
            for email in candidates:
                # Rate limiting per domain
                now = time.time()
                if domain in domain_check_times:
                    elapsed = now - domain_check_times[domain]
                    if elapsed < (60 / MAX_PER_DOMAIN_PER_MIN):
                        wait = (60 / MAX_PER_DOMAIN_PER_MIN) - elapsed
                        time.sleep(wait)

                result = verify_email_smtp(email, mx)
                domain_check_times[domain] = time.time()

                if result == 'valid':
                    found_email = email
                    break
                elif result == 'blocked':
                    print("PORT BLOCKED")
                    # Mark remaining as unverifiable
                    supabase.table('investor_leads').update({
                        'verification_status': 'unverifiable',
                        'verified_email': candidates[1] if len(candidates) > 1 else candidates[0],
                    }).eq('id', lead['id']).execute()
                    unverifiable_count += 1
                    break

                time.sleep(DELAY_BETWEEN_CHECKS)

            if found_email:
                print(f"VERIFIED: {found_email}")
                supabase.table('investor_leads').update({
                    'verified_email': found_email,
                    'verification_status': 'verified',
                    'verified_at': datetime.now(timezone.utc).isoformat(),
                }).eq('id', lead['id']).execute()
                verified_count += 1
            elif result != 'blocked':
                print("NONE VALID")
                # Use first.last as best guess even though unverified
                best_guess = candidates[1] if len(candidates) > 1 else (candidates[0] if candidates else None)
                supabase.table('investor_leads').update({
                    'verification_status': 'failed',
                    'verified_email': best_guess,
                }).eq('id', lead['id']).execute()
                failed_count += 1

    # Summary
    print(f"\n{'=' * 65}")
    print(f"  VERIFICATION SUMMARY")
    print(f"{'=' * 65}")
    print(f"  Verified:      {verified_count}")
    print(f"  Failed:        {failed_count}")
    print(f"  Catch-all:     {catch_all_count}")
    print(f"  Unverifiable:  {unverifiable_count}")
    if catch_all_domains:
        print(f"\n  Catch-all domains:")
        for d in sorted(catch_all_domains):
            print(f"    {d}")
    print(f"{'=' * 65}\n")


if __name__ == '__main__':
    main()
