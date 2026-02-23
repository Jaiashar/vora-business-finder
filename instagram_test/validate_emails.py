#!/usr/bin/env python3
"""
Thorough email validation for remaining unsent consumer_leads.
1. Syntax validation (RFC-compliant format)
2. MX record check (does the domain actually accept email?)
3. Aggressive pattern scrub (fakes, trolls, placeholders, junk)
4. SMTP mailbox verification for Gmail addresses (checks if account exists)
"""

import json, re, os, sys, socket, smtplib, dns.resolver, time
from collections import Counter

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

EMAIL_RE = re.compile(
    r'^[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?'
    r'(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}$'
)

JUNK_PATTERNS = [
    'example', 'test@', 'fake', 'nobody@', 'noreply', 'donotreply',
    'placeholder', 'asdf', 'qwerty', 'abcdef', 'aaaa@', 'xxxx',
    'spam@', 'trash@', 'junk@', 'temp@', 'throwaway', 'burner',
    'sample@', 'dummy@', 'null@', 'void@', 'none@', 'na@',
    'notreal', 'noname', 'anonymous@', 'anon@', 'user@',
    'myemail', 'emailme', 'sendit', 'sendme', 'writeme',
    'reachme', 'hitmeup', 'contactme', 'mailme',
    'someone@', 'something@', 'anything@', 'nothing@',
    'address@', 'email@gmail', 'me@gmail.com',
    'your', 'mine@', 'here@', 'there@',
    '123@', 'abc@gmail', 'xyz@',
    'admin@gmail', 'info@gmail', 'help@gmail', 'support@gmail',
    'hello@gmail', 'hi@gmail', 'hey@gmail',
]

OFFENSIVE = [
    'drugs', 'porn', 'xxx', 'slut', 'fuck', 'shit',
    'penis', 'vagina', 'cock', 'bitch', 'whore',
    'nazi', 'hitler', 'murder', 'rape',
    'moron', 'idiot', 'retard',
    'blazing_slut', 'adult', 'escort', 'hookup', 'onlyfans',
    'kittenfucker', 'cunslut', 'lyingwhore', 'loismustdie',
    'manwhore', 'killyour',
]

DISPOSABLE_DOMAINS = {
    'mailinator.com', 'guerrillamail.com', 'tempmail.com', 'throwaway.email',
    'yopmail.com', 'sharklasers.com', 'guerrillamailblock.com', 'grr.la',
    'dispostable.com', 'mailnesia.com', 'maildrop.cc', 'discard.email',
    'trashmail.com', 'trashmail.me', 'fakeinbox.com', '10minutemail.com',
    'tempail.com', 'tempr.email', 'temp-mail.org', 'getnada.com',
    'mohmal.com', 'burnermail.io', 'guerrillamail.info', 'trash-mail.com',
}

mx_cache = {}

def check_mx(domain):
    if domain in mx_cache:
        return mx_cache[domain]
    try:
        answers = dns.resolver.resolve(domain, 'MX', lifetime=5)
        result = len(answers) > 0
    except:
        result = False
    mx_cache[domain] = result
    return result


def smtp_verify_gmail(email):
    """Check if a Gmail address exists by connecting to Gmail's MX server."""
    try:
        mx_host = 'gmail-smtp-in.l.google.com'
        with smtplib.SMTP(mx_host, 25, timeout=10) as smtp:
            smtp.helo('askvora.com')
            smtp.mail('verify@askvora.com')
            code, _ = smtp.rcpt(email)
            return code == 250
    except:
        return None  # couldn't verify, don't discard


def validate_email(email):
    """Returns (valid, reason) tuple."""
    email_lower = email.lower().strip()

    if not EMAIL_RE.match(email_lower):
        return False, 'bad_syntax'

    if email_lower.startswith(('.', '-', '+', '_', '%')):
        return False, 'starts_special_char'

    local = email_lower.split('@')[0]
    domain = email_lower.split('@')[1] if '@' in email_lower else ''

    if len(local) < 2 or len(local) > 64:
        return False, 'local_part_length'

    if domain in DISPOSABLE_DOMAINS:
        return False, 'disposable_domain'

    for pattern in JUNK_PATTERNS:
        if pattern in email_lower:
            return False, f'junk_pattern:{pattern}'

    for word in OFFENSIVE:
        if word in email_lower:
            return False, f'offensive:{word}'

    # Plus aliases with test/spam patterns
    if '+' in local:
        plus_part = local.split('+')[1] if '+' in local else ''
        junk_plus = ['test', 'spam', 'trash', 'junk', 'orders', 'tag',
                     'delete', 'ignore', 'throwaway', 'temp', 'fake']
        for jp in junk_plus:
            if jp in plus_part:
                return False, f'plus_alias:{jp}'

    # Too short generic gmail (3-4 random chars)
    if domain == 'gmail.com' and re.match(r'^[a-z]{2,4}$', local):
        return False, 'too_short_generic'

    # Mostly numbers
    alpha = re.sub(r'[^a-z]', '', local)
    digits = re.sub(r'[^0-9]', '', local)
    if len(local) > 3 and len(digits) > len(alpha) * 3:
        return False, 'mostly_numbers'

    # All same character
    if len(set(local.replace('.', '').replace('_', ''))) <= 2 and len(local) > 4:
        return False, 'repeated_chars'

    return True, 'ok'


def main():
    emails_data = json.load(open(os.path.join(BASE_DIR, 'unsent_full.json')))
    print(f"Validating {len(emails_data)} unsent emails...\n")

    valid = []
    invalid = []
    reasons = Counter()

    # Phase 1: Syntax + pattern check
    print("Phase 1: Syntax & pattern validation...")
    for contact in emails_data:
        email = contact['email']
        ok, reason = validate_email(email)
        if ok:
            valid.append(contact)
        else:
            invalid.append({'email': email, 'reason': reason})
            reasons[reason] += 1

    print(f"  Passed: {len(valid)}")
    print(f"  Failed: {len(invalid)}")
    for r, c in reasons.most_common(20):
        print(f"    {r}: {c}")

    # Phase 2: MX record check on unique domains
    print(f"\nPhase 2: MX record validation...")
    domains = set(c['email'].split('@')[1] for c in valid)
    print(f"  Checking {len(domains)} unique domains...")

    bad_domains = set()
    for i, domain in enumerate(domains):
        if not check_mx(domain):
            bad_domains.add(domain)
        if (i + 1) % 50 == 0:
            print(f"    Checked {i+1}/{len(domains)} domains, {len(bad_domains)} bad so far...")

    mx_failed = []
    mx_passed = []
    for contact in valid:
        domain = contact['email'].split('@')[1]
        if domain in bad_domains:
            mx_failed.append({'email': contact['email'], 'reason': f'no_mx:{domain}'})
            reasons[f'no_mx:{domain}'] += 1
        else:
            mx_passed.append(contact)

    print(f"  MX passed: {len(mx_passed)}")
    print(f"  MX failed: {len(mx_failed)}")
    if bad_domains:
        print(f"  Bad domains: {bad_domains}")

    # Phase 3: SMTP verify a sample of Gmail addresses
    print(f"\nPhase 3: SMTP verification (Gmail sample)...")
    gmail_contacts = [c for c in mx_passed if c['email'].endswith('@gmail.com')]
    non_gmail = [c for c in mx_passed if not c['email'].endswith('@gmail.com')]
    print(f"  Gmail: {len(gmail_contacts)}, Other: {len(non_gmail)}")

    sample_size = min(50, len(gmail_contacts))
    import random
    sample = random.sample(gmail_contacts, sample_size)

    smtp_ok = 0
    smtp_bad = 0
    smtp_unknown = 0
    smtp_bad_emails = []

    for i, contact in enumerate(sample):
        result = smtp_verify_gmail(contact['email'])
        if result is True:
            smtp_ok += 1
        elif result is False:
            smtp_bad += 1
            smtp_bad_emails.append(contact['email'])
        else:
            smtp_unknown += 1
        if (i + 1) % 10 == 0:
            print(f"    Verified {i+1}/{sample_size}: {smtp_ok} ok, {smtp_bad} bad, {smtp_unknown} unknown")
        time.sleep(0.5)

    print(f"  Sample results: {smtp_ok} ok, {smtp_bad} bad, {smtp_unknown} unknown")
    if smtp_bad_emails:
        print(f"  Bad sample emails: {smtp_bad_emails[:10]}")

    estimated_bad_rate = smtp_bad / max(smtp_ok + smtp_bad, 1)
    print(f"  Estimated Gmail bad rate: {estimated_bad_rate*100:.1f}%")

    # If bad rate is high, verify ALL Gmail addresses
    final_valid = list(non_gmail)
    if estimated_bad_rate > 0.05:
        print(f"\n  Bad rate > 5%, verifying ALL {len(gmail_contacts)} Gmail addresses...")
        verified_gmail = []
        gmail_removed = []
        for i, contact in enumerate(gmail_contacts):
            result = smtp_verify_gmail(contact['email'])
            if result is False:
                gmail_removed.append(contact['email'])
            else:
                verified_gmail.append(contact)
            if (i + 1) % 100 == 0:
                print(f"    Verified {i+1}/{len(gmail_contacts)}, removed {len(gmail_removed)} so far...")
            time.sleep(0.3)
        print(f"  Gmail verified: {len(verified_gmail)}, removed: {len(gmail_removed)}")
        final_valid.extend(verified_gmail)

        with open(os.path.join(BASE_DIR, 'gmail_removed.json'), 'w') as f:
            json.dump(gmail_removed, f, indent=2)
    else:
        print(f"\n  Bad rate acceptable, keeping all Gmail addresses.")
        final_valid.extend(gmail_contacts)

    # Save results
    all_invalid = invalid + mx_failed
    with open(os.path.join(BASE_DIR, 'validated_unsent.json'), 'w') as f:
        json.dump(final_valid, f)
    with open(os.path.join(BASE_DIR, 'invalid_emails.json'), 'w') as f:
        json.dump(all_invalid, f, indent=2)

    print(f"\n{'='*60}")
    print(f"  VALIDATION COMPLETE")
    print(f"  Started with: {len(emails_data)}")
    print(f"  Pattern/syntax removed: {len(invalid)}")
    print(f"  MX removed: {len(mx_failed)}")
    print(f"  Final valid: {len(final_valid)}")
    print(f"{'='*60}")
    print(f"\nSaved validated_unsent.json and invalid_emails.json")


if __name__ == "__main__":
    main()
