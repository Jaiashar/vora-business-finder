#!/usr/bin/env python3
"""
SMTP verify all Gmail emails from the unsent list.
Gmail is 88% of the list and the only major provider that gives reliable
SMTP RCPT TO responses. Yahoo/Hotmail/Outlook accept-all or greylist,
so SMTP verification doesn't work for them.

Uses 3-second delays + MX server rotation for reliable results.
Saves progress every 50 emails for safe resume.
"""

import json, os, smtplib, time, socket, dns.resolver, sys
from collections import Counter

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

gmail_mx_hosts = None

def get_gmail_mx():
    global gmail_mx_hosts
    if gmail_mx_hosts:
        return gmail_mx_hosts
    answers = dns.resolver.resolve('gmail.com', 'MX', lifetime=10)
    gmail_mx_hosts = [str(r.exchange).rstrip('.') for r in sorted(answers, key=lambda x: x.preference)]
    print(f"Gmail MX servers: {gmail_mx_hosts}")
    return gmail_mx_hosts


def smtp_verify_gmail(email, attempt=0):
    """Returns True (exists), False (doesn't), None (can't tell)."""
    hosts = get_gmail_mx()
    mx = hosts[attempt % len(hosts)]
    try:
        with smtplib.SMTP(mx, 25, timeout=15) as smtp:
            smtp.helo('mail.askvora.com')
            smtp.mail('noreply@askvora.com')
            code, _ = smtp.rcpt(email)
            if code == 250:
                return True
            elif code in (550, 551, 552, 553, 554):
                return False
            return None
    except Exception:
        return None


def load_progress():
    path = os.path.join(BASE_DIR, 'smtp_progress.json')
    if os.path.exists(path):
        return json.load(open(path))
    return {'checked': {}}


def save_progress(progress):
    with open(os.path.join(BASE_DIR, 'smtp_progress.json'), 'w') as f:
        json.dump(progress, f)


def main():
    unsent = json.load(open(os.path.join(BASE_DIR, 'validated_unsent.json')))
    extra = set(json.load(open(os.path.join(BASE_DIR, 'extra_remove.json'))))
    remaining = [c for c in unsent if c['email'] not in extra]

    gmail = [c for c in remaining if c['email'].endswith('@gmail.com') or c['email'].endswith('@googlemail.com')]
    non_gmail = [c for c in remaining if c not in gmail]

    print(f"Total remaining: {len(remaining)}")
    print(f"Gmail to verify: {len(gmail)}")
    print(f"Non-Gmail (kept as-is): {len(non_gmail)}")

    # Reset progress for Gmail-only run
    progress = load_progress()
    already_done = set(progress['checked'].keys())
    to_check = [c for c in gmail if c['email'] not in already_done]
    print(f"Already verified: {len(already_done)}")
    print(f"Remaining to check: {len(to_check)}\n")

    stats = Counter()
    for v in progress['checked'].values():
        stats[v] += 1

    get_gmail_mx()

    for i, contact in enumerate(to_check):
        email = contact['email']
        result = smtp_verify_gmail(email, attempt=i)

        if result is True:
            progress['checked'][email] = 'ok'
            stats['ok'] += 1
        elif result is False:
            progress['checked'][email] = 'bad'
            stats['bad'] += 1
        else:
            time.sleep(2)
            result2 = smtp_verify_gmail(email, attempt=i + 1)
            if result2 is True:
                progress['checked'][email] = 'ok'
                stats['ok'] += 1
            elif result2 is False:
                progress['checked'][email] = 'bad'
                stats['bad'] += 1
            else:
                progress['checked'][email] = 'unknown'
                stats['unknown'] += 1

        total_done = len(progress['checked'])
        if total_done % 50 == 0:
            save_progress(progress)
            pct = total_done / len(gmail) * 100
            est_remaining = (len(gmail) - total_done) * 3.5 / 60
            print(f"  [{total_done}/{len(gmail)}] ({pct:.0f}%) ok={stats['ok']} bad={stats['bad']} unk={stats['unknown']} | ~{est_remaining:.0f}min left")
            sys.stdout.flush()

        time.sleep(3)

    save_progress(progress)

    bad_emails = [e for e, v in progress['checked'].items() if v == 'bad']
    good_gmail = [c for c in gmail if progress['checked'].get(c['email']) != 'bad']
    final = good_gmail + non_gmail

    print(f"\n{'='*60}")
    print(f"  SMTP VERIFICATION COMPLETE")
    print(f"  Gmail checked: {len(progress['checked'])}")
    print(f"  Verified good: {stats['ok']}")
    print(f"  Confirmed bad: {stats['bad']} ({stats['bad']/max(len(progress['checked']),1)*100:.1f}%)")
    print(f"  Unknown (kept): {stats['unknown']}")
    print(f"  Non-Gmail (kept): {len(non_gmail)}")
    print(f"  FINAL READY TO SEND: {len(final)}")
    print(f"{'='*60}")

    with open(os.path.join(BASE_DIR, 'verified_ready.json'), 'w') as f:
        json.dump(final, f)
    with open(os.path.join(BASE_DIR, 'smtp_bad.json'), 'w') as f:
        json.dump(bad_emails, f, indent=2)

    print(f"\nSaved verified_ready.json ({len(final)} contacts)")
    print(f"Saved smtp_bad.json ({len(bad_emails)} bad emails)")

    if bad_emails:
        print(f"\nAll bad emails:")
        for e in bad_emails:
            print(f"  {e}")


if __name__ == "__main__":
    main()
