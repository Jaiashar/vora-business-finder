#!/usr/bin/env python3
"""
Parallel SMTP verification using 5 Gmail MX servers.
Each worker gets its own MX server, 3s between checks per worker.
Effective throughput: ~1 email per 0.6s = ~22 min for 2,200 emails.
Non-Gmail emails are kept as-is (Yahoo/Hotmail don't support RCPT TO verification).
"""

import json, os, smtplib, time, socket, sys
import dns.resolver
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

print_lock = Lock()
progress_lock = Lock()
results = {}  # email -> 'ok' | 'bad' | 'unknown'


def get_gmail_mx():
    answers = dns.resolver.resolve('gmail.com', 'MX', lifetime=10)
    return [str(r.exchange).rstrip('.') for r in sorted(answers, key=lambda x: x.preference)]


def verify_one(email, mx_host):
    try:
        with smtplib.SMTP(mx_host, 25, timeout=15) as smtp:
            smtp.helo('mail.askvora.com')
            smtp.mail('noreply@askvora.com')
            code, _ = smtp.rcpt(email)
            if code == 250:
                return 'ok'
            elif code in (550, 551, 552, 553, 554):
                return 'bad'
            return 'unknown'
    except Exception:
        return 'unknown'


def worker(chunk, mx_host, worker_id):
    local_ok = 0
    local_bad = 0
    local_unk = 0

    for i, email in enumerate(chunk):
        result = verify_one(email, mx_host)

        if result == 'unknown':
            time.sleep(2)
            result = verify_one(email, mx_host)

        with progress_lock:
            results[email] = result

        if result == 'ok':
            local_ok += 1
        elif result == 'bad':
            local_bad += 1
        else:
            local_unk += 1

        if (i + 1) % 50 == 0:
            with print_lock:
                print(f"  [W{worker_id}] {i+1}/{len(chunk)} ok={local_ok} bad={local_bad} unk={local_unk}")
                sys.stdout.flush()

        time.sleep(3)

    return local_ok, local_bad, local_unk


def main():
    contacts = json.load(open(os.path.join(BASE_DIR, 'final_unsent.json')))
    print(f"Total contacts: {len(contacts)}")

    gmail = [c for c in contacts if '@gmail.com' in c['email'] or '@googlemail.com' in c['email']]
    non_gmail = [c for c in contacts if c not in gmail]
    gmail_emails = [c['email'] for c in gmail]

    print(f"Gmail to verify: {len(gmail_emails)}")
    print(f"Non-Gmail (kept): {len(non_gmail)}")

    mx_hosts = get_gmail_mx()
    num_workers = min(5, len(mx_hosts))
    print(f"MX servers: {mx_hosts}")
    print(f"Workers: {num_workers}")

    chunk_size = len(gmail_emails) // num_workers + 1
    chunks = [gmail_emails[i:i+chunk_size] for i in range(0, len(gmail_emails), chunk_size)]

    est_time = len(gmail_emails) * 3 / num_workers / 60
    print(f"Estimated time: ~{est_time:.0f} minutes\n")

    total_ok = total_bad = total_unk = 0

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {}
        for idx, chunk in enumerate(chunks):
            mx = mx_hosts[idx % len(mx_hosts)]
            f = executor.submit(worker, chunk, mx, idx)
            futures[f] = idx

        for f in as_completed(futures):
            ok, bad, unk = f.result()
            total_ok += ok
            total_bad += bad
            total_unk += unk
            wid = futures[f]
            print(f"  Worker {wid} done: ok={ok} bad={bad} unk={unk}")

    bad_emails = [e for e, v in results.items() if v == 'bad']
    good_gmail = [c for c in gmail if results.get(c['email']) != 'bad']
    final = good_gmail + non_gmail

    print(f"\n{'='*60}")
    print(f"  VERIFICATION COMPLETE")
    print(f"  Gmail verified OK: {total_ok}")
    print(f"  Gmail confirmed BAD: {total_bad}")
    print(f"  Gmail unknown (kept): {total_unk}")
    print(f"  Non-Gmail (kept): {len(non_gmail)}")
    print(f"  TOTAL READY TO SEND: {len(final)}")
    print(f"  Bad rate: {total_bad}/{len(gmail_emails)} ({total_bad/max(len(gmail_emails),1)*100:.1f}%)")
    print(f"{'='*60}")

    with open(os.path.join(BASE_DIR, 'verified_ready.json'), 'w') as f:
        json.dump(final, f)
    with open(os.path.join(BASE_DIR, 'smtp_bad_all.json'), 'w') as f:
        json.dump(bad_emails, f, indent=2)

    print(f"\nSaved verified_ready.json ({len(final)} contacts)")
    print(f"Saved smtp_bad_all.json ({len(bad_emails)} bad emails)")

    if bad_emails[:30]:
        print(f"\nSample bad emails:")
        for e in bad_emails[:30]:
            print(f"  {e}")


if __name__ == '__main__':
    main()
