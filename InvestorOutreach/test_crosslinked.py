#!/usr/bin/env python3
"""Test CrossLinked for finding investor team members at VC/angel firms via Google dorking LinkedIn.
CrossLinked searches Google/Bing for LinkedIn profiles of employees at a company,
extracts names, and generates email patterns."""

import subprocess
import json
import csv
import os
import time


# Target firms - mix of VC, angel networks, health-focused
FIRMS = [
    {'name': '7wireVentures', 'domain': '7wireventures.com', 'type': 'health_vc'},
    {'name': 'Rock Health', 'domain': 'rockhealth.com', 'type': 'health_vc'},
    {'name': 'Andreessen Horowitz', 'domain': 'a16z.com', 'type': 'vc'},
    {'name': 'General Catalyst', 'domain': 'generalcatalyst.com', 'type': 'vc'},
    {'name': 'Founders Fund', 'domain': 'foundersfund.com', 'type': 'vc'},
    {'name': 'Khosla Ventures', 'domain': 'khoslaventures.com', 'type': 'health_vc'},
    {'name': 'Accel Partners', 'domain': 'accel.com', 'type': 'vc'},
]


def run_crosslinked(company, domain, output_dir='crosslinked_output'):
    """Run crosslinked for a single company."""
    os.makedirs(output_dir, exist_ok=True)

    # CrossLinked generates email patterns from names found via Google dorking LinkedIn
    # Format: {first}.{last}@domain
    email_format = f'{{first}}.{{last}}@{domain}'
    output_file = os.path.join(output_dir, f'{domain.replace(".", "_")}')

    cmd = [
        'crosslinked', company,
        '-f', email_format,
        '-o', output_file + '.txt',
        '-t', '10',  # timeout
        '-j', '1',   # jitter (seconds between requests)
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=output_dir,
        )
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return '', 'TIMEOUT', -1
    except FileNotFoundError:
        return '', 'crosslinked not found in PATH', -1


def parse_crosslinked_output(output_dir, domain):
    """Parse CrossLinked output files."""
    txt_file = os.path.join(output_dir, f'{domain.replace(".", "_")}.txt')
    csv_file = os.path.join(output_dir, 'names.csv')

    emails = []
    names = []

    if os.path.exists(txt_file):
        with open(txt_file) as f:
            for line in f:
                line = line.strip()
                if line and '@' in line:
                    emails.append(line)

    if os.path.exists(csv_file):
        with open(csv_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                names.append(row)

    return emails, names


def main():
    print(f"\n{'=' * 70}")
    print(f"  CROSSLINKED TEST - Google Dork LinkedIn for Investor Emails")
    print(f"  Testing {len(FIRMS)} firms")
    print(f"{'=' * 70}\n")

    output_dir = 'crosslinked_output'
    all_results = {}
    total_emails = 0
    total_names = 0

    for firm in FIRMS:
        name = firm['name']
        domain = firm['domain']
        print(f"  {name} ({domain})")

        stdout, stderr, rc = run_crosslinked(name, domain, output_dir)

        if rc != 0:
            print(f"    Error (rc={rc}): {stderr[:100]}")
            all_results[name] = {'error': stderr[:200], 'emails': [], 'names': []}
            continue

        # Show CrossLinked output
        if stdout:
            for line in stdout.strip().split('\n'):
                if line.strip():
                    print(f"    {line.strip()}")

        # Parse results
        emails, names = parse_crosslinked_output(output_dir, domain)
        total_emails += len(emails)
        total_names += len(names)

        all_results[name] = {
            'domain': domain,
            'type': firm['type'],
            'emails_generated': emails[:20],  # cap for readability
            'email_count': len(emails),
            'names_found': names[:20],
            'name_count': len(names),
        }

        if emails:
            print(f"    Generated {len(emails)} email patterns:")
            for e in emails[:8]:
                print(f"      {e}")
            if len(emails) > 8:
                print(f"      ... and {len(emails) - 8} more")
        else:
            print(f"    No emails generated")

        if names:
            print(f"    Found {len(names)} names")

        print()
        time.sleep(2)

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Total emails generated: {total_emails}")
    print(f"  Total names found: {total_names}")
    print()

    for name, data in all_results.items():
        count = data.get('email_count', 0)
        print(f"  {name}: {count} emails")
    print()

    with open('test_crosslinked_results.json', 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"  Results saved to test_crosslinked_results.json\n")


if __name__ == '__main__':
    main()
