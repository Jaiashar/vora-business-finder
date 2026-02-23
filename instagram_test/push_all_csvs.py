#!/usr/bin/env python3
"""Push ALL discovered CSV files to Supabase college_contacts table."""

import csv
import json
import urllib.request
import urllib.error
import ssl
import os
import glob
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

# Valid roles for the constraint
VALID_ROLES = {'student', 'staff', 'faculty', 'coach', 'student_org', 'department', 'director', 'coordinator'}

SKIP_EMAILS = {
    'webmaster@ucla.edu', 'communications@ucla.edu', 'info@ucla.edu',
    'fsl@ucla.edu', 'david.clewett@psych.ucla.edu',
}

def classify_role(email, raw_role=''):
    """Classify as student/staff/faculty."""
    raw = raw_role.lower() if raw_role else ''
    if any(kw in raw for kw in ['faculty', 'pi', 'professor']):
        return 'faculty'
    if any(kw in raw for kw in ['staff', 'admin', 'manager']):
        return 'staff'
    if '@g.ucla.edu' in email:
        return 'student'
    if any(kw in raw for kw in ['student', 'grad', 'phd', 'undergrad', 'rotation', 'ms ', 'likely_student', 'post-bacc']):
        return 'student'
    if any(kw in raw for kw in ['researcher', 'postdoc']):
        return 'staff'
    # Default: if it's a @humnet, @math, etc. subdomain in a grad student directory, likely student
    return 'student'


def push_row(email, name, department, role, source):
    """Push a single row to Supabase."""
    email = email.lower().strip()
    if not email or email in SKIP_EMAILS:
        return 'skip'
    if '@' not in email:
        return 'skip'
    
    # Must be UCLA-related
    if 'ucla.edu' not in email:
        return 'skip'

    mapped_role = classify_role(email, role)
    if mapped_role not in VALID_ROLES:
        mapped_role = 'student'

    segment = 'grad_student' if '@g.ucla.edu' in email else 'student'

    row = {
        "email": email,
        "name": name.strip() if name and name.strip() and name.strip() != 'Contact Information' else None,
        "department": department.strip() if department else None,
        "role": mapped_role,
        "university": "UCLA",
        "source_url": source,
        "segment": segment,
    }
    
    url = f"{SUPABASE_URL}/rest/v1/college_contacts"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    data = json.dumps([row]).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method='POST')
    try:
        resp = urllib.request.urlopen(req, context=SSL_CTX)
        return 'inserted'
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        if 'duplicate' in body.lower() or '23505' in body:
            return 'dupe'
        return 'error'


def process_csv(filepath):
    """Process a CSV file and push to Supabase."""
    inserted = 0
    dupes = 0
    skips = 0
    errors = 0
    
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames
        
        for row in reader:
            email = row.get('email', '').strip()
            name = row.get('name', '').strip()
            dept = row.get('department', row.get('dept', '')).strip()
            role = row.get('role', row.get('type', '')).strip()
            source = row.get('source_url', row.get('source', '')).strip()
            
            result = push_row(email, name, dept, role, source)
            if result == 'inserted':
                inserted += 1
            elif result == 'dupe':
                dupes += 1
            elif result == 'skip':
                skips += 1
            else:
                errors += 1
    
    return inserted, dupes, skips, errors


def main():
    base = os.path.dirname(os.path.abspath(__file__))
    parent = os.path.dirname(base)
    
    # Find all UCLA CSV files
    csv_files = sorted(set(
        glob.glob(os.path.join(parent, 'ucla_*.csv'))
    ))
    # Exclude old master files from instagram_test
    csv_files = [f for f in csv_files if 'instagram_test' not in f]
    
    print(f"Found {len(csv_files)} CSV files to process\n")
    
    total_inserted = 0
    total_dupes = 0
    
    for fp in csv_files:
        fname = os.path.basename(fp)
        inserted, dupes, skips, errors = process_csv(fp)
        total_inserted += inserted
        total_dupes += dupes
        print(f"  {fname:<40} → +{inserted} new, {dupes} dupes, {skips} skips, {errors} errors")
    
    print(f"\n{'='*60}")
    print(f"  Total new: {total_inserted}")
    print(f"  Total dupes: {total_dupes}")
    
    # Final count
    count_headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Prefer": "count=exact"}
    
    req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/college_contacts?select=id&university=eq.UCLA", headers=count_headers)
    resp = urllib.request.urlopen(req, context=SSL_CTX)
    print(f"\n  Total UCLA contacts: {resp.getheader('Content-Range')}")
    
    req2 = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/college_contacts?select=id&role=eq.student", headers=count_headers)
    resp2 = urllib.request.urlopen(req2, context=SSL_CTX)
    print(f"  Students: {resp2.getheader('Content-Range')}")
    
    req3 = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/college_contacts?select=id&role=eq.faculty", headers=count_headers)
    resp3 = urllib.request.urlopen(req3, context=SSL_CTX)
    print(f"  Faculty: {resp3.getheader('Content-Range')}")
    
    req4 = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/college_contacts?select=id&role=eq.staff", headers=count_headers)
    resp4 = urllib.request.urlopen(req4, context=SSL_CTX)
    print(f"  Staff: {resp4.getheader('Content-Range')}")


if __name__ == "__main__":
    main()
