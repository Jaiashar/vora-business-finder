#!/usr/bin/env python3
"""Push all USC CSV files to Supabase college_contacts table."""

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

VALID_ROLES = {'student', 'staff', 'faculty', 'coach', 'student_org', 'department', 'director', 'coordinator'}

SKIP_EMAILS = {'info@usc.edu', 'webmaster@usc.edu', 'communications@usc.edu',
               'uscgreeklife@usc.edu', 'admission@usc.edu'}


def classify_role(email, raw_role='', dept=''):
    raw = (raw_role + ' ' + dept).lower()
    if any(kw in raw for kw in ['faculty', 'professor', 'pi', 'principal investigator']):
        return 'faculty'
    if any(kw in raw for kw in ['staff', 'admin', 'manager', 'coordinator', 'senior scientist']):
        return 'staff'
    if any(kw in raw for kw in ['student', 'grad', 'phd', 'doctoral', 'undergrad', 'postdoc', 'researcher', 'ms ']):
        return 'student'
    if 'usg' in email.lower():
        return 'student_org'
    # Default for USC: assume student in academic contexts
    return 'student'


def push_row(email, name, department, role, source):
    email = email.lower().strip()
    if not email or email in SKIP_EMAILS or '@' not in email:
        return 'skip'
    # Must be USC-related
    if 'usc.edu' not in email:
        return 'skip'

    mapped_role = classify_role(email, role, department)
    if mapped_role not in VALID_ROLES:
        mapped_role = 'student'

    row = {
        "email": email,
        "name": name.strip() if name and name.strip() else None,
        "department": department.strip() if department else None,
        "role": mapped_role,
        "university": "USC",
        "source_url": source if source else None,
        "segment": "student",
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
        if '23514' in body:  # Check constraint violation (role)
            # Try with default role
            row['role'] = 'student'
            data = json.dumps([row]).encode('utf-8')
            req2 = urllib.request.Request(url, data=data, headers=headers, method='POST')
            try:
                urllib.request.urlopen(req2, context=SSL_CTX)
                return 'inserted'
            except:
                return 'error'
        return 'error'


def process_csv(filepath):
    inserted = dupes = skips = errors = 0
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = row.get('email', '').strip()
            name = row.get('name', '').strip()
            dept = row.get('department', row.get('school', row.get('program', row.get('lab_or_affiliation', '')))).strip()
            role = row.get('role', row.get('type', '')).strip()
            source = row.get('source_url', row.get('source', '')).strip()
            
            result = push_row(email, name, dept, role, source)
            if result == 'inserted': inserted += 1
            elif result == 'dupe': dupes += 1
            elif result == 'skip': skips += 1
            else: errors += 1
    return inserted, dupes, skips, errors


def main():
    parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_files = sorted(glob.glob(os.path.join(parent, 'usc_*.csv')))
    
    print(f"Found {len(csv_files)} USC CSV files\n")
    total_inserted = total_dupes = 0
    
    for fp in csv_files:
        fname = os.path.basename(fp)
        inserted, dupes, skips, errors = process_csv(fp)
        total_inserted += inserted
        total_dupes += dupes
        print(f"  {fname:<40} → +{inserted} new, {dupes} dupes, {skips} skips, {errors} errors")
    
    print(f"\n{'='*60}")
    print(f"  Total new: {total_inserted}")
    print(f"  Total dupes: {total_dupes}")
    
    count_headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Prefer": "count=exact"}
    
    req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/college_contacts?select=id&university=eq.USC", headers=count_headers)
    resp = urllib.request.urlopen(req, context=SSL_CTX)
    print(f"\n  Total USC contacts: {resp.getheader('Content-Range')}")
    
    req2 = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/college_contacts?select=id&university=eq.USC&role=eq.student", headers=count_headers)
    resp2 = urllib.request.urlopen(req2, context=SSL_CTX)
    print(f"  USC Students: {resp2.getheader('Content-Range')}")

    # Overall totals
    req3 = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/college_contacts?select=id", headers=count_headers)
    resp3 = urllib.request.urlopen(req3, context=SSL_CTX)
    print(f"\n  GRAND TOTAL (all universities): {resp3.getheader('Content-Range')}")


if __name__ == "__main__":
    main()
