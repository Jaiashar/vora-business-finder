#!/usr/bin/env python3
"""Push CSV files for any university to Supabase college_contacts table."""

import csv
import json
import urllib.request
import urllib.error
import ssl
import os
import sys
import glob
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

VALID_ROLES = {'student', 'staff', 'faculty', 'coach', 'student_org', 'department', 'director', 'coordinator'}

# Map file prefix to (university name, email domain)
SCHOOL_MAP = {
    'ucla_': ('UCLA', 'ucla.edu'),
    'usc_': ('USC', 'usc.edu'),
    'stanford_': ('Stanford', 'stanford.edu'),
    'berkeley_': ('UC Berkeley', 'berkeley.edu'),
    'michigan_': ('University of Michigan', 'umich.edu'),
    'uf_': ('University of Florida', 'ufl.edu'),
    'texas_': ('UT Austin', 'utexas.edu'),
    'osu_': ('Ohio State', 'osu.edu'),
    'uga_': ('University of Georgia', 'uga.edu'),
    'unc_': ('UNC Chapel Hill', 'unc.edu'),
    'duke_': ('Duke', 'duke.edu'),
    'notredame_': ('Notre Dame', 'nd.edu'),
    'oregon_': ('University of Oregon', 'uoregon.edu'),
    'lsu_': ('LSU', 'lsu.edu'),
    'bama_': ('University of Alabama', 'ua.edu'),
    'penn_state_': ('Penn State', 'psu.edu'),
    'fsu_': ('Florida State', 'fsu.edu'),
    'auburn_': ('Auburn', 'auburn.edu'),
    'clemson_': ('Clemson', 'clemson.edu'),
    'wisconsin_': ('Wisconsin', 'wisc.edu'),
    'iowa_': ('University of Iowa', 'uiowa.edu'),
    'msu_': ('Michigan State', 'msu.edu'),
    'tamu_': ('Texas A&M', 'tamu.edu'),
    'tennessee_': ('University of Tennessee', 'utk.edu'),
    'oklahoma_': ('University of Oklahoma', 'ou.edu'),
    'gatech_': ('Georgia Tech', 'gatech.edu'),
    'vt_': ('Virginia Tech', 'vt.edu'),
    'mit_': ('MIT', 'mit.edu'),
    'purdue_': ('Purdue', 'purdue.edu'),
    'northwestern_': ('Northwestern', 'northwestern.edu'),
    'asu_': ('Arizona State', 'asu.edu'),
    'uw_': ('University of Washington', 'uw.edu'),
    'colorado_': ('University of Colorado', 'colorado.edu'),
    'cornell_': ('Cornell', 'cornell.edu'),
    'columbia_': ('Columbia', 'columbia.edu'),
    'harvard_': ('Harvard', 'harvard.edu'),
    'jhu_': ('Johns Hopkins', 'jhu.edu'),
    'cmu_': ('Carnegie Mellon', 'cmu.edu'),
    'yale_': ('Yale', 'yale.edu'),
    'minnesota_': ('University of Minnesota', 'umn.edu'),
    'illinois_': ('University of Illinois', 'illinois.edu'),
    'indiana_': ('Indiana University', 'indiana.edu'),
    'vanderbilt_': ('Vanderbilt', 'vanderbilt.edu'),
    'olemiss_': ('Ole Miss', 'olemiss.edu'),
    'arkansas_': ('University of Arkansas', 'uark.edu'),
    'kentucky_': ('University of Kentucky', 'uky.edu'),
    'sc_': ('University of South Carolina', 'sc.edu'),
    'miami_': ('University of Miami', 'miami.edu'),
}

SKIP_GENERIC = {'info@', 'webmaster@', 'communications@', 'admission@', 'contact@', 'help@', 'support@', 'noreply@'}


def detect_school(filename):
    fname = os.path.basename(filename).lower()
    for prefix, (uni, domain) in SCHOOL_MAP.items():
        if fname.startswith(prefix):
            return uni, domain
    return None, None


def classify_role(email, raw_role='', dept=''):
    raw = (raw_role + ' ' + dept).lower()
    if any(kw in raw for kw in ['faculty', 'professor', 'principal investigator']):
        return 'faculty'
    if any(kw in raw for kw in ['coach', 'coaching']):
        return 'coach'
    if any(kw in raw for kw in ['staff', 'admin', 'manager', 'coordinator', 'senior scientist', 'director']):
        return 'staff'
    if any(kw in raw for kw in ['student org', 'assu', 'asuc', 'usg', 'gsg', 'student government']):
        return 'student_org'
    if any(kw in raw for kw in ['student', 'grad', 'phd', 'doctoral', 'undergrad', 'postdoc', 'researcher', 'ms ']):
        return 'student'
    # Default
    return 'student'


def push_row(email, name, department, role, source, university):
    email = email.lower().strip()
    if not email or '@' not in email:
        return 'skip'
    # Skip generic emails
    for skip in SKIP_GENERIC:
        if email.startswith(skip):
            return 'skip'
    # Skip non-edu emails
    if '.edu' not in email:
        return 'skip'

    mapped_role = classify_role(email, role, department)
    if mapped_role not in VALID_ROLES:
        mapped_role = 'student'

    # Clean name field
    clean_name = name.strip() if name and name.strip() else None
    if clean_name and len(clean_name) > 200:
        clean_name = clean_name[:200]

    row = {
        "email": email,
        "name": clean_name,
        "department": department.strip()[:200] if department and department.strip() else None,
        "role": mapped_role,
        "university": university,
        "source_url": source[:500] if source else None,
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

    for attempt in range(3):
        try:
            req = urllib.request.Request(url, data=data, headers=headers, method='POST')
            urllib.request.urlopen(req, context=SSL_CTX)
            return 'inserted'
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            if 'duplicate' in body.lower() or '23505' in body:
                return 'dupe'
            if '23514' in body:
                row['role'] = 'student'
                data = json.dumps([row]).encode('utf-8')
                continue
            return 'error'
        except (urllib.error.URLError, OSError):
            import time
            time.sleep(2 * (attempt + 1))
            continue
    return 'error'


def process_csv(filepath, university):
    inserted = dupes = skips = errors = 0
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = row.get('email', '').strip()
            name = row.get('name', '').strip()
            dept = (row.get('department', '') or row.get('school', '') or row.get('program', '') or row.get('lab_or_affiliation', '')).strip()
            role = (row.get('role', '') or row.get('type', '')).strip()
            source = (row.get('source_url', '') or row.get('source', '')).strip()

            result = push_row(email, name, dept, role, source, university)
            if result == 'inserted': inserted += 1
            elif result == 'dupe': dupes += 1
            elif result == 'skip': skips += 1
            else: errors += 1
    return inserted, dupes, skips, errors


def get_count(filter_str=''):
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Prefer": "count=exact"}
    url = f"{SUPABASE_URL}/rest/v1/college_contacts?select=id{filter_str}"
    req = urllib.request.Request(url, headers=headers)
    resp = urllib.request.urlopen(req, context=SSL_CTX)
    cr = resp.getheader('Content-Range')
    return cr.split('/')[-1] if cr else '?'


def main():
    if len(sys.argv) < 2:
        print("Usage: python push_schools.py <prefix>  (e.g., stanford_, berkeley_)")
        sys.exit(1)

    prefix = sys.argv[1]
    university, domain = None, None
    for p, (u, d) in SCHOOL_MAP.items():
        if p == prefix or p.rstrip('_') == prefix.rstrip('_'):
            university, domain = u, d
            prefix = p
            break

    if not university:
        print(f"Unknown school prefix: {prefix}")
        sys.exit(1)

    parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_files = sorted(glob.glob(os.path.join(parent, f'{prefix}*.csv')))

    print(f"Pushing {university} data ({len(csv_files)} CSV files)\n")
    total_inserted = total_dupes = 0

    for fp in csv_files:
        fname = os.path.basename(fp)
        inserted, dupes, skips, errors = process_csv(fp, university)
        total_inserted += inserted
        total_dupes += dupes
        print(f"  {fname:<45} → +{inserted} new, {dupes} dupes, {skips} skips, {errors} errors")

    print(f"\n{'='*65}")
    print(f"  Total new {university}: {total_inserted}")
    print(f"  Total dupes: {total_dupes}")

    # Counts
    uni_filter = f"&university=eq.{urllib.parse.quote(university)}"
    print(f"\n  {university} total: {get_count(uni_filter)}")
    print(f"  {university} students: {get_count(uni_filter + '&role=eq.student')}")
    print(f"\n  GRAND TOTAL (all): {get_count()}")


import urllib.parse

if __name__ == "__main__":
    main()
