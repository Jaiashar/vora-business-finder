#!/usr/bin/env python3
"""
Scrape UCLA IDP and research center student directories for email addresses.
"""

import requests
import re
import time
import json
import csv
import sys
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Flush output immediately
sys.stdout.reconfigure(line_buffering=True)

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

all_emails = []  # list of dicts: {name, email, program, url}

def extract_emails_from_text(text):
    """Extract all @g.ucla.edu and @ucla.edu emails from text."""
    pattern = r'[\w.+-]+@(?:g\.)?ucla\.edu'
    return list(set(re.findall(pattern, text, re.IGNORECASE)))

def fetch_page(url, timeout=8):
    """Fetch a page and return BeautifulSoup object."""
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, 'html.parser')
    except Exception as e:
        print(f"  Error fetching {url}: {e}")
        return None

def fetch_text(url, timeout=8):
    """Fetch a page and return raw text."""
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        return None

# ============================================================
# 1. MBI/MBIDP - Scrape individual student profile pages
# ============================================================
print("=" * 60)
print("1. MBI/MBIDP - Molecular Biology Institute")
print("=" * 60)

mbi_base = "https://www.mbi.ucla.edu/mbidp/current-students"
print(f"  Fetching {mbi_base}...")
soup = fetch_page(mbi_base, timeout=15)
mbi_student_urls = []
if soup:
    for link in soup.find_all('a', href=True):
        href = link['href']
        if '/archives/students/' in href:
            full_url = urljoin(mbi_base, href)
            if full_url not in mbi_student_urls:
                mbi_student_urls.append(full_url)
    print(f"  Found {len(mbi_student_urls)} MBI student profile links")
    
    for i, url in enumerate(mbi_student_urls):
        try:
            text = fetch_text(url, timeout=8)
            if not text:
                continue
            emails = extract_emails_from_text(text)
            
            s = BeautifulSoup(text, 'html.parser')
            h1 = s.find('h1')
            name = h1.get_text(strip=True) if h1 else url.rstrip('/').split('/')[-1].replace('-', ' ').title()
            
            for email in emails:
                if email.lower() in ['mbigrad@lifesci.ucla.edu', 'mbiasst@mednet.ucla.edu']:
                    continue
                all_emails.append({
                    'name': name,
                    'email': email.lower(),
                    'program': 'MBI/MBIDP',
                    'source_url': url
                })
                print(f"  [{i+1}/{len(mbi_student_urls)}] {name}: {email}")
            
            if not emails and (i+1) % 50 == 0:
                print(f"  [{i+1}/{len(mbi_student_urls)}] processing...")
            
            time.sleep(0.2)
        except Exception as e:
            print(f"  Error on {url}: {e}")
else:
    print("  Could not fetch MBI student listing")

# ============================================================
# 2. UCLA Communication Department (confirmed emails present)
# ============================================================
print("\n" + "=" * 60)
print("2. UCLA Communication Department")
print("=" * 60)

comm_url = "https://comm.ucla.edu/people/graduate-students/"
print(f"  Fetching {comm_url}...")
soup = fetch_page(comm_url)
if soup:
    full_text = soup.get_text()
    page_emails = extract_emails_from_text(full_text)
    
    # Try to pair names with emails from the structured content
    for email in page_emails:
        all_emails.append({
            'name': 'Unknown',
            'email': email.lower(),
            'program': 'Communication',
            'source_url': comm_url
        })
        print(f"  Found: {email}")
    
    if not page_emails:
        print("  No emails found")

# Hard-coded Communication emails from manual fetch (confirmed present)
comm_emails_confirmed = [
    ('Elias Acevedo', 'eacevedo89@g.ucla.edu'),
    ('Gulsah Akcakir', 'gakcakir@ucla.edu'),
    ('Constance Bainbridge', 'cbainbridge@g.ucla.edu'),
    ('Mia Carbone', 'miacarbone@g.ucla.edu'),
    ('Je Hoon Chae', 'chae@g.ucla.edu'),
    ('Abhinanda Dash', 'abhinandadash99@g.ucla.edu'),
    ('Siyi Gong', 'siyi.gong@ucla.edu'),
    ('Pooriya Jamie', 'pjamie@ucla.edu'),
    ('Joyce Yanru Jiang', 'yanrujiang@g.ucla.edu'),
    ('Jennifer Jiyoung Hwang', 'jiyhwang@g.ucla.edu'),
    ('Prianka Koya', 'priankakoya@g.ucla.edu'),
    ('Catherine Lacsamana', 'clacsama8@g.ucla.edu'),
    ('Lin Lin', 'llin001@g.ucla.edu'),
    ('Jeffrey Mai', 'maijiahao@g.ucla.edu'),
    ('Grace Qiyuan Miao', 'q.miao@ucla.edu'),
    ('Seonhye Noh', 'shnoh@g.ucla.edu'),
    ('Zachary Rosen', 'z.p.rosen@ucla.edu'),
    ('Gabriella Skollar', 'gabiskollar@g.ucla.edu'),
    ('Yingjia Wan', 'alisawan@g.ucla.edu'),
]

existing_comm = {e['email'] for e in all_emails if e['program'] == 'Communication'}
for name, email in comm_emails_confirmed:
    if email.lower() not in existing_comm:
        all_emails.append({
            'name': name,
            'email': email.lower(),
            'program': 'Communication',
            'source_url': comm_url
        })
        existing_comm.add(email.lower())

# ============================================================
# 3. Neuroscience IDP - Check individual profiles
# ============================================================
print("\n" + "=" * 60)
print("3. Neuroscience IDP (NSIDP)")
print("=" * 60)

nsidp_url = "https://www.neuroscience.ucla.edu/current-students"
print(f"  Fetching {nsidp_url}...")
soup = fetch_page(nsidp_url, timeout=15)
nsidp_student_urls = []
if soup:
    for link in soup.find_all('a', href=True):
        href = link['href']
        if '/student/' in href:
            full_url = urljoin(nsidp_url, href)
            if full_url not in nsidp_student_urls:
                nsidp_student_urls.append(full_url)
    print(f"  Found {len(nsidp_student_urls)} NSIDP student profile links")
    
    # Sample some profiles to check for emails
    for i, url in enumerate(nsidp_student_urls[:10]):
        try:
            text = fetch_text(url, timeout=8)
            if text:
                emails = extract_emails_from_text(text)
                for email in emails:
                    s = BeautifulSoup(text, 'html.parser')
                    h1 = s.find('h1')
                    name = h1.get_text(strip=True) if h1 else 'Unknown'
                    all_emails.append({
                        'name': name,
                        'email': email.lower(),
                        'program': 'Neuroscience IDP',
                        'source_url': url
                    })
                    print(f"  {name}: {email}")
            time.sleep(0.2)
        except:
            pass
    
    if not any(e['program'] == 'Neuroscience IDP' for e in all_emails):
        print("  No emails on student profiles (profiles only show name + lab)")

# ============================================================
# 4-12. Try all other UCLA pages with shorter timeouts
# ============================================================
print("\n" + "=" * 60)
print("4-12. Other UCLA department/center pages")
print("=" * 60)

other_urls = [
    ('https://bioinformatics.ucla.edu/people/students/', 'Bioinformatics IDP'),
    ('https://biomath.ucla.edu/people/students/', 'Biomathematics'),
    ('https://biomath.ucla.edu/people/', 'Biomathematics'),
    ('https://www.biostat.ucla.edu/people/students/', 'Biostatistics'),
    ('https://ph.ucla.edu/departments/biostatistics', 'Biostatistics'),
    ('https://mimg.ucla.edu/people/search', 'MIMG'),
    ('https://www.cmsi.ucla.edu/people/', 'CMSI'),
    ('https://www.bri.ucla.edu/people', 'BRI'),
    ('https://www.bri.ucla.edu/research/people', 'BRI'),
    ('https://www.jccc.ucla.edu/people/', 'JCCC'),
    ('https://ctsi.ucla.edu/people/', 'CTSI'),
    ('https://www.cresst.org/about/staff/', 'CRESST'),
    ('https://www.cens.ucla.edu/people', 'CENS'),
    ('https://mstp.healthsciences.ucla.edu/students/', 'MSTP'),
    ('https://www.chemistry.ucla.edu/research/student-directory/', 'Chemistry'),
    ('https://statistics.ucla.edu/index.php/people1/active-graduate-students/', 'Statistics'),
    ('https://statistics.ucla.edu/index.php/people1/active-graduate-students/ph-d-students/', 'Statistics PhD'),
    ('https://www.ioes.ucla.edu/people/', 'IoES'),
    ('https://www.genetics.ucla.edu/people/graduate-students/', 'Human Genetics'),
]

for url, program in other_urls:
    print(f"\n  Trying {program}: {url}")
    soup = fetch_page(url, timeout=10)
    if soup:
        text = soup.get_text()
        emails = extract_emails_from_text(text)
        for email in emails:
            all_emails.append({
                'name': 'Unknown',
                'email': email.lower(),
                'program': program,
                'source_url': url
            })
            print(f"    Found: {email}")
        if not emails:
            print(f"    No emails found")
        
        # Also check for links to student profiles
        student_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if any(x in href.lower() for x in ['/student/', '/people/', '/person/']):
                full = urljoin(url, href)
                if full != url and full not in student_links:
                    student_links.append(full)
        
        if student_links and not emails:
            print(f"    Found {len(student_links)} sub-links, checking first few...")
            for sub_url in student_links[:5]:
                try:
                    text = fetch_text(sub_url, timeout=8)
                    if text:
                        sub_emails = extract_emails_from_text(text)
                        for email in sub_emails:
                            s = BeautifulSoup(text, 'html.parser')
                            h1 = s.find('h1')
                            name = h1.get_text(strip=True) if h1 else 'Unknown'
                            all_emails.append({
                                'name': name,
                                'email': email.lower(),
                                'program': program,
                                'source_url': sub_url
                            })
                            print(f"    {name}: {email}")
                    time.sleep(0.2)
                except:
                    pass

# ============================================================
# De-duplicate and save results
# ============================================================
print("\n" + "=" * 60)
print("RESULTS SUMMARY")
print("=" * 60)

# De-duplicate by email
seen_emails = {}
for entry in all_emails:
    email = entry['email'].lower()
    if email not in seen_emails:
        seen_emails[email] = entry

unique_entries = list(seen_emails.values())

# Save to JSON
with open('ucla_idp_emails.json', 'w') as f:
    json.dump(unique_entries, f, indent=2)

# Save to CSV
with open('ucla_idp_emails.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['name', 'email', 'program', 'source_url'])
    writer.writeheader()
    writer.writerows(unique_entries)

# Print summary
programs = {}
for entry in unique_entries:
    prog = entry['program']
    programs[prog] = programs.get(prog, 0) + 1

print(f"\nTotal unique emails: {len(unique_entries)}")
print("\nBy program:")
for prog, count in sorted(programs.items(), key=lambda x: -x[1]):
    print(f"  {prog}: {count}")

print("\nAll emails:")
for entry in sorted(unique_entries, key=lambda x: (x['program'], x['name'])):
    print(f"  [{entry['program']}] {entry['name']}: {entry['email']}")

print(f"\nSaved to ucla_idp_emails.json and ucla_idp_emails.csv")
