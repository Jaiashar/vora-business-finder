#!/usr/bin/env python3
"""
USC Viterbi Scraper V2 - Extended search for more lab pages and student emails.
Loads existing results from V1 and adds new ones.
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import json
import time
from urllib.parse import urljoin, urlparse

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

# More specific lab/group pages with known student listings
EXTENDED_URLS = [
    # ===== CS Department Labs =====
    ("https://glamor-usc.github.io/people/", "GLAMOR Lab"),
    ("https://glamor-usc.github.io/team/", "GLAMOR Lab"),
    ("https://usc-isi-i2.github.io/people/", "ISI I2 Lab"),
    ("https://ink-usc.github.io/people/", "INK Lab (NLP)"),
    ("https://ink-usc.github.io/members/", "INK Lab (NLP)"),
    ("https://lil.nlp.cornell.edu/people/", "NLP Lab"),
    
    # USC CS labs on GitHub
    ("https://clvrai.github.io/people/", "CLVR Lab"),
    ("https://icaros-usc.github.io/people/", "ICAROS Lab"),
    
    # Direct lab pages
    ("https://slanglab.cs.usc.edu/people/", "SLANG Lab"),
    ("https://slanglab.cs.usc.edu/team/", "SLANG Lab"),
    ("https://rasc.usc.edu/people/", "RASC Lab"),
    ("https://rasc.usc.edu/team/", "RASC Lab"),
    ("https://resl.usc.edu/people/", "RESL Lab"),
    ("https://resl.usc.edu/team/", "RESL Lab"),
    
    # USC ISI labs
    ("https://www.isi.edu/research-groups/", "ISI Research Groups"),
    ("https://www.isi.edu/research/nlg/", "ISI NLG"),
    ("https://www.isi.edu/~ulf/", "ISI Hermjakob Lab"),
    
    # Known CS faculty lab pages
    ("https://stephentu.github.io/", "Stephen Tu Group"),
    ("https://viterbi-web.usc.edu/~swartMDR/people.html", "MDR Lab"),
    ("https://viterbi-web.usc.edu/~shangMDL/people.html", "MDL Lab"),
    
    # ===== EE Department Labs =====
    ("https://cores.ee.usc.edu/people/", "CORES Lab (EE)"),
    ("https://cores.ee.usc.edu/team/", "CORES Lab (EE)"),
    ("https://www.avestimehr.com/people", "Avestimehr Lab"),
    ("https://www.avestimehr.com/team", "Avestimehr Lab"),
    ("https://www.avestimehr.com/members", "Avestimehr Lab"),
    ("https://anrg.usc.edu/www/people/", "ANRG Lab"),
    ("https://anrg.usc.edu/people/", "ANRG Lab"),
    ("https://sportscience.usc.edu/about/people/", "Sports Science Lab"),
    
    # ===== BME Labs =====
    ("https://sites.usc.edu/kllab/people/", "KL Lab (BME)"),
    ("https://sites.usc.edu/kllab/team/", "KL Lab (BME)"),
    ("https://sites.usc.edu/madanipour/people/", "Madanipour Lab"),
    ("https://sites.usc.edu/brainlab/people/", "Brain Lab (BME)"),
    ("https://sites.usc.edu/neuro/people/", "Neuroengineering Lab"),
    ("https://sites.usc.edu/valerolab/people/", "Valero Lab"),
    ("https://sites.usc.edu/valerolab/team/", "Valero Lab"),
    ("https://sites.usc.edu/valerolab/members/", "Valero Lab"),
    ("https://sites.usc.edu/cbml/people/", "CBML Lab"),
    ("https://sites.usc.edu/cbml/team/", "CBML Lab"),
    
    # ===== AME Labs =====
    ("https://sites.usc.edu/wanglab/people/", "Wang Lab (AME)"),
    ("https://sites.usc.edu/wanglab/team/", "Wang Lab (AME)"),
    ("https://sites.usc.edu/tianshou/people/", "Tianshou Lab"),
    ("https://sites.usc.edu/tianshou/team/", "Tianshou Lab"),
    ("https://sites.usc.edu/lusk/people/", "Lusk Lab"),
    ("https://sites.usc.edu/lusk/team/", "Lusk Lab"),
    ("https://sites.usc.edu/combustion/people/", "Combustion Lab"),
    ("https://sites.usc.edu/combustion/team/", "Combustion Lab"),
    ("https://sites.usc.edu/spacecraft/people/", "Spacecraft Lab"),
    ("https://sites.usc.edu/spacecraft/team/", "Spacecraft Lab"),
    ("https://sites.usc.edu/zohar/people/", "Zohar Lab"),
    ("https://sites.usc.edu/zohar/team/", "Zohar Lab"),
    
    # ===== CEE Labs =====
    ("https://sites.usc.edu/abdelzaher/people/", "Abdelzaher Lab (CEE)"),
    ("https://sites.usc.edu/abdelzaher/team/", "Abdelzaher Lab (CEE)"),
    ("https://sites.usc.edu/erturun/people/", "Erturun Lab (CEE)"),
    ("https://sites.usc.edu/erturun/team/", "Erturun Lab (CEE)"),
    
    # ===== ISE Labs =====
    ("https://sites.usc.edu/qed/people/", "QED Lab (ISE)"),
    ("https://sites.usc.edu/qed/team/", "QED Lab (ISE)"),
    
    # ===== General sites.usc.edu labs =====
    ("https://sites.usc.edu/mcl/people/", "MCL Lab"),
    ("https://sites.usc.edu/mcl/team/", "MCL Lab"),
    ("https://sites.usc.edu/robustai/people/", "Robust AI Lab"),
    ("https://sites.usc.edu/robustai/team/", "Robust AI Lab"),
    ("https://sites.usc.edu/prasanna/people/", "Prasanna Lab"),
    ("https://sites.usc.edu/prasanna/team/", "Prasanna Lab"),
    ("https://sites.usc.edu/seelab/people/", "SEE Lab"),
    ("https://sites.usc.edu/seelab/team/", "SEE Lab"),
    
    # ===== Additional known student-heavy pages =====
    ("https://www.cs.usc.edu/people/research-faculty/", "CS Research Faculty"),
    ("https://www.cs.usc.edu/people/", "CS People"),
    ("https://www.cs.usc.edu/faculty-and-research/", "CS Faculty & Research"),
    
    # BME student pages
    ("https://bme.usc.edu/about/people/", "BME People"),
    ("https://bme.usc.edu/directory/", "BME Directory"),
    
    # AME pages
    ("https://ame.usc.edu/about/people/", "AME People"),
    
    # CEE pages
    ("https://cee.usc.edu/about/people/", "CEE People"),
    ("https://cee.usc.edu/directory/", "CEE Directory"),
    
    # ISE pages
    ("https://ise.usc.edu/about/people/", "ISE People"),
    ("https://ise.usc.edu/directory/", "ISE Directory"),
    
    # Viterbi School-wide
    ("https://viterbi.usc.edu/academics/phd/", "Viterbi PhD"),
    ("https://viterbischool.usc.edu/academics/phd/", "Viterbi PhD"),
    
    # Known large lab pages with many students
    ("https://netlab.usc.edu/people/", "NetLab"),
    ("https://netlab.usc.edu/team/", "NetLab"),
    ("https://vgl.ict.usc.edu/Research/", "VGL (ICT)"),
    ("https://ict.usc.edu/about-us/people/", "ICT People"),
    ("https://ict.usc.edu/people/", "ICT"),
    ("https://sail.usc.edu/", "SAIL Lab Homepage"),
    
    # More specific faculty lab pages
    ("https://sites.usc.edu/resl/people/", "RESL Lab"),
    ("https://sites.usc.edu/resl/team/", "RESL Lab"),
    ("https://sites.usc.edu/leana/people/", "Leana Lab"),
    ("https://sites.usc.edu/leana/team/", "Leana Lab"),
    ("https://sites.usc.edu/jihaolab/people/", "Jihao Lab"),
    ("https://sites.usc.edu/jihaolab/team/", "Jihao Lab"),
    ("https://sites.usc.edu/fslab/people/", "FS Lab"),
    ("https://sites.usc.edu/fslab/team/", "FS Lab"),
    
    # USC robotics
    ("https://robotics.usc.edu/interaction/", "Interaction Lab"),
    ("https://robotics.usc.edu/resl/", "RESL"),
    ("https://robotics.usc.edu/", "USC Robotics"),
    
    # Additional research centers
    ("https://csse.usc.edu/people/", "CSSE"),
    ("https://csse.usc.edu/team/", "CSSE"),
    
    # Ming Hsieh sub pages
    ("https://minghsiehece.usc.edu/research/area/communications/", "ECE Communications"),
    ("https://minghsiehece.usc.edu/research/area/signal-image-processing/", "ECE Signal Processing"),
    ("https://minghsiehece.usc.edu/research/area/systems/", "ECE Systems"),
    ("https://minghsiehece.usc.edu/research/area/vlsi-cad-mems/", "ECE VLSI"),
    ("https://minghsiehece.usc.edu/research/area/electrophysics/", "ECE Electrophysics"),
    ("https://minghsiehece.usc.edu/research/area/power-energy/", "ECE Power/Energy"),
]


def extract_usc_emails_from_text(text):
    """Extract all USC email addresses from text."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*usc\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*usc\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    return list(cleaned)


def extract_emails_from_mailto(soup):
    """Extract USC emails from mailto: links."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            email_match = re.search(r'mailto:([\w.+-]+@(?:[\w-]+\.)*usc\.edu)', href, re.IGNORECASE)
            if email_match:
                emails.append(email_match.group(1).lower())
    return emails


def is_admin_email(email):
    """Check if email is an administrative/generic email."""
    admin_patterns = [
        'info@', 'admin@', 'office@', 'dept@', 'webmaster@', 'help@',
        'support@', 'contact@', 'registrar@', 'admissions@',
        'advising@', 'dean@', 'chair@', 'reception@', 'enroll@',
        'gradadm@', 'viterbi@', 'communications@', 'department@',
        'ece.student', 'ece.faculty', 'eceadmin@', 'eepadmin@',
        'vasephd@', 'vasems@', 'sure@vase',
        '.department@', 'services@', 'affairs@',
    ]
    for p in admin_patterns:
        if p in email:
            return True
    if email.startswith('email'):
        return True
    return False


def try_get_name_for_email(soup, email):
    """Try to find a name associated with an email."""
    for a_tag in soup.find_all('a', href=True):
        if email in a_tag.get('href', '').lower():
            parent = a_tag.parent
            for _ in range(5):
                if parent is None:
                    break
                text = parent.get_text(separator=' ', strip=True)
                parts = text.split(email)
                for part in parts:
                    part = part.strip(' ,|-•·\n\t')
                    words = part.split()
                    if 2 <= len(words) <= 5:
                        name_candidate = ' '.join(words[:4])
                        if not any(x in name_candidate.lower() for x in
                                   ['student', 'professor', 'phd', 'email', 'phone',
                                    'address', 'http', 'www', 'lab', 'research',
                                    'department', 'office', 'contact']):
                            return name_candidate
                parent = parent.parent

    email_text_elements = soup.find_all(string=re.compile(re.escape(email), re.IGNORECASE))
    for elem in email_text_elements:
        parent = elem.parent
        for _ in range(5):
            if parent is None:
                break
            name_tags = parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b'])
            for tag in name_tags:
                name = tag.get_text(strip=True)
                if name and not any(x in name.lower() for x in
                                    ['email', 'contact', '@', 'student', 'people', 'phone',
                                     'department', 'office']):
                    return name
            parent = parent.parent

    return ""


def scrape_page(url, department, session):
    """Scrape a single page for USC emails."""
    results = []
    try:
        response = session.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        if response.status_code != 200:
            print(f"  [{response.status_code}] {url}")
            return results

        soup = BeautifulSoup(response.text, 'html.parser')
        page_text = soup.get_text()

        text_emails = extract_usc_emails_from_text(page_text)
        mailto_emails = extract_emails_from_mailto(soup)

        obfuscated = re.findall(r'([\w.+-]+)\s*\[at\]\s*((?:[\w-]+\.)*usc\.edu)', page_text, re.IGNORECASE)
        obfuscated_emails = [f"{m[0]}@{m[1]}".lower() for m in obfuscated]

        script_emails = []
        for script in soup.find_all('script'):
            if script.string:
                script_emails.extend(extract_usc_emails_from_text(script.string))

        all_emails = list(set(text_emails + mailto_emails + obfuscated_emails + script_emails))
        filtered_emails = [e for e in all_emails if not is_admin_email(e)]

        if filtered_emails:
            print(f"  [OK] {url} -> {len(filtered_emails)} emails")

        for email in filtered_emails:
            name = try_get_name_for_email(soup, email)
            results.append({
                'email': email,
                'name': name,
                'department': department,
                'source_url': url
            })

    except requests.exceptions.ConnectionError:
        pass  # Silent for connection errors
    except requests.exceptions.Timeout:
        pass
    except Exception as e:
        print(f"  [ERR] {url}: {e}")

    return results


def load_existing():
    """Load existing emails from V1."""
    existing = set()
    try:
        with open('usc_viterbi_emails.json', 'r') as f:
            data = json.load(f)
            for r in data:
                existing.add(r['email'].lower().strip())
        print(f"Loaded {len(existing)} existing emails from V1")
    except FileNotFoundError:
        print("No V1 data found")
    return existing


def main():
    existing_emails = load_existing()
    all_new = []
    seen_emails = set(existing_emails)
    visited_urls = set()
    session = requests.Session()

    print("\n" + "=" * 70)
    print("USC VITERBI SCRAPER V2 - EXTENDED LAB SEARCH")
    print("=" * 70)

    for url, department in EXTENDED_URLS:
        if url in visited_urls:
            continue
        visited_urls.add(url)

        results = scrape_page(url, department, session)
        for r in results:
            email = r['email'].lower().strip()
            if email not in seen_emails:
                seen_emails.add(email)
                all_new.append(r)
        time.sleep(0.3)

    print(f"\n\nNEW EMAILS FOUND IN V2: {len(all_new)}")

    # Load V1 results and merge
    try:
        with open('usc_viterbi_emails.json', 'r') as f:
            v1_data = json.load(f)
    except FileNotFoundError:
        v1_data = []

    all_results = v1_data + all_new

    # Save combined results
    output_csv = 'usc_viterbi_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in sorted(all_results, key=lambda x: x['department']):
            writer.writerow(r)
    print(f"Saved {len(all_results)} total emails to {output_csv}")

    output_json = 'usc_viterbi_emails.json'
    with open(output_json, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"Also saved to {output_json}")

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY BY DEPARTMENT/SOURCE:")
    print(f"{'='*70}")
    dept_counts = {}
    for r in all_results:
        dept = r['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        print(f"  {dept}: {count} emails")

    # Print NEW emails
    print(f"\n{'='*70}")
    print(f"NEW EMAILS (V2): {len(all_new)}")
    print(f"{'='*70}")
    for r in sorted(all_new, key=lambda x: x['email']):
        name_str = f" ({r['name']})" if r['name'] else ""
        print(f"  {r['email']}{name_str} - {r['department']}")

    print(f"\n\nTOTAL COMBINED: {len(all_results)} emails")

    return all_results


if __name__ == '__main__':
    main()
