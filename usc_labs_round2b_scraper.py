#!/usr/bin/env python3
"""
USC Research Labs Round 2B - Targeted scraping of specific discovered lab pages.
Uses curl for reliability. Merges with round2 results.
"""

import subprocess
from bs4 import BeautifulSoup
import re
import csv
import json
import time


def log(msg):
    print(msg, flush=True)


def extract_usc_emails(text):
    """Extract all @usc.edu and @med.usc.edu emails."""
    pattern = r'[\w.+-]+@(?:[\w-]+\.)*usc\.edu'
    emails = re.findall(pattern, text, re.IGNORECASE)
    cleaned = set()
    for e in emails:
        e = e.lower().strip()
        # Fix prefix artifacts (e.g., "chairliang.chen@usc.edu")
        # Look for common prefix words that shouldn't be part of emails
        prefixes_to_strip = [
            'chair', 'committee', 'manager', 'studies', 'director',
            'professor', 'associate', 'assistant', 'program', 'faculty',
            'student', 'graduate', 'research', 'senior', 'junior',
            'staff', 'chief', 'head', 'lab', 'group', 'center',
        ]
        for prefix in prefixes_to_strip:
            if e.startswith(prefix) and not e.startswith(prefix + '@'):
                # Check if removing the prefix gives a valid email
                stripped = e[len(prefix):]
                if re.match(r'^[a-z][\w.+-]*@', stripped):
                    e = stripped
                    break
        
        # Fix phone-number prefix artifacts
        match = re.match(r'(?:\d[\d-]*\d)?([a-zA-Z][\w.+-]*@(?:[\w-]+\.)*usc\.edu)', e)
        if match:
            cleaned.add(match.group(1).lower())
        else:
            cleaned.add(e)
    
    # Also find [at] obfuscated emails
    obfuscated = re.findall(r'([\w.+-]+)\s*\[at\]\s*((?:[\w-]+\.)*usc\.edu)', text, re.IGNORECASE)
    for m in obfuscated:
        cleaned.add(f"{m[0]}@{m[1]}".lower())
    
    # Also find _at_ and (at) patterns
    obf2 = re.findall(r'([\w.+-]+)\s*(?:\(at\)|_at_)\s*((?:[\w-]+\.)*usc\.edu)', text, re.IGNORECASE)
    for m in obf2:
        cleaned.add(f"{m[0]}@{m[1]}".lower())
    
    return list(cleaned)


def extract_mailto_emails(soup):
    """Extract USC emails from mailto: links, handling HTML entity obfuscation."""
    emails = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if 'mailto:' in href.lower():
            match = re.search(r'mailto:\s*([\w.+-]+@(?:[\w-]+\.)*usc\.edu)', href, re.IGNORECASE)
            if match:
                emails.append(match.group(1).lower().strip())
    return list(set(emails))


def is_admin_email(email):
    """Filter out administrative/generic emails."""
    admin_patterns = [
        'info@', 'admin@', 'office@', 'dept@', 'webmaster@', 'help@',
        'support@', 'contact@', 'registrar@', 'admissions@',
        'advising@', 'dean@', 'chair@', 'reception@', 'enroll@',
        'gradadm@', 'viterbi@', 'communications@', 'department@',
        '.department@', 'services@', 'affairs@',
        'chemmail@', 'chemgrad@', 'english@', 'philosophy@',
        'physics@', 'poir@', 'dornsife@', 'graduate@',
        'news@', 'events@', 'pr@', 'media@', 'marketing@',
        'library@', 'alumni@', 'development@', 'giving@',
        'keck@', 'research@', 'grants@', 'web@', 'gero@',
        'uscnews@', 'dornsife.communications',
        'pharmacy@', 'pharmacyschool@', 'annenberg@',
        'dworakpeck@', 'price@', 'marshall@',
        'stemcell@', 'norris@', 'cancer@',
        'bci@', 'dni@', 'earthsci@', 'studenthealth@',
        'uschr@', 'dpsrecords@', 'pharmcom@',
        'hr@', 'jobs@', 'hiring@', 'career@',
        'safety@', 'security@', 'emergency@',
        'it@', 'tech@', 'helpdesk@',
        'provost@', 'president@', 'chancellor@',
        'keckfa@', 'admission',
    ]
    email_lower = email.lower()
    for p in admin_patterns:
        if p in email_lower:
            return True
    if email_lower.startswith('email'):
        return True
    local = email_lower.split('@')[0]
    if len(local) <= 3 and '.' not in local:
        return True
    return False


def try_get_name_for_email(soup, email):
    """Try to find name for an email."""
    # Strategy 1: mailto link parent
    for a_tag in soup.find_all('a', href=True):
        if email in a_tag.get('href', '').lower():
            parent = a_tag.parent
            for _ in range(6):
                if parent is None:
                    break
                for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b']):
                    name = tag.get_text(strip=True)
                    if name and '@' not in name and 3 < len(name) < 60:
                        words = name.split()
                        if 2 <= len(words) <= 5:
                            if not any(x in name.lower() for x in
                                       ['email', 'contact', 'student', 'people', 'phone',
                                        'department', 'office', 'lab', 'member', 'research',
                                        'about', 'home', 'principal', 'investigator']):
                                return name
                parent = parent.parent

    # Strategy 2: text near email
    email_elems = soup.find_all(string=re.compile(re.escape(email), re.IGNORECASE))
    for elem in email_elems:
        parent = elem.parent
        for _ in range(6):
            if parent is None:
                break
            for tag in parent.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'span']):
                name = tag.get_text(strip=True)
                if name and '@' not in name and 3 < len(name) < 60:
                    words = name.split()
                    if 2 <= len(words) <= 5:
                        if not any(x in name.lower() for x in
                                   ['email', 'contact', 'student', 'people', 'phone',
                                    'department', 'office', 'lab', 'member', 'research']):
                            return name
            parent = parent.parent
    
    return ""


def fetch_with_curl(url):
    """Fetch URL using curl."""
    try:
        result = subprocess.run(
            ['curl', '-s', '-L', '-m', '15',
             '-H', 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
             '-H', 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
             url],
            capture_output=True, text=True, timeout=20
        )
        if result.returncode == 0 and result.stdout and len(result.stdout) > 200:
            return result.stdout
    except:
        pass
    return None


def scrape_page(url, department):
    """Scrape a page for USC emails using curl."""
    results = []
    log(f"  Scraping: {url}")
    
    html = fetch_with_curl(url)
    if not html:
        log(f"    No content")
        return results
    
    soup = BeautifulSoup(html, 'html.parser')
    page_text = soup.get_text()
    
    text_emails = extract_usc_emails(page_text)
    mailto_emails = extract_mailto_emails(soup)
    
    # Also check raw HTML for obfuscated mailto
    raw_emails = extract_usc_emails(html)
    
    all_emails = list(set(text_emails + mailto_emails + raw_emails))
    filtered = [e for e in all_emails if not is_admin_email(e)]
    
    if filtered:
        log(f"    Found {len(filtered)} emails")
    
    for email in filtered:
        name = try_get_name_for_email(soup, email)
        results.append({
            'email': email,
            'name': name,
            'department': department,
            'source_url': url,
        })
    
    return results


def main():
    all_results = []
    seen_emails = set()
    visited_urls = set()
    
    def add_results(results):
        count = 0
        for r in results:
            email = r['email'].lower().strip()
            if email and email not in seen_emails:
                seen_emails.add(email)
                all_results.append(r)
                count += 1
        return count
    
    log("=" * 70)
    log("USC RESEARCH LABS ROUND 2B - TARGETED LAB SCRAPER")
    log("=" * 70)
    
    # Specific lab pages discovered via Google search
    targeted_labs = [
        # ===== BIOLOGY / STEM CELL / KECK LABS =====
        ("https://jadhavlab.usc.edu/people/", "Keck - Jadhav Lab (Stem Cell/GI)"),
        ("https://ronglulab.usc.edu/people/", "Keck - Rong Lu Lab (Stem Cell)"),
        ("https://ronglulab.usc.edu/lab-members/", "Keck - Rong Lu Lab (Stem Cell)"),
        ("https://mrnadisplay.usc.edu/current-members/", "Chemistry - Roberts Lab (mRNA Display)"),
        ("https://quadratolab.usc.edu/people/", "Keck - Quadrato Lab (Brain Organoids)"),
        ("https://katritchlab.usc.edu/people.html", "Biology - Katritch Lab (GPCR)"),
        ("https://katritch.usc.edu/people.html", "Biology - Katritch Lab (GPCR)"),
        ("https://csbl.usc.edu/people/", "BME - Computational Systems Bio Lab"),
        ("https://csbl.usc.edu/contact/", "BME - Computational Systems Bio Lab"),
        ("https://nseip.usc.edu/people/", "ECE - NSEIP Lab (Shanechi)"),
        
        # ===== GERONTOLOGY LABS =====
        ("https://gero.usc.edu/labs/lifecoglab/people/", "Gerontology - Lifespan Cognition Lab"),
        ("https://gero.usc.edu/labs/matherlab/people/", "Gerontology - Mather Emotion Lab"),
        ("https://gero.usc.edu/labs/matherlab/contact/", "Gerontology - Mather Emotion Lab"),
        
        # ===== KECK STEM CELL =====
        ("https://stemcell.keck.usc.edu/directory/", "Keck - Stem Cell Directory"),
        ("https://stemcell.keck.usc.edu/", "Keck - Stem Cell"),
        
        # ===== MORE LABS FROM SEARCH =====
        ("https://inklab.usc.edu/contact.html", "CS - INK Research Lab"),
        ("https://inklab.usc.edu/", "CS - INK Research Lab"),
        ("https://inklab.usc.edu/people/", "CS - INK Research Lab"),
        ("https://wugroup.usc.edu/", "Physics/EE - Wu Research Group"),
        ("https://wugroup.usc.edu/people/", "Physics/EE - Wu Research Group"),
        ("https://wugroup.usc.edu/members/", "Physics/EE - Wu Research Group"),
        ("https://grahamlab.usc.edu/members/", "ChemE - Graham Lab"),
        ("https://grahamlab.usc.edu/", "ChemE - Graham Lab"),
        ("https://macleanlab.usc.edu/contact/", "MacLean Lab"),
        ("https://macleanlab.usc.edu/people/", "MacLean Lab"),
        ("https://macleanlab.usc.edu/", "MacLean Lab"),
        
        # ===== sites.usc.edu LABS WITH LIKELY EMAILS =====
        ("https://sites.usc.edu/coganlab/", "Pharmacology - Cogan Lab"),
        ("https://sites.usc.edu/coganlab/people/", "Pharmacology - Cogan Lab"),
        ("https://sites.usc.edu/coganlab/members/", "Pharmacology - Cogan Lab"),
        ("https://sites.usc.edu/ligroup/", "Li Research Group"),
        ("https://sites.usc.edu/ligroup/people/", "Li Research Group"),
        ("https://sites.usc.edu/ligroup/members/", "Li Research Group"),
        ("https://sites.usc.edu/mousavi/current-members/", "BME - MAD Lab (Mousavi)"),
        ("https://sites.usc.edu/mousavi/", "BME - MAD Lab (Mousavi)"),
        ("https://sites.usc.edu/ampl/people/", "Applied Movement & Pain Lab"),
        ("https://sites.usc.edu/zhanglab/lab-members-2/", "Zhang Lab (Neuroscience)"),
        ("https://sites.usc.edu/hti/people/", "Human Technology Interaction Lab"),
        ("https://sites.usc.edu/ldrlab/members/", "Learning Dev & Rehab Lab"),
        ("https://sites.usc.edu/duncanlab/staff/", "Duncan Lab (Neuro)"),
        ("https://sites.usc.edu/duncanlab/", "Duncan Lab (Neuro)"),
        ("https://sites.usc.edu/pricemanlab/", "Priceman Lab (Cancer Immunotherapy)"),
        ("https://sites.usc.edu/pricemanlab/people/", "Priceman Lab (Cancer Immunotherapy)"),
        ("https://sites.usc.edu/pricemanlab/members/", "Priceman Lab (Cancer Immunotherapy)"),
        
        # ===== NEUROSCIENCE GRAD PROGRAM =====
        ("https://ngp.usc.edu/research-faculty/directory-list-2/", "Neuroscience Grad Program - Faculty"),
        
        # ===== MORE SPECIFIC LAB SUBDOMAIN SITES =====
        ("https://bonaguidilab.usc.edu/people/", "Keck - Bonaguidi Lab (Stem Cell)"),
        ("https://bonaguidilab.usc.edu/", "Keck - Bonaguidi Lab (Stem Cell)"),
        ("https://cannonlab.usc.edu/people/", "Keck - Cannon Lab"),
        ("https://cannonlab.usc.edu/", "Keck - Cannon Lab"),
        ("https://mcmahonlab.usc.edu/people/", "Keck - McMahon Lab"),
        ("https://mcmahonlab.usc.edu/", "Keck - McMahon Lab"),
        ("https://humayunlab.usc.edu/people/", "Keck - Humayun Lab (Ophthalmology)"),
        ("https://humayunlab.usc.edu/", "Keck - Humayun Lab (Ophthalmology)"),
        
        # ===== ADDITIONAL KNOWN LABS =====
        ("https://vohradisplay.usc.edu/people/", "Vohra Lab"),
        ("https://sites.usc.edu/bonaguidilab/people/", "Bonaguidi Lab"),
        ("https://sites.usc.edu/bonaguidilab/", "Bonaguidi Lab"),
        ("https://sites.usc.edu/mcmahonlab/people/", "McMahon Lab"),
        ("https://sites.usc.edu/mcmahonlab/", "McMahon Lab"),
        
        # ===== ADDITIONAL DORNSIFE LABS =====
        ("https://dornsife.usc.edu/labs/", "Dornsife Labs Index"),
        ("https://dornsife.usc.edu/qcb/faculty/", "QCB Faculty"),
        ("https://dornsife.usc.edu/qcb/graduate-students/", "QCB Graduate Students"),
        
        # ===== Keck department faculty directories =====
        ("https://keck.usc.edu/preventive-medicine/directory/", "Keck - Prev Med Directory"),
        ("https://keck.usc.edu/biochemistry-and-molecular-medicine/directory/", "Keck - Biochem Directory"),
        ("https://keck.usc.edu/physiology-and-neuroscience/directory/", "Keck - Physiology Directory"),
        ("https://keck.usc.edu/pathology/directory/", "Keck - Pathology Directory"),
    ]
    
    for url, dept in targeted_labs:
        if url in visited_urls:
            continue
        visited_urls.add(url)
        
        results = scrape_page(url, dept)
        n = add_results(results)
        if n > 0:
            log(f"    => {n} new emails")
        time.sleep(0.5)
    
    log(f"\nTotal from targeted scraping: {len(all_results)} unique emails")
    
    # ================================================================
    # Load existing results from round 2 and previous scrapers
    # ================================================================
    log("\n\nMERGING WITH PREVIOUS RESULTS")
    log("=" * 70)
    
    existing_emails = set()
    existing_files = [
        'usc_viterbi_emails.csv',
        'usc_dornsife_grad_emails.csv',
        'usc_pro_school_emails.csv',
        'usc_emails.csv',
        'usc_labs_round2_emails.csv',
    ]
    
    for fname in existing_files:
        try:
            with open(fname, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('email'):
                        existing_emails.add(row['email'].lower().strip())
            log(f"  Loaded from {fname}")
        except FileNotFoundError:
            pass
    
    log(f"  Total existing emails: {len(existing_emails)}")
    
    # Also load round2 results to merge
    round2_results = []
    try:
        with open('usc_labs_round2_emails.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                round2_results.append(row)
    except:
        pass
    
    # Filter new results
    new_only = [r for r in all_results if r['email'].lower().strip() not in existing_emails]
    log(f"  New emails from this round: {len(new_only)}")
    
    # Combine round2 + new results
    combined = list(round2_results)
    combined_emails = set(r['email'].lower().strip() for r in combined if r.get('email'))
    
    for r in new_only:
        email = r['email'].lower().strip()
        if email not in combined_emails:
            combined.append(r)
            combined_emails.add(email)
    
    # Also clean up the combined results - fix known artifacts
    cleaned_combined = []
    for r in combined:
        email = r['email'].lower().strip()
        
        # Fix known prefix artifacts
        prefixes = ['chair', 'committee', 'manager', 'studies', 'director', 
                     'professor', 'associate', 'program', 'faculty']
        for p in prefixes:
            if email.startswith(p) and not email.startswith(p + '@'):
                stripped = email[len(p):]
                if re.match(r'^[a-z][\w.+-]*@', stripped):
                    email = stripped
                    r['email'] = email
                    break
        
        # Skip admin-like emails
        if is_admin_email(email):
            continue
        
        # Skip if name contains ")" or odd artifacts
        if r.get('name'):
            r['name'] = r['name'].strip()
            if r['name'].startswith(')') or r['name'].startswith('('):
                r['name'] = ''
            # Clean up "Staff Graduate and Undergraduate Advisor" etc
            if any(x in r['name'].lower() for x in ['staff', 'advisor', 'division chief', 
                                                       'associate chief', 'equal opportunity',
                                                       'tell us', 'for assistance']):
                r['name'] = ''
        
        cleaned_combined.append(r)
    
    # Save final combined CSV
    output_csv = 'usc_labs_round2_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in sorted(cleaned_combined, key=lambda x: x.get('department', '')):
            writer.writerow(r)
    log(f"\nSaved {len(cleaned_combined)} emails to {output_csv}")
    
    output_json = 'usc_labs_round2_emails.json'
    with open(output_json, 'w') as f:
        json.dump(cleaned_combined, f, indent=2)
    log(f"Also saved to {output_json}")
    
    # Summary
    log(f"\n{'='*70}")
    log("SUMMARY BY DEPARTMENT/LAB:")
    log(f"{'='*70}")
    dept_counts = {}
    for r in cleaned_combined:
        dept = r.get('department', 'Unknown')
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        log(f"  {dept}: {count} emails")
    
    log(f"\n{'='*70}")
    log("ALL EMAILS:")
    log(f"{'='*70}")
    for r in sorted(cleaned_combined, key=lambda x: x.get('email', '')):
        name_str = f" ({r['name']})" if r.get('name') else ""
        log(f"  {r['email']}{name_str} - {r.get('department', '')}")
    
    log(f"\n\nTOTAL: {len(cleaned_combined)} unique emails in final CSV")


if __name__ == '__main__':
    main()
