#!/usr/bin/env python3
"""
UCLA Student & Staff Email Scraper V2
=======================================
Aggressive multi-source scraper targeting STUDENTS.

Sources:
  1. Research labs (dozens of lab pages → @g.ucla.edu student emails)
  2. Varsity sport rosters (student-athlete names → email pattern guesses)
  3. UCLA Recreation staff directory
  4. UCLA Athletics staff directory
  5. Greek life chapter presidents (names → verified by FSL page)
  6. Student orgs & government
  7. Daily Bruin student journalists
"""

import os
import re
import csv
import time
import json
import urllib.request
import urllib.error
import ssl
from datetime import datetime
from collections import defaultdict

# ─── Setup ───────────────────────────────────────────────────────
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.5",
}

# Match emails with @ or with the " at_" obfuscation pattern (parklab-style)
EMAIL_AT_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
EMAIL_OBFUSCATED_RE = re.compile(r'([a-zA-Z0-9._%+\-]+)\s+at_([a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})')

SKIP_EMAILS = {
    'webmaster@ucla.edu', 'communications@ucla.edu', 'info@ucla.edu',
    'registrar@ucla.edu', 'accessibility@ucla.edu', 'noreply@ucla.edu',
    'its@ucla.edu', 'postmaster@ucla.edu', 'abuse@ucla.edu',
}

SKIP_DOMAINS = {
    'sentry.io', 'example.com', 'test.com', 'localhost',
    'w3.org', 'schema.org', 'googleusercontent.com',
}


def fetch_page(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        return resp.read().decode('utf-8', errors='ignore')
    except Exception as e:
        return None


def extract_all_emails(text):
    """Extract emails from text, handling both @ and obfuscated 'at_' patterns."""
    if not text:
        return []

    found = set()

    # Standard @-style emails
    for m in EMAIL_AT_RE.findall(text):
        found.add(m.lower().strip())

    # Obfuscated " at_" style (e.g. "jsmith at_g.ucla.edu")
    for user, domain in EMAIL_OBFUSCATED_RE.findall(text):
        found.add(f"{user.lower().strip()}@{domain.lower().strip()}")

    # Filter
    cleaned = []
    for e in found:
        domain = e.split('@')[-1]
        if domain in SKIP_DOMAINS:
            continue
        if e in SKIP_EMAILS:
            continue
        if any(kw in e for kw in ['noreply', 'no-reply', 'donotreply', 'webmaster', 'postmaster']):
            continue
        if len(e) < 5 or len(e) > 80:
            continue
        cleaned.append(e)

    return cleaned


def extract_name_email_pairs(html):
    """Extract (name, email, title) from HTML with h2/h3 name blocks."""
    pairs = []
    blocks = re.split(r'<h[23][^>]*>', html)
    for block in blocks[1:]:
        name_match = re.match(r'([^<]+)</h[23]>', block)
        if not name_match:
            continue
        name = name_match.group(1).strip()
        if not name or len(name) > 80 or len(name) < 3:
            continue
        if any(kw in name.lower() for kw in ['join us', 'alumni', 'contact', 'welcome']):
            continue

        emails = extract_all_emails(block[:600])
        ucla_emails = [e for e in emails if 'ucla.edu' in e]
        if not ucla_emails:
            continue

        title = ""
        title_match = re.search(r'</h[23]>\s*\n?\s*([A-Z][^<\n]{3,80})', block)
        if title_match:
            title = title_match.group(1).strip()
            title = re.sub(r'\d{3}[\.\-]\d{3}[\.\-]\d{4}', '', title).strip()

        pairs.append({
            "name": name,
            "email": ucla_emails[0],
            "title": title,
        })

    return pairs


# ═══════════════════════════════════════════════════════════════════
# SOURCE 1: UCLA RESEARCH LABS (student @g.ucla.edu emails)
# ═══════════════════════════════════════════════════════════════════

LAB_URLS = [
    # ── CONFIRMED HIGH-YIELD (student @g.ucla.edu emails found) ──
    ("https://virus.chem.ucla.edu/lab-members", "Virus Group (Chem)"),
    ("https://parklab.ucla.edu/members", "Park Lab"),
    ("https://systems.crump.ucla.edu/lab-member/", "Graeber Lab"),
    ("https://sites.lifesci.ucla.edu/eeb-kraft/lab-members/", "Kraft Lab (EEB)"),
    ("https://epss.ucla.edu/tag/graduate-student", "EPSS Grad Students"),
    ("https://comm.ucla.edu/people/graduate-students/", "Communication Grad Students"),
    ("https://garg.chem.ucla.edu/group-members", "Garg Lab (Chem)"),
    ("https://addictions.psych.ucla.edu/people/", "Addictions Lab (Psych)"),
    ("https://languagelab.humanities.ucla.edu/en/people/", "Language Acquisition Lab"),
    ("https://wacd.ucla.edu/people/graduate-students", "World Arts Grad Students"),
    ("https://bhadurilab.dgsom.ucla.edu/contact-us", "Bhaduri Lab"),
    # ── Medical / Health Sciences ──
    ("https://bradleylab.dgsom.ucla.edu/lab-members", "Bradley Lab"),
    ("https://golshanilab.neurology.ucla.edu/lab-members", "Golshani Lab"),
    ("https://neilharrislab.dgsom.ucla.edu/lab-members", "Harris Lab"),
    ("https://waynelab.eeb.ucla.edu/lab-members/", "Wayne Lab"),
    ("https://daboussilab.healthsciences.ucla.edu/lab-members", "Daboussi Lab"),
    ("https://www.nursing.ucla.edu/about/faculty-directory", "Nursing"),
    ("https://ph.ucla.edu/about/faculty-staff-directory", "Public Health"),
    # ── Chemistry / Biochemistry ──
    ("https://www.chemistry.ucla.edu/people/", "Chemistry Dept"),
    # ── Engineering ──
    ("https://www.ee.ucla.edu/people/", "Electrical Engineering"),
    ("https://samueli.ucla.edu/people/", "Samueli Engineering"),
    # ── CS / Stats ──
    ("https://starai.cs.ucla.edu/members/", "StarAI Lab (CS)"),
    ("https://vcla.stat.ucla.edu/people.html", "VCLA (Stats/CS)"),
    # ── Life Sciences ──
    ("https://www.mcdb.ucla.edu/people/", "MCDB"),
    ("https://www.mbi.ucla.edu/people/", "MBI"),
    ("https://www.ibp.ucla.edu/faculty/", "IBP Faculty"),
    ("https://lifesciences.ucla.edu/people/", "Life Sciences"),
    # ── Social Sciences ──
    ("https://www.polisci.ucla.edu/people/graduate-students", "PoliSci Grad Students"),
    ("https://www.soc.ucla.edu/people/graduate-students", "Sociology Grad Students"),
    ("https://www.psych.ucla.edu/people/", "Psychology"),
    ("https://www.econ.ucla.edu/people/graduate-students", "Economics Grad Students"),
    ("https://www.anthro.ucla.edu/people/graduate-students", "Anthropology Grad Students"),
    ("https://www.history.ucla.edu/people/graduate-students", "History Grad Students"),
    # ── Neuroscience / Psychology ──
    ("https://ccn.ucla.edu/people/", "CCN"),
    ("https://anxiety.psych.ucla.edu/people", "Anxiety Lab"),
    ("https://www.neuroscience.ucla.edu/people/", "Neuroscience IDP"),
    ("https://mdcune.psych.ucla.edu/people", "MDCUNE Psych"),
    # ── Environment / Sustainability ──
    ("https://www.ioes.ucla.edu/people/", "Environment & Sustainability"),
    ("https://www.eeb.ucla.edu/Faculty/", "EEB Faculty"),
    # ── Public Affairs ──
    ("https://luskin.ucla.edu/faculty-and-staff", "Luskin School"),
    # ── Specific labs ──
    ("https://arislab.seas.ucla.edu/people/", "ARIS Lab"),
    ("https://compmed.ucla.edu/people", "Computational Medicine"),
    ("https://bionano.ucla.edu/people/", "BioNano Lab"),
    # ── Math / Physical Sciences ──
    ("https://www.math.ucla.edu/people/grad", "Math Grad Students"),
    ("https://www.physics.ucla.edu/people/graduate-students/", "Physics Grad Students"),
    ("https://www.stat.ucla.edu/people/graduate-students/", "Statistics Grad Students"),
    # ── ROUND 2: Confirmed high-yield dept pages ──
    ("https://gender.ucla.edu/people/graduate-students/", "Gender Studies Grad Students"),
    ("https://linguistics.ucla.edu/grads/", "Linguistics Grad Students"),
    ("https://slavic.ucla.edu/grad/", "Slavic Grad Students"),
    ("https://www.eeb.ucla.edu/current-graduate-students/", "EEB Grad Students"),
    ("https://asianam.ucla.edu/people/graduate-students/", "Asian American Studies Grad Students"),
    ("https://www.chavez.ucla.edu/people/graduate-students/", "Chicano Studies Grad Students"),
    # ── ROUND 2: Additional dept pages to try ──
    ("https://www.geog.ucla.edu/people/graduate-students/", "Geography Grad Students"),
    ("https://biolchem.ucla.edu/people/graduate-students", "Biochem Grad Students"),
    ("https://www.philosophy.ucla.edu/people/graduate-students/", "Philosophy Grad Students"),
    ("https://www.english.ucla.edu/people/graduate-students/", "English Grad Students"),
    ("https://www.art.ucla.edu/graduate-students/", "Art MFA Students"),
    ("https://www.chemistry.ucla.edu/directory/", "Chemistry Directory"),
    ("https://socialwelfare.ucla.edu/people/students/", "Social Welfare Students"),
    ("https://www.polisci.ucla.edu/people/graduate-students/", "PoliSci Grad Students"),
    ("https://www.law.ucla.edu/faculty/profiles/", "Law School Faculty"),
    ("https://gseis.ucla.edu/directory/students/", "Info Studies Grad Students"),
    ("https://www.management.ucla.edu/phd/current-students", "Anderson PhD Students"),
    # ── ROUND 2: More research labs ──
    ("https://www.chemistry.ucla.edu/contact-us/", "Chemistry Contacts"),
    ("https://www.bioeng.ucla.edu/students/", "Bioengineering Students"),
]


def scrape_research_labs():
    """Scrape UCLA research lab pages for student emails."""
    print("\n  ┌─────────────────────────────────────────────────┐")
    print("  │  SOURCE 1: UCLA RESEARCH LABS                   │")
    print("  └─────────────────────────────────────────────────┘")

    results = []
    seen = set()
    total_student = 0

    for url, label in LAB_URLS:
        html = fetch_page(url)
        if not html:
            print(f"    ✗ {label}")
            continue

        # Extract all emails
        emails = extract_all_emails(html)
        pairs = extract_name_email_pairs(html)

        count = 0
        for p in pairs:
            e = p["email"]
            if e not in seen:
                is_student = '@g.ucla.edu' in e
                results.append({
                    "name": p["name"],
                    "email": e,
                    "title": p["title"],
                    "department": label,
                    "source_url": url,
                    "role": "student" if is_student else "faculty",
                })
                seen.add(e)
                count += 1
                if is_student:
                    total_student += 1

        # Catch emails not in name pairs
        for e in emails:
            if e not in seen and 'ucla.edu' in e:
                is_student = '@g.ucla.edu' in e
                results.append({
                    "name": "", "email": e, "title": "",
                    "department": label, "source_url": url,
                    "role": "student" if is_student else "faculty",
                })
                seen.add(e)
                count += 1
                if is_student:
                    total_student += 1

        if count > 0:
            print(f"    ✓ {label:<30} → {count} emails")
        else:
            print(f"    · {label:<30} → 0")

        time.sleep(0.5)

    print(f"\n    Lab total: {len(results)} contacts ({total_student} students)")
    return results


# ═══════════════════════════════════════════════════════════════════
# SOURCE 2: UCLA VARSITY ROSTERS (student-athlete names)
# ═══════════════════════════════════════════════════════════════════

ROSTER_SPORTS = [
    "baseball", "mens-basketball", "womens-basketball", "beach-volleyball",
    "mens-cross-country", "womens-cross-country", "football",
    "mens-golf", "womens-golf", "gymnastics",
    "mens-soccer", "womens-soccer", "softball",
    "mens-swimming-and-diving", "womens-swimming-and-diving",
    "mens-tennis", "womens-tennis",
    "mens-track-and-field", "womens-track-and-field",
    "mens-volleyball", "womens-volleyball",
    "mens-water-polo", "womens-water-polo", "rowing",
]


def extract_roster_names(html):
    """Extract student-athlete names from UCLA roster page HTML."""
    names = []

    # Pattern: roster pages have <a ...>First Last</a> with class patterns
    # Or <h3>Name</h3> blocks
    # Common: <span class="sidearm-roster-player-name">...</span>
    name_patterns = [
        r'class="[^"]*roster[^"]*player[^"]*name[^"]*"[^>]*>([^<]+)<',
        r'<h3[^>]*>\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*</h3>',
        r'aria-label="([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)"',
        # Sidearm roster cards
        r'sidearm-roster-player-name[^>]*>\s*<(?:a|span)[^>]*>([^<]+)<',
    ]

    for pat in name_patterns:
        for m in re.findall(pat, html):
            name = m.strip()
            if 2 < len(name) < 50 and ' ' in name:
                if not any(kw in name.lower() for kw in ['coaching', 'staff', 'head coach', 'roster', 'schedule']):
                    names.append(name)

    # Deduplicate preserving order
    seen = set()
    unique = []
    for n in names:
        if n not in seen:
            unique.append(n)
            seen.add(n)

    return unique


def scrape_varsity_rosters():
    """Scrape UCLA varsity sport roster pages for student-athlete names."""
    print("\n  ┌─────────────────────────────────────────────────┐")
    print("  │  SOURCE 2: UCLA VARSITY ROSTERS                 │")
    print("  └─────────────────────────────────────────────────┘")

    results = []
    seen_names = set()
    athlete_names = []

    for sport in ROSTER_SPORTS:
        # Try both year formats
        for year_suffix in ["2025-26", "2025", "2024-25"]:
            url = f"https://uclabruins.com/sports/{sport}/roster/{year_suffix}"
            html = fetch_page(url)
            if html and len(html) > 5000:
                names = extract_roster_names(html)

                # Also extract any emails on the page (usually coaches)
                emails = extract_all_emails(html)
                for e in emails:
                    if 'ucla.edu' in e and e not in seen_names:
                        results.append({
                            "name": "", "email": e, "title": "",
                            "department": f"UCLA {sport.replace('-', ' ').title()}",
                            "source_url": url,
                            "role": "coach",
                        })
                        seen_names.add(e)

                for name in names:
                    if name not in seen_names:
                        athlete_names.append({
                            "name": name,
                            "sport": sport.replace('-', ' ').title(),
                            "source_url": url,
                        })
                        seen_names.add(name)

                print(f"    ✓ {sport:<30} → {len(names)} athletes, {len(emails)} coach emails")
                break  # Got data for this sport, move on
            time.sleep(0.3)
        else:
            print(f"    ✗ {sport}")

    print(f"\n    Roster total: {len(athlete_names)} athlete names, {len(results)} coach emails")
    return results, athlete_names


# ═══════════════════════════════════════════════════════════════════
# SOURCE 3: UCLA RECREATION STAFF
# ═══════════════════════════════════════════════════════════════════
def scrape_recreation():
    print("\n  ┌─────────────────────────────────────────────────┐")
    print("  │  SOURCE 3: UCLA RECREATION STAFF                │")
    print("  └─────────────────────────────────────────────────┘")

    url = "https://recreation.ucla.edu/about/contact"
    html = fetch_page(url)
    if not html:
        print("    ✗ Could not fetch")
        return []

    pairs = extract_name_email_pairs(html)
    emails = extract_all_emails(html)

    results = []
    seen = set()

    for p in pairs:
        if p["email"] not in seen and 'ucla.edu' in p["email"]:
            results.append({
                "name": p["name"], "email": p["email"], "title": p["title"],
                "department": "UCLA Recreation", "source_url": url,
                "role": "staff",
            })
            seen.add(p["email"])

    for e in emails:
        if e not in seen and 'ucla.edu' in e:
            results.append({
                "name": "", "email": e, "title": "",
                "department": "UCLA Recreation", "source_url": url,
                "role": "staff",
            })
            seen.add(e)

    print(f"    ✓ {len(results)} contacts")
    return results


# ═══════════════════════════════════════════════════════════════════
# SOURCE 4: UCLA ATHLETICS STAFF
# ═══════════════════════════════════════════════════════════════════
def scrape_athletics_staff():
    print("\n  ┌─────────────────────────────────────────────────┐")
    print("  │  SOURCE 4: UCLA ATHLETICS STAFF DIRECTORY       │")
    print("  └─────────────────────────────────────────────────┘")

    url = "https://uclabruins.com/staff-directory"
    html = fetch_page(url)
    if not html:
        print("    ✗ Could not fetch")
        return []

    emails = extract_all_emails(html)
    results = []
    seen = set()

    for e in emails:
        if e not in seen and 'ucla.edu' in e:
            results.append({
                "name": "", "email": e, "title": "",
                "department": "UCLA Athletics", "source_url": url,
                "role": "staff",
            })
            seen.add(e)

    print(f"    ✓ {len(results)} contacts")
    return results


# ═══════════════════════════════════════════════════════════════════
# SOURCE 5: GREEK LIFE CHAPTER PRESIDENTS
# ═══════════════════════════════════════════════════════════════════
def get_greek_presidents():
    """Parse the FSL community directory for chapter president names."""
    print("\n  ┌─────────────────────────────────────────────────┐")
    print("  │  SOURCE 5: GREEK LIFE CHAPTER PRESIDENTS        │")
    print("  └─────────────────────────────────────────────────┘")

    url = "https://fsl.ucla.edu/community-directory"
    html = fetch_page(url)

    results = []
    president_names = []

    if not html:
        print("    ✗ Could not fetch FSL directory")
        return results, president_names

    # Extract emails directly from page
    emails = extract_all_emails(html)
    seen = set()

    for e in emails:
        if e not in seen:
            is_student = '@g.ucla.edu' in e or '@gmail.com' in e
            results.append({
                "name": "", "email": e, "title": "Chapter/Council President",
                "department": "UCLA Greek Life", "source_url": url,
                "role": "student" if is_student else "student_org",
            })
            seen.add(e)

    # Extract president names from the structured list
    # Pattern: "President: First Last" or just names in president context
    pres_pattern = re.compile(r'President:\s*([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z\-]+)+)')
    for match in pres_pattern.findall(html):
        name = match.strip()
        if name and name not in president_names:
            president_names.append(name)

    print(f"    ✓ {len(results)} direct emails, {len(president_names)} president names")
    for name in president_names[:5]:
        print(f"      {name}")
    if len(president_names) > 5:
        print(f"      ... and {len(president_names)-5} more")

    return results, president_names


# ═══════════════════════════════════════════════════════════════════
# SOURCE 6: STUDENT GOVERNMENT & PUBLICATIONS
# ═══════════════════════════════════════════════════════════════════
def scrape_student_sources():
    print("\n  ┌─────────────────────────────────────────────────┐")
    print("  │  SOURCE 6: STUDENT GOVT, PUBLICATIONS, ORGS     │")
    print("  └─────────────────────────────────────────────────┘")

    URLS = [
        ("https://usac.ucla.edu/", "USAC Student Government"),
        ("https://usac.ucla.edu/about/officers", "USAC Officers"),
        ("https://swcucla.org/", "Student Wellness Commission"),
        ("https://dailybruin.com/staff", "Daily Bruin Staff"),
        ("https://dailybruin.com/about", "Daily Bruin About"),
        ("https://bewellbruin.ucla.edu/", "Be Well Bruin"),
        ("https://community.ucla.edu/studentorgs/pre-professional", "Pre-Professional Orgs"),
        ("https://community.ucla.edu/studentorgs/medical", "Medical Orgs"),
        ("https://community.ucla.edu/studentorgs", "All Student Orgs"),
        ("https://bruinwalk.com/", "BruinWalk"),
    ]

    results = []
    seen = set()

    for url, label in URLS:
        html = fetch_page(url)
        if not html:
            print(f"    ✗ {label}")
            continue

        emails = extract_all_emails(html)
        count = 0
        for e in emails:
            if e not in seen and ('ucla.edu' in e or '@gmail.com' in e):
                is_student = '@g.ucla.edu' in e
                results.append({
                    "name": "", "email": e, "title": "",
                    "department": label, "source_url": url,
                    "role": "student" if is_student else "student_org",
                })
                seen.add(e)
                count += 1

        print(f"    {'✓' if count else '·'} {label:<35} → {count}")
        time.sleep(0.5)

    print(f"    Total: {len(results)} contacts")
    return results


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
def main():
    print("╔════════════════════════════════════════════════════════════╗")
    print("║       UCLA EMAIL SCRAPER V2 — STUDENT FOCUSED            ║")
    print(f"║       {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<53}║")
    print("╚════════════════════════════════════════════════════════════╝")

    all_contacts = []
    all_athlete_names = []
    all_president_names = []

    # Run all sources
    all_contacts.extend(scrape_research_labs())

    roster_contacts, athlete_names = scrape_varsity_rosters()
    all_contacts.extend(roster_contacts)
    all_athlete_names = athlete_names

    all_contacts.extend(scrape_recreation())
    all_contacts.extend(scrape_athletics_staff())

    greek_contacts, president_names = get_greek_presidents()
    all_contacts.extend(greek_contacts)
    all_president_names = president_names

    all_contacts.extend(scrape_student_sources())

    # ── Deduplicate ──
    seen = set()
    unique = []
    for c in all_contacts:
        if c["email"] not in seen:
            unique.append(c)
            seen.add(c["email"])

    # ── Stats ──
    print("\n" + "="*60)
    print("  RESULTS")
    print("="*60)

    students = [c for c in unique if c["role"] == "student" or '@g.ucla.edu' in c["email"]]
    staff = [c for c in unique if c not in students]

    roles = defaultdict(int)
    for c in unique:
        roles[c["role"]] += 1

    depts = defaultdict(int)
    for c in unique:
        depts[c["department"]] += 1

    print(f"\n  Total unique emails: {len(unique)}")
    print(f"  ✦ STUDENTS: {len(students)}")
    print(f"  ✦ Staff/Faculty: {len(staff)}")
    print(f"  ✦ Athlete names (no email yet): {len(all_athlete_names)}")
    print(f"  ✦ Greek president names: {len(all_president_names)}")

    print(f"\n  By role:")
    for r, count in sorted(roles.items(), key=lambda x: -x[1]):
        print(f"    {r:<25} {count}")

    print(f"\n  By source:")
    for d, count in sorted(depts.items(), key=lambda x: -x[1]):
        print(f"    {d:<35} {count}")

    # ── Show student emails ──
    if students:
        print(f"\n  ★ Student emails found ({len(students)}):")
        for s in students:
            print(f"    {s['email']:<40} {s['name'][:20]:<22} {s['department']}")

    # ── Save CSV ──
    base = os.path.dirname(os.path.abspath(__file__))

    # Main contacts CSV
    csv_path = os.path.join(base, "ucla_contacts_v2.csv")
    fieldnames = ["name", "email", "title", "department", "role", "source_url"]
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unique)

    # Athletes CSV (names for future email pattern matching)
    athletes_csv = os.path.join(base, "ucla_athletes.csv")
    with open(athletes_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["name", "sport", "source_url"])
        writer.writeheader()
        writer.writerows(all_athlete_names)

    # Greek presidents CSV
    presidents_csv = os.path.join(base, "ucla_greek_presidents.csv")
    with open(presidents_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["name", "org"])
        writer.writeheader()
        for name in all_president_names:
            writer.writerow({"name": name, "org": "UCLA Greek Life"})

    # JSON (all data)
    json_path = os.path.join(base, "ucla_all_data.json")
    with open(json_path, 'w') as f:
        json.dump({
            "contacts": unique,
            "athlete_names": all_athlete_names,
            "greek_presidents": all_president_names,
            "scrape_date": datetime.now().isoformat(),
            "stats": {
                "total_emails": len(unique),
                "student_emails": len(students),
                "staff_emails": len(staff),
                "athlete_names": len(all_athlete_names),
                "president_names": len(all_president_names),
            }
        }, f, indent=2)

    print(f"\n  ── Files saved ──")
    print(f"  Contacts CSV:   {csv_path}")
    print(f"  Athletes CSV:   {athletes_csv}")
    print(f"  Presidents CSV: {presidents_csv}")
    print(f"  Full JSON:      {json_path}")
    print(f"\n  Done! ✓")


if __name__ == "__main__":
    main()
