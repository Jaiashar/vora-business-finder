#!/usr/bin/env python3
"""
Merge all UCLA email sources into one master CSV.
Combines scraper V2 output + manually discovered emails from agents.
"""
import csv, json, os
from datetime import datetime

BASE = os.path.dirname(os.path.abspath(__file__))

# Load existing contacts from scraper
existing = {}
csv_path = os.path.join(BASE, "ucla_contacts_v2.csv")
with open(csv_path, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        existing[row["email"].lower()] = row

print(f"Loaded {len(existing)} existing contacts from scraper")

# ── Additional emails discovered by agents ──
AGENT_EMAILS = {
    # Gender Studies (17)
    "Gender Studies Grad Students": [
        ("Leor Avramovich", "lavramovich@g.ucla.edu"),
        ("Celestina Castillo", "castillomontana@ucla.edu"),
        ("Da In Choi", "dainachoi@g.ucla.edu"),
        ("Lauren Dixon", "ldixon9@ucla.edu"),
        ("Kristen Dorsey", "kristendorsey@g.ucla.edu"),
        ("James Fremland", "jfremland@ucla.edu"),
        ("Yurim Lee", "yurimlee@g.ucla.edu"),
        ("Nashra", "nashra94@ucla.edu"),
        ("Tiffany Marcelino", "tmarcelino@ucla.edu"),
        ("Elliot Rouse", "ero85@g.ucla.edu"),
        ("Josephine Ong", "josephineong25@ucla.edu"),
        ("Ludmila Porto", "ludmilaporto@g.ucla.edu"),
        ("", "yaquesita881@gmail.com"),
        ("Mel Stockton", "mrstockton@ucla.edu"),
        ("Kate Ambree", "katambree@ucla.edu"),
        ("Yi-Uwei", "yiuwei@g.ucla.edu"),
        ("", "ladanzarabadi21@ucla.edu"),
    ],
    # Linguistics (40)
    "Linguistics Grad Students": [
        ("", "aabr@g.ucla.edu"), ("", "hashmita@ucla.edu"), ("", "fahad95@ucla.edu"),
        ("", "dhruvbarot@ucla.edu"), ("", "isacabrera@ucla.edu"), ("", "kalen@g.ucla.edu"),
        ("", "joeclass22@ucla.edu"), ("", "c96rocio@g.ucla.edu"), ("", "ericasolis@g.ucla.edu"),
        ("", "baichendu@ucla.edu"), ("", "dinhuistics@ucla.edu"),
        ("", "corrfuller127@ucla.edu"), ("", "anissadg@ucla.edu"), ("", "eglass63@ucla.edu"),
        ("", "avnig@ucla.edu"), ("", "evanhochstein@g.ucla.edu"), ("", "hunterjohnson@ucla.edu"),
        ("", "andrewkato@ucla.edu"), ("", "adamleif@g.ucla.edu"), ("", "kevinliang8@g.ucla.edu"),
        ("", "mliotta@g.ucla.edu"), ("", "hannahlippard@ucla.edu"), ("", "shalinee30@g.ucla.edu"),
        ("", "mateos@ucla.edu"), ("", "jmcgahay@g.ucla.edu"), ("", "zachmetzler@g.ucla.edu"),
        ("", "cmuxica@g.ucla.edu"), ("", "jnarkar@ucla.edu"), ("", "nedsanger@ucla.edu"),
        ("", "jennifershin@ucla.edu"), ("", "jlsiah@g.ucla.edu"), ("", "sakshisinghxyz@ucla.edu"),
        ("", "elsol4@ucla.edu"), ("", "averbil@ucla.edu"), ("", "iwarren@g.ucla.edu"),
        ("", "zhongshixu@ucla.edu"), ("", "lilyokc@ucla.edu"), ("", "corzanda@g.ucla.edu"),
        ("", "wzimmermann1@ucla.edu"),
    ],
    # Slavic (8)
    "Slavic Grad Students": [
        ("Cooper Lynn", "cooperlynn95@g.ucla.edu"),
        ("Elena Makarova", "lemaka@g.ucla.edu"),
        ("Emilia McLennan", "mclennan@ucla.edu"),
        ("David Miller", "davidhillel@gmail.com"),
        ("Marianna Petiaskina", "mmarrianna@g.ucla.edu"),
        ("Lydia Roberts", "lydia.h.roberts@ucla.edu"),
        ("Assem Shamarova", "shamarova@ucla.edu"),
        ("Polina Varfolomeeva", "pvarfolomeeva@g.ucla.edu"),
    ],
    # Asian American Studies (16)
    "Asian American Studies Grad Students": [
        ("", "ahemler@ucla.edu"), ("", "evechen@g.ucla.edu"),
        ("", "karnsouvong@ucla.edu"), ("", "kellimiho23@g.ucla.edu"),
        ("", "kevvthor@g.ucla.edu"), ("", "kyw@ucla.edu"),
        ("", "laurrbancos25@g.ucla.edu"), ("", "lindazhang27@g.ucla.edu"),
        ("", "lindseybchou@g.ucla.edu"),
        ("", "pjyuen@g.ucla.edu"), ("", "psoun11@g.ucla.edu"),
        ("", "seansugai23@g.ucla.edu"), ("", "svertido@ucla.edu"),
    ],
    # Chicano Studies (8)
    "Chicano Studies Grad Students": [
        ("", "bcantero1@g.ucla.edu"), ("", "carlosnrogel@ucla.edu"),
        ("", "dannybonitz@g.ucla.edu"), ("", "gperez2031@ucla.edu"),
        ("", "hirugami@ucla.edu"), ("", "jdpineda@ucla.edu"),
        ("", "lvilchiszarate@ucla.edu"), ("", "mrac@ucla.edu"),
    ],
    # Sociology (from first successful run)
    "Sociology Grad Students": [
        # These were found in the first run but may have been lost due to rate limiting
        # Re-adding known @g.ucla.edu from sociology if not already in CSV
    ],
    # Anthropology (from first successful run)
    "Anthropology Grad Students": [
        # Same situation
    ],
    # EEB extra (from agent that found 43)
    "EEB Grad Students (Agent)": [
        ("", "abaustin1223@ucla.edu"), ("", "amurran@ucla.edu"),
        ("", "aohowens@ucla.edu"), ("", "bbadillo@ucla.edu"),
        ("", "boslough@ucla.edu"), ("", "csayers2@ucla.edu"),
        ("", "eonokerns@ucla.edu"), ("", "geiman@ucla.edu"),
        ("", "hspeck@ucla.edu"), ("", "hstouter@ucla.edu"),
        ("", "hyangg@ucla.edu"), ("", "jamflores@ucla.edu"),
        ("", "janinef@ucla.edu"), ("", "jegomez97@ucla.edu"),
        ("", "jfdiliberto@ucla.edu"), ("", "jillcarpenter@ucla.edu"),
        ("", "joannaxwu@ucla.edu"), ("", "jordynregier@ucla.edu"),
        ("", "kcschott@ucla.edu"), ("", "kgahm@ucla.edu"),
        ("", "khannibal@ucla.edu"), ("", "kreckling@ucla.edu"),
        ("", "lglevanik@ucla.edu"), ("", "lindbesh13@ucla.edu"),
        ("", "madeleinegp@ucla.edu"), ("", "miabrecht@ucla.edu"),
        ("", "msagarin@ucla.edu"), ("", "nicoandrade@ucla.edu"),
        ("", "nidhivinod20@ucla.edu"), ("", "onny.marwayana@ucla.edu"),
        ("", "peterlaurin@ucla.edu"), ("", "sarahmd1113@ucla.edu"),
        ("", "seanofallon@ucla.edu"), ("", "xortizross@ucla.edu"),
    ],
    # Graeber Lab (from fetched page)
    "Graeber Lab": [
        ("Anna Allnutt", "annamarie67@g.ucla.edu"),
        ("Jack Freeland", "jfreeland@g.ucla.edu"),
        ("Seoyeong Tae", "sytaee@g.ucla.edu"),
    ],
    # Communication extra (names from fetched page)
    "Communication Grad Students (Names)": [
        ("Elias Acevedo", "eacevedo89@g.ucla.edu"),
        ("Constance Bainbridge", "cbainbridge@g.ucla.edu"),
        ("Mia Carbone", "miacarbone@g.ucla.edu"),
        ("Je Hoon Chae", "chae@g.ucla.edu"),
        ("Abhinanda Dash", "abhinandadash99@g.ucla.edu"),
        ("Joyce Yanru Jiang", "yanrujiang@g.ucla.edu"),
        ("Jennifer Jiyoung Hwang", "jiyhwang@g.ucla.edu"),
        ("Prianka Koya", "priankakoya@g.ucla.edu"),
        ("Catherine Lacsamana", "clacsama8@g.ucla.edu"),
        ("Lin Lin", "llin001@g.ucla.edu"),
        ("Jeffrey Mai", "maijiahao@g.ucla.edu"),
        ("Seonhye Noh", "shnoh@g.ucla.edu"),
        ("Gabriella Skollar", "gabiskollar@g.ucla.edu"),
        ("Yingjia Wan", "alisawan@g.ucla.edu"),
    ],
    # EPSS extra (from fetched page)
    "EPSS Grad Students (Extra)": [
        ("", "chenweiyu@g.ucla.edu"),
        ("", "benjaminqtan@g.ucla.edu"),
        ("", "yueyuan2003@g.ucla.edu"),
        ("", "sophye@g.ucla.edu"),
        ("", "aolitt23@g.ucla.edu"),
        ("", "mng362@g.ucla.edu"),
    ],
}

added = 0
for dept, entries in AGENT_EMAILS.items():
    for name, email in entries:
        email = email.lower().strip()
        if email not in existing:
            is_student = '@g.ucla.edu' in email or '@gmail.com' in email
            existing[email] = {
                "name": name,
                "email": email,
                "title": "",
                "department": dept,
                "role": "student" if is_student else "staff",
                "source_url": "agent_discovery",
            }
            added += 1

print(f"Added {added} new emails from agent discoveries")
print(f"Total unique contacts: {len(existing)}")

# ── Save master CSV ──
master_csv = os.path.join(BASE, "ucla_master.csv")
fieldnames = ["name", "email", "title", "department", "role", "source_url"]
contacts = list(existing.values())

with open(master_csv, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(contacts)

# ── Stats ──
students = [c for c in contacts if c["role"] == "student" or '@g.ucla.edu' in c["email"]]
staff = [c for c in contacts if c not in students]

# Count by domain
g_ucla = sum(1 for c in contacts if '@g.ucla.edu' in c["email"])
ucla = sum(1 for c in contacts if '@ucla.edu' in c["email"] and '@g.ucla.edu' not in c["email"])
gmail = sum(1 for c in contacts if '@gmail.com' in c["email"])
other = len(contacts) - g_ucla - ucla - gmail

print(f"\n{'='*60}")
print(f"  UCLA MASTER CONTACT LIST")
print(f"{'='*60}")
print(f"  Total unique contacts: {len(contacts)}")
print(f"  ★ STUDENTS: {len(students)}")
print(f"  Staff/Faculty: {len(staff)}")
print(f"\n  By email domain:")
print(f"    @g.ucla.edu:  {g_ucla}")
print(f"    @ucla.edu:    {ucla}")
print(f"    @gmail.com:   {gmail}")
print(f"    other:        {other}")
print(f"\n  Master CSV: {master_csv}")

# Save JSON too
master_json = os.path.join(BASE, "ucla_master.json")
with open(master_json, 'w') as f:
    json.dump({
        "contacts": contacts,
        "scrape_date": datetime.now().isoformat(),
        "stats": {
            "total": len(contacts),
            "students": len(students),
            "staff": len(staff),
            "g_ucla_edu": g_ucla,
            "ucla_edu": ucla,
            "gmail": gmail,
        }
    }, f, indent=2)

print(f"  Master JSON: {master_json}")
