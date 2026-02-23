#!/usr/bin/env python3
"""
Fetch remaining Stanford profile pages and clean up generic emails.
"""

import csv
import re
import time
import urllib.request
import ssl

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def fetch_page(url, retries=2):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            })
            with urllib.request.urlopen(req, context=ctx, timeout=15) as response:
                return response.read().decode('utf-8', errors='replace')
        except Exception as e:
            if attempt < retries:
                time.sleep(1)
            else:
                return None

def extract_email(html):
    if not html:
        return None
    match = re.search(r'<span[^>]*>([^<]+)</span>\s*\[at\]\s*<span[^>]*>stanford\.edu</span>', html)
    if match:
        username = match.group(1).strip()
        email = f"{username}@stanford.edu"
        # Filter out generic department emails
        generic = ['sociology@stanford.edu', 'econ-undergrad@stanford.edu', 
                   'economics@stanford.edu', 'history@stanford.edu',
                   'polisci@stanford.edu', 'philosophy@stanford.edu']
        if email.lower() in generic:
            return None
        return email
    match = re.search(r'mailto:([\w.-]+@stanford\.edu)', html)
    if match:
        email = match.group(1)
        if email.split('@')[0] not in ['sociology', 'economics', 'history', 'econ-undergrad']:
            return email
    return None

def slug_from_name(name, base_url):
    clean = name.lower()
    clean = re.sub(r'\([^)]*\)', '', clean)
    clean = clean.replace('.', '').replace("'", '').replace("'", '').replace("'", '')
    clean = clean.strip()
    clean = re.sub(r'\s+', '-', clean)
    clean = re.sub(r'-+', '-', clean)
    return f"{base_url}/{clean}"

# Remaining departments to fetch
departments = {
    "Philosophy": {
        "base_url": "https://philosophy.stanford.edu/people",
        "source_url": "https://philosophy.stanford.edu/people/graduate-students",
        "students": [
            "Conor Fei", "Adam Feng", "Ziyu Guo", "Ryan Tan",
            "Mariel Goddu", "Stewart Huang", "Anton Skott",
            "Samantha Bennett", "Andrew Biondo", "Claire Ganiban",
            "Konstantinos Konstantinou", "Shuting Liang", "Toby Tricks",
            "Chloe Van Steertegem", "Que Wu",
            "Lukas Apsel", "Robinson Erhardt", "Alexa Hazel", "PM Irvin", "Juanhe TJ Tan",
            "Maximilian Forster", "Sydney Jordan", "Bendix Kemmann",
            "Alexander Pereira", "William Grant Ray",
            "Lis Benossi", "Julian Davis", "Jacqueline Harding",
            "Hayden Kajercline", "Thomas Ladendorf", "Zihan Wang",
            "Austen Friesacher", "Shayan Koeksal", "Rupert Sparling",
            "Elise Sugarman", "Sally Tilton", "Cesar Valenzuela",
            "Yasin Al-Amin", "Sarah Brophy", "Zachary Hall",
            "Seyoung Kang", "Penn Lawrence", "Soham Shiva",
            "Jonathan Amaral", "Marianna Bible", "Pat B. Hope",
            "Chenxuan (Aileen) Luo", "Lara Spencer",
            "Grant Bartolome Dowling", "Dave Gottlieb", "Rob Bassett",
        ]
    },
    "Anthropology": {
        "base_url": "https://anthropology.stanford.edu/people",
        "source_url": "https://anthropology.stanford.edu/people/graduate-students",
        "students": [
            "Huzaafa", "Noor Amr", "Gabriella Armstrong", "Paras Arora",
            "S. Gokce Atici", "Reem Badr", "Benjamin Baker", "Rachel Broun",
            "Miray Cakiroglu", "Samil Can", "Ronald Chen", "Alisha Elizabeth Cherian",
            "Deniz Demir", "Salma Elkhaoudi", "Carmen Ervin", "Eduard Fanthome",
            "Ayodele Foster-McCray", "Byron Gray", "Margaret Zhang Grobowski",
            "Emilia Groupp", "Shubhangni Gupta", "Tien-Dung Ha", "Rachael Healy",
            "Aaron Hopes", "Nina Dewi Toft Djanegara", "Shan Huang", "Saad Lakhani",
            "Jaime Landinez Aceros", "Khando Langri", "Jocelyn Lee",
            "Angela Leocata", "Zaith Lopez", "Stefania Manfio", "Aaron Mascarenhas",
            "Kristin McFadden", "Richard McGrail", "Mercedes Martinez Milantchi",
            "Jameelah Imani Morris", "Bilal Nadeem", "Jose-Alberto Navarro",
            "Shikha Nehra", "Shantanu Nevrekar", "Teathloach Wal Nguot",
            "Gabriela Oppitz", "Sunidhi Pacharne", "Matthew Padgett",
            "Victor Manuel Marquez Padrenan", "Ryan Michael Penney",
            "Benjamin Trujillo Perez", "Venolia Rabodiba", "Poornima Rajeshwar",
            "Valentina Ramia", "Elliott Reichardt", "Alexa Russo",
            "Esteban Salmon Perrilliat", "Isabel M. Salovaara", "Chen Shen",
            "Haoran Shi", "Utsavi Singh", "Juliet Tempest", "Mahder Takele Teshome",
            "Weronika Tomczyk", "Zoe VanGelder", "Shandana Waheed",
            "Chun-Yu Wang", "Shan Yang", "Daniel Yi", "Syed Ali Mehdi Zaidi",
            "Adela Zhang",
        ]
    },
}

# Read existing CSV
existing = {}
with open("/Users/jaiashar/Documents/VoraBusinessFinder/stanford_dept_emails.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        key = (row["name"], row["department"])
        existing[key] = row

# First, clean up generic department emails
generic_emails = ['sociology@stanford.edu', 'econ-undergrad@stanford.edu']
cleaned = 0
for key, row in existing.items():
    if row["email"] in generic_emails:
        row["email"] = ""
        cleaned += 1
print(f"Cleaned {cleaned} generic department emails")

# Fetch remaining departments
found_count = 0
total_fetched = 0

for dept_name, dept_info in departments.items():
    print(f"\n--- {dept_name} ({len(dept_info['students'])} students) ---")
    for student_name in dept_info["students"]:
        profile_url = slug_from_name(student_name, dept_info["base_url"])
        html = fetch_page(profile_url)
        total_fetched += 1
        
        email = extract_email(html) if html else None
        
        key = (student_name, dept_name)
        if email and key in existing:
            existing[key]["email"] = email
            found_count += 1
            print(f"  ✓ {student_name}: {email}")
        
        if total_fetched % 10 == 0:
            time.sleep(0.5)

print(f"\nFound {found_count} new emails from {total_fetched} profile pages")

# Write updated CSV
output_path = "/Users/jaiashar/Documents/VoraBusinessFinder/stanford_dept_emails.csv"
rows = list(existing.values())
with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["email", "name", "department", "source_url"])
    writer.writeheader()
    writer.writerows(rows)

# Final summary
total = len(rows)
with_email = sum(1 for r in rows if r["email"])

depts = {}
for r in rows:
    d = r["department"]
    if d not in depts:
        depts[d] = {"total": 0, "with_email": 0}
    depts[d]["total"] += 1
    if r["email"]:
        depts[d]["with_email"] += 1

print(f"\n=== Final Summary ===")
print(f"Total entries: {total}")
print(f"With @stanford.edu email: {with_email}")
print(f"\nDepartment breakdown:")
for dept, counts in sorted(depts.items()):
    print(f"  {dept}: {counts['total']} students ({counts['with_email']} with email)")
print(f"\nSaved to: {output_path}")
