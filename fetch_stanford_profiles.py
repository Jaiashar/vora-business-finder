#!/usr/bin/env python3
"""
Fetch individual Stanford graduate student profile pages to extract emails.
Uses the [at] pattern found on individual profile pages.
"""

import csv
import re
import time
import urllib.request
import urllib.error
import ssl

# Disable SSL verification for simplicity
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def fetch_page(url, retries=2):
    """Fetch a page and return its content."""
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
    """Extract email from Stanford profile page."""
    if not html:
        return None
    # Pattern 1: Obfuscated with span tags: <span class="u">username</span> [at] <span class="d">stanford.edu</span>
    match = re.search(r'<span[^>]*>([^<]+)</span>\s*\[at\]\s*<span[^>]*>stanford\.edu</span>', html)
    if match:
        username = match.group(1).strip()
        return f"{username}@stanford.edu"
    # Pattern 2: Plain text "username [at] stanford.edu"
    match = re.search(r'(\w[\w.-]*)\s*\[at\]\s*stanford\.edu', html)
    if match:
        return f"{match.group(1)}@stanford.edu"
    # Pattern 3: Direct mailto: links
    match = re.search(r'mailto:([\w.-]+@stanford\.edu)', html)
    if match:
        return match.group(1)
    return None

def slug_from_name(name, dept_url):
    """Create profile URL slug from name."""
    # Clean the name
    clean = name.lower()
    # Remove parenthetical parts like "(Sharon)" or "(Cedric)"
    clean = re.sub(r'\([^)]*\)', '', clean)
    # Remove periods
    clean = clean.replace('.', '')
    # Remove special chars but keep hyphens and spaces
    clean = re.sub(r"[''']", '', clean)
    # Replace spaces with hyphens
    clean = clean.strip()
    clean = re.sub(r'\s+', '-', clean)
    # Remove double hyphens
    clean = re.sub(r'-+', '-', clean)
    return f"{dept_url}/{clean}"

# Define departments with students who need email lookups
departments = {
    "Political Science": {
        "base_url": "https://politicalscience.stanford.edu/people",
        "source_url": "https://politicalscience.stanford.edu/people/graduate-students",
        "students": [
            "Shirin Abrishami Kashani", "Ameze Belo-Osagie", "Liam Bethlendy",
            "Justin Braun", "Rick Brown", "Christopher Buckley", "Luka Bulic Braculj",
            "Dominic Bustillos", "Natalie Chaudhuri", "Alicia Chen", "Joseph Cloward",
            "Madison Dalton", "Chris Dann", "Sierra Davis Thomander", "Marcus Ellinas",
            "Davi Ferreira Veronese", "Emerald Fikejs", "Chris Flores", "Hanna Folsz",
            "Yiqin Fu", "Morgan Gillespie", "Paige Hill", "Kazumi Hoshino-Macdonald",
            "Qianmin Hu", "Naiyu Jiang", "Jonathan Kay", "Rabia Kutlu Karasu",
            "Douglas Leonard", "Sunny Li", "Xiaoxiao Li", "Yunchong Ling",
            "Jiehan Liu", "Victoria Liu", "Sebastian Lucek", "Mae MacDonald",
            "Maddie Materna", "Alexandra Minsk", "Andrew Myers", "Vladimir Novikov",
            "Malloy Owen", "Xinru Pan", "Peter Park", "Natasha Patel",
            "Alexander Pumerantz", "Abhinav Ramaswamy", "Kasey Rhee", "Luz Rodriguez",
            "Emily Russell", "Elijah Scott", "Sandro Sharashenidze", "Simon Siskel",
            "Alena Smith", "Mahda Soltani", "Aaron Spikol", "Orane Steffann",
            "Chloe Stowell", "Nico Studen", "Johannes Stupperich", "Miryea Sturdivant",
            "Yongkang Tai", "Cole Tanigawa-Lau", "Bryan Terrazas", "Phedias Theophanous",
            "Michael Thomas", "Naomi Tilles", "Kesley Townsend", "Natalia Vasilenok",
            "Andy Wang", "Abrianna Wieland", "Brian Wu", "Jennifer Wu", "Victor Wu",
            "Zihan Xie", "Shun Yamaya", "Jake Yeager",
        ]
    },
    "Economics": {
        "base_url": "https://economics.stanford.edu/people",
        "source_url": "https://economics.stanford.edu/people/graduate-students",
        "students": [
            "Mohamad Adhami", "Mert Akan", "Romain Paul Angotti", "Sumhith Aradhyula",
            "Tina Aubrun", "Adrian Blattner", "Sarah Bogl", "John Bonney",
            "Miguel Borrero Ridaura", "Lea Bottmer", "Lorenzo Bruno", "Shelby Buckman",
            "Alvaro Calderon", "Nick Cao", "Carlos Alberto Belchior Doria Carneiro",
            "Luiz Guilherme Carpizo Fernandes Costa", "Renan Chaves Yoshida",
            "Camilla Cherubini", "Lautaro Chittaro", "Mihai Codreanu", "Juliette Coly",
            "Rafael Costa Berriel Abreu", "Kathryn Cranney", "Michael Crystal",
            "Bruno Dare Riotto Malta Campos", "Ben Davies", "Dante Domenella",
            "Maya Elise Donovan", "Maya Durvasula", "Anna Carolina Dutra Saraiva",
            "Lavar Edmonds", "Jonas Enders", "Corey Feldman", "Leticia Fernandes",
            "Tomer Fidelman", "Evan Flack", "Giacomo Fraccaroli",
            "Asia-Kim Francavilla", "Joao Francisco Pugliese", "Rebecca Frost",
            "Kyra Frye", "Vitor Furtado Farias", "Gaston Garcia Zavaleta",
            "Samira Gholami", "Nick Grasley", "Joshua Gross", "Mariana Guido",
            "Sirig Gurung", "Alexander Haberman", "James Han", "Danielle Handel",
            "Alexander Hansen", "Lauren Harris", "Jonathan Hartley",
            "Lilian Abdul Roberto Hartmann", "Calvin He", "Gregor Heilborn",
            "Florencia Hnilo", "Katja Hofmann", "Zong Huang", "Robert Huang",
            "Thibault Ingrand", "Helen Kissel", "Augustus Kmetz", "Akhila Kovvuri",
            "Joanna Krysta", "Emmanuella Kyei Manu", "Reiko Laski", "Eva Lestant",
            "Marta Leva", "Kevin Michael Li", "Mukun (Will) Liu", "Bing Liu",
            "Manuela Magalhaes", "Henry Manley", "Federico Marciano", "Olivia Martin",
            "Tamri Matiashvili", "Marco Alejandro Medina Salgado", "Carl Meyer",
            "Gideon Moore", "Brendan Moore", "Cecilia Moreira", "Isabel Munoz",
            "Yailin Navarro", "Janelle Nelson", "Taryn O'Connor", "Flint O'Neil",
            "Alexia Olaizola", "Francesca Pagnotta", "Spencer Pantoja", "Marco Panunzi",
            "Julia Park", "Alexis Payne", "Silvia Pedersoli", "Bianca Piccirillo",
            "Rio Popper", "Vlasta Rasocha", "Gabriela Rays Wahba", "Mary Reader",
            "Kate Reinmuth", "Hector Gabriel Reyes Figueroa", "Peter Robertson",
            "Helena Roy", "Otavio Rubiao", "Thomas Rutter", "Omer Faruk Sahin",
            "Ben Sampson", "Anirudh Sankar", "Stuti Saria", "Nicholas Scott-Hearn",
            "Marcelo Sena", "Martin Serramo", "Jack Shane", "Yash Singh",
            "Anand Siththaranjan", "Tess Snyder", "Edwin Song", "Janet Stefanov",
            "Sahana Subramanyam", "Grace Elizabeth Sventek", "Tomas Tapak",
            "Zahra Thabet", "Samuel Thau", "Monia Tomasella", "Alex Tordjman",
            "Juan David Torres", "Santiago Varela Seoane", "Anna Vdovina",
            "Amar Venugopal", "Sarah Vicol", "Elena Vollmer", "Angie Wang",
            "Crystal Huayang Wang", "Lucas Warwar", "Jason Weitze", "Sam Wycherley",
            "Roshie Xing", "David Xu", "Chris Xue", "Serdil Tinda Yalcin",
            "Ni Yan", "Tamar Yerushalmi", "Wendy Yin", "Justin Young",
            "Pedro Henrique Zecchin Costa", "Qiyi Zhao",
        ]
    },
    "Sociology": {
        "base_url": "https://sociology.stanford.edu/people",
        "source_url": "https://sociology.stanford.edu/people/phd-students",
        "students": [
            "Madeline Anderson", "Giora Ashkenazi", "Lorena Aviles Trujillo",
            "David Sebastian Broska", "Michelle Casas", "Emma Casey",
            "Michael Cerda-Jara", "Ariel Chan", "Alex Chow", "Britiny Cook",
            "Lizzie Deneen", "Allex Desronvil", "Eve Dill", "Daniella Efrat",
            "Terresa Eun", "Monica Gao", "Haley M. Gordon", "Daniel Grubbs-Donovan",
            "Nya Kai Hardaway", "Angela He", "Tessa Holtzman", "Amy Casselman-Hontalas",
            "Tianhao Hou", "Swan Htut", "Lisa Hummel", "Isabel Iturrios-Fourzan",
            "Olivia Jin", "Emily Johnson", "Elisa Kim", "Haesol Kim", "Hye Jee Kim",
            "Louis Liang-Yu Ko", "Elizabeth Kuhlman", "Shihao Li", "Qiwei Lin",
            "Kelly Liu", "Renee Louis", "Erin Macke", "Tyler W. McDaniel",
            "Brenden McKinney", "Joe Mernyk", "Caylin Louis Moore",
            "Tanajia Moye-Green", "Colin Peterson", "Rosina Pradhananga",
            "Hanzhang Ren", "Kassandra Roeser", "Nick Sherefkin", "Samantha Sheridan",
            "Sheridan Stewart", "Yuze Sui", "Helen Webley-Brown", "Maleah Webster",
            "Emma Williams-Baron", "Yao Xu", "Marisol Zarate", "Iris Zhang",
        ]
    },
    "History": {
        "base_url": "https://history.stanford.edu/people",
        "source_url": "https://history.stanford.edu/people/graduate-students",
        "students": [
            "Lindsay Allebest", "Nesi Altaras", "Alara Aygen", "Mathew Ayodele",
            "Eva Baudler", "Farah Bazzi", "Katherine Booska", "Margaret Borozan",
            "Yeseul Byeon", "Alina Bykova", "Linxi Cai", "Mariana Calvo",
            "Luther Cox Cenci", "Bhavya Chauhan", "Yi-Ting Chung", "Austin Clements",
            "Amanda Coate", "Jon Cooper", "Federico Cortigiani", "Emre Can Daglioglu",
            "Nina de Meira Borba", "Marina Del Cassio", "Jennifer Depew",
            "Nathan Deschamps", "Ozgur Dikmen", "Aliyah Dunn-Salahuddin",
            "Yanling (Sharon) Feng", "Max Fennell-Chametzky", "Julia Fine",
            "Kelly (Keming) Fu", "Zoe Gioja", "Mahishan Gnanaseharan",
            "Daniela Goodman Rabner", "Emily Bradley Greenfield",
            "Muhammad Haram Gulzar", "Mustafa Gunaydi", "Kayra Guven",
            "Mehdi Hakimi", "Anwar Haneef", "Kyle Harmse", "Ciel Haviland",
            "Nathaniel Hay", "Yuki Hoshino", "Xincheng Hou", "Jackson Huston",
            "Borys Jastrzebski", "Baird Johnson", "Emerson Johnston",
            "Hannah Johnston", "Karventha Kuppusamy", "Joaquin Lara Midkiff",
            "Matthew Levine", "Yoav Levinson-Sela", "Nathan J. Lilje",
            "Eric Lindheim-Marx", "Ellie Luchini", "Courtney MacPhee",
            "Audrey Martel-Dion", "Bailey Martin", "Makena Mezistrano",
            "Marsha Morabu", "Janice Ndegwa", "Ana C. Nunez", "Jackie Olson",
            "Ozgul Ozdemir", "Gabriel Panuco-Mercado", "William Parish IV",
            "Fyza Parviz Jazra", "Ayesha Pasha", "Olavo Passos de Souza",
            "Miri Powell", "Preetam Prakash", "Christian Robles-Baez",
            "Aaron Schimmel", "Sonya Schoenberger", "Serena Shah", "Bella Shahani",
            "Matthew Signer", "Gillian Smith", "Hong Song", "Lucy Stark",
            "Adele Leigh Stock", "Joshua Tapper",
        ]
    },
}

# Read existing CSV to merge
existing = {}
with open("/Users/jaiashar/Documents/VoraBusinessFinder/stanford_dept_emails.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        key = (row["name"], row["department"])
        existing[key] = row

print("Fetching individual profile pages for emails...")
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
        elif not email:
            # Try alternate URL patterns
            pass
        
        # Be polite - small delay
        if total_fetched % 10 == 0:
            time.sleep(0.5)

print(f"\n\nFound {found_count} new emails from {total_fetched} profile pages")

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
print(f"\nUpdated CSV: {total} entries, {with_email} with email")
print(f"Saved to: {output_path}")
