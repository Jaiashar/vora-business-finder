#!/usr/bin/env python3
"""Push round 4 (chemistry, physics, psych, lifesci, engineering) to Supabase."""

import json, urllib.request, urllib.error, ssl, os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

NEW_EMAILS = [
    # ═══ Kang Research Group (Physics) ═══
    ("rqk@g.ucla.edu", "Robert Kao", "Kang Group (Physics)", "student"),
    ("dpadi022@g.ucla.edu", "Diego Padilla", "Kang Group (Physics)", "student"),
    ("peternguyen0128@g.ucla.edu", "Peter Nguyen", "Kang Group (Physics)", "student"),
    ("maxzhang2002@g.ucla.edu", "Congyue Zhang", "Kang Group (Physics)", "student"),
    # ═══ Bozovic Lab (Physics) ═══
    ("metzler@g.ucla.edu", "Charles Metzler-Winslow", "Bozovic Lab (Physics)", "student"),
    ("gmunozhe@g.ucla.edu", "Gabriela Munoz-Hernandez", "Bozovic Lab (Physics)", "student"),
    # ═══ Physics VR Group ═══
    ("andrewsu485@g.ucla.edu", "Andrew Su", "Physics VR Group", "student"),
    ("nathanjoshua@g.ucla.edu", "Nathan Joshua", "Physics VR Group", "student"),
    ("bjobilal@g.ucla.edu", "Benjamin Jobilal", "Physics VR Group", "student"),
    ("aryanmp@g.ucla.edu", "Aryan Pour", "Physics VR Group", "student"),
    ("oratzkoff@g.ucla.edu", "Omri Ratzkoff", "Physics VR Group", "student"),
    # ═══ Kwon Lab (Chem) ═══
    ("ohyunk@g.ucla.edu", "Ohyun Kwon", "Kwon Lab (Chem)", "student"),
    # ═══ Barber Lab extras ═══
    ("rgarrity@ucla.edu", "Rose Garrity", "Barber Lab (Chem)", "student"),
    ("tylermartinez1@ucla.edu", "Tyler Martinez", "Barber Lab (Chem)", "student"),
    # ═══ Virus Group extra ═══
    ("nharpell@ucla.edu", "Nina Harpell", "Virus Group (Chem)", "student"),
    # ═══ Rissman Memory Lab (Psych) ═══
    ("dymoon@g.ucla.edu", "Da Yeoun Moon", "Rissman Lab (Psych)", "student"),
    ("smwalters@g.ucla.edu", "Samantha Walters", "Rissman Lab (Psych)", "student"),
    ("fpeck@g.ucla.edu", "Fleming Peck", "Rissman Lab (Psych)", "student"),
    ("stephaniewert@g.ucla.edu", "Stephanie Wert", "Rissman Lab (Psych)", "student"),
    ("jonmorrow@g.ucla.edu", "Jonathan Morrow", "Rissman Lab (Psych)", "student"),
    ("npallishassani@g.ucla.edu", "Natalia Pallis-Hassani", "Rissman Lab (Psych)", "student"),
    ("juliapratt@g.ucla.edu", "Julia Pratt", "Rissman Lab (Psych)", "student"),
    # ═══ Bjork Lab (Psych) ═══
    ("jbrabec@ucla.edu", "Jordan Brabec", "Bjork Lab (Psych)", "student"),
    ("meganimundo@ucla.edu", "Megan Imundo", "Bjork Lab (Psych)", "student"),
    ("dmurphy8@ucla.edu", "Dillon Murphy", "Bjork Lab (Psych)", "student"),
    ("ashleychen1@g.ucla.edu", "Ashley Chen", "Bjork Lab (Psych)", "student"),
    # ═══ Castel Lab (Psych) ═══
    ("albertsk@g.ucla.edu", "Kylie Alberts", "Castel Lab (Psych)", "student"),
    ("karadefrost@g.ucla.edu", "Karina Agadzhanyan", "Castel Lab (Psych)", "student"),
    ("ekandel1@g.ucla.edu", "Emma Kandel", "Castel Lab (Psych)", "student"),
    # ═══ Huo Lab extras (Psych) ═══
    ("demaree@ucla.edu", "Tanner Demaree", "Huo Lab (Psych)", "student"),
    ("moreu@ucla.edu", "Gil Moreu", "Huo Lab (Psych)", "student"),
    # ═══ Shih Lab (Psych) ═══
    ("makotanaka@ucla.edu", "Sophie Mako Tanaka", "Shih Lab (Psych)", "student"),
    ("ghigginbotham@ucla.edu", "Gerald Higginbotham", "Shih Lab (Psych)", "student"),
    # ═══ Knowlton Lab (Psych) ═══
    ("corfran001@g.ucla.edu", "Corinna Franco", "Knowlton Lab (Psych)", "student"),
    ("dstaley@g.ucla.edu", "Donni Staley", "Knowlton Lab (Psych)", "student"),
    # ═══ Grether Lab (EEB) ═══
    ("sachancellor@g.ucla.edu", "Stephanie Chancellor", "Grether Lab (EEB)", "student"),
    ("janinef@g.ucla.edu", "Janine Fischer", "Grether Lab (EEB)", "student"),
    ("mlhopkins@g.ucla.edu", "Eugene Hopkins", "Grether Lab (EEB)", "student"),
    ("mezuercher@g.ucla.edu", "Madeleine Zuercher", "Grether Lab (EEB)", "student"),
    # ═══ Lloyd-Smith Lab (EEB) ═══
    ("scardenas1@g.ucla.edu", "Santiago Cardenas", "Lloyd-Smith Lab (EEB)", "student"),
    # ═══ Jacobsen Lab (MCDB) ═══
    ("trevorweiss@g.ucla.edu", "Trevor Weiss", "Jacobsen Lab (MCDB)", "student"),
    ("ly22@g.ucla.edu", "Ye Liu", "Jacobsen Lab (MCDB)", "student"),
    ("lucasjar@g.ucla.edu", "Lucas Jarry", "Jacobsen Lab (MCDB)", "student"),
    ("arjunojha@g.ucla.edu", "Arjun Ojha", "Jacobsen Lab (MCDB)", "student"),
    ("amerasekera@g.ucla.edu", "Jasmine Amerasekera", "Jacobsen Lab (MCDB)", "student"),
    ("yuxingzhou@g.ucla.edu", "Yuxing Zhou", "Jacobsen Lab (MCDB)", "student"),
    ("runtianwu@g.ucla.edu", "Runtian Wu", "Jacobsen Lab (MCDB)", "student"),
    ("justinrwei@g.ucla.edu", "Justin Wei", "Jacobsen Lab (MCDB)", "student"),
    ("yerin173@g.ucla.edu", "Janice Shin", "Jacobsen Lab (MCDB)", "student"),
    ("badubofour@g.ucla.edu", "Benjamin Adubofour", "Jacobsen Lab (MCDB)", "student"),
    ("sfiliz@g.ucla.edu", "Selin Filiz", "Jacobsen Lab (MCDB)", "student"),
    ("lucyshi1218@g.ucla.edu", "Lucy Shi", "Jacobsen Lab (MCDB)", "student"),
    ("kkerrychen@g.ucla.edu", "Kerry Chen", "Jacobsen Lab (MCDB)", "student"),
    ("aliceingelsson@g.ucla.edu", "Alice Ingelsson", "Jacobsen Lab (MCDB)", "student"),
    ("maddyhoekstra11@g.ucla.edu", "Madeline Hoekstra", "Jacobsen Lab (MCDB)", "student"),
    ("ruthafriat@g.ucla.edu", "Rutie Afriat", "Jacobsen Lab (MCDB)", "student"),
    ("kendallc437@g.ucla.edu", "Kendall Calderon", "Jacobsen Lab (MCDB)", "student"),
    ("sabinodelacueva@g.ucla.edu", "Sabino De La Cueva", "Jacobsen Lab (MCDB)", "student"),
    ("graceguo5@g.ucla.edu", "Grace Guo", "Jacobsen Lab (MCDB)", "student"),
    ("peterwang@g.ucla.edu", "Peter Wang", "Jacobsen Lab (MCDB)", "student"),
    ("tatekim@g.ucla.edu", "Tate Kim", "Jacobsen Lab (MCDB)", "student"),
    ("vinaypanchal106@g.ucla.edu", "Vinay Panchal", "Jacobsen Lab (MCDB)", "student"),
    ("alexburd@g.ucla.edu", "Alexander Burd", "Jacobsen Lab (MCDB)", "student"),
    ("qinliyun@g.ucla.edu", "Maggie Qin", "Jacobsen Lab (MCDB)", "student"),
    ("charlesmatthews@g.ucla.edu", "Max Matthews", "Jacobsen Lab (MCDB)", "student"),
    ("adelineh7@g.ucla.edu", "Adeline Hung", "Jacobsen Lab (MCDB)", "student"),
    ("xbohao@g.ucla.edu", "Bohao Xu", "Jacobsen Lab (MCDB)", "student"),
    ("mkkamalu@ucla.edu", "Maris Kamalu", "Jacobsen Lab (MCDB)", "student"),
    ("charlizechoo@ucla.edu", "Charlize Choo", "Jacobsen Lab (MCDB)", "student"),
    ("noahsteinmetz@ucla.edu", "Noah Steinmetz", "Jacobsen Lab (MCDB)", "student"),
    ("nickkraemer@ucla.edu", "Nicholas Kraemer", "Jacobsen Lab (MCDB)", "student"),
    ("kaylanchang@ucla.edu", "Kaylan Chang", "Jacobsen Lab (MCDB)", "student"),
    ("eroshannai@ucla.edu", "Elnaz Roshannai", "Jacobsen Lab (MCDB)", "student"),
    ("jesspowell13@ucla.edu", "Jessica Powell", "Jacobsen Lab (MCDB)", "student"),
    ("jinheelee22@ucla.edu", "Jinhee Lee", "Jacobsen Lab (MCDB)", "student"),
    ("natalielin73@ucla.edu", "Natalie Lin", "Jacobsen Lab (MCDB)", "student"),
    ("tiaramonemi@ucla.edu", "Tiara Monemi", "Jacobsen Lab (MCDB)", "student"),
    # ═══ Goldberg Lab (MCDB) ═══
    ("jiaxinli852@g.ucla.edu", "Jiaxin Li", "Goldberg Lab (MCDB)", "student"),
    ("oliviabielskis@g.ucla.edu", "Olivia Bielskis", "Goldberg Lab (MCDB)", "student"),
    ("gschoenbaum@g.ucla.edu", "Gwyneth Schoenbaum", "Goldberg Lab (MCDB)", "student"),
    # ═══ Tobin Lab (MCDB) ═══
    ("eleon@ucla.edu", "Ernesto Leon", "Tobin Lab (MCDB)", "student"),
    ("alozano7@ucla.edu", "Ana Laura Lozano", "Tobin Lab (MCDB)", "student"),
    ("ssschan@ucla.edu", "Chan Shing Sun", "Tobin Lab (MCDB)", "student"),
    # ═══ Lloyd-Smith Lab extras ═══
    ("eonokerns@ucla.edu", "Erika Ono-Kerns", "Lloyd-Smith Lab (EEB)", "student"),
    ("kcschott@ucla.edu", "Kristie Schott", "Lloyd-Smith Lab (EEB)", "student"),
]

print(f"Total new emails to push: {len(NEW_EMAILS)}")

url = f"{SUPABASE_URL}/rest/v1/college_contacts"
headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

inserted = 0
dupes = 0
errors = 0

for email, name, dept, role in NEW_EMAILS:
    row = {
        "email": email.lower().strip(),
        "name": name,
        "department": dept,
        "role": role,
        "university": "UCLA",
        "source_url": "agent_discovery_round4",
        "segment": "grad_student" if "@g.ucla.edu" in email.lower() else "student",
    }
    data = json.dumps([row]).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method='POST')
    try:
        resp = urllib.request.urlopen(req, context=SSL_CTX)
        inserted += 1
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        if 'duplicate' in body.lower() or '23505' in body:
            dupes += 1
        else:
            errors += 1
            if errors <= 3:
                print(f"  Error for {email}: {body[:100]}")

print(f"\nInserted: {inserted}, Dupes: {dupes}, Errors: {errors}")

count_headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Prefer": "count=exact"}
req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/college_contacts?select=id&university=eq.UCLA", headers=count_headers)
resp = urllib.request.urlopen(req, context=SSL_CTX)
print(f"\n=== SUPABASE TOTALS ===")
print(f"Total UCLA contacts: {resp.getheader('Content-Range')}")

req2 = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/college_contacts?select=id&role=eq.student", headers=count_headers)
resp2 = urllib.request.urlopen(req2, context=SSL_CTX)
print(f"Students: {resp2.getheader('Content-Range')}")
