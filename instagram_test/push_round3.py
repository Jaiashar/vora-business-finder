#!/usr/bin/env python3
"""Push round 3 lab discoveries to Supabase."""

import json, urllib.request, urllib.error, ssl, os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

# All new emails from round 3 agent discoveries
NEW_EMAILS = [
    # ═══ Pellegrini Lab (MCDB) ═══
    ("wbguo@g.ucla.edu", "Wenbin Guo", "Pellegrini Lab (MCDB)", "student"),
    ("aespinoza01@g.ucla.edu", "Alejandro Espinoza", "Pellegrini Lab (MCDB)", "student"),
    ("lajoycemboning@g.ucla.edu", "Lajoyce Mboning", "Pellegrini Lab (MCDB)", "student"),
    ("mvflores2026@g.ucla.edu", "Maria Flores", "Pellegrini Lab (MCDB)", "student"),
    ("ronanbennett@g.ucla.edu", "Ronan Bennett", "Pellegrini Lab (MCDB)", "student"),
    ("davidlopez@g.ucla.edu", "David Lopez", "Pellegrini Lab (MCDB)", "student"),
    ("feimanh@g.ucla.edu", "Fei-man Hsu", "Pellegrini Lab (MCDB)", "student"),
    # ═══ Johnson Lab (MCDB) ═══
    ("amitpaul@g.ucla.edu", "Amit Paul", "Johnson Lab (MCDB)", "student"),
    ("nidhikumari@g.ucla.edu", "Nidhi Kumari", "Johnson Lab (MCDB)", "student"),
    ("emmavandal@g.ucla.edu", "Emma Vandal", "Johnson Lab (MCDB)", "student"),
    ("jmeshreky13@g.ucla.edu", "Jeremiah Meshreky", "Johnson Lab (MCDB)", "student"),
    ("nichelle25@g.ucla.edu", "Nichelle Jefferson", "Johnson Lab (MCDB)", "student"),
    # ═══ Torres Lab (Chem) ═══
    ("dew001@g.ucla.edu", "Dennis Wang", "Torres Lab (Chem)", "student"),
    ("bellayang@g.ucla.edu", "Isabella Yang", "Torres Lab (Chem)", "student"),
    ("slanch@g.ucla.edu", "Sandy Lanch", "Torres Lab (Chem)", "student"),
    ("yaishwarya@g.ucla.edu", "Aishwarya Yuvaraj", "Torres Lab (Chem)", "student"),
    ("ccnocon@g.ucla.edu", "Claire Nocon", "Torres Lab (Chem)", "student"),
    # ═══ Guo Lab (MBI/BioChem) ═══
    ("ehaufpisoni@g.ucla.edu", "Elena Hauf Pisoni", "Guo Lab (BioChem)", "student"),
    ("dlsrb91356978@g.ucla.edu", "Kyu Yang", "Guo Lab (BioChem)", "student"),
    ("jmpi@g.ucla.edu", "Justin Pi", "Guo Lab (BioChem)", "student"),
    ("ethantai2@g.ucla.edu", "Ethan Tai", "Guo Lab (BioChem)", "student"),
    # ═══ Virus Group / Gelbart Lab (Chem) ═══
    ("noerod1317@g.ucla.edu", "Noe Rodriguez", "Virus Group (Chem)", "student"),
    ("taylordk@g.ucla.edu", "Daniel Kyle Taylor", "Virus Group (Chem)", "student"),
    ("matthewtsai12@g.ucla.edu", "Matthew Tsai", "Virus Group (Chem)", "student"),
    ("sjuang@g.ucla.edu", "Selina Juang", "Virus Group (Chem)", "student"),
    ("helmer1@g.ucla.edu", "Helen Elmer", "Virus Group (Chem)", "student"),
    # ═══ Sack Lab (EEB) ═══
    ("nicoandrade@g.ucla.edu", "Nico Andrade", "Sack Lab (EEB)", "student"),
    ("jegomez97@g.ucla.edu", "Jesse Gomez", "Sack Lab (EEB)", "student"),
    # ═══ Kraft Lab (EEB) ═══
    ("alcisneroscarey@g.ucla.edu", "Alexandra Cisneros Carey", "Kraft Lab (EEB)", "student"),
    ("hsearles2@g.ucla.edu", "Henri Searles", "Kraft Lab (EEB)", "student"),
    ("kmschneider@g.ucla.edu", "Kami Schneider", "Kraft Lab (EEB)", "student"),
    ("lglevanik@g.ucla.edu", "Lauren Glevanik", "Kraft Lab (EEB)", "student"),
    ("msupple@g.ucla.edu", "Maia Supple", "Kraft Lab (EEB)", "student"),
    ("msagarin@g.ucla.edu", "Maya Sagarin", "Kraft Lab (EEB)", "student"),
    # ═══ Terahertz Electronics Lab (ECE) ═══
    ("xinghej@g.ucla.edu", "Xinghe Jiang", "Terahertz Lab (ECE)", "student"),
    ("atabassum@g.ucla.edu", "Anika Tabassum", "Terahertz Lab (ECE)", "student"),
    ("tygan@g.ucla.edu", "Tianyi Gan", "Terahertz Lab (ECE)", "student"),
    ("sean821115@g.ucla.edu", "Szu-An Tsao", "Terahertz Lab (ECE)", "student"),
    ("sezumrat@g.ucla.edu", "Shahed-E-Zumrat", "Terahertz Lab (ECE)", "student"),
    ("nafizimtiaz@g.ucla.edu", "Nafiz Imtiaz", "Terahertz Lab (ECE)", "student"),
    # ═══ NTRG Lab (MAE) ═══
    ("hjkz3514@g.ucla.edu", "Min Jong Kil", "NTRG (MAE)", "student"),
    ("zwong888@g.ucla.edu", "Zachary Wong", "NTRG (MAE)", "student"),
    ("bheronimus@g.ucla.edu", "Benjamin Heronimus", "NTRG (MAE)", "student"),
    ("yandawang926@g.ucla.edu", "Yanda Wang", "NTRG (MAE)", "student"),
    ("dbpark2@g.ucla.edu", "Brian Park", "NTRG (ChemE)", "student"),
    ("priyanthelango@g.ucla.edu", "Priyanth Elango", "NTRG (MAE)", "student"),
    ("rafamcpr9@g.ucla.edu", "Rafael Maldonado Comas", "NTRG (MAE)", "student"),
    ("soyoungjo@g.ucla.edu", "Soyoung Jo", "NTRG (MAE)", "student"),
    ("jons0226@g.ucla.edu", "Jonathan Park", "NTRG (MAE)", "student"),
    ("sprak011@g.ucla.edu", "Snigdha Prakash", "NTRG (MAE)", "student"),
    ("sundiwin6612@g.ucla.edu", "Sundi Win", "NTRG (MAE)", "student"),
    ("jjunaedi10@g.ucla.edu", "Joanne Junaedi", "NTRG (MAE)", "student"),
    ("gabrielcenteno@g.ucla.edu", "Gabriel Centeno", "NTRG (MAE)", "student"),
    ("madsgar315@g.ucla.edu", "Madison Garcia", "NTRG (MAE)", "student"),
    ("lukasostien@g.ucla.edu", "Lukas Ostien", "NTRG (MAE)", "student"),
    ("aidenkg28@g.ucla.edu", "Aiden Georgiev", "NTRG (MAE)", "student"),
    ("jmoreno117@g.ucla.edu", "Jonathan Moreno", "NTRG (MAE)", "student"),
    ("watanabetomo@g.ucla.edu", "Tomo Watanabe", "NTRG (MAE)", "student"),
    # ═══ iOpticsLab (BioE) ═══
    ("zzang@g.ucla.edu", "Zihan Zang", "iOpticsLab (BioE)", "student"),
    ("shuqimu@g.ucla.edu", "Shuqi Mu", "iOpticsLab (BioE)", "student"),
    ("chenqinyi@g.ucla.edu", "Qinyi Chen", "iOpticsLab (BioE)", "student"),
    ("plapenda@g.ucla.edu", "Do Young Kim", "iOpticsLab (BioE)", "student"),
    ("wyq6@g.ucla.edu", "Yuanqin Wang", "iOpticsLab (BioE)", "student"),
    # ═══ DRL (ECE/MSE) ═══
    ("cheng991@g.ucla.edu", "Yang Cheng", "DRL (ECE)", "student"),
    ("malharbi@g.ucla.edu", "Majed Alharbi", "DRL (MSE)", "student"),
    ("huangpy@g.ucla.edu", "Puyang Huang", "DRL (ECE)", "student"),
    ("xido7265@g.ucla.edu", "Xiang Dong", "DRL (ECE)", "student"),
    ("anand7@g.ucla.edu", "Anand Johnson", "DRL (MSE)", "student"),
    # ═══ Nano Intelligent Systems (SEAS) ═══
    ("daweigao@g.ucla.edu", "Dawei Gao", "Nano Intelligent Sys (MAE)", "student"),
    ("yingruizhang21@g.ucla.edu", "Yingrui Zhang", "Nano Intelligent Sys (MAE)", "student"),
    ("allisonliu62@g.ucla.edu", "Allison Liu", "Nano Intelligent Sys (MAE)", "student"),
    # ═══ VAST Lab (CS) ═══
    ("jameszhang23@g.ucla.edu", "Jiahao Zhang", "VAST Lab (CS)", "student"),
    ("yufandu@g.ucla.edu", "Yufan Du", "VAST Lab (CS)", "student"),
    # ═══ Galvan Lab (Psych) ═══
    ("jasminevh@g.ucla.edu", "Jasmine Hernandez", "Galvan Lab (Psych)", "student"),
    # ═══ Shams Lab extras (Psych) ═══
    ("skylartsai22@g.ucla.edu", "Skylar Tsai", "Shams Lab (Psych)", "student"),
    ("kimtyoun8849@g.ucla.edu", "Brian Kim", "Shams Lab (Psych)", "student"),
    ("joedibernardo@g.ucla.edu", "Joe DiBernardo", "Shams Lab (Psych)", "student"),
    ("chatrir2003@g.ucla.edu", "Chatri Rajapaksha", "Shams Lab (Psych)", "student"),
    ("jonathanj0321@g.ucla.edu", "Jonathan Jiang", "Shams Lab (Psych)", "student"),
    # ═══ Castrellon Lab (Psych) ═══
    ("melanieruiz@g.ucla.edu", "Melanie Ruiz", "Castrellon Lab (Psych)", "student"),
    ("esha05@g.ucla.edu", "Esha Dadbhawala", "Castrellon Lab (Psych)", "student"),
    # ═══ Huo Lab (Psych) ═══
    ("alexairgonzalez@g.ucla.edu", "Alexair Gonzalez", "Huo Lab (Psych)", "student"),
    # ═══ Sociology extras ═══
    ("cmabel16@g.ucla.edu", "Charlotte Abel", "Sociology", "student"),
    ("taquino7@g.ucla.edu", "Taylor Aquino", "Sociology", "student"),
    ("prashastib@g.ucla.edu", "Prashasti Bhatnagar", "Sociology", "student"),
    ("nbluth@g.ucla.edu", "Natasha Bluth", "Sociology", "student"),
    ("rileyceperich@g.ucla.edu", "Riley Ceperich", "Sociology", "student"),
    ("danchai@g.ucla.edu", "Dan Chai", "Sociology", "student"),
    ("achalfoun@g.ucla.edu", "Andrew Chalfoun", "Sociology", "student"),
    ("lcheeks@g.ucla.edu", "Lillian Cheeks", "Sociology", "student"),
    ("jcontino@g.ucla.edu", "Jason Contino", "Sociology", "student"),
    ("roxannecorbeil@g.ucla.edu", "Roxanne Corbeil", "Sociology", "student"),
    ("mcowhey@g.ucla.edu", "Maureen Cowhey", "Sociology", "student"),
    ("ccrooke@g.ucla.edu", "Catherine Crooke", "Sociology", "student"),
    ("ravendev@g.ucla.edu", "Raven Deverux", "Sociology", "student"),
    ("ndirago@g.ucla.edu", "Nicholas DiRago", "Sociology", "student"),
    ("duyanji@g.ucla.edu", "Yanji Du", "Sociology", "student"),
    ("vfloegel@g.ucla.edu", "Valentina Floegel", "Sociology", "student"),
    ("hflowers@g.ucla.edu", "Hilary Flowers", "Sociology", "student"),
    ("simasghaddar@g.ucla.edu", "Sima Ghaddar", "Sociology", "student"),
    ("dmgoodwin@g.ucla.edu", "Deja Goodwin", "Sociology", "student"),
    ("nathanihoff@g.ucla.edu", "Nathan Hoffmann", "Sociology", "student"),
    ("zepkalb@g.ucla.edu", "Zep Kalb", "Sociology", "student"),
    ("rkaufmanr@g.ucla.edu", "Rebecca Kaufman", "Sociology", "student"),
    ("skumar42@g.ucla.edu", "Surya Kumar", "Sociology", "student"),
    ("seraskwon@g.ucla.edu", "Sera Kwon", "Sociology", "student"),
    ("ktliao@g.ucla.edu", "Kristin Liao", "Sociology", "student"),
    ("madisoncliao@g.ucla.edu", "Madison Liao", "Sociology", "student"),
    ("mlopez2@g.ucla.edu", "Marina Lopez", "Sociology", "student"),
    ("mmarinello@g.ucla.edu", "Michelle Marinello", "Sociology", "student"),
    ("amandamorris@g.ucla.edu", "Amanda Morris", "Sociology", "student"),
    ("jcmurph@g.ucla.edu", "Joseph Murphy", "Sociology", "student"),
    # ═══ Urban Humanities ═══
    ("drodmora@g.ucla.edu", "Daniel Rodriguez Mora", "Urban Humanities", "student"),
    ("krt2129@g.ucla.edu", "Katherine Taylor-Hasty", "Urban Humanities", "student"),
    # ═══ Garg Lab new additions ═══
    ("georgiamscherer@g.ucla.edu", "Georgia Scherer", "Garg Lab (Chem)", "student"),
    ("allisonhands@g.ucla.edu", "Allison Hands", "Garg Lab (Chem)", "student"),
    ("danielturner@g.ucla.edu", "Daniel Turner", "Garg Lab (Chem)", "student"),
    ("zachgwalters@g.ucla.edu", "Zachary Walters", "Garg Lab (Chem)", "student"),
    ("ngilbertson@g.ucla.edu", "Noah Gilbertson", "Garg Lab (Chem)", "student"),
    ("sarahfrench@g.ucla.edu", "Sarah French", "Garg Lab (Chem)", "student"),
    ("mdeliaval@g.ucla.edu", "Marie Deliaval", "Garg Lab (Chem)", "student"),
    ("bsprague03@g.ucla.edu", "Breanna Sprague", "Garg Lab (Chem)", "student"),
    ("abbywilson@g.ucla.edu", "Abby Wilson", "Garg Lab (Chem)", "student"),
    ("kasey.chung@g.ucla.edu", "Kasey Chung", "Garg Lab (Chem)", "student"),
    ("amaarzs@g.ucla.edu", "Amaar Siddiqui", "Garg Lab (Chem)", "student"),
    # ═══ Arispe Lab (MCDB) ═══
    ("jmeshreky13@g.ucla.edu", "Jeremiah Meshreky", "Arispe Lab (MCDB)", "student"),
    # ═══ USAC Officers ═══
    ("president@usac.ucla.edu", "Diego Bollo", "USAC Student Government", "student_org"),
    ("ivp@usac.ucla.edu", "Tommy Contreras", "USAC Student Government", "student_org"),
    ("evp@usac.ucla.edu", "Sherry Zhou", "USAC Student Government", "student_org"),
    ("gr1@usac.ucla.edu", "Talia Davood", "USAC Student Government", "student_org"),
    ("gr2@usac.ucla.edu", "Jayha Buhs Jackson", "USAC Student Government", "student_org"),
    ("gr3@usac.ucla.edu", "Brett Berndt", "USAC Student Government", "student_org"),
    ("aac@usac.ucla.edu", "Cristopher Espino", "USAC Student Government", "student_org"),
    ("cec@usac.ucla.edu", "Daniel Leal", "USAC Student Government", "student_org"),
    ("csc@usac.ucla.edu", "Edison Chua", "USAC Student Government", "student_org"),
    ("culturalaffairs@usac.ucla.edu", "Divine Trewick", "USAC Student Government", "student_org"),
    ("fac@usac.ucla.edu", "Joy Huang", "USAC Student Government", "student_org"),
    ("fsc@usac.ucla.edu", "Nico Morrone", "USAC Student Government", "student_org"),
    ("swc@usac.ucla.edu", "Hannah Yip", "USAC Student Government", "student_org"),
    ("tsr@usac.ucla.edu", "Hyerim Yoon", "USAC Student Government", "student_org"),
    ("isr@usac.ucla.edu", "Keya Tanna", "USAC Student Government", "student_org"),
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
        "source_url": "agent_discovery_round3",
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

# Final totals
count_headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Prefer": "count=exact"}
req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/college_contacts?select=id&university=eq.UCLA", headers=count_headers)
resp = urllib.request.urlopen(req, context=SSL_CTX)
print(f"\nTotal UCLA contacts in Supabase: {resp.getheader('Content-Range')}")

req2 = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/college_contacts?select=id&role=eq.student", headers=count_headers)
resp2 = urllib.request.urlopen(req2, context=SSL_CTX)
print(f"Students: {resp2.getheader('Content-Range')}")

req3 = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/college_contacts?select=id&role=eq.student_org", headers=count_headers)
resp3 = urllib.request.urlopen(req3, context=SSL_CTX)
print(f"Student orgs: {resp3.getheader('Content-Range')}")
