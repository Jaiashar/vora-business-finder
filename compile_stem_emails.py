#!/usr/bin/env python3
"""
Compile all UCLA STEM lab emails collected from web scraping.
Combines automated scraper results with manually extracted data.
"""

import csv
import json

# Load existing emails to avoid duplicates
existing_emails = set()
for csv_file in ['ucla_psych_lab_emails.csv', 'ucla_seas_lab_emails.csv']:
    try:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_emails.add(row['email'].lower().strip())
    except FileNotFoundError:
        pass

print(f"Loaded {len(existing_emails)} existing emails to deduplicate against")

# All manually extracted data from web scraping sessions
all_contacts = []

# ============================================================
# QCB (Quantitative & Computational Biology) - from qcb.ucla.edu/people/
# ============================================================
qcb_contacts = [
    ("sbavisetty@g.ucla.edu", "Sai Bavisetty", "PhD Student", "QCB"),
    ("giodelv@g.ucla.edu", "Giorgia Del Vecchio", "PhD Student", "QCB"),
    ("xiaoluguo@g.ucla.edu", "Xiaolu Guo", "PhD Student", "QCB"),
    ("shreyasrajesh38@g.ucla.edu", "Shreyas Rajesh", "PhD Student", "QCB"),
    ("xingboshang@g.ucla.edu", "Xingbo Shang", "PhD Student", "QCB"),
    ("gmtong@g.ucla.edu", "Gregory Tong", "PhD Student", "QCB"),
    ("mweissman97@g.ucla.edu", "Maya Weissman", "PhD Student", "QCB"),
    ("wbguo@g.ucla.edu", "Wenbin Guo", "Researcher", "QCB"),
    ("haripriyavn@g.ucla.edu", "Haripriya Vaidehi Narayanan", "PhD Student", "QCB"),
    ("cgill22@g.ucla.edu", "Cameron Gill", "MS Student", "QCB"),
    ("kos@g.ucla.edu", "Seyoon Ko", "PhD Student", "QCB"),
    ("panliu23@g.ucla.edu", "Pan Liu", "PhD Student", "QCB"),
    ("cokus@ucla.edu", "Shawn Cokus", "Researcher", "QCB"),
    ("kkaczor@ucla.edu", "Karolina Kaczor-Urbanowicz", "Researcher", "QCB"),
    ("menglinli@ucla.edu", "Menglin Li", "Researcher", "QCB"),
    ("ivyxiong@ucla.edu", "Ivy Xiong", "PhD Student", "QCB"),
    ("fmxie@ucla.edu", "Fangming Xie", "PhD Student", "QCB"),
    ("kcjorgensen@ucla.edu", "Kelsey Jorgensen", "PhD Student", "QCB"),
    ("smha118@ucla.edu", "Daniel Ha", "PhD Student", "QCB"),
    ("feimanh@ucla.edu", "Fei-Man Hsu", "PhD Student", "QCB"),
    ("giovas@ucla.edu", "Giovanni Quinones Valdez", "Researcher", "QCB"),
    ("matteop@mcdb.ucla.edu", "Matteo Pellegrini", "Faculty", "QCB"),
    ("eloylopez@lifesci.ucla.edu", "Eloy Lopez", "Staff", "QCB"),
    ("apalermo@mednet.ucla.edu", "Amelia Palermo", "Researcher", "QCB"),
    ("lukasz@mbi.ucla.edu", "Lukasz Salwinski", "Researcher", "QCB"),
    ("arudas@mednet.ucla.edu", "Akos Rudas", "PhD Student", "QCB"),
    ("xianglongtan@mednet.ucla.edu", "Xianglong Tan", "PhD Student", "QCB"),
    ("wyan@chem.ucla.edu", "Weihong Yan", "Researcher", "QCB"),
]

# ============================================================
# IPAM (Institute for Pure & Applied Math)
# ============================================================
ipam_contacts = [
    ("kklowden@ipam.ucla.edu", "K. Klowden", "Staff", "IPAM"),
    ("cratsch@ipam.ucla.edu", "C. Ratsch", "Staff", "IPAM"),
    ("sbanuelos@ipam.ucla.edu", "S. Banuelos", "Staff", "IPAM"),
    ("shlyakht@ipam.ucla.edu", "S. Shlyakhtenko", "Staff", "IPAM"),
    ("tao@math.ucla.edu", "T. Tao", "Faculty", "IPAM"),
    ("afain@ipam.ucla.edu", "Ann Fain", "Staff", "IPAM"),
    ("irizov@ipam.ucla.edu", "Ivan Rizov", "Staff", "IPAM"),
    ("jwan@ipam.ucla.edu", "Jun Wan", "Staff", "IPAM"),
    ("rbartlebaugh@ipam.ucla.edu", "Rich Bartlebaugh", "Staff", "IPAM"),
    ("yzhao@ipam.ucla.edu", "Y. Zhao", "Staff", "IPAM"),
]

# ============================================================
# NTRG (Nano Transport Research Group) - from ntrg.seas.ucla.edu/members/
# ============================================================
ntrg_contacts = [
    ("tsfisher@g.ucla.edu", "Timothy S. Fisher", "PI / Professor", "NTRG / MAE"),
    ("hjkz3514@g.ucla.edu", "Min Jong Kil", "PhD Student", "NTRG / MAE"),
    ("zwong888@g.ucla.edu", "Zachary Wong", "PhD Student", "NTRG / MAE"),
    ("msnaaren2330@ucla.edu", "Naarendharan Meenakshi Sundaram", "PhD Student", "NTRG / MAE"),
    ("bheronimus@g.ucla.edu", "Benjamin Heronimus", "PhD Student", "NTRG / Aerospace"),
    ("yandawang926@g.ucla.edu", "Yanda Wang", "PhD Student", "NTRG / MAE"),
    ("dbpark2@g.ucla.edu", "Brian Park", "PhD Student", "NTRG / ChemE"),
    ("wychoi@ucla.edu", "Woo-Young Choi", "PhD Student", "NTRG / MSE"),
    ("priyanthelango@g.ucla.edu", "Priyanth Elango", "PhD Student", "NTRG / MAE"),
    ("rafamcpr9@g.ucla.edu", "Rafael Maldonado Comas", "MS Student", "NTRG / MAE"),
    ("soyoungjo@g.ucla.edu", "Soyoung Jo", "MS Student", "NTRG / MAE"),
    ("byang164@ucla.edu", "Ben Yang", "MS Student", "NTRG / EE"),
    ("jons0226@g.ucla.edu", "Jonathan Park", "MS Student", "NTRG / MAE"),
    ("sprak011@g.ucla.edu", "Snigdha Prakash", "Undergraduate", "NTRG / MAE"),
    ("sundiwin6612@g.ucla.edu", "Sundi Win", "Undergraduate", "NTRG / MAE"),
    ("jjunaedi10@g.ucla.edu", "Joanne Junaedi", "Undergraduate", "NTRG / MAE"),
    ("gabrielcenteno@g.ucla.edu", "Gabriel Centeno", "Undergraduate", "NTRG / MAE"),
    ("madsgar315@g.ucla.edu", "Madison Garcia", "Undergraduate", "NTRG / MAE"),
    ("lukasostien@g.ucla.edu", "Lukas Ostien", "Undergraduate", "NTRG / MAE"),
    ("aidenkg28@g.ucla.edu", "Aiden Georgiev", "Undergraduate", "NTRG / Aerospace"),
    ("jmoreno117@g.ucla.edu", "Jonathan Moreno", "Undergraduate", "NTRG / Aerospace"),
    ("watanabetomo@g.ucla.edu", "Tomo Watanabe", "Undergraduate", "NTRG / MAE"),
]

# ============================================================
# Visual Machines Group (EE) - from visual.ee.ucla.edu/people/
# ============================================================
vmg_contacts = [
    ("vilesov@ucla.edu", "Alexander (Sasha) Vilesov", "PhD Student", "Visual Machines Group / EE"),
    ("ellinz@ucla.edu", "Ellin Zhao", "PhD Student", "Visual Machines Group / EE"),
    ("hwdz15508@g.ucla.edu", "Howard Zhang", "PhD Student", "Visual Machines Group / EE"),
    ("rishiu@ucla.edu", "Rishi Upadhyay", "PhD Student", "Visual Machines Group / EE"),
    ("shijiezhou@ucla.edu", "Shijie Zhou", "PhD Student", "Visual Machines Group / EE"),
    ("selim.can@ucla.edu", "Selim Emir Can", "Undergraduate", "Visual Machines Group / EE"),
    ("rajeshwari@ucla.edu", "Rajeshwari Jadhav", "Undergraduate", "Visual Machines Group / EE"),
    ("jli5@g.ucla.edu", "Jerry Li", "Undergraduate", "Visual Machines Group / EE"),
    ("noemotamedi@g.ucla.edu", "Noe Motamedi", "Undergraduate", "Visual Machines Group / EE"),
    ("ziyipeng@g.ucla.edu", "Ziyi Peng", "Undergraduate", "Visual Machines Group / EE"),
    ("ajpfahnl@ucla.edu", "Arnold Pfahnl", "Undergraduate", "Visual Machines Group / EE"),
    ("aletheasm@ucla.edu", "Alethea Sung-Miller", "Undergraduate", "Visual Machines Group / EE"),
    ("tianyuanchen@g.ucla.edu", "Tianyuan Chen", "Undergraduate", "Visual Machines Group / EE"),
    ("atilaye@g.ucla.edu", "Ameya Tilaye", "Undergraduate", "Visual Machines Group / EE"),
    ("asuzuki100@ucla.edu", "Akira Suzuki", "Undergraduate", "Visual Machines Group / EE"),
]

# ============================================================
# Wong Lab (Bioengineering) - from wonglab.seas.ucla.edu/members
# ============================================================
wong_contacts = [
    ("WSchmidt96@ucla.edu", "William Schmidt", "PhD Student", "Wong Lab / Bioengineering"),
    ("elizabethwcluo@ucla.edu", "Wei-Chia Luo", "PhD Student", "Wong Lab / Bioengineering"),
    ("jwhc@ucla.edu", "Jonathan Chen", "PhD Student", "Wong Lab / Bioengineering"),
    ("yangrena@ucla.edu", "Rena Yang", "PhD Student", "Wong Lab / Bioengineering"),
    ("txue25@ucla.edu", "Tracy Xue", "Undergraduate", "Wong Lab / Bioengineering"),
    ("whsun@ucla.edu", "Wenhong Tony Sun", "Undergraduate", "Wong Lab / Bioengineering"),
    ("sylviadeng319@g.ucla.edu", "Sylvia Deng", "Undergraduate", "Wong Lab / Bioengineering"),
    ("bingwi@g.ucla.edu", "Bing Wi", "Undergraduate", "Wong Lab / Bioengineering"),
]

# ============================================================
# Yuhuang Lab (MSE/ChemE) - from yuhuanglab.seas.ucla.edu
# ============================================================
yuhuang_contacts = [
    ("yxjh0037@g.ucla.edu", "Yuxiao He", "PhD Student", "Yuhuang Lab / MSE"),
    ("ranwang111@g.ucla.edu", "Ran Wang", "PhD Student", "Yuhuang Lab / MSE"),
    ("boxuanzhou@ucla.edu", "Boxuan Zhou", "PhD Student", "Yuhuang Lab / MSE"),
    ("yzhang2018@g.ucla.edu", "Yucheng Zhang", "PhD Student", "Yuhuang Lab / MSE"),
    ("zhangao9706@ucla.edu", "Ao Zhang", "PhD Student", "Yuhuang Lab / MSE"),
    ("yangliu0607@g.ucla.edu", "Yang Liu", "PhD Student", "Yuhuang Lab / MSE"),
    ("zjx80028003@g.ucla.edu", "Jingxuan Zhou", "PhD Student", "Yuhuang Lab / MSE"),
    ("jiccai@g.ucla.edu", "Jin Cai", "PhD Student", "Yuhuang Lab / MSE"),
    ("dongxu2@ucla.edu", "Dong Xu", "PhD Student", "Yuhuang Lab / MSE"),
    ("lzysz@ucla.edu", "Zeyan Liu", "PhD Student", "Yuhuang Lab / MSE"),
    ("wangxue@chem.ucla.edu", "Wang Xue", "PhD Student", "Yuhuang Lab / MSE"),
    ("htliu117@ucla.edu", "Haotian Liu", "PhD Student", "Yuhuang Lab / MSE"),
    ("jinhuang@ucla.edu", "Jin Huang", "PhD Student", "Yuhuang Lab / MSE"),
    ("huangzh@ucla.edu", "Zhihong Huang", "PhD Student", "Yuhuang Lab / MSE"),
    ("mmfe@ucla.edu", "Michelle Flores Espinosa", "PhD Student", "Yuhuang Lab / MSE"),
    ("lsjoon@ucla.edu", "Sung-Joon Lee", "PhD Student", "Yuhuang Lab / MSE"),
    ("intotheyou@ucla.edu", "Chungsuk Choi", "PhD Student", "Yuhuang Lab / MSE"),
]

# ============================================================
# Huffaker Lab (EE/NanoMaterials) - from seas.ucla.edu/~huffaker/members.html
# ============================================================
huffaker_contacts = [
    ("tychang1014n@ucla.edu", "Ting-Yuan Chang", "PhD Student", "Huffaker Lab / EE"),
    ("rongzixuan789@ucla.edu", "Zixuan Rong", "MS Student", "Huffaker Lab / EE"),
]

# ============================================================
# FEC Members (Faculty) - from fec.seas.ucla.edu/fec-members/
# ============================================================
fec_contacts = [
    ("gaol@g.ucla.edu", "Liang Gao", "Faculty", "Bioengineering / FEC"),
    ("jenniferwilson@g.ucla.edu", "Jennifer Wilson", "Faculty", "Bioengineering / FEC"),
    ("mohanty@g.ucla.edu", "Sanjay Mohanty", "Faculty", "Civil & Environmental Eng / FEC"),
    ("idilakin@g.ucla.edu", "Idil Akin", "Faculty", "Civil & Environmental Eng / FEC"),
    ("kakoulli@g.ucla.edu", "Ioanna Kakoulli", "Faculty", "Materials Science & Eng / FEC"),
    ("ireneachen@ucla.edu", "Irene Chen", "Faculty", "Chemical & Biomolecular Eng / FEC"),
    ("chohsieh@cs.ucla.edu", "Cho-Jui Hsieh", "Faculty", "Computer Science / FEC"),
    ("violetpeng@cs.ucla.edu", "Nanyun (Violet) Peng", "Faculty", "Computer Science / FEC"),
]

# ============================================================
# UCLA Math Graduate Students - from math.ucla.edu/people/grad
# Email pattern: username@math.ucla.edu (extracted from reversed emails on profile pages)
# ============================================================
math_grad_usernames = [
    ("rarbon", "Ryan Arbon"), ("abaeumer", "Albert Baeumer"), ("baijr", "Jinru Bai"),
    ("zmb", "Zachary Baugher"), ("akriegman", "Aaron Berkowitz Kriegman"),
    ("brownr", "Thomas Rush Brown"), ("hsch", "Hyunsik Chae"),
    ("chang314", "William Chang"), ("snek", "James Chen"), ("nchen", "Norman Chen"),
    ("yunuoch", "Yunuo Chen"), ("arianagchin", "Ariana Chin"),
    ("ctcollins", "Carson Collins"), ("evandavis", "Evan Davis"),
    ("gurkirand", "Gurkiran Dhaliwal"), ("jiahandu", "Jiahan Du"),
    ("jlenwright1", "Joshua Enwright"), ("benjaminfaktor", "Benjamin Faktor"),
    ("dfunk23", "Davis Michael Funk"), ("emilg", "Emil Geisler"),
    ("mgluchowski", "Maciej Jan Gluchowski"), ("bengoldman", "Benjamin Goldman"),
    ("luna", "Luna Gonzalez"), ("yifangu", "Yifan Gu"),
    ("seanguan", "Sean Guan"), ("bobhale", "Robert Joseph Hale"),
    ("hanzbtom", "Zhaobo Han"), ("hhm", "Harris Hardiman-Mostow"),
    ("xinjieh", "Xinjie He"), ("hillery", "Jonathan Hillery"),
    ("jshopper", "John Hopper"), ("fyhung", "Fan-Yun (Matthew) Hung"),
    ("uijeongjang", "Uijeong Jang"), ("chuyinjiang", "Chuyin Jiang"),
    ("ebjrade", "Ely Jrade"), ("jkalaric", "Joseph Kalarickal"),
    ("akim", "Andrew Dabin Kim"), ("ericykim", "Young Han Kim"),
    ("annika", "Annika King"), ("adamknudson", "Adam Knudson"),
    ("rkommerell", "Rhea Kommerell"), ("markkong", "Mark Kong"),
    ("mattkowalski", "Matthew Kowalski"), ("koziol", "Matthew Koziol"),
    ("romakrut", "Roman Krutovskiy"), ("seanku", "Sean Ku"),
    ("alebovit", "Audric Lebovitz"), ("runji", "Runji Li"),
    ("xiangli", "Xiang Li"), ("zililim", "Zi Li Lim"),
    ("hunterliu", "Hunter Liu"), ("jingyiliu1015", "Jingyi Liu"),
    ("yxliu", "Yuxuan Liu"), ("liuzhongyue", "ZhongYue Liu"),
    ("tgloe", "Trevor Loe"), ("alu", "Annie Lu"),
    ("gluan", "Qitong Luan"), ("jqluong", "Jack Luong"),
    ("lyuhaoyang", "Haoyang Lyu"), ("benmajor", "Benjamin Major"),
    ("malkov", "Stepan Malkov"), ("esm", "Elias Manuelides"),
    ("tmartinez", "Thomas Martinez"), ("jmauro", "Jack Mauro"),
    ("wmilgrim", "Wyatt Milgrim"), ("jackmoffatt", "Jack Moffatt"),
    ("sidmulherkar", "Siddharth Mulherkar"), ("jwmurri", "Jacob Murri"),
    ("anad", "Arian Nadjimzadah"), ("khang", "Khang Nguyen"),
    ("colinni", "Colin Ni"), ("tomokioda0723", "Tomoki Oda"),
    ("cosborne", "Calvin Scott Osborne"), ("papenburg", "Hagen Papenburg"),
    ("harahmpark", "Harahm Park"), ("ryanpark7", "Jaesung Ryan Park"),
    ("ishaanpatkar", "Ishaan Patkar"), ("griffinpinney", "Griffin Pinney"),
    ("dpopovic", "David Popovic"), ("victoriaquijano", "Victoria Quijano"),
    ("sdqunell", "Samuel Qunell"), ("rushil", "Rushil Raghavan"),
    ("krai", "Kartikeya Rai"), ("advika", "Advika Rajapakse"),
    ("mreyesrivas", "Moises Reyes Rivas"), ("danrui", "Daniel Rui"),
    ("luke.russo", "Luke Russo"), ("jasonsch", "Jason Aaron Schuchardt"),
    ("karthiks", "Karthik Sellakumaran Latha"), ("ksereesu", "Khunpob Sereesuchart"),
    ("olha", "Olha Shevchenko"), ("kwshi", "Kye Shi"),
    ("ishors", "Ian Shors"), ("alexsietsema", "Alexander Sietsema"),
    ("jas", "Jaspreet Singh"), ("btslater", "Benjamin Slater"),
    ("zachslonim", "Zachary Slonim"), ("jsolheid", "Jackson Solheid"),
    ("spitzer", "Reed Spitzer"), ("jungjoos", "Jung Joo Suh"),
    ("jaswenberg", "Jacob Swenberg"), ("indraneel", "Indraneel Tambe"),
    ("maxtanhk", "Hong Kiat Tan"), ("zhengtan", "Zheng Tan"),
    ("ytao", "Yan Tao"), ("steven", "Steven Khang Truong"),
    ("zntu02", "Zachary Tu"), ("mtyler", "Matthew Tyler"),
    ("emmyvr", "Emmy Van Rooy"), ("zerrinvural", "Zerrin Vural"),
    ("jiahewang", "Jiahe Wang"), ("jonwoo", "Jonathan Daniel Woo"),
    ("chwu93", "Chi-Hao Wu"), ("kathyxing", "Kathy Xing"),
    ("alexxue", "Alexander Xue"), ("jonahkyoshida", "Jonah Kaiana Yoshida"),
    ("tracy", "Xinyue Yu"), ("zender", "Mia Zender"),
    ("aaz", "Adam Zheleznyak"), ("johnzhou2001", "John Zhou"),
    ("lunji", "Lunji Zhu"), ("zxz", "Xinzhe Zuo"),
]

math_contacts = [(f"{username}@math.ucla.edu", name, "Graduate Student", "Mathematics")
                  for username, name in math_grad_usernames]

# ============================================================
# Combine all contacts
# ============================================================
all_groups = [
    ("https://qcb.ucla.edu/people/", qcb_contacts),
    ("https://www.ipam.ucla.edu/people/", ipam_contacts),
    ("https://ntrg.seas.ucla.edu/members/", ntrg_contacts),
    ("https://visual.ee.ucla.edu/people/", vmg_contacts),
    ("https://wonglab.seas.ucla.edu/members", wong_contacts),
    ("http://yuhuanglab.seas.ucla.edu/qita/CurrentMembers/Professors/", yuhuang_contacts),
    ("https://www.seas.ucla.edu/~huffaker/members.html", huffaker_contacts),
    ("https://fec.seas.ucla.edu/fec-members/", fec_contacts),
    ("https://www.math.ucla.edu/people/grad", math_contacts),
]

results = []
seen_emails = set()

for source_url, contacts in all_groups:
    for contact in contacts:
        email = contact[0].lower().strip()
        name = contact[1]
        role = contact[2]
        department = contact[3]
        
        if email not in seen_emails and email not in existing_emails:
            seen_emails.add(email)
            results.append({
                'email': email,
                'name': name,
                'role': role,
                'department': department,
                'source_url': source_url
            })

# Also add any results from the automated scraper that aren't duplicates
try:
    with open('ucla_stem_lab_emails.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = row['email'].lower().strip()
            if email not in seen_emails and email not in existing_emails:
                seen_emails.add(email)
                results.append(row)
except FileNotFoundError:
    pass

# Sort by department then name
results.sort(key=lambda x: (x['department'], x['name']))

# Save to CSV
output_file = 'ucla_stem_lab_emails.csv'
with open(output_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['email', 'name', 'role', 'department', 'source_url'])
    writer.writeheader()
    for r in results:
        writer.writerow(r)

# Also save as JSON
with open('ucla_stem_lab_emails.json', 'w') as f:
    json.dump(results, f, indent=2)

# Print summary
print(f"\n{'='*70}")
print(f"TOTAL NEW UNIQUE EMAILS: {len(results)}")
print(f"{'='*70}")

print(f"\nBREAKDOWN BY DEPARTMENT:")
print(f"{'-'*70}")
dept_counts = {}
for r in results:
    dept = r['department']
    dept_counts[dept] = dept_counts.get(dept, 0) + 1

for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
    print(f"  {dept}: {count}")

print(f"\nBREAKDOWN BY ROLE:")
print(f"{'-'*70}")
role_counts = {}
for r in results:
    role = r['role']
    role_counts[role] = role_counts.get(role, 0) + 1

for role, count in sorted(role_counts.items(), key=lambda x: -x[1]):
    print(f"  {role}: {count}")

# Print all emails with names
print(f"\n{'='*70}")
print(f"COMPLETE EMAIL LIST:")
print(f"{'='*70}")
for r in results:
    print(f"  {r['email']:40s} | {r['name']:35s} | {r['role']:20s} | {r['department']}")

print(f"\nSaved to {output_file} and ucla_stem_lab_emails.json")
