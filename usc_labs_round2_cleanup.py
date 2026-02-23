#!/usr/bin/env python3
"""
Clean up the round 2 CSV - fix prefix artifacts, add manually extracted emails,
remove duplicates.
"""

import csv
import json
import re


def clean_email(email):
    """Clean email by removing role/title prefix artifacts."""
    email = email.lower().strip()
    
    # Known prefix artifacts to strip
    prefixes = [
        'principalinvestigator', 'investigator', 'principalinv',
        'postdoctoralscholar', 'postdoctoral', 'postdoc',
        'graduatestudent', 'graduate', 'student',
        'undergraduateresearchassistant', 'undergraduate',
        'phdstudent', 'phdcandidate', 'phd',
        'mastersthesisstudent', 'mastersstudent', 'masters',
        'researchassociate', 'researchfaculty', 'researchassistant',
        'associateprofessor', 'assistantprofessor', 'professor',
        'labmanager', 'manager', 'labtechnician', 'technician',
        'administrativeteam', 'administrative', 'administrator',
        'supportteam', 'support', 'seniorresearch',
        'executivedirectorofdevelopment', 'executivesecretary', 'executive',
        'publiccommunicationsmanager', 'publiccommunications',
        'postdoctoral', 'clinical', 'visiting',
        'choipostdoctoralfellow', 'choipostdoctoral',
        'schmidtsciencefellow', 'fellow',
        'circmbridgesstudent', 'cirmstudent', 'cirm',
        'cirmcompassundergraduatestudent',
        'principalinvestigator', 'scientist', 'senior',
        'researchlabtech', 'researchlab', 'lab',
        'chair', 'committee', 'director', 'studies',
        'program', 'faculty', 'associate',
        'analystsycheung@', 'analyst',
        'secretarylaura', 'secretary',
        'strategistlytal', 'strategist',
        'techyunbzhan', 'tech',
        'scholarotb',
        # Specific known bad prefixes from the data
        'bonheurphdstudent', 'sharmaphdstudent',
        'yephdstudent', 'lorzadehpostdoc',
        'investigatormbonagui', 'investigatorquadrato',
        'investigatorujadhav', 'investigatorronglu',
        'fellowakbanerj', 'fellowbirtele', 'fellowegeyer',
        'carlaliacipostdoc', 'mohammadshariqpostdoc',
        'kandypostdoc', 'zhongjiepostdoctoralscholar',
        'undergraduatehhle',
        'developmentjessie', 'sjuliane',
        'administratorlaura',
    ]
    
    for p in sorted(prefixes, key=len, reverse=True):
        if email.startswith(p):
            rest = email[len(p):]
            if rest and rest[0] != '@' and re.match(r'^[a-z]', rest):
                # Check if what remains looks like a valid email
                if '@' in rest and re.match(r'^[\w.+-]+@', rest):
                    return rest
    
    return email


def is_admin_email(email):
    """Check if email is administrative/generic."""
    admin_patterns = [
        'info@', 'admin@', 'office@', 'dept@', 'webmaster@', 'help@',
        'support@', 'contact@', 'registrar@', 'admissions@',
        'advising@', 'dean@', 'chair@', 'reception@', 'enroll@',
        'gradadm@', 'viterbi@', 'communications@', 'department@',
        'news@', 'events@', 'pr@', 'media@', 'marketing@',
        'library@', 'alumni@', 'development@', 'giving@',
        'keck@', 'research@', 'grants@', 'web@', 'gero@',
        'uscnews@', 'pharmacy@', 'pharmacyschool@',
        'stemcell@', 'norris@', 'cancer@',
        'bci@', 'dni@', 'earthsci@', 'studenthealth@',
        'uschr@', 'dpsrecords@', 'pharmcom@',
        'hr@', 'jobs@', 'hiring@', 'career@',
        'safety@', 'security@', 'emergency@',
        'it@', 'tech@', 'helpdesk@',
        'provost@', 'president@', 'chancellor@',
        'keckfa@', 'fraserlab@', 'matherlab@',
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


def main():
    # Load existing round 2 results
    results = []
    try:
        with open('usc_labs_round2_emails.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                results.append(row)
    except FileNotFoundError:
        pass
    
    print(f"Loaded {len(results)} existing round 2 results")
    
    # Add manually extracted emails from web fetches
    manual_additions = [
        # Jadhav Lab - corrected
        {"email": "ujadhav@usc.edu", "name": "Unmesh Jadhav", "department": "Keck - Jadhav Lab (Stem Cell/GI)", "source_url": "https://jadhavlab.usc.edu/people/"},
        {"email": "ericsanc@usc.edu", "name": "Eric Sanchez", "department": "Keck - Jadhav Lab", "source_url": "https://jadhavlab.usc.edu/people/"},
        {"email": "lorzadeh@usc.edu", "name": "Alireza Lorzadeh", "department": "Keck - Jadhav Lab", "source_url": "https://jadhavlab.usc.edu/people/"},
        {"email": "swetasha@usc.edu", "name": "Sweta Sharma", "department": "Keck - Jadhav Lab", "source_url": "https://jadhavlab.usc.edu/people/"},
        {"email": "bonheur@usc.edu", "name": "Moise Bonheur", "department": "Keck - Jadhav Lab", "source_url": "https://jadhavlab.usc.edu/people/"},
        {"email": "georgeye@usc.edu", "name": "George Ye", "department": "Keck - Jadhav Lab", "source_url": "https://jadhavlab.usc.edu/people/"},
        {"email": "tanushag@usc.edu", "name": "Tanush Agrawal", "department": "Keck - Jadhav Lab", "source_url": "https://jadhavlab.usc.edu/people/"},
        
        # Roberts Lab (mRNA Display) - corrected
        {"email": "richrob@usc.edu", "name": "Richard W. Roberts", "department": "Chemistry - Roberts Lab", "source_url": "https://mrnadisplay.usc.edu/current-members/"},
        {"email": "tttakahashi@usc.edu", "name": "Terry Takahashi", "department": "Chemistry - Roberts Lab", "source_url": "https://mrnadisplay.usc.edu/current-members/"},
        {"email": "crhughes@usc.edu", "name": "Chris Hughes", "department": "Chemistry - Roberts Lab", "source_url": "https://mrnadisplay.usc.edu/current-members/"},
        {"email": "ajparks@usc.edu", "name": "Bud Parks", "department": "Chemistry - Roberts Lab", "source_url": "https://mrnadisplay.usc.edu/current-members/"},
        {"email": "zhenzhuq@usc.edu", "name": "Pearl Qi", "department": "Chemistry - Roberts Lab", "source_url": "https://mrnadisplay.usc.edu/current-members/"},
        {"email": "cleaf@usc.edu", "name": "Colin Leaf", "department": "Chemistry - Roberts Lab", "source_url": "https://mrnadisplay.usc.edu/current-members/"},
        {"email": "yli75177@usc.edu", "name": "Yutong Li", "department": "Chemistry - Roberts Lab", "source_url": "https://mrnadisplay.usc.edu/current-members/"},
        
        # Quadrato Lab - corrected
        {"email": "quadrato@usc.edu", "name": "Giorgia Quadrato", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "tuannguy@usc.edu", "name": "Tuan Jojo Nguyen", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "yunbzhan@usc.edu", "name": "Van (Jennifer) Truong", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "birtele@usc.edu", "name": "Marcella Birtele", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "cl_245@usc.edu", "name": "Carla Liaci", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "akbanerj@usc.edu", "name": "Abhik Banerjee", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "egeyer@chla.usc.edu", "name": "Eduardo Geyer", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "negarhos@usc.edu", "name": "Negar Hosseini", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "jurenda@usc.edu", "name": "Jean-Paul Urenda", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "qingyuel@usc.edu", "name": "Qingyue Li", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "nwnguyen@usc.edu", "name": "Nathan Nguyen", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "tsenthil@usc.edu", "name": "Thabbani Senthilnathan", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "minajung@usc.edu", "name": "Mina Jung", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "junyulu@usc.edu", "name": "Junyu (Joanna) Lu", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "nylu@usc.edu", "name": "Nicole Lu", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "kjphung@usc.edu", "name": "Kenneth Phung", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        {"email": "otb_098@usc.edu", "name": "Olga Bianciotto", "department": "Keck - Quadrato Lab", "source_url": "https://quadratolab.usc.edu/people/"},
        
        # Katritch Lab - corrected
        {"email": "katritch@usc.edu", "name": "Vsevolod Katritch", "department": "Biology - Katritch Lab", "source_url": "https://katritchlab.usc.edu/people.html"},
        {"email": "saheemza@usc.edu", "name": "Saheem Zaidi", "department": "Biology - Katritch Lab", "source_url": "https://katritchlab.usc.edu/people.html"},
        {"email": "zarzycka@usc.edu", "name": "Barbara Zarzycka", "department": "Biology - Katritch Lab", "source_url": "https://katritchlab.usc.edu/people.html"},
        {"email": "sadybeko@usc.edu", "name": "Arman Sadybekov", "department": "Biology - Katritch Lab", "source_url": "https://katritchlab.usc.edu/people.html"},
        {"email": "nilkantp@usc.edu", "name": "Nilkanth Patel", "department": "Biology - Katritch Lab", "source_url": "https://katritchlab.usc.edu/people.html"},
        {"email": "asadybek@usc.edu", "name": "Anastasiia Sadybekov", "department": "Biology - Katritch Lab", "source_url": "https://katritchlab.usc.edu/people.html"},
        {"email": "binczews@usc.edu", "name": "Natalie Binczewski", "department": "Biology - Katritch Lab", "source_url": "https://katritchlab.usc.edu/people.html"},
        
        # NSEIP Lab (Shanechi) - manually extracted from HTML entities
        {"email": "shanechi@usc.edu", "name": "Maryam Shanechi", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "ghasemsani@usc.edu", "name": "Omid G. Sani", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "hanlinhs@usc.edu", "name": "Han-Lin Hsieh", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "hj_380@usc.edu", "name": "HyeongChan Jo", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "tyulmank@usc.edu", "name": "Danil Tyulmankov", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "ahmadipo@usc.edu", "name": "Parima Ahmadipouranari", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "pvahidi@usc.edu", "name": "Parsa Vahidi", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "dongkyuk@usc.edu", "name": "DongKyu Kim", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "yanyuli@usc.edu", "name": "Yanyu Li", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "eerturk@usc.edu", "name": "Eray Erturk", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "lucine.oganesian@usc.edu", "name": "Lucine Oganesian", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "tjani@usc.edu", "name": "Trisha Jani", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "ebilgin@usc.edu", "name": "Enes Burak Bilgin", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "hoseini@usc.edu", "name": "Sayed Mohammad Hosseini", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "azizeddi@usc.edu", "name": "Parastoo Azizeddin", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "djilani@usc.edu", "name": "Daniel Jilani", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "saba.hashemi@usc.edu", "name": "Saba Hashemi", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        {"email": "vazirigo@usc.edu", "name": "Kiarash Vaziri", "department": "ECE - NSEIP Lab (Shanechi)", "source_url": "https://nseip.usc.edu/people/"},
        
        # Gerontology - Lifespan Cognition Lab
        {"email": "teich@usc.edu", "name": "Teal Eich", "department": "Gerontology - Lifespan Cognition Lab", "source_url": "https://gero.usc.edu/labs/lifecoglab/people/"},
        {"email": "joanjime@usc.edu", "name": "Joan Jimenez-Balado", "department": "Gerontology - Lifespan Cognition Lab", "source_url": "https://gero.usc.edu/labs/lifecoglab/people/"},
        {"email": "chihyuan@usc.edu", "name": "Jessie Chih-Yuan Chien", "department": "Gerontology - Lifespan Cognition Lab", "source_url": "https://gero.usc.edu/labs/lifecoglab/people/"},
        {"email": "dokyung@usc.edu", "name": "Dokyung Yoon", "department": "Gerontology - Lifespan Cognition Lab", "source_url": "https://gero.usc.edu/labs/lifecoglab/people/"},
        
        # Gerontology - Mather Lab
        {"email": "mara.mather@usc.edu", "name": "Mara Mather", "department": "Gerontology - Mather Emotion Lab", "source_url": "https://gero.usc.edu/labs/matherlab/people/"},
        
        # Rong Lu Lab
        {"email": "ronglu@usc.edu", "name": "Rong Lu", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "nogalska@usc.edu", "name": "Ania Nogalska", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "tzhongji@usc.edu", "name": "Jay Tang", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "yirandua@usc.edu", "name": "Rae Duan", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "slydon@usc.edu", "name": "Sara Lydon", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "zthomas@usc.edu", "name": "Zachary Thomas", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "antran@usc.edu", "name": "An Tran", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "mcv_714@usc.edu", "name": "Mary Vergel", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "bowenw@usc.edu", "name": "Bowen Wang", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "eerdeng@usc.edu", "name": "Jiya Eerdeng", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "ap43852@usc.edu", "name": "Abhinav Parameshwaran", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "bohnert@usc.edu", "name": "Asha Bohnert", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "danicago@usc.edu", "name": "Danica Gonzalez", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "targett@usc.edu", "name": "Ava Targett", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "boohar@usc.edu", "name": "Wade Boohar", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "mav_782@usc.edu", "name": "Melissa Valenzuela", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        {"email": "anggito@usc.edu", "name": "Bryan Anggito", "department": "Keck - Rong Lu Lab", "source_url": "https://ronglulab.usc.edu/lab-members/"},
        
        # INK Lab
        {"email": "xiangren@usc.edu", "name": "Xiang Ren", "department": "CS - INK Research Lab", "source_url": "https://inklab.usc.edu/contact.html"},
        
        # CSBL Lab
        {"email": "sfinley@usc.edu", "name": "Stacey D. Finley", "department": "BME - Computational Systems Bio Lab", "source_url": "https://csbl.usc.edu/contact/"},
    ]
    
    # Merge manual additions with existing results
    seen_emails = set()
    
    # First pass: collect emails from existing results after cleaning
    cleaned_results = []
    for r in results:
        email = clean_email(r.get('email', ''))
        if not email or is_admin_email(email):
            continue
        if email not in seen_emails:
            seen_emails.add(email)
            r['email'] = email
            # Clean name
            name = r.get('name', '')
            if name and any(x in name.lower() for x in ['staff', 'advisor', 'division chief',
                                                          'associate chief', 'equal opportunity',
                                                          'tell us', 'for assistance', 'mailing address',
                                                          'faculty profile', 'administrative']):
                r['name'] = ''
            cleaned_results.append(r)
    
    # Second pass: add manual additions
    for r in manual_additions:
        email = r['email'].lower().strip()
        if email not in seen_emails and not is_admin_email(email):
            seen_emails.add(email)
            cleaned_results.append(r)
    
    # Load previous scrapers to filter
    existing_emails = set()
    for fname in ['usc_viterbi_emails.csv', 'usc_dornsife_grad_emails.csv', 
                  'usc_pro_school_emails.csv', 'usc_emails.csv']:
        try:
            with open(fname, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('email'):
                        existing_emails.add(row['email'].lower().strip())
        except FileNotFoundError:
            pass
    
    print(f"Existing emails from previous scrapers: {len(existing_emails)}")
    
    # Filter
    final_results = [r for r in cleaned_results if r['email'].lower().strip() not in existing_emails]
    
    print(f"Final cleaned results: {len(final_results)} unique emails")
    
    # Save
    output_csv = 'usc_labs_round2_emails.csv'
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'name', 'department', 'source_url'])
        writer.writeheader()
        for r in sorted(final_results, key=lambda x: x.get('department', '')):
            writer.writerow(r)
    print(f"Saved to {output_csv}")
    
    output_json = 'usc_labs_round2_emails.json'
    with open(output_json, 'w') as f:
        json.dump(final_results, f, indent=2)
    print(f"Also saved to {output_json}")
    
    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY BY DEPARTMENT/LAB:")
    print(f"{'='*70}")
    dept_counts = {}
    for r in final_results:
        dept = r.get('department', 'Unknown')
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1]):
        print(f"  {dept}: {count} emails")
    
    print(f"\n{'='*70}")
    print("ALL EMAILS:")
    print(f"{'='*70}")
    for r in sorted(final_results, key=lambda x: x.get('email', '')):
        name_str = f" ({r['name']})" if r.get('name') else ""
        print(f"  {r['email']}{name_str} - {r.get('department', '')}")
    
    print(f"\n\nTOTAL: {len(final_results)} unique new emails")


if __name__ == '__main__':
    main()
