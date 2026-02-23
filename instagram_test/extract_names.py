#!/usr/bin/env python3
"""
Extract first names from email addresses for consumer_leads where name is NULL.
Only extracts when pattern is OBVIOUS (firstname.lastname@).
Validates against a large common-names list. Defaults to no name if unsure.
"""

import os, json, re, urllib.request, ssl
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(os.path.dirname(BASE_DIR), '.env'))

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']

SSL_CTX = ssl.create_default_context()

COMMON_NAMES = {
    'aaron','abby','abdiel','abdul','abe','abel','abigail','abraham','abram','ada','adah',
    'adam','adan','addie','addison','adela','adelaide','adele','adelia','adeline','aden',
    'adolfo','adolph','adrian','adriana','adrianna','adrienne','agnes','agustin','ahmad',
    'ahmed','aida','aidan','aiden','aileen','aimee','aisha','al','alan','alana','alanna',
    'albert','alberta','alberto','alden','aleah','alec','aleena','alejandra','alejandro',
    'alena','alesha','alessandra','alessandro','alex','alexa','alexander','alexandra',
    'alexandro','alexia','alexis','alexus','alfredo','ali','alice','alicia','alina',
    'alisa','alisha','alison','alissa','alivia','aliyah','allan','allen','allie','allison',
    'allyson','alma','alondra','alonzo','alton','alva','alvaro','alvin','alysa','alyson',
    'alyssa','amalia','amanda','amani','amara','amari','amber','amelia','america','amie',
    'amina','amir','amira','amos','amy','ana','anabel','anahi','anais','anastasia','anderson',
    'andre','andrea','andreas','andres','andrew','andy','angel','angela','angelia','angelica',
    'angelina','angeline','angelo','angie','anika','anissa','anita','aniya','aniyah','ann',
    'anna','annabel','annabella','annabelle','annalise','anne','annette','annie','annika',
    'ansley','anthony','antoinette','anton','antonia','antonio','antwan','april','aquila',
    'arabella','archie','arden','areli','aria','ariana','arianna','ariel','ariella','arielle',
    'arjun','arlene','arlo','armand','armando','armani','arnav','arnold','aron','arron',
    'art','arthur','arturo','arya','asa','ash','ashanti','asher','ashlee','ashleigh','ashley',
    'ashlyn','ashlynn','ashton','asia','aspen','athena','atticus','aubree','aubrey','audra',
    'audrey','august','augustine','aurora','austin','autumn','ava','avah','averi','averie',
    'avery','ayla','aylin','barack','barbara','barnaby','barney','baron','barrett','barry',
    'bart','baylor','beatrice','beau','beck','beckett','becky','belinda','bella','ben',
    'benedict','benjamin','bennett','bennie','benny','benson','bentley','benton','bernadette',
    'bernard','bernardo','bernice','bert','bertha','bertie','bessie','beth','bethany','betsy',
    'bette','betty','beverly','bianca','bill','billie','billy','birdie','bishop','blaine',
    'blair','blaise','blake','blanca','blanche','blaze','bo','bob','bobbie','bobby','bodhi',
    'bonita','bonnie','boris','boston','bowen','brad','braden','bradford','bradley','brady',
    'braeden','brandan','branden','brandi','brandon','brandy','brant','braxton','brayan',
    'brayden','braydon','brea','breanna','bree','brenda','brendan','brenden','brendon',
    'brenna','brennan','brent','brenton','bret','brett','bria','brian','briana','brianna',
    'bridget','bridgette','brielle','brigitte','brinley','brisa','brit','britney','britt',
    'brittany','brittney','brock','brodie','brody','bronson','brook','brooke','brooklyn',
    'brooks','bruce','bruno','bryan','bryant','bryce','brylee','brynn','bryson','buck',
    'buddy','byron','cade','caden','cadence','caesar','caitlin','caitlyn','caleb','cali',
    'calista','callan','callie','callum','calvin','camden','cameron','camila','camilla',
    'camille','campbell','camron','camryn','candace','candice','candy','cara','carina',
    'carl','carla','carlo','carlos','carly','carmen','carol','carolina','caroline','carolyn',
    'carrie','carson','carter','casey','casper','cassandra','cassidy','cassie','catalina',
    'catherine','cathy','cayden','cecelia','cecil','cecilia','cedric','celeste','celestine',
    'celia','cesar','chad','chance','chandler','chanel','charity','charlene','charles',
    'charley','charlie','charlotte','chase','chaya','chelsea','cherie','cheryl','chester',
    'chet','cheyenne','china','chloe','chris','christa','christian','christiana','christie',
    'christina','christine','christopher','christy','chuck','ciara','cielo','ciera','cindy',
    'claire','clara','clare','clarence','clarice','clarissa','clark','claude','claudia',
    'clay','clayton','clementine','cleo','cliff','clifford','clifton','clint','clinton',
    'clive','clyde','coby','cody','cohen','colby','cole','coleman','colette','colin',
    'colleen','collin','collins','colt','colten','colton','conner','connie','connor','conor',
    'constance','cooper','cora','coral','corey','corina','corinne','cornelius','cortez',
    'cory','courtney','craig','cristian','cristina','cristopher','cruz','crystal','cullen',
    'curt','curtis','cynthia','cyrus','dag','dahlia','daisy','dakota','dale','dalilah',
    'dallas','dalton','damaris','damian','damien','damion','damon','dan','dana','dandre',
    'dane','dangelo','dania','daniel','daniela','daniella','danielle','danika','danny',
    'dante','daphne','darby','darcy','darian','darin','dario','darius','darla','darlene',
    'darnell','darrel','darrell','darren','darrin','darryl','darwin','daryl','dave','david',
    'davin','davis','dawn','dawson','dax','daxton','dayana','dayton','dean','deandre',
    'deangelo','deanna','debbie','deborah','debra','declan','dee','deena','deidre','deirdre',
    'deja','delaney','delilah','della','delores','demetrius','demi','dena','denis','denise',
    'dennis','denny','denver','derek','derick','derrick','deshawn','desiree','desmond',
    'destiny','devin','devon','devonte','devyn','dexter','diamond','diana','diane','dianna',
    'dianne','diego','dillon','dina','dion','dirk','dixie','dolly','dolores','domenic',
    'domingo','dominic','dominick','dominique','don','donald','donna','donnie','donovan',
    'dora','doreen','dorian','doris','dorothy','dot','doug','douglas','doyle','drake',
    'drew','duane','duke','duncan','dustin','dusty','dwayne','dwight','dylan','earl',
    'earnest','easton','ebony','ed','eddie','eden','edgar','edith','edmund','edna','eduardo',
    'edward','edwin','effie','eileen','elaina','elaine','elana','eleanor','elena','eli',
    'eliana','elias','elijah','elisa','elisabeth','elise','elisha','eliza','elizabeth','ella',
    'ellen','ellie','elliot','elliott','ellis','elmer','eloise','elsa','elsie','elton',
    'elva','elvira','elvis','elyse','emanuel','emely','emerald','emerson','emery','emilee',
    'emilia','emiliano','emilie','emilio','emily','emma','emmanuel','emmett','emory','enid',
    'enrique','erica','erick','erik','erika','erin','ernest','ernesto','ervin','erwin',
    'esmeralda','esperanza','esteban','estella','estelle','esther','ethan','ethel','eugene',
    'eunice','eva','evan','evangeline','eve','evelin','evelyn','everett','ezekiel','ezra',
    'fabian','faith','fallon','fannie','farrah','fatima','faye','felicia','felicity','felipe',
    'felix','fern','fernanda','fernando','fidel','finley','finn','fiona','fletcher','flora',
    'florence','floyd','flynn','forrest','foster','frances','francesca','francis','francisco',
    'frank','frankie','franklin','fred','freda','freddie','freddy','frederick','fredrick',
    'gabriel','gabriela','gabriella','gabrielle','gage','gail','galen','garret','garrett',
    'garrison','garry','gary','gavin','gaylord','gemma','gene','genesis','genevieve',
    'geoffrey','george','georgette','georgia','gerald','geraldine','gerard','gerardo',
    'gerry','gertrude','gianna','gideon','gigi','gilbert','gilda','gillian','gina',
    'giovanna','giovanni','giselle','gladys','glen','glenda','glenn','gloria','gordon',
    'grace','gracelyn','gracie','grady','graham','grant','grayson','greg','gregg','gregory',
    'greta','gretchen','greyson','griffin','grover','guadalupe','guillermo','gunnar',
    'gunner','gus','gustavo','guy','gwen','gwendolyn','hadley','hailey','hailee','hal',
    'haley','hallie','hamilton','hana','hank','hanna','hannah','hans','harlan','harley',
    'harmony','harold','harper','harriet','harris','harrison','harry','harvey','hasan',
    'hassan','hattie','haven','hayden','haylee','hayley','hazel','heath','heather','hector',
    'heidi','helen','helena','helene','hendrix','henry','herbert','herman','herschel',
    'hester','hilary','hillary','holden','hollie','holly','homer','hope','horacio','houston',
    'howard','hubert','hudson','hugh','hugo','humberto','hunter','ian','ibrahim','ida',
    'ignacio','ike','iliana','imani','immanuel','india','indiana','indigo','ines','ingrid',
    'ira','irene','iris','irma','irvin','irving','irwin','isaac','isabel','isabela',
    'isabella','isabelle','isaiah','isaias','isidro','isis','isla','ismael','israel',
    'issac','ivan','ivette','ivy','jace','jack','jackie','jackson','jacob','jacqueline',
    'jacquelyn','jada','jade','jaden','jadyn','jaeden','jagger','jaiden','jaime','jake',
    'jakob','jalen','jaliyah','jamal','jamar','james','jameson','jamie','jamison','jan',
    'jana','jane','janelle','janessa','janet','janette','janice','janie','janine','janna',
    'jaquan','jared','jarrett','jarrod','jasmin','jasmine','jason','jasper','javier','jax',
    'jaxon','jaxson','jay','jayce','jayda','jayden','jayla','jaylen','jaylin','jaylon',
    'jayson','jean','jeanette','jeanine','jeanne','jeannette','jeannie','jeff','jefferson',
    'jeffery','jeffrey','jelani','jena','jenifer','jenna','jennie','jennifer','jenny',
    'jerald','jeremiah','jeremy','jericho','jermaine','jerome','jerry','jesse','jessica',
    'jessie','jesus','jett','jewel','jill','jillian','jim','jimmie','jimmy','jo','joan',
    'joann','joanna','joanne','joaquin','jocelyn','jodi','jodie','jody','joe','joel',
    'joelle','joey','johanna','john','johnathan','johnathon','johnnie','johnny','jon',
    'jonah','jonas','jonathan','jonathon','jordan','jordana','jordi','jordon','jordyn',
    'jorge','jose','joselyn','joseph','josephine','josh','joshua','josiah','josie','josue',
    'journee','journey','joy','joyce','juan','juana','juanita','judah','jude','judith',
    'judy','julia','julian','juliana','julianna','julianne','julie','julien','juliet',
    'juliette','julio','julius','june','junior','justice','justin','justina','justine',
    'kade','kaden','kai','kaiden','kaila','kailey','kaitlin','kaitlyn','kaitlynn','kaleb',
    'kali','kallie','kamden','kameron','kami','kamila','kamryn','kane','kara','kareem',
    'karen','kari','karina','karissa','karl','karla','karlee','karlene','karly','karol',
    'karson','karter','kasey','kassandra','kassidy','kate','katelin','katelyn','katelynn',
    'katharine','katherine','kathleen','kathryn','kathy','katie','katlyn','katrina','katy',
    'kay','kaya','kayden','kayla','kaylee','kaylen','kayleigh','kaylie','kaylin','keagan',
    'keanu','keaton','keegan','keenan','keila','keira','keith','kelby','kellen','kelley',
    'kelli','kellie','kelly','kelsey','kelton','kelvin','ken','kena','kendal','kendall',
    'kendra','kendrick','kennedy','kenneth','kenny','kent','kenya','kenzie','keri','kermit',
    'kerri','kerry','kevin','khadijah','khalil','khloe','kian','kiana','kiara','kiera',
    'kieran','kiley','kim','kimberlee','kimberley','kimberly','king','kingsley','kingston',
    'kinley','kinsley','kira','kirsten','kobe','koby','kody','kolby','kole','kolton',
    'konner','konnor','korbin','korey','kori','kristian','kristen','kristin','kristina',
    'kristine','kristopher','kristy','krystal','kurt','kurtis','kyla','kyle','kylee',
    'kyler','kylie','kyra','kyrie','lacey','laci','lacie','lacy','laila','lailah','lainey',
    'lamar','lamont','lana','lance','landon','landry','lane','laney','lara','larissa',
    'larry','latasha','latisha','latonya','latoya','laura','laurel','lauren','laurence',
    'laurie','lauryn','laverne','lawrence','layla','lea','leah','leandro','leann','leanna',
    'leanne','lee','leia','leif','leigh','leila','leilani','lela','leland','lena','lennie',
    'lenny','leo','leon','leona','leonard','leonardo','leonel','leroy','lesa','lesley',
    'leslie','lester','leticia','levi','lewis','lexi','lexie','lia','liam','liana',
    'libby','liberty','lila','lilah','lilian','liliana','lillian','lilliana','lillie',
    'lilly','lily','lincoln','linda','lindsay','lindsey','lionel','lisa','livia','liz',
    'liza','lloyd','logan','lois','lola','london','lonnie','lora','loren','lorena',
    'lorenzo','loretta','lori','lorna','lorraine','lottie','louie','louis','louisa',
    'louise','lourdes','lowell','lucas','lucia','luciana','lucien','lucille','lucy','luis',
    'luisa','lukas','luke','luna','luther','luz','lydia','lyla','lynda','lyndon','lynn',
    'lyric','mabel','mac','mack','mackenzie','macy','maddison','maddox','madeline',
    'madelyn','madelynn','madison','mae','maeve','maggie','magnolia','magnus','mahogany',
    'makayla','makenna','makenzie','malachi','malakai','malaya','malcolm','malia','malik',
    'mallory','malone','mamie','mandy','manuela','mara','marc','marcel','marcela','marcella',
    'marcelo','marcia','marco','marcos','marcus','margaret','margarita','marge','margot',
    'marguerite','maria','mariah','mariam','marian','mariana','marianna','marianne','maribel',
    'marie','mariela','marilyn','marina','mario','marion','marisa','marisol','marissa',
    'maritza','marjorie','mark','markus','marla','marlena','marlene','marley','marlon',
    'marquis','marquise','marsha','marshall','marta','martha','martin','martina','marvin',
    'mary','maryann','mason','mateo','mathew','mathias','matilda','matt','matteo','matthew',
    'mattie','maura','maureen','maurice','mauricio','max','maxim','maximilian','maximiliano',
    'maximillian','maximus','maxine','maxwell','may','maya','mckenna','mckenzie','meadow',
    'meagan','meaghan','megan','meghan','melanie','melinda','melissa','melody','melvin',
    'mercedes','mercy','meredith','merle','mia','micaela','micah','michael','michaela',
    'micheal','michele','michelle','miguel','mikaela','mikayla','mike','mikhail','mila',
    'milagros','milan','mildred','miles','miley','millicent','millie','milo','milton',
    'mimi','mindy','minerva','ming','minnie','miracle','miranda','miriam','misty','mitch',
    'mitchell','mohammad','mohammed','moises','mollie','molly','mona','monica','monique',
    'monroe','monte','montgomery','monty','morgan','morris','moses','murphy','murray',
    'mya','myles','myra','myrna','myrtle','nadia','nadine','nancy','naomi','nash',
    'nasir','natalia','natalie','natasha','nate','nathan','nathanael','nathaniel','nayeli',
    'neal','ned','nehemiah','neil','nellie','nelson','nestor','nettie','neva','nevaeh',
    'nia','nicholas','nichole','nick','nickolas','nicky','nico','nicole','nigel','nikita',
    'nikki','nikolas','nina','noah','noe','noel','noelle','noemi','nola','nolan','nora',
    'norma','norman','nova','nyla','oakley','octavia','octavio','olive','oliver','olivia',
    'omar','opal','ophelia','orlando','oscar','osvaldo','otis','otto','owen','pablo',
    'padma','paige','paisley','pam','pamela','paola','paris','parker','pat','patience',
    'patricia','patrick','patsy','patti','patty','paul','paula','paulette','paulina',
    'pauline','paxton','payton','pearl','pedro','peggy','penelope','penny','percy','perla',
    'perry','pete','peter','petra','peyton','phil','philip','phillip','phoebe','phoenix',
    'phyllis','pierce','pierre','piper','polly','porter','precious','presley','preston',
    'prince','princess','priscilla','quinn','quinton','rachael','rachel','rafael','raiden',
    'raina','ralph','ramiro','ramon','ramona','randal','randall','randolph','randy','raphael',
    'raquel','rashad','raul','raven','ray','raymond','rayna','reagan','rebecca','rebekah',
    'reece','reed','reese','regan','reggie','regina','reginald','reid','reign','remy',
    'rene','renee','rex','reynaldo','rhea','rhett','rhiannon','rhonda','ricardo','rich',
    'richard','richie','richmond','rick','rickey','ricky','rider','riley','rita','river',
    'rob','robbie','robby','robert','roberta','roberto','robin','rocco','rochelle','rocio',
    'rocky','rod','roderick','rodger','rodney','rodolfo','rodrigo','roger','rogers','roland',
    'rolando','roman','romeo','ron','ronald','ronaldo','ronan','ronin','ronnie','ronny',
    'rory','rosa','rosalie','rosalind','rosalyn','rosanna','rosario','rose','rosella',
    'roselyn','rosemarie','rosemary','rosie','roslyn','ross','rowan','rowena','roxanne',
    'roy','royce','ruben','ruby','rudolph','rudy','rufus','rupert','russ','russell',
    'rusty','ruth','ruthie','ryan','ryann','ryder','ryker','rylan','rylee','ryleigh',
    'sabrina','sadie','sage','saige','sal','sallie','sally','salvador','salvatore','sam',
    'samantha','samara','samira','sammie','sammy','sampson','samson','samuel','sandra',
    'sandy','santiago','santos','sara','sarah','sarahi','sarai','sasha','saul','savanna',
    'savannah','sawyer','scarlett','scott','scottie','scotty','sean','sebastian','selah',
    'selena','selene','serena','serenity','sergio','seth','seymour','shaina','shana',
    'shane','shania','shannon','sharon','shawn','shawna','shayla','shayna','shea','sheena',
    'sheila','shelby','sheldon','shelia','shelley','shelly','shelton','sheri','sherri',
    'sherry','sheryl','shirley','shonda','shyanne','sid','sidney','sienna','sierra','silas',
    'silvia','simeon','simon','simone','sincere','skylar','skyler','sloane','sofia','sol',
    'soledad','solomon','sonia','sonja','sonny','sonya','sophia','sophie','spencer','stacey',
    'staci','stacie','stacy','stan','stanford','stanley','star','stefan','stefanie',
    'stella','stephan','stephanie','stephen','sterling','steve','steven','stevie','stewart',
    'stuart','sue','summer','sunny','susan','susana','susanna','susanne','susie','suzanne',
    'suzette','sybil','sydney','sylvia','tabatha','tabitha','tad','talia','tamara','tami',
    'tamika','tammy','tana','tandy','tania','tanisha','tanner','tanya','tara','taryn',
    'tasha','tatiana','tatum','taylor','ted','teddy','teodoro','teresa','teri','terrance',
    'terrell','terrence','terri','terry','tess','tessa','thaddeus','thalia','thea','thelma',
    'theodore','theresa','therese','thomas','thompson','tia','tiana','tiara','tiffany',
    'tim','timothy','tina','titus','tobias','toby','tod','todd','tom','tomas','tommy',
    'toni','tonia','tony','tonya','tori','trace','tracey','traci','tracie','tracy','travis',
    'trent','trenton','trevor','trey','tricia','trina','trinity','tripp','trisha','tristan',
    'tristen','tristin','troy','trudy','truman','tucker','turner','ty','tyler','tyra',
    'tyree','tyrell','tyrone','tyson','ulises','ulysses','unique','uri','uriel','ursula',
    'valentina','valentine','valeria','valerie','van','vance','vanessa','vaughn','vera',
    'verna','vernon','veronica','vicki','vickie','vicky','victor','victoria','vince',
    'vincent','vincenzo','viola','violet','violeta','virgil','virginia','vivian','viviana',
    'vivien','vladimir','wade','walker','wallace','wally','walt','walter','wanda','ward',
    'warren','watson','wayne','wendell','wendy','wes','wesley','weston','whitney','wilbert',
    'wilbur','wilda','wiley','wilford','wilfred','wilhelmina','will','willa','willard',
    'william','willie','willis','willow','wilma','wilson','winnie','winston','wm','wolf',
    'woodrow','wyatt','xander','xavier','ximena','xiomara','yael','yahir','yamilet',
    'yareli','yasmin','yazmin','yesenia','yolanda','york','yoshiko','young','yvette',
    'yvonne','zachariah','zachary','zachery','zack','zackary','zackery','zaire','zander',
    'zane','zara','zariah','zayden','zayne','zeke','zelda','zelma','zena','zendaya',
    'zion','zita','zoe','zoey','zoie','zola','zora','zuri',
}


def fetch_nameless_emails():
    all_emails = []
    batch = 1000
    for offset in range(0, 20000, batch):
        url = (f"{SUPABASE_URL}/rest/v1/consumer_leads?select=email"
               f"&name=is.null&limit={batch}&offset={offset}&order=email")
        req = urllib.request.Request(url, headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        })
        resp = urllib.request.urlopen(req, context=SSL_CTX)
        data = json.loads(resp.read().decode())
        if not data:
            break
        all_emails.extend(r['email'] for r in data)
        print(f"  Fetched {len(all_emails)} so far...")
    return list(set(all_emails))


def extract_name(email):
    local = email.split('@')[0].lower().strip()

    local = re.sub(r'^[._\-+]+', '', local)
    local = re.sub(r'[._\-+]+$', '', local)

    if not local or len(local) < 3:
        return None

    parts = re.split(r'[._]', local)

    if len(parts) < 2:
        return None

    candidate = parts[0]

    if not candidate.isalpha():
        return None

    if len(candidate) < 3:
        return None

    if candidate not in COMMON_NAMES:
        return None

    remaining = parts[1]
    if not remaining or len(remaining) < 2:
        return None
    if not re.match(r'^[a-z]+$', remaining):
        return None

    biz_words = {'shop','store','fitness','gym','coach','team','sport','media',
                 'music','tech','game','info','help','news','blog','work','dev',
                 'test','mail','web','app','bot','auto','data','code','hack','bet',
                 'run','running','lift','strength','strong','health','fit','nutrition',
                 'yoga','crossfit','training','trainer','muscle','cardio','wellness',
                 'biz','business','marketing','sales','pro','official','brand',
                 'art','design','photo','video','studio','creative','digital',
                 'real','estate','crypto','invest','trade','fund'}
    full_local = '.'.join(parts)
    for bw in biz_words:
        if bw in full_local:
            return None

    dummy_patterns = {'doe','example','test','fake','sample','nobody','noreply',
                      'anon','anonymous','temp','placeholder','spam','junk'}
    for part in parts:
        if part in dummy_patterns:
            return None

    celeb_emails = {'joe.pesci', 'john.doe', 'jane.doe', 'mike.tyson',
                    'tom.cruise', 'brad.pitt', 'kim.kardashian'}
    local_joined = '.'.join(parts[:2])
    if local_joined in celeb_emails:
        return None

    if len(remaining) > 15:
        return None

    return candidate.title()


def main():
    print("Fetching nameless emails from Supabase...")
    emails = fetch_nameless_emails()
    print(f"Found {len(emails)} emails without names.\n")

    found = {}
    for email in emails:
        name = extract_name(email)
        if name:
            found[email] = name

    print(f"Extracted {len(found)} confident first names.\n")

    print("Sample of extracted names:")
    for i, (email, name) in enumerate(sorted(found.items())[:50]):
        print(f"  {name:15s} <- {email}")

    print(f"\n{'='*60}")
    print(f"Total confident extractions: {len(found)} out of {len(emails)}")
    print(f"{'='*60}")

    with open(os.path.join(BASE_DIR, 'extracted_names.json'), 'w') as f:
        json.dump(found, f, indent=2)
    print(f"\nSaved to extracted_names.json for review before pushing.")


if __name__ == "__main__":
    main()
