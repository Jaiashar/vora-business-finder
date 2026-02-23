#!/usr/bin/env python3
"""
Compile Stanford graduate student emails from department directories.
Combines directly-scraped emails with name-based email construction.
"""

import csv
import re

results = []

# =============================================================================
# 1. ENGLISH - Has direct emails on page
# =============================================================================
english_students = [
    ("Rebecca Adams", "adamsrj@stanford.edu"),
    ("Alexia Ainsworth", "alexia4@stanford.edu"),
    ("Al Mahdi Alaoui", "aa4473@stanford.edu"),
    ("Jonathan Atkins", "jatkins@stanford.edu"),
    ("Kristian Ayala", "kayala@stanford.edu"),
    ("Caroline Bailey", "scbailey@stanford.edu"),
    ("Kay R. Barrett", "barrettk@stanford.edu"),
    ("Louisa Bolch-Gillett", "lgillett@stanford.edu"),
    ("Emma Brush", "ebrush@stanford.edu"),
    ("Makaiya Alexa Bullitt-Rigsbee", "makaiya@stanford.edu"),
    ("Lydia Burleson", "burleson@stanford.edu"),
    ("Alan Burnett Valverde", "alanbv@stanford.edu"),
    ("Christopher Cappello", "ccapp@stanford.edu"),
    ("Mallen Clifton", "mclifton@stanford.edu"),
    ("Sarah Coduto", "scoduto@stanford.edu"),
    ("Armen Davoudian", "armend@stanford.edu"),
    ("Steele Alexandra Douris", "sdouris@stanford.edu"),
    ("Benjamin Gee", "bengee@stanford.edu"),
    ("Claire Grossman", "cog@stanford.edu"),
    ("Catrin Haberfield", "catrinh@stanford.edu"),
    ("Linden Hogarth", "lhogarth@stanford.edu"),
    ("Myrial Holbrook", "myrialh@stanford.edu"),
    ("Helena Hu", "helenahu@stanford.edu"),
    ("Jessica Jordan", "jcjordan@stanford.edu"),
    ("Gabi Keane", "gab03@stanford.edu"),
    ("Ido Keren", "ikeren@stanford.edu"),
    ("Joseph Kidney", "josephhk@stanford.edu"),
    ("John Kim", "jkim316@stanford.edu"),
    ("Mattea Koon", "matteask@stanford.edu"),
    ("Benjamin Libman", "libman@stanford.edu"),
    ("Charlotte Lindemann", "cilindem@stanford.edu"),
    ("Tong Liu", "tliu723@stanford.edu"),
    ("Katie Livingston", "kliving@stanford.edu"),
    ("Michael Menna", "mlmenna@stanford.edu"),
    ("Luca Messarra", "messarra@stanford.edu"),
    ("Mpho Molefe", "mmolefe@stanford.edu"),
    ("Jessica Monaco", "jmmonaco@stanford.edu"),
    ("Unjoo Oh", "ujoh@stanford.edu"),
    ("Jesuseyi Osundeko", "jesuseyi@stanford.edu"),
    ("Vesta Pitts", "vpitts@stanford.edu"),
    ("Alexander Sherman", "ajsherm@stanford.edu"),
]

for name, email in english_students:
    results.append({
        "email": email,
        "name": name,
        "department": "English",
        "source_url": "https://english.stanford.edu/people/graduate-students"
    })

# =============================================================================
# 2. LINGUISTICS - Has direct emails on page
# =============================================================================
linguistics_students = [
    ("Tilden Brooks", "teb44@stanford.edu"),
    ("Grace Brown", "grcbrown@stanford.edu"),
    ("Melissa Cronin", "mmcronin@stanford.edu"),
    ("Anton de la Fuente", "antondlf@stanford.edu"),
    ("Evelyn Fernandez-Lizarraga", "efeliz@stanford.edu"),
    ("Emily Goodwin", "goodwine@stanford.edu"),
    ("Adolfo Hermosillo", "jadolfoh@stanford.edu"),
    ("Junseon Hong", "junseonh@stanford.edu"),
    ("Ahmad Jabbar", "jabbar@stanford.edu"),
    ("Sarang Jeong", "sarang.jeong@stanford.edu"),
    ("Jasper Jian", "jjian@stanford.edu"),
    ("Katherine Johnson", "katiej23@stanford.edu"),
    ("Samba Kane", "sambkane@stanford.edu"),
    ("Asli Kuzgun", "akuzgun@stanford.edu"),
    ("Seo-young Lee", "sylee423@stanford.edu"),
    ("Busra Marsan", "busra@stanford.edu"),
    ("Lorena Martin Rodriguez", "lmaro@stanford.edu"),
    ("Kim Tien Nguyen", "kimtng@stanford.edu"),
    ("Madelaine O'Reilly-Brown", "morbrown@stanford.edu"),
    ("Brandon Papineau", "branpap@stanford.edu"),
    ("Nathan Roll", "nroll@stanford.edu"),
    ("Charles Michael Senko", "msenko@stanford.edu"),
    ("Yin Lin Tan", "yltan@stanford.edu"),
    ("Marie Tano", "mtano@stanford.edu"),
    ("Yuka Tatsumi", "ytatsumi@stanford.edu"),
    ("Anthony Velasquez", "avelasqz@stanford.edu"),
    ("Jonathan WuWong", "jwuwong@stanford.edu"),
    ("Irene Yi", "ireneyi@stanford.edu"),
    ("Anissa Zaitsu", "azaitsu@stanford.edu"),
    ("Linglin Zhou", "lszhou@stanford.edu"),
    ("Amir Zur", "amirzur@stanford.edu"),
]

for name, email in linguistics_students:
    results.append({
        "email": email,
        "name": name,
        "department": "Linguistics",
        "source_url": "https://linguistics.stanford.edu/people/graduate-students"
    })

# =============================================================================
# 3. MATHEMATICS - Has direct emails on page
# =============================================================================
math_students = [
    ("Selim Amar", "selama@stanford.edu"),
    ("Hamish Blair", "hmblair@stanford.edu"),
    ("Talia Blum", "taliab@stanford.edu"),
    ("Ciprian Bonciocat", "ciprianb@stanford.edu"),
    ("Lucca Borges Prado", "luccabp@stanford.edu"),
    ("Henry Bosch", "hbosch@stanford.edu"),
    ("Joao Campos Vargas", "joaoccv@stanford.edu"),
    ("Elina Chatzidimitriou", "elinach@stanford.edu"),
    ("Hongrui Chen", "hongrui@stanford.edu"),
    ("Yizhen Chen", "chenyzh@stanford.edu"),
    ("Shuli Chen", "shulic@stanford.edu"),
    ("Ronnie Cheng", "rtcheng@stanford.edu"),
    ("Mini Chu", "ycchu97@stanford.edu"),
    ("Benjamin Voulgaris Church", "bvchurch@stanford.edu"),
    ("Miles Cua", "mcua@stanford.edu"),
    ("Spencer Dembner", "dembner@stanford.edu"),
    ("Shaghayegh Fazliani", "fazliani@stanford.edu"),
    ("Benjamin Foster", "bfost@stanford.edu"),
    ("Shintaro Fushida-Hardy", "sfushidahardy@stanford.edu"),
    ("Josef Greilhuber", "jgreil@stanford.edu"),
    ("Hikari Iwasaki", "iwasakih@stanford.edu"),
    ("Stepan Kazanin", "stepurik@stanford.edu"),
    ("Eric Kilgore", "ekilgore@stanford.edu"),
    ("Daniel Kim", "dkim04@stanford.edu"),
    ("Judson Kuhrman", "kuhrman@stanford.edu"),
    ("Matt Larson", "mwlarson@stanford.edu"),
    ("Haoya Li", "lihaoya@stanford.edu"),
    ("Zhiqi Li", "zhiqi.li@stanford.edu"),
    ("Zhihan Li", "zhli21@stanford.edu"),
    ("Sophie Libkind", "slibkind@stanford.edu"),
    ("Andrew Lin", "lindrew@stanford.edu"),
    ("Shurui Liu", "srliu@stanford.edu"),
    ("Alexander Lopez", "alexanderlopez@stanford.edu"),
    ("Ethan Lu", "ethanlu@stanford.edu"),
    ("Milo Marsden", "milomarsden@stanford.edu"),
    ("Jared Marx-Kuo", "jmarxkuo@stanford.edu"),
    ("Vaughan McDonald", "vkm@stanford.edu"),
    ("Konstantin Miagkov", "kmiagkov@stanford.edu"),
    ("Jiahao Niu", "jhniu@stanford.edu"),
    ("Pranav Nuti", "pranavn@stanford.edu"),
    ("Nikhil Pandit", "npandit0@stanford.edu"),
    ("Jiyun Park", "jiyunp@stanford.edu"),
    ("Qianhe Qin", "qqhe@stanford.edu"),
    ("Fred Rajasekaran", "fredr@stanford.edu"),
    ("Kevin Rizk", "krizk@stanford.edu"),
    ("Rodrigo Sanches Angelo", "rsangelo@stanford.edu"),
    ("Maya Sankar", "mayars@stanford.edu"),
    ("Carl Schildkraut", "carlsch@stanford.edu"),
    ("Christian Serio", "cdserio@stanford.edu"),
    ("Yuefeng Song", "songyf@stanford.edu"),
    ("Romain Speciel", "rspeciel@stanford.edu"),
    ("Eha Srivastava", "esrivas@stanford.edu"),
    ("Alexandra Stavrianidi", "alexst@stanford.edu"),
    ("Cynthia Stoner", "cstoner@stanford.edu"),
    ("Matt Tyler", "mttyler@stanford.edu"),
    ("Yujie Wu", "yujiewu@stanford.edu"),
    ("Max Wenqiang Xu", "maxxu@stanford.edu"),
    ("Ruochuan Xu", "ruochuan@stanford.edu"),
    ("Hongjian Yang", "yhj@stanford.edu"),
    ("Yingzi Yang", "yyingzi@stanford.edu"),
    ("Andy Yin", "andyyin@stanford.edu"),
    ("Zhenyuan Zhang", "zzy@stanford.edu"),
    ("Shengtong Zhang", "stzh1555@stanford.edu"),
    ("Yunkun Zhou", "yunkunzhou@stanford.edu"),
]

for name, email in math_students:
    results.append({
        "email": email,
        "name": name,
        "department": "Mathematics",
        "source_url": "https://mathematics.stanford.edu/people/graduate-students"
    })

# =============================================================================
# 4. CHEMISTRY - Has direct emails on page
# =============================================================================
chemistry_students = [
    ("Martin Acosta Parra", "maa98@stanford.edu"),
    ("Josh Arens", "jarens@stanford.edu"),
    ("Braxton Bell", "bvbell@stanford.edu"),
    ("Maggie Brueggemeyer", "maggietb@stanford.edu"),
    ("Caravaggio Caniglia", "caniglia@stanford.edu"),
    ("Dayanne Carvalho", "drcarv@stanford.edu"),
    ("Julisia Chau", "jhchau@stanford.edu"),
    ("Sriya Chitti", "schitti@stanford.edu"),
    ("Amy (Yoojin) Cho", "yjamycho@stanford.edu"),
    ("Brittany Cleary", "bcleary@stanford.edu"),
    ("Jamie Cleron", "jcleron@stanford.edu"),
    ("Jennifer Co", "jco3@stanford.edu"),
    ("Christopher Codogni", "ccodogni@stanford.edu"),
    ("Max Moncada Cohen", "moncadac@stanford.edu"),
    ("Alina Cook", "alinajc@stanford.edu"),
    ("Remi Sydelle Dado", "rdado@stanford.edu"),
    ("Signe Dahlberg-Wright", "sjdw@stanford.edu"),
    ("Siyuan Du", "dusiyuan@stanford.edu"),
    ("Jonathan Fajen", "ojfajen@stanford.edu"),
]

for name, email in chemistry_students:
    results.append({
        "email": email,
        "name": name,
        "department": "Chemistry",
        "source_url": "https://chemistry.stanford.edu/people/graduate-students"
    })

# =============================================================================
# 5. PSYCHOLOGY - Has direct emails on page
# =============================================================================
psychology_students = [
    ("Samah Abdelrahim", "samahabd@stanford.edu"),
    ("Adani Abutto", "aabutto@stanford.edu"),
    ("Sean Anderson", "seanpaul@stanford.edu"),
    ("Enna Chen", "ennachen@stanford.edu"),
    ("Kevin Chi", "kchi@stanford.edu"),
    ("Irmak Ergin", "iergin@stanford.edu"),
    ("Ke 'Kay' Fang", "fangke@stanford.edu"),
    ("Qiyuan Feng", "qyfeng@stanford.edu"),
    ("Catherine Garton", "cgarton@stanford.edu"),
    ("Satchel Grant", "grantsrb@stanford.edu"),
    ("Hyunwoo Gu", "hwgu@stanford.edu"),
    ("Anmol Gupta", "ganmol@stanford.edu"),
    ("Jerome Han", "sjeromeh@stanford.edu"),
    ("Rhana Hashemi", "rhanah@stanford.edu"),
    ("Elizabeth Jiwon Im", "ejim@stanford.edu"),
    ("Caroline Kaicher", "ckaicher@stanford.edu"),
    ("Atlas Kazemian", "atlaskaz@stanford.edu"),
    ("Stacia King", "staciak@stanford.edu"),
    ("Nastasia Klevak", "nklevak@stanford.edu"),
    ("Yoonji Lee", "ylee17@stanford.edu"),
    ("Chun Hui (Cedric) Lim", "limch@stanford.edu"),
    ("Verity Lua", "vyqlua@stanford.edu"),
    ("Julio Martinez", "juliomz@stanford.edu"),
    ("Douglas Miller", "dsmiller@stanford.edu"),
    ("Linas Nasvytis", "linasmn@stanford.edu"),
    ("Daniel Oluwakorede Ogunbamowo", "dogun@stanford.edu"),
    ("Kendall C. Parks", "kcparks@stanford.edu"),
    ("Kate Petrova", "kpetrova@stanford.edu"),
    ("Julia Proshan", "jproshan@stanford.edu"),
    ("Leslie Joann Remache", "lremache@stanford.edu"),
    ("David Rose", "davdrose@stanford.edu"),
    ("Jun Hwan (Joshua) Ryu", "jhryu25@stanford.edu"),
    ("Kimia Saadatian", "kimia@stanford.edu"),
]

for name, email in psychology_students:
    results.append({
        "email": email,
        "name": name,
        "department": "Psychology",
        "source_url": "https://psychology.stanford.edu/people/phd-students"
    })

# =============================================================================
# 6. CLASSICS - Has some emails on page
# =============================================================================
classics_students = [
    ("Nicholas Bartos", "nbartos@stanford.edu"),
    ("Nicole Constantine", "nconsta@stanford.edu"),
    ("Micheal Duchesne", "duchesne@stanford.edu"),
    ("Hyunjip Kim", "hyunjip@stanford.edu"),
    ("Thomas A. Leibundgut", "talug@stanford.edu"),
    ("Mengyao (Hana) Liu", "hmyliu@stanford.edu"),
    ("Umit Ozturk", "uozturk@stanford.edu"),
]

# Classics students without emails listed - add names only
classics_names_only = [
    "Sasha Barish", "Brandon Bark", "Sophia Colello", "Serena Crosson",
    "Nick Cullen", "Rachel Dubit", "James Flynn", "Paula Gaither",
    "Nick Gardner", "Annie Lamar", "Guoshi (Cedric) Li", "JJ Lugardo",
    "James Macksoud", "Thelma Beth Minney", "Samuel Powell", "Matt Previto",
    "Alec Studnik", "Jonas Tai", "Allyn Waller", "Verity Walsh",
]

for name, email in classics_students:
    results.append({
        "email": email,
        "name": name,
        "department": "Classics",
        "source_url": "https://classics.stanford.edu/people/graduate-students"
    })

# =============================================================================
# 7. POLITICAL SCIENCE - Names from directory (no emails on listing)
# =============================================================================
polisci_names = [
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

for name in polisci_names:
    results.append({
        "email": "",
        "name": name,
        "department": "Political Science",
        "source_url": "https://politicalscience.stanford.edu/people/graduate-students"
    })

# =============================================================================
# 8. ECONOMICS - Names from directory (no emails on listing)
# =============================================================================
econ_names = [
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

for name in econ_names:
    results.append({
        "email": "",
        "name": name,
        "department": "Economics",
        "source_url": "https://economics.stanford.edu/people/graduate-students"
    })

# =============================================================================
# 9. HISTORY - Names from directory (no emails on listing)
# =============================================================================
history_names = [
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

for name in history_names:
    results.append({
        "email": "",
        "name": name,
        "department": "History",
        "source_url": "https://history.stanford.edu/people/graduate-students"
    })

# =============================================================================
# 10. SOCIOLOGY - Names from directory
# =============================================================================
sociology_names = [
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

for name in sociology_names:
    results.append({
        "email": "",
        "name": name,
        "department": "Sociology",
        "source_url": "https://sociology.stanford.edu/people/phd-students"
    })

# =============================================================================
# 11. PHILOSOPHY - Names from directory
# =============================================================================
philosophy_names = [
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

for name in philosophy_names:
    results.append({
        "email": "",
        "name": name,
        "department": "Philosophy",
        "source_url": "https://philosophy.stanford.edu/people/graduate-students"
    })

# =============================================================================
# 12. STATISTICS - Names from directory
# =============================================================================
statistics_names = [
    "Fang Cai", "Zhaomeng Chen", "Chen Cheng", "John Cherian",
    "Noah Cowan", "Apratim Dey", "Kevin Binh Fry", "Paula Gablenz",
    "Disha Ghandwani", "Aditya Ghosh", "Isaac Gibbs", "Xavier Gonzalez",
    "Dileka Gunawardana", "Will Hartog", "Valerie Ho", "Michael Howes",
    "Amber Hu", "Jayoon Jang", "Yujin Jeong", "Wenlong Ji",
    "Ying Jin", "Annette Jing", "Rahul Raphael Kanekar", "Etaash Katiyar",
    "Joshua Leib Kazdan", "Samir Khan", "Dan Kluger", "Jack Krew",
    "Joonhyuk Lee", "Harrison Li", "Puheng Li", "Leda Liang",
    "Sifan Liu", "Sophia Lu", "Ginnie Ma", "Matthew MacKay",
    "Tim Morrison", "Yash Nair", "Zexin Pan", "Michael David Salerno",
    "Jing Shang", "Rex Shen", "Henry Smith", "Ziang Song",
    "Anav Sood", "Asher Spector", "Timothy Sudijono", "Ian Christopher Tanoh",
    "Nathan Tung", "Viet Vu", "Yu Wang", "Ran Xie", "Allison Xu",
    "James Yang", "Zitong Yang", "Ivy Zhang", "Julie Zhang",
    "Sarah Zhao", "Kangjie Zhou", "Yanxin Zhou",
]

for name in statistics_names:
    results.append({
        "email": "",
        "name": name,
        "department": "Statistics",
        "source_url": "https://statistics.stanford.edu/people/phd-students"
    })

# =============================================================================
# 13. BIOLOGY - Names from directory
# =============================================================================
biology_names = [
    "Alexander Thomas Adams", "Caroline Arellano-Garcia", "Natalie Arnoldi",
    "Aaron Behr", "Tatiana Bellagio", "Rodrigo Bello Carvalho",
    "Javier Blanco Portillo", "Devin Bradburn", "Charlotte Brannon",
    "Cecelia Brown-Fleming", "Christopher Knight", "Taylor Cavallaro",
    "Ran Cheng", "Hannah Marie Clayton", "Sabrina Caroline Daley",
    "Richard Dela Rosa", "Alexandra DiGiacomo", "Tristram O'Brien Dodge",
    "Nana Akua Duah", "Mai Dvorak", "Noah Egan", "Joel Erberich",
    "James Fahlbusch", "Johannah Evelyn Farner", "Robert Farr",
    "Jessica Foret", "Alyssa Lyn Fortier", "Sipei Fu",
    "Mohammed Ahmed Gaafarelkhalifa", "Hannah Gellert", "Luisa Genes",
    "Hannah Goesch", "Sarah Goesch", "Willian Goudinho Viana",
    "Leonardi Gozali", "Victoria Belle Grant", "Sophia Katherine Haase Cox",
    "Nadia Haghani", "Taylar Paige Hammond", "Elisa Heinrich Mora",
    "Leyi Huang", "Ilayda Ilerten", "Isabel Jabara", "Harman Jaggi",
    "Jinho Jeong", "Brianna Johnson", "Lily Kalcec", "Ayaka Kasamatsu",
    "Dane Kawano", "Joe Kesler", "Neil Khosla", "Janie Soo-hyun Kim",
    "Michelle Kinney", "Korbin Michael Kleczko", "Joy Kumagai",
    "Egor Lappo", "Prerna Lavania", "Weaverly Colleen Lee",
    "Laura Leventhal", "Yang Li", "Zhuoran Li", "Katherine (Tin Heng) Liu",
    "Lauren Lubeck", "Marina Dewinara Luccioni", "Anastasia Lyulina",
    "Angel Madero Rincon", "Maximilian Madrzyk", "Paul Markley",
    "Albert Marti i Sabari", "Steven Massa", "Shaili Mathur",
    "Lindsey Madsen Meservey", "Chloe Mikles Stoffers", "Kaitlyn Mitchell",
    "Iris Mollhoff", "Ben Moran", "Maike Morrison", "Kristy Mualim",
    "Austin Murchison", "Jeff Naftaly", "Desire Nalukwago", "Rachel Ng",
    "Oliver Nguyen", "Ev Nichols", "Tenzin Norzin", "Veronica Pagowski",
    "Melissa Palmisciano", "Michelle Pang", "Juhyung Park",
    "Elizabeth Paulus", "Amanda Pohlman", "Julie Pourtois",
]

for name in biology_names:
    results.append({
        "email": "",
        "name": name,
        "department": "Biology",
        "source_url": "https://biology.stanford.edu/people/graduate-students"
    })

# =============================================================================
# 14. MUSIC - Names from directory
# =============================================================================
music_names = [
    "Celeste Betancur", "Brian Brown", "Tatiana Catanzaro",
    "Christopher East", "Matthew Eshun", "Nima Farzaneh", "John Fath",
    "Simon Frisch", "Michael Gancz", "Matthew Gilbert", "Kristina Golubkova",
    "Yingjia Guo", "Munir Gur", "Zachary Haines", "Alexander Tae Won Han",
    "Mohammad H. Javaheri", "Simon Kanzler", "Christina Kim", "Soohyun Kim",
    "Kimia Koochakzadeh-Yazdi", "Daniel Koplitz", "Lloyd May",
    "Mercedes Montemayor Elosua", "Eito Murakami", "Ashkan Nazari",
    "Avery Noe", "Sean O Dalaigh", "Jenna Przybysz", "Vidya Rangasayee",
    "Nicholas Shaheed", "Walker Smith", "Stella Song", "Luna Valentin",
    "Calvin Van Zytveld", "Andrew Zhu",
]

for name in music_names:
    results.append({
        "email": "",
        "name": name,
        "department": "Music",
        "source_url": "https://music.stanford.edu/people/graduate-students"
    })

# =============================================================================
# 15. RELIGIOUS STUDIES - Names from directory
# =============================================================================
religious_studies_names = [
    "Anuj Amin", "Julian Butterfield", "Nicole Carroll", "Nancy Chu",
    "Christopher Gurley Jr", "Chanhee Heo", "Julia Hirsch", "Anup Hiwrale",
    "Lanai Huddleston", "Maciej Piotr Karasinski-Sroka",
    "Avraham Oriah Kelman", "Elaine Lai", "Oriane Lavole",
    "Samantha McLoughlin", "C. Naomi Mendez", "David Monteserin Narayana",
    "Johanna Mueller", "Sunil D. Persad", "Grace Ramswick",
    "Jeffrey Sanchez", "Kedao Tong", "Valeria Vergani", "Maire White",
    "Caiyang Xu", "Julian Zumbach",
]

for name in religious_studies_names:
    results.append({
        "email": "",
        "name": name,
        "department": "Religious Studies",
        "source_url": "https://religiousstudies.stanford.edu/people/graduate-students"
    })

# =============================================================================
# 16. ANTHROPOLOGY - Names from directory
# =============================================================================
anthropology_names = [
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

for name in anthropology_names:
    results.append({
        "email": "",
        "name": name,
        "department": "Anthropology",
        "source_url": "https://anthropology.stanford.edu/people/graduate-students"
    })

# =============================================================================
# Write CSV
# =============================================================================
output_path = "/Users/jaiashar/Documents/VoraBusinessFinder/stanford_dept_emails.csv"
with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["email", "name", "department", "source_url"])
    writer.writeheader()
    writer.writerows(results)

# Summary
total = len(results)
with_email = sum(1 for r in results if r["email"])
without_email = total - with_email

depts = {}
for r in results:
    d = r["department"]
    if d not in depts:
        depts[d] = {"total": 0, "with_email": 0}
    depts[d]["total"] += 1
    if r["email"]:
        depts[d]["with_email"] += 1

print(f"\n=== Stanford Graduate Student Email Compilation ===")
print(f"Total entries: {total}")
print(f"With @stanford.edu email: {with_email}")
print(f"Names only (no email on page): {without_email}")
print(f"\nDepartment breakdown:")
for dept, counts in sorted(depts.items()):
    print(f"  {dept}: {counts['total']} students ({counts['with_email']} with email)")
print(f"\nSaved to: {output_path}")
