import numpy as np
import random

def get_name_data():
    # Pre-compute name groups with expanded lists
    name_groups = {
        'Black': [
            ('Sipho', 'Mokoena'), ('Thabo', 'Nkosi'), ('Nokuthula', 'Dlamini'), ('Lerato', 'Mthembu'),
            ('Tshepo', 'Zulu'), ('Palesa', 'Mabena'), ('Sibusiso', 'Ndlovu'), ('Thandiwe', 'Khumbuza'),
            ('Mpho', 'Mkhize'), ('Zanele', 'Ngobeni'), ('Bongani', 'Sithole'), ('Nomvula', 'Mahlangu'),
            ('Thulani', 'Tshabalala'), ('Khanyisile', 'Mnguni'), ('Siyabonga', 'Khoza'),
            ('Nompumelelo', 'Buthelezi'), ('Lungile', 'Mofokeng'), ('Zodwa', 'Hlatshwayo'),
            ('Mandla', 'Zungu'), ('Busisiwe', 'Mtshali'), ('Ayanda', 'Mabaso'), ('Siphiwe', 'Ngcobo'),
            ('Nthabiseng', 'Molefe'), ('Vusi', 'Mabuza'), ('Lindiwe', 'Shabalala'), ('Themba', 'Maseko'),
            ('Nombulelo', 'Ndaba'), ('Sizwe', 'Msimang'), ('Phumzile', 'Khanyile'), ('Musa', 'Dube'),
            ('Nonhlanhla', 'Mabuza'), ('Sifiso', 'Nxumalo'), ('Zandile', 'Mthethwa'), ('Bhekizizwe', 'Gumede'),
            ('Thokozani', 'Mhlongo'), ('Nosipho', 'Cele'), ('Jabulani', 'Hadebe'), ('Nontokozo', 'Zondi'),
            ('Sandile', 'Ngwenya'), ('Thandeka', 'Sokhela'), ('Siphamandla', 'Mabunda'), ('Zinhle', 'Nkomo'),
            ('Mthokozisi', 'Sibiya'), ('Nokwanda', 'Mkhwanazi'), ('Bonginkosi', 'Mtshali'), ('Lungani', 'Ndebele'),
            ('Sindiswa', 'Mavuso'), ('Vuyani', 'Mdluli'), ('Nqobile', 'Xaba'), ('Sfiso', 'Malinga'),
            ('Phindile', 'Nhlapo'), ('Sanele', 'Mthembu')
        ],
        'Afrikaans': [
            ('Jan', 'van der Merwe'), ('Pieter', 'Botha'), ('Elsa', 'Smit'), ('Marelize', 'Kruger'),
            ('Johan', 'Pretorius'), ('Annelise', 'Venter'), ('Hendrik', 'de Klerk'), ('Marike', 'Coetzee'),
            ('Willem', 'van Wyk'), ('Lizette', 'Nel'), ('Christo', 'du Plessis'), ('Elmarie', 'Steyn'),
            ('Gerhard', 'Fourie'), ('Anri', 'le Roux'), ('Jacques', 'Pienaar'), ('Susanna', 'Swart'),
            ('Andre', 'van Niekerk'), ('Marissa', 'Joubert'), ('Dirk', 'Vermaak'), ('Elna', 'Oosthuizen'),
            ('Ruan', 'de Villiers'), ('Annette', 'Snyman'), ('Cornelis', 'van Zyl'), ('Hannelie', 'Bester'),
            ('Frik', 'Lombard'), ('Lize', 'Muller'), ('Gideon', 'Roux'), ('Tania', 'van Rooyen'),
            ('Herman', 'Schoeman'), ('Retha', 'du Toit'), ('Kobus', 'Engelbrecht'), ('Elize', 'Bothma'),
            ('Deon', 'Marais'), ('Martie', 'Visser'), ('Nico', 'Steenkamp'), ('Carina', 'van Staden'),
            ('Jaco', 'Erasmus'), ('Suzette', 'Liebenberg'), ('Wouter', 'van Heerden'), ('Ilse', 'de Wet'),
            ('Schalk', 'Viljoen'), ('Anja', 'Grobler'), ('Pieter', 'Swanepoel'), ('Marlene', 'de Jager'),
            ('Rikus', 'Breytenbach'), ('Elizebeth', 'Olivier'), ('Danie', 'Jacobs'), ('Chantelle', 'van den Berg'),
            ('Hannes', 'Ferreira'), ('Tersia', 'Nel'), ('Louis', 'Kotze')
        ],
        'English': [
            ('John', 'Smith'), ('Mary', 'Brown'), ('David', 'Johnson'), ('Grace', 'Williams'),
            ('Michael', 'Taylor'), ('Emma', 'Wilson'), ('James', 'Davis'), ('Sarah', 'Clark'),
            ('William', 'Harris'), ('Elizabeth', 'Lewis'), ('Thomas', 'Walker'), ('Rebecca', 'Hall'),
            ('Robert', 'Young'), ('Jennifer', 'Allen'), ('Richard', 'King'), ('Susan', 'Wright'),
            ('Charles', 'Scott'), ('Patricia', 'Green'), ('Joseph', 'Baker'), ('Linda', 'Adams'),
            ('George', 'Nelson'), ('Barbara', 'Carter'), ('Edward', 'Mitchell'), ('Nancy', 'Perez'),
            ('Steven', 'Roberts'), ('Karen', 'Turner'), ('Paul', 'Phillips'), ('Deborah', 'Campbell'),
            ('Mark', 'Parker'), ('Carol', 'Evans'), ('Daniel', 'Edwards'), ('Ruth', 'Collins'),
            ('Matthew', 'Stewart'), ('Betty', 'Sanchez'), ('Andrew', 'Morris'), ('Margaret', 'Rogers'),
            ('Peter', 'Reed'), ('Helen', 'Cook'), ('Frank', 'Morgan'), ('Jane', 'Bell'),
            ('Gary', 'Murphy'), ('Shirley', 'Bailey'), ('Stephen', 'Rivera'), ('Christine', 'Cooper'),
            ('Philip', 'Richardson'), ('Catherine', 'Cox'), ('Brian', 'Howard'), ('Dorothy', 'Ward'),
            ('Alan', 'Torres'), ('Joan', 'Peterson')
        ],
        'Indian': [
            ('Rajesh', 'Naidoo'), ('Aisha', 'Pillay'), ('Sunil', 'Singh'), ('Nisha', 'Patel'),
            ('Amit', 'Govender'), ('Priya', 'Chetty'), ('Vikram', 'Reddy'), ('Anjali', 'Naicker'),
            ('Sanjay', 'Moodley'), ('Rani', 'Raman'), ('Kiran', 'Gounden'), ('Deepa', 'Padayachee'),
            ('Vishal', 'Maharaj'), ('Shalini', 'Govindasamy'), ('Ravi', 'Perumal'), ('Meera', 'Naidu'),
            ('Arjun', 'Sewpersad'), ('Pooja', 'Ramkissoon'), ('Naveen', 'Chellan'), ('Divya', 'Singh'),
            ('Rahul', 'Pillai'), ('Sonia', 'Moodley'), ('Krishna', 'Naicker'), ('Tara', 'Govender'),
            ('Vivek', 'Ramdass'), ('Lakshmi', 'Chetty'), ('Anand', 'Patel'), ('Neha', 'Reddy'),
            ('Rakesh', 'Naidoo'), ('Shilpa', 'Maharaj'), ('Dinesh', 'Gounden'), ('Kavita', 'Padayachee'),
            ('Suresh', 'Perumal'), ('Rina', 'Raman'), ('Ajay', 'Sewpersad'), ('Rekha', 'Ramkissoon'),
            ('Vijay', 'Chellan'), ('Sunita', 'Singh'), ('Mohan', 'Pillai'), ('Pavitra', 'Moodley'),
            ('Nikhil', 'Naicker'), ('Aarti', 'Govender'), ('Prakash', 'Ramdass'), ('Seema', 'Chetty'),
            ('Sameer', 'Patel'), ('Jyoti', 'Reddy'), ('Kamal', 'Naidoo'), ('Reshma', 'Maharaj'),
            ('Rohan', 'Gounden'), ('Nalini', 'Padayachee'), ('Vinod', 'Perumal')
        ],
        'Coloured': [
            ('Rene', 'Adams'), ('Liezl', 'Davids'), ('Abdul', 'Williams'), ('Fazila', 'Johnson'),
            ('Waseem', 'Abrahams'), ('Natasha', 'Jacobs'), ('Ibrahim', 'Petersen'), ('Shereen', 'Fortuin'),
            ('Shaun', 'Hendricks'), ('Zainab', 'Isaacs'), ('Mogamat', 'Khan'), ('Candice', 'Brown'),
            ('Ebrahim', 'Arendse'), ('Nadia', 'Manuel'), ('Yusuf', 'Cupido'), ('Marissa', 'Daniels'),
            ('Imraan', 'Jacobs'), ('Ayesha', 'Samuels'), ('Riyaad', 'Fisher'), ('Tamaryn', 'Adams'),
            ('Faheem', 'Davids'), ('Soraya', 'Williams'), ('Ismail', 'Johnson'), ('Chantel', 'Abrahams'),
            ('Rashied', 'Petersen'), ('Bianca', 'Fortuin'), ('Tawfeeq', 'Hendricks'), ('Leila', 'Isaacs'),
            ('Ridwaan', 'Khan'), ('Michelle', 'Brown'), ('Abdullah', 'Arendse'), ('Taryn', 'Manuel'),
            ('Muneer', 'Cupido'), ('Janine', 'Daniels'), ('Nazeem', 'Jacobs'), ('Fatima', 'Samuels'),
            ('Ashraf', 'Fisher'), ('Carla', 'Adams'), ('Siraaj', 'Davids'), ('Zahra', 'Williams'),
            ('Mikhail', 'Johnson'), ('Rochelle', 'Abrahams'), ('Hishaam', 'Petersen'), ('Kim', 'Fortuin'),
            ('Junaid', 'Hendricks'), ('Amina', 'Isaacs'), ('Saleem', 'Khan'), ('Vanessa', 'Brown'),
            ('Zaid', 'Arendse'), ('Melissa', 'Manuel'), ('Raashid', 'Cupido')
        ]
    }
    zw_names = [
        ('Tariro', 'Dube'), ('Farai', 'Moyo'), ('Kudzai', 'Chirwa'), ('Ruvimbo', 'Ndlovu'),
        ('Tatenda', 'Sibanda'), ('Chipo', 'Mhlope'), ('Tinashe', 'Gumbo'), ('Nyasha', 'Mapfumo'),
        ('Tendai', 'Ncube'), ('Rumbidzai', 'Muzenda'), ('Tafadzwa', 'Chigumba'), ('Kudakwashe', 'Mare'),
        ('Blessing', 'Mushonga'), ('Shamiso', 'Chiweshe'), ('Tapiwa', 'Mutasa'), ('Rudo', 'Makoni'),
        ('Tinotenda', 'Chirwa'), ('Vimbai', 'Nkomo'), ('Munyaradzi', 'Dube'), ('Tsitsi', 'Moyo'),
        ('Tawanda', 'Sibanda'), ('Netsai', 'Mhlope'), ('Simbarashe', 'Gumbo'), ('Chiedza', 'Mapfumo'),
        ('Takudzwa', 'Ncube'), ('Ropafadzo', 'Muzenda'), ('Taurai', 'Chigumba'), ('Rutendo', 'Mare'),
        ('Tafara', 'Mushonga'), ('Vongai', 'Chiweshe')
    ]
    ethnicity_probs = np.array([0.65, 0.14, 0.11, 0.054, 0.055])
    ethnicity_probs = ethnicity_probs / ethnicity_probs.sum()
    ethnicity_keys = list(name_groups.keys())

    return name_groups, zw_names, ethnicity_probs, ethnicity_keys

def generate_name(is_sa_prob=0.85):
    name_groups, zw_names, ethnicity_probs, ethnicity_keys = get_name_data()
    is_sa = random.random() < is_sa_prob
    if is_sa:
        ethnicity_idx = np.random.choice(len(ethnicity_keys), p=ethnicity_probs)
        ethnicity = ethnicity_keys[ethnicity_idx]
        first, last = random.choice(name_groups[ethnicity])
        nationality = 'South Africa'
        citizenship = 'ZA'
    else:
        first, last = random.choice(zw_names)
        ethnicity = 'Foreign National'
        nationality = 'Zimbabwe'
        citizenship = 'ZW'
    full_name = f"{first} {last}"
    return full_name, nationality, citizenship, ethnicity