import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import date
import os
from tqdm import tqdm

def generate_customer_data(year):
    fake = Faker()
    SEED = 42
    random.seed(SEED)
    np.random.seed(SEED)

    # Adjust customer volumes based on COVID impact
    if year == 2020:
        # Minimal registrations due to strict lockdowns (March-May 2020) and overall impact
        NUM_INDIVIDUALS = random.randint(0, 500)  # Very low or zero
        NUM_COMPANIES = random.randint(0, 100)    # Very low
        print("Note: 2020 year - Reduced registrations due to COVID-19 lockdowns in South Africa.")
    elif year == 2019:
        # Slight reduction anticipating early COVID effects
        NUM_INDIVIDUALS = random.randint(12000, 20000)  # 20% reduction from normal
        NUM_COMPANIES = random.randint(400, 1200)       # 20% reduction
        print("Note: 2019 year - Slight reduction due to early COVID-19 anticipation.")
    else:
        # Normal volumes
        NUM_INDIVIDUALS = random.randint(15000, 25000)
        NUM_COMPANIES = random.randint(500, 1500)

    # Ethnic groups and name pools for South Africa
    black_sa_first_names = ['Sipho', 'Thabo', 'Nokuthula', 'Lerato', 'Tshepo', 'Palesa', 'Sibusiso', 'Thandiwe', 'Mpho', 'Zanele', 'Bongani', 'Nomvula', 'Thulani', 'Khanyisile', 'Siyabonga', 'Nompumelelo', 'Lungile', 'Zodwa', 'Mandla', 'Busisiwe', 'Vusi', 'Nontsikelelo', 'Themba', 'Phumzile']
    black_sa_last_names = ['Mokoena', 'Nkosi', 'Dlamini', 'Mthembu', 'Zulu', 'Mabena', 'Ndlovu', 'Khumbuza', 'Mkhize', 'Ngobeni', 'Sithole', 'Mahlangu', 'Tshabalala', 'Mnguni', 'Khoza', 'Buthelezi', 'Mofokeng', 'Hlatshwayo', 'Zungu', 'Mtshali', 'Ngcobo', 'Nxumalo', 'Gumede', 'Shabangu']
    afr_first_names = ['Jan', 'Pieter', 'Elsa', 'Marelize', 'Johan', 'Annelise', 'Hendrik', 'Marike', 'Willem', 'Lizette', 'Christo', 'Elmarie', 'Gerhard', 'Anri', 'Jacques', 'Susanna', 'Ruan', 'Carina', 'Theunis', 'Marissa', 'Frik', 'Elna', 'Deon', 'Riëtte']
    afr_last_names = ['van der Merwe', 'Botha', 'Smit', 'Kruger', 'Janse van Rensburg', 'Pretorius', 'Venter', 'de Klerk', 'Coetzee', 'van Wyk', 'Nel', 'du Plessis', 'Steyn', 'Vermaak', 'Joubert', 'Fourie', 'le Roux', 'van Niekerk', 'Pienaar', 'Swart', 'Bester', 'de Wet', 'Lombard', 'Malan']
    eng_first_names = ['John', 'Mary', 'David', 'Grace', 'Michael', 'Emma', 'James', 'Sarah', 'William', 'Elizabeth', 'Thomas', 'Rebecca', 'Charles', 'Emily', 'George', 'Charlotte', 'Henry', 'Victoria', 'Edward', 'Sophie', 'Richard', 'Laura', 'Daniel', 'Hannah']
    eng_last_names = ['Smith', 'Brown', 'Johnson', 'Williams', 'Taylor', 'Wilson', 'Davis', 'Clark', 'Harris', 'Lewis', 'Walker', 'Hall', 'Allen', 'Young', 'King', 'Wright', 'Scott', 'Green', 'Baker', 'Adams', 'Nelson', 'Carter', 'Mitchell', 'Turner']
    ind_first_names = ['Rajesh', 'Aisha', 'Sunil', 'Nisha', 'Amit', 'Priya', 'Vikram', 'Anjali', 'Ravi', 'Shalini', 'Arun', 'Divya', 'Sanjay', 'Pooja', 'Kiran', 'Meera', 'Rahul', 'Sneha', 'Deepak', 'Kavita', 'Vijay', 'Neha', 'Rakesh', 'Suman']
    ind_last_names = ['Naidoo', 'Pillay', 'Singh', 'Dadoo', 'Moodley', 'Govender', 'Chetty', 'Patel', 'Reddy', 'Naicker', 'Rama', 'Gopal', 'Khan', 'Mistry', 'Parbhoo', 'Sookraj', 'Maharaj', 'Nair', 'Desai', 'Chunder', 'Ramdass', 'Sewpersad', 'Brijlal', 'Lalla']
    col_first_names = ['Rene', 'Liezl', 'Hendrik', 'Mariette', 'Abdul', 'Fazila', 'Waseem', 'Natasha', 'Ibrahim', 'Shereen', 'Yusuf', 'Ameena', 'Ebrahim', 'Zainab', 'Mogamat', 'Fatima', 'Ismail', 'Raeeqa', 'Hassan', 'Nadia', 'Rashied', 'Soraya', 'Achmat', 'Zubeida']
    col_last_names = ['Adams', 'Davids', 'Williams', 'Johnson', 'Abrahams', 'Jacobs', 'Petersen', 'Fortuin', 'Arendse', 'Isaacs', 'Daniels', 'Hendricks', 'Manuel', 'Brown', 'Cupido', 'Saunders', 'Le Roux', 'Smith', 'Beukes', 'Jardine', 'Goliath', 'Salie', 'Kannemeyer', 'Parker']
    zw_first_names = ['Tariro', 'Farai', 'Kudzai', 'Ruvimbo', 'Tatenda', 'Chipo', 'Tinashe', 'Nyasha', 'Tapiwa', 'Rumbidzai', 'Tawanda', 'Shamiso', 'Tafadzwa', 'Mufaro', 'Tendai', 'Chiedza', 'Simbarashe', 'Nokutenda', 'Munyaradzi', 'Gamuchirai', 'Tinotenda', 'Rutendo', 'Kundai', 'Tsitsi']
    zw_last_names = ['Dube', 'Moyo', 'Chirwa', 'Ndlovu', 'Mutsvangwa', 'Sibanda', 'Mhlope', 'Gumbo', 'Mapfumo', 'Chigumbu', 'Mushonga', 'Makoni', 'Chinamasa', 'Mpofu', 'Shumba', 'Tshuma', 'Mabika', 'Chiweshe', 'Marufu', 'Zivai', 'Nyathi', 'Banda', 'Mudzuri', 'Gwande']

    # Combine name pools
    all_sa_first_names = black_sa_first_names + afr_first_names + eng_first_names + ind_first_names + col_first_names
    all_sa_last_names = black_sa_last_names + afr_last_names + eng_last_names + ind_last_names + col_last_names
    all_zw_first_names = zw_first_names
    all_zw_last_names = zw_last_names

    # Demographic weights for SA ethnicities
    sa_ethnicity_weights = {'Black': 0.8, 'Afrikaans': 0.06, 'English': 0.09, 'Indian': 0.03, 'Coloured': 0.02}
    ethnicity_keys, ethnicity_weights = list(sa_ethnicity_weights.keys()), list(sa_ethnicity_weights.values())

    # South African provinces and cities
    sa_provinces = ["Gauteng", "KwaZulu-Natal", "Western Cape", "Eastern Cape", "Limpopo", "Mpumalanga", "Free State", "Northern Cape", "North West"]
    sa_cities_weights = {
        'Johannesburg': 0.25, 'Cape Town': 0.18, 'Durban': 0.15, 'Pretoria': 0.12,
        'Port Elizabeth': 0.05, 'Bloemfontein': 0.03, 'East London': 0.03, 'Pietermaritzburg': 0.02, 'Other': 0.17
    }
    cities, city_weights = list(sa_cities_weights.keys()), list(sa_cities_weights.values())

    # Visa types
    visa_types = ['Work', 'Student', 'Tourist', 'Business', 'Diplomatic', 'Transit', 'Medical', 'Exchange', 'Permanent Resident', 'Refugee']

    # Occupations and industries
    industry_occupation_map = {
        'Finance': ['Accountant', 'Banker', 'Financial Advisor', 'Auditor', 'Risk Manager', 'Loan Officer'],
        'Retail': ['Salesperson', 'Cashier', 'Store Manager', 'Inventory Specialist', 'Customer Service'],
        'Manufacturing': ['Engineer', 'Mechanic', 'Quality Control Inspector', 'Production Manager', 'Assembler'],
        'Healthcare': ['Nurse', 'Doctor', 'Pharmacist', 'Dentist', 'Therapist', 'Medical Technician'],
        'IT': ['Software Developer', 'Data Analyst', 'System Administrator', 'Network Engineer', 'IT Support'],
        'Transport': ['Driver', 'Logistician', 'Fleet Manager', 'Dispatcher', 'Warehouse Worker'],
        'Construction': ['Carpenter', 'Electrician', 'Plumber', 'Construction Worker', 'Site Manager'],
        'Consulting': ['Consultant', 'Business Analyst', 'Project Manager', 'Management Consultant'],
        'Agriculture': ['Farmer', 'Agricultural Technician', 'Animal Handler', 'Farm Manager'],
        'Education': ['Teacher', 'Professor', 'Librarian', 'Counselor', 'Administrator'],
        'Energy': ['Engineer', 'Technician', 'Safety Officer', 'Energy Analyst'],
        'Mining': ['Miner', 'Geologist', 'Safety Officer', 'Engineer'],
        'Hospitality': ['Chef', 'Waiter', 'Hotel Manager', 'Receptionist', 'Housekeeper'],
        'Tourism': ['Tour Guide', 'Travel Agent', 'Hotel Staff', 'Event Coordinator'],
        'Real Estate': ['Real Estate Agent', 'Property Manager', 'Appraiser', 'Broker'],
        'Telecommunications': ['Network Engineer', 'Technician', 'Salesperson', 'Customer Service'],
        'Media': ['Journalist', 'Editor', 'Photographer', 'Producer'],
        'Entertainment': ['Musician', 'Actor', 'Producer', 'Director'],
        'Logistics': ['Logistician', 'Warehouse Worker', 'Dispatcher', 'Driver'],
        'Automotive': ['Mechanic', 'Salesperson', 'Technician', 'Customer Service'],
        'Pharmaceuticals': ['Pharmacist', 'Researcher', 'Lab Technician', 'Sales Representative'],
        'Biotechnology': ['Researcher', 'Lab Technician', 'Scientist'],
        'Aerospace': ['Engineer', 'Pilot', 'Technician'],
        'Defense': ['Engineer', 'Technician', 'Security Specialist'],
        'Environmental Services': ['Environmental Scientist', 'Technician', 'Consultant'],
        'Waste Management': ['Waste Collector', 'Technician', 'Manager'],
        'Utilities': ['Engineer', 'Technician', 'Manager'],
        'Insurance': ['Underwriter', 'Claims Adjuster', 'Agent', 'Actuary'],
        'Legal Services': ['Lawyer', 'Paralegal', 'Legal Secretary'],
        'Marketing and Advertising': ['Marketing Specialist', 'Graphic Designer', 'Copywriter'],
        'Human Resources': ['HR Specialist', 'Recruiter', 'Trainer'],
        'Public Administration': ['Civil Servant', 'Policy Analyst', 'Administrator'],
        'Non-Profit': ['Program Manager', 'Fundraiser', 'Volunteer Coordinator'],
        'Research and Development': ['Scientist', 'Researcher', 'Lab Technician'],
        'Food and Beverage': ['Chef', 'Waiter', 'Food Scientist'],
        'Textiles': ['Designer', 'Tailor', 'Technician'],
        'Chemicals': ['Chemist', 'Lab Technician', 'Engineer'],
        'Electronics': ['Engineer', 'Technician', 'Assembler'],
        'Fashion': ['Designer', 'Model', 'Retail Worker'],
        'Sports and Recreation': ['Coach', 'Trainer', 'Athlete']
    }
    all_occupations = list({occ for occs in industry_occupation_map.values() for occ in occs}) + ['Unemployed', 'Student', 'Politician', 'Manager', 'Entrepreneur', 'Self-Employed']
    industry_sectors = list(industry_occupation_map.keys())
    company_sizes = ['Micro (<10)', 'Small (10-50)', 'Medium (51-250)', 'Large (>250)']

    # Big companies
    big_companies = [
        'Sasol', 'MTN South Africa', 'Shoprite', 'Standard Bank', 'Nedbank', 'Vodacom', 
        'Bidvest Group', 'Anglo American', 'FirstRand', 'Absa Group', 'Telkom', 
        'Eskom', 'Pick n Pay', 'Barloworld', 'Impala Platinum', 'Gold Fields', 
        'Discovery Limited', 'Sanlam', 'Old Mutual', 'MultiChoice Group', 'Aspen Pharmacare', 
        'Exxaro Resources', 'Kumba Iron Ore', 'Woolworths Holdings', 'AngloGold Ashanti', 
        'South African Breweries', 'Naspers', 'Mediclinic International', 'RCL Foods', 
        'Tiger Brands', 'Distell Group', 'Clicks Group', 'Spar Group', 'Investec', 
        'Liberty Holdings', 'Remgro', 'Pepkor Holdings', 'Capitec Bank', 'Anglo Platinum', 
        'Sappi'
    ]
    faker_companies = [fake.company() for _ in range(500)]

    def weighted_choice(choices, weights):
        return random.choices(choices, weights=weights, k=1)[0]

    def generate_realistic_age():
        age_ranges = [(18, 25), (26, 35), (36, 45), (46, 55), (56, 65), (66, 80)]
        weights = [0.25, 0.30, 0.20, 0.15, 0.07, 0.03]
        age_range = random.choices(age_ranges, weights=weights)[0]
        return random.randint(*age_range)

    def generate_income_and_risk(age, occupation, employer):
        income_ranges = {
            'Doctor': (800000, 2000000), 'Lawyer': (600000, 1500000), 'Engineer': (400000, 800000),
            'Teacher': (250000, 400000), 'Nurse': (300000, 500000), 'Unemployed': (0, 0),
            'Student': (0, 50000), 'Manager': (500000, 1200000)
        }
        base_range = income_ranges.get(occupation, (200000, 600000))
        age_multiplier = 1.0 + (age - 25) * 0.02 if age > 25 else 0.8
        employer_multiplier = 1.3 if employer in big_companies else 1.0
        annual_income = int(random.uniform(*base_range) * age_multiplier * employer_multiplier)
        base_risk = 0.1 + (1 / (annual_income + 1)) * 100000
        if occupation == 'Unemployed':
            base_risk += 0.3
        if age < 25:
            base_risk += 0.1
        if employer == 'Self-Employed':
            base_risk += 0.15
        return annual_income, min(base_risk, 1.0)

    def generate_realistic_address(is_sa):
        if is_sa:
            city = weighted_choice(cities, city_weights)
            if city != 'Other':
                return f"{fake.street_address()}, {city}, South Africa"
            else:
                return f"{fake.street_address()}, {random.choice(sa_provinces)}, South Africa"
        else:
            return fake.address().replace('\n', ', ')

    def enhanced_company_data():
        return {
            'CompanyAge': random.randint(1, 50),
            'NumberOfEmployees': weighted_choice([5, 25, 100, 500], [0.6, 0.25, 0.10, 0.05]),
            'AnnualTurnover': random.randint(1000000, 100000000),
            'DirectorsCount': random.randint(1, 8),
            'ShareholdersCount': random.randint(1, 15),
            'BEELevel': random.randint(1, 8),
            'VATRegistered': random.random() < 0.7,
            'IndustryRiskRating': random.choice(['Low', 'Medium', 'High'])
        }

    def get_acquisition_date_realistic(year):
        if year == 2020:
            # Bias towards post-lockdown (after May 2020) or very few in early year
            if random.random() < 0.1:  # 10% chance for pre-lockdown (Jan-Feb)
                return fake.date_between(start_date=date(year, 1, 1), end_date=date(year, 2, 29))
            else:
                return fake.date_between(start_date=date(year, 6, 1), end_date=date(year, 12, 31))
        else:
            quarter_weights = [0.3, 0.2, 0.2, 0.3]
            quarter = random.choices([1, 2, 3, 4], weights=quarter_weights)[0]
            if quarter == 1:
                return fake.date_between(start_date=date(year, 1, 1), end_date=date(year, 3, 31))
            elif quarter == 2:
                return fake.date_between(start_date=date(year, 4, 1), end_date=date(year, 6, 30))
            elif quarter == 3:
                return fake.date_between(start_date=date(year, 7, 1), end_date=date(year, 9, 30))
            else:
                return fake.date_between(start_date=date(year, 10, 1), end_date=date(year, 12, 31))

    def generate_sa_name(is_sa):
        if is_sa:
            group = weighted_choice(ethnicity_keys, ethnicity_weights)
            first_names = {'Black': black_sa_first_names, 'Afrikaans': afr_first_names, 'English': eng_first_names, 
                           'Indian': ind_first_names, 'Coloured': col_first_names}
            last_names = {'Black': black_sa_last_names, 'Afrikaans': afr_last_names, 'English': eng_last_names, 
                          'Indian': ind_last_names, 'Coloured': col_last_names}
            return random.choice(first_names[group]), random.choice(last_names[group])
        else:
            nationality = random.choice(['Zimbabwe', 'Botswana', 'Namibia', 'Mozambique'])
            return random.choice(all_zw_first_names), random.choice(all_zw_last_names), nationality

    def generate_individual_customer(idx):
        is_sa = random.random() < 0.8
        if is_sa:
            first, last = generate_sa_name(True)
            nationality, citizenship = 'South Africa', 'ZA'
        else:
            first, last, nationality = generate_sa_name(False)
            citizenship = {'Zimbabwe': 'ZW', 'Botswana': 'BW', 'Namibia': 'NA', 'Mozambique': 'MZ'}[nationality]

        full_name = f"{first} {last}"
        age = generate_realistic_age()
        birth_date = date.today() - pd.DateOffset(years=age)  # Approximate birth date
        gender = random.choices(['M', 'F', 'Other', 'Prefer not to say'], weights=[0.48, 0.48, 0.02, 0.02])[0]
        id_type = 'National ID' if is_sa else 'Passport'
        id_number = ''.join(str(random.randint(0, 9)) for _ in range(13)) if id_type == 'National ID' else fake.bothify(text='??######')
        expiry_date = None if id_type == 'National ID' else fake.date_between(start_date=date(2020, 1, 1), end_date=date(2025, 12, 31))
        visa_type = random.choice(visa_types) if not is_sa else None
        visa_expiry = fake.date_between(start_date=date(2020, 1, 1), end_date=date(2025, 12, 31)) if not is_sa else None
        occupation = 'Unemployed' if age < 35 and random.random() < 0.65 else random.choice(all_occupations)
        employer = None if occupation in ['Unemployed', 'Student'] else (
            'Self-Employed' if random.random() < 0.15 else
            'Informal Business' if random.random() < 0.3 else
            random.choice(big_companies) if random.random() < 0.4 else
            random.choice(faker_companies)
        )
        annual_income, risk_score = generate_income_and_risk(age, occupation, employer)
        source_of_funds = (
            random.choice(['Inheritance', 'Allowance']) if occupation == 'Student' else
            random.choice(['Grants', 'Savings', 'Pension']) if occupation == 'Unemployed' else
            weighted_choice(['Salary', 'Inheritance', 'Business', 'Grants', 'Savings', 'Pension'], [0.6, 0.05, 0.1, 0.05, 0.15, 0.05])
        )
        if age > 50 and random.random() < 0.1:
            source_of_funds = 'Inheritance'
        marital_status = random.choices(['Single', 'Married', 'Divorced', 'Widowed'], weights=[0.45, 0.4, 0.1, 0.05])[0]
        is_pep = random.random() < 0.02
        tax_id = ''.join(str(random.randint(0, 9)) for _ in range(10))
        residential_address = generate_realistic_address(is_sa)
        preferred_contact = random.choice(['Email', 'Phone', 'SMS', 'Mail'])
        date_of_entry = get_acquisition_date_realistic(year)

        return {
            'CustomerID': f'IND{idx:06d}', 'CustomerType': 'Individual', 'FullName': full_name, 'BirthDate': birth_date,
            'Citizenship': citizenship, 'ResidentialAddress': residential_address, 'CommercialAddress': None,
            'Email': fake.email(), 'PhoneNumber': fake.phone_number(), 'IDType': id_type, 'IDNumber': id_number,
            'ExpiryDate': expiry_date, 'VisaType': visa_type, 'VisaExpiryDate': visa_expiry, 'IsPEP': is_pep,
            'SanctionedCountry': False, 'RiskScore': risk_score, 'TaxIDNumber': tax_id, 'Occupation': occupation,
            'EmployerName': employer, 'SourceOfFunds': source_of_funds, 'MaritalStatus': marital_status,
            'Nationality': nationality, 'Gender': gender, 'PreferredContactMethod': preferred_contact, 'NextOfKin': None,
            'DateOfEntry': date_of_entry, 'AnnualIncome': annual_income, 'Age': age
        }

    def generate_company_customer(idx):
        company_name = fake.company()
        reg_number = f"{random.randint(1900, year)}/{random.randint(100000, 999999)}/{random.randint(1, 99)}"
        industry = random.choice(industry_sectors)
        size = random.choice(company_sizes)
        risk_score = round(np.random.beta(a=1.5, b=7), 3)
        tax_id = ''.join(str(random.randint(0, 9)) for _ in range(10))
        commercial_address = generate_realistic_address(True)
        preferred_contact = random.choice(['Email', 'Phone', 'SMS', 'Mail'])
        next_of_kin = fake.name()
        date_of_entry = get_acquisition_date_realistic(year)
        company_details = enhanced_company_data()

        return {
            'CustomerID': f'COM{idx:06d}', 'CustomerType': 'Company', 'FullName': company_name, 'BirthDate': None,
            'Citizenship': 'ZA', 'ResidentialAddress': None, 'CommercialAddress': commercial_address,
            'Email': fake.company_email(), 'PhoneNumber': fake.phone_number(), 'IDType': 'Registration Number',
            'IDNumber': reg_number, 'ExpiryDate': None, 'VisaType': None, 'VisaExpiryDate': None, 'IsPEP': False,
            'SanctionedCountry': False, 'RiskScore': risk_score, 'TaxIDNumber': tax_id, 'Occupation': industry,
            'EmployerName': None, 'SourceOfFunds': industry, 'MaritalStatus': None, 'Nationality': 'South Africa',
            'Gender': None, 'PreferredContactMethod': preferred_contact, 'NextOfKin': next_of_kin,
            'DateOfEntry': date_of_entry, **company_details
        }

    print(f"Starting generation for year {year}...")

    if NUM_INDIVIDUALS == 0 and NUM_COMPANIES == 0:
        print("No customers generated for this year due to COVID-19 impact.")
        df = pd.DataFrame()
    else:
        # Generate individuals and companies with tqdm
        individuals = [generate_individual_customer(i + 1) for i in tqdm(range(NUM_INDIVIDUALS), desc="Generating Individuals")]
        companies = [generate_company_customer(i + 1) for i in tqdm(range(NUM_COMPANIES), desc="Generating Companies")]

        all_customers = individuals + companies
        df = pd.DataFrame(all_customers)

        # Assign NextOfKin for 20% of individuals
        if not df.empty and 'CustomerType' in df.columns:
            ind_rows = df[df['CustomerType'] == 'Individual'].copy()
            if len(ind_rows) > 0:
                mask = np.random.random(len(ind_rows)) < 0.2
                selected_indices = ind_rows[mask].index

                next_of_kin = []
                for idx in selected_indices:
                    row = df.loc[idx]
                    if random.random() < 0.5:
                        possible_kin = ind_rows[ind_rows['CustomerID'] != row['CustomerID']]['CustomerID'].values
                        next_of_kin.append(np.random.choice(possible_kin) if len(possible_kin) > 0 else None)
                    else:
                        kin_first = random.choice(all_sa_first_names) if row['Citizenship'] == 'ZA' else random.choice(all_zw_first_names)
                        kin_last = row['FullName'].split()[-1] if random.random() < 0.7 else random.choice(all_sa_last_names if row['Citizenship'] == 'ZA' else all_zw_last_names)
                        next_of_kin.append(f"{kin_first} {kin_last}")

                df.loc[selected_indices, 'NextOfKin'] = next_of_kin

        # Shuffle final dataframe
        df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)

    # Save to file
    github_repo_path = '/content/Datasets-Final'
    os.makedirs(github_repo_path, exist_ok=True)
    output_file = f'{github_repo_path}/customers_{year}.parquet'
    df.to_parquet(output_file, index=False)

    print(f"Generated {len(df)} customers (Individuals: {NUM_INDIVIDUALS}, Companies: {NUM_COMPANIES}) for year {year}")
    print(f"Saved to {output_file}")

    return df

if __name__ == "__main__":
    generate_customer_data(2018)