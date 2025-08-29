import pandas as pd
import numpy as np
from faker import Faker
import random
import os
from datetime import date, timedelta
from tqdm import tqdm
import re

fake = Faker()
random.seed(42)
np.random.seed(42)

NUM_INDIVIDUALS = random.randint(25865, 389272)
NUM_COMPANIES = 40

ENTRY_MODES = ['Website', 'Application', 'Agent', 'Walk-in']

# Country-to-province-to-city mapping
COUNTRY_PROVINCES_CITIES = {
    'South Africa': {
        'Western Cape': ['Cape Town', 'Stellenbosch', 'George'],
        'Gauteng': ['Johannesburg', 'Pretoria', 'Soweto'],
        'KwaZulu-Natal': ['Durban', 'Pietermaritzburg']
    },
    'United Kingdom': {
        'England': ['London', 'Manchester', 'Birmingham'],
        'Scotland': ['Edinburgh', 'Glasgow'],
        'Wales': ['Cardiff']
    },
    'United States': {
        'California': ['Los Angeles', 'San Francisco', 'San Diego'],
        'New York': ['New York City', 'Buffalo'],
        'Texas': ['Houston', 'Austin']
    },
    'Canada': {
        'Ontario': ['Toronto', 'Ottawa'],
        'British Columbia': ['Vancouver', 'Victoria'],
        'Quebec': ['Montreal', 'Quebec City']
    },
    'Germany': {
        'Bavaria': ['Munich', 'Nuremberg'],
        'Berlin': ['Berlin'],
        'Hamburg': ['Hamburg']
    },
    'Zimbabwe': {
        'Harare': ['Harare'],
        'Bulawayo': ['Bulawayo'],
        'Manicaland': ['Mutare']
    },
    'India': {
        'Maharashtra': ['Mumbai', 'Pune'],
        'Delhi': ['New Delhi'],
        'Karnataka': ['Bangalore']
    },
    'Other': {
        'Generic Province': ['Generic City']  # Renamed for clarity
    }
}

# Phone number plans
PHONE_PLANS = {
    'South Africa': {'cc': '+27', 'nsn_length': 9, 'mobile_prefixes': ['60', '71', '82']},
    'United Kingdom': {'cc': '+44', 'nsn_length': 10, 'mobile_prefixes': ['7']},
    'United States': {'cc': '+1', 'nsn_length': 10, 'mobile_prefixes': ['2', '3', '4', '5', '6', '7', '8', '9']},
    'Canada': {'cc': '+1', 'nsn_length': 10, 'mobile_prefixes': ['2', '3', '4', '5', '6', '7', '8', '9']},
    'Germany': {'cc': '+49', 'nsn_length': 10, 'mobile_prefixes': ['15', '16', '17']},
    'Zimbabwe': {'cc': '+263', 'nsn_length': 9, 'mobile_prefixes': ['71', '73', '77']},
    'India': {'cc': '+91', 'nsn_length': 10, 'mobile_prefixes': ['7', '8', '9']},
}

# ID and Passport formats
ID_PASSPORT_FORMATS = {
    'South Africa': {
        'ID': lambda: ''.join(str(random.randint(0,9)) for _ in range(13)),
        'Passport': lambda: fake.bothify(text='A#######')
    },
    'United Kingdom': {
        'ID': lambda: fake.bothify(text='??######'),
        'Passport': lambda: ''.join(str(random.randint(0,9)) for _ in range(9))
    },
    'United States': {
        'ID': lambda: fake.bothify(text='###-##-####'),
        'Passport': lambda: ''.join(str(random.randint(0,9)) for _ in range(9))
    },
    'Canada': {
        'ID': lambda: fake.bothify(text='###-###-###'),
        'Passport': lambda: fake.bothify(text='??######')
    },
    'Germany': {
        'ID': lambda: ''.join(str(random.randint(0,9)) for _ in range(11)),
        'Passport': lambda: fake.bothify(text='C##???###')
    },
    'Zimbabwe': {
        'ID': lambda: fake.bothify(text='##-######-?##'),
        'Passport': lambda: fake.bothify(text='??######')  # 2 letters + 6 digits
    },
    'India': {
        'ID': lambda: ''.join(str(random.randint(0,9)) for _ in range(12)),
        'Passport': lambda: fake.bothify(text='?########')
    },
}

# Company registration number formats
COMPANY_REG_FORMATS = {
    'South Africa': lambda: f'{random.randint(1000,9999)}/{random.randint(100000,999999)}/07',
    'United Kingdom': lambda: ''.join(str(random.randint(0,9)) for _ in range(8)),
    'United States': lambda: fake.bothify(text='##-#######'),
    'Canada': lambda: ''.join(str(random.randint(0,9)) for _ in range(9)),
    'Germany': lambda: fake.bothify(text='HRB ######'),
    'Zimbabwe': lambda: f'CR/{random.randint(1000,9999)}/{random.randint(20,25)}',
    'India': lambda: fake.bothify(text='U#####MH####PTC###'),
}

# TLD suggestions
COUNTRY_TLDS = {
    'South Africa': '.co.za',
    'United Kingdom': '.co.uk',
    'United States': '.com',
    'Canada': '.ca',
    'Germany': '.de',
    'Zimbabwe': '.zw',
    'India': '.in',
}

def clean_domain(name: str) -> str:
    base = name.lower()
    base = re.sub(r'[^a-z0-9]+', '', base)
    return base or 'company'

def tld_for_country(country: str) -> str:
    return COUNTRY_TLDS.get(country, '.com')

def phone_for_country(country: str) -> str:
    plan = PHONE_PLANS.get(country, PHONE_PLANS['United States'])
    cc = plan['cc']
    prefix = random.choice(plan['mobile_prefixes'])
    remaining = plan['nsn_length'] - len(prefix)
    tail = ''.join(str(random.randint(0,9)) for _ in range(max(0, remaining)))
    if cc == '+1':
        area_first = str(random.randint(2,9))
        area_rest = ''.join(str(random.randint(0,9)) for _ in range(2))
        exch_first = str(random.randint(2,9))
        exch_rest = ''.join(str(random.randint(0,9)) for _ in range(2))
        line = ''.join(str(random.randint(0,9)) for _ in range(4))
        return f"{cc}{area_first}{area_rest}{exch_first}{exch_rest}{line}"
    return f"{cc}{prefix}{tail}"

def country_from_headquarters(hq: str) -> str:
    if not isinstance(hq, str):
        return 'Other'  # Use 'Other' for invalid HQ
    parts = [p.strip() for p in hq.split(',') if p.strip()]
    return parts[-1] if parts else 'Other'

def get_province_city(country: str) -> tuple:
    # Handle unmapped countries
    if country not in COUNTRY_PROVINCES_CITIES:
        country = 'Other'
    provinces = COUNTRY_PROVINCES_CITIES[country]
    province = random.choice(list(provinces.keys()))
    city = random.choice(provinces[province])
    return province, city

def generate_individual_data(num):
    data = []
    for _ in tqdm(range(num), desc="Generating Individuals"):
        name = fake.name()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
        gender = random.choices(['M', 'F', 'Other', 'Prefer not to say'], weights=[0.48, 0.48, 0.02, 0.02])[0]
        nationality = random.choice(list(PHONE_PLANS.keys()))

        state, city = get_province_city(nationality)
        id_type = random.choices(['ID', 'Passport', "Driver's License"], weights=[0.6, 0.3, 0.1])[0]
        formats = ID_PASSPORT_FORMATS.get(nationality, ID_PASSPORT_FORMATS['United States'])
        if id_type == 'ID':
            id_number = formats['ID']()
            expiry = None
        elif id_type == 'Passport':
            id_number = formats['Passport']()
            expiry = fake.date_between(start_date=date(2016, 1, 1), end_date=date(2025, 12, 31))
        else:
            id_number = fake.bothify(text='??######')
            expiry = None

        email_local = re.sub(r'[^a-z0-9]+', '.', name.lower())
        email = f"{email_local}@{random.choice(['gmail.com', 'yahoo.com', 'outlook.com', 'icloud.com'])}"
        phone = phone_for_country(nationality)
        marketing_consent = random.choice([True, False])
        comm_pref = random.choice(['Email', 'SMS', 'Phone', 'Mail'])
        date_registered = fake.date_between(start_date=date(2000, 1, 1), end_date=date.today())
        blacklist_flag = random.random() < 0.02
        blacklist_date = fake.date_between(start_date=date_registered + timedelta(days=1), end_date=date.today()) if blacklist_flag else None
        entry_mode = random.choices(ENTRY_MODES, weights=[0.4, 0.3, 0.2, 0.1])[0]

        data.append({
            'Customer_ID': None,
            'Customer_Name': name,
            'Date_of_Birth': dob,
            'Gender': gender,
            'Nationality': nationality,
            'ID_Number': id_number,
            'ID_Type': id_type,
            'Email_Address': email,
            'Phone_Number': phone,
            'Address_Line1': fake.street_address(),
            'City': city,
            'State_Province': state,
            'Marketing_Consent': marketing_consent,
            'Travel_Document_Expiry': expiry,
            'Communication_Preference': comm_pref,
            'Date_Registered': date_registered,
            'Blacklist_Flag': blacklist_flag,
            'Blacklist_Date': blacklist_date,
            'Customer_Entry_Mode': entry_mode,
            'Company_Registration_Number': None,
            'Industry_Sector': None,
            'Company_Size': None,
            'Headquarters': None,
        })
    return data

def generate_company_data(num):
    sectors = ['Finance', 'IT', 'Manufacturing', 'Healthcare', 'Retail', 'Transportation', 'Consulting']
    data = []
    for _ in tqdm(range(num), desc="Generating Companies"):
        name = fake.company()
        hq_country = random.choice(list(PHONE_PLANS.keys()))
        state, hq_city = get_province_city(hq_country)  # Use get_province_city for consistency
        headquarters = f"{hq_city}, {hq_country}"

        comp_reg_num = COMPANY_REG_FORMATS.get(hq_country, COMPANY_REG_FORMATS['United States'])()
        industry_sector = random.choice(sectors)
        company_size = random.choice([10, 50, 200, 500, 1000, 5000])
        phone = phone_for_country(hq_country)
        domain_root = clean_domain(name)
        tld = tld_for_country(hq_country)
        email = f"info@{domain_root}{tld}"
        marketing_consent = random.choice([True, False])
        comm_pref = random.choice(['Email', 'SMS', 'Phone', 'Mail'])
        date_registered = fake.date_between(start_date=date(1985, 1, 1), end_date=date.today())
        blacklist_flag = random.random() < 0.01
        blacklist_date = fake.date_between(start_date=date_registered + timedelta(days=1), end_date=date.today()) if blacklist_flag else None
        entry_mode = random.choices(['Website', 'Application', 'Agent'], weights=[0.5, 0.3, 0.2])[0]

        data.append({
            'Customer_ID': None,
            'Customer_Name': name,
            'Date_of_Birth': None,
            'Gender': None,
            'Nationality': None,
            'ID_Number': None,
            'ID_Type': None,
            'Email_Address': email,
            'Phone_Number': phone,
            'Address_Line1': fake.street_address(),
            'City': hq_city,
            'State_Province': state,
            'Marketing_Consent': marketing_consent,
            'Travel_Document_Expiry': None,
            'Communication_Preference': comm_pref,
            'Date_Registered': date_registered,
            'Blacklist_Flag': blacklist_flag,
            'Blacklist_Date': blacklist_date,
            'Customer_Entry_Mode': entry_mode,
            'Company_Registration_Number': comp_reg_num,
            'Industry_Sector': industry_sector,
            'Company_Size': company_size,
            'Headquarters': headquarters,
        })
    return data

# Generate data
individuals = generate_individual_data(NUM_INDIVIDUALS)
companies = generate_company_data(NUM_COMPANIES)

# Assign IDs
for i, customer in enumerate(individuals):
    customer['Customer_ID'] = f'CUS{i+1:06d}'
for j, customer in enumerate(companies):
    customer['Customer_ID'] = f'COM{j+1:06d}'

# Combine & save
all_customers = individuals + companies
df = pd.DataFrame(all_customers).sort_values('Customer_ID').reset_index(drop=True)

output_folder = 'airline_data'
os.makedirs(output_folder, exist_ok=True)
df.to_parquet(f'{output_folder}/customers.parquet', index=False)

print(f"Customer dataset saved to '{output_folder}/customers.parquet'")
print(f"Total records: {len(df)} (Individuals: {NUM_INDIVIDUALS}, Companies: {NUM_COMPANIES})")