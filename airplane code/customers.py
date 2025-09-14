import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, date, timedelta
from tqdm import tqdm
import random
import os
import re

# Set random seeds for reproducibility
seed_bytes = os.urandom(4)
seed_int = int.from_bytes(seed_bytes, byteorder='big')
random.seed(seed_int)
np.random.seed(seed_int)


# Constants
TARGET_YEAR = 2024
NUM_INDIVIDUALS = random.randint(random.randint(70000, 200000), random.randint(200000, 500000))
ENTRY_MODES = ['Website', 'Mobile Application', 'Agent', 'Walk-in']

# Phone plans dictionary with Faker locales
PHONE_PLANS = {
    'South Africa': {'cc': '+27', 'nsn_length': 9, 'mobile_prefixes': ['60','61','62','63','64','65','66','67','68','71','72','73','74','76','78','79','81','82','83','84'], 'faker_locale': 'zu_ZA'},
    'United Kingdom': {'cc': '+44', 'nsn_length': 10, 'mobile_prefixes': ['7'], 'faker_locale': 'en_GB'},
    'United States': {'cc': '+1', 'nsn_length': 10, 'mobile_prefixes': ['2','3','4','5','6','7','8','9'], 'faker_locale': 'en_US'},
    'Canada': {'cc': '+1', 'nsn_length': 10, 'mobile_prefixes': ['2','3','4','5','6','7','8','9'], 'faker_locale': 'en_CA'},
    'Germany': {'cc': '+49', 'nsn_length': 10, 'mobile_prefixes': ['15','16','17'], 'faker_locale': 'de_DE'},
    'France': {'cc': '+33', 'nsn_length': 9, 'mobile_prefixes': ['6','7'], 'faker_locale': 'fr_FR'},
    'India': {'cc': '+91', 'nsn_length': 10, 'mobile_prefixes': ['6','7','8','9'], 'faker_locale': 'hi_IN'},
    'Nigeria': {'cc': '+234', 'nsn_length': 10, 'mobile_prefixes': ['70','80','81','90','91'], 'faker_locale': 'en_GB'},
    'Zimbabwe': {'cc': '+263', 'nsn_length': 9, 'mobile_prefixes': ['71','73','77','78'], 'faker_locale': 'en_GB'},
    'Kenya': {'cc': '+254', 'nsn_length': 9, 'mobile_prefixes': ['7','1'], 'faker_locale': 'en_GB'},
    'Australia': {'cc': '+61', 'nsn_length': 9, 'mobile_prefixes': ['4'], 'faker_locale': 'en_AU'},
    'Brazil': {'cc': '+55', 'nsn_length': 11, 'mobile_prefixes': ['9'], 'faker_locale': 'pt_BR'},
    'United Arab Emirates': {'cc': '+971', 'nsn_length': 9, 'mobile_prefixes': ['50','52','54','55','56','58'], 'faker_locale': 'ar_AE'},
    'Netherlands': {'cc': '+31', 'nsn_length': 9, 'mobile_prefixes': ['6'], 'faker_locale': 'nl_NL'},
    'Spain': {'cc': '+34', 'nsn_length': 9, 'mobile_prefixes': ['6','7'], 'faker_locale': 'es_ES'},
    'Italy': {'cc': '+39', 'nsn_length': 10, 'mobile_prefixes': ['3'], 'faker_locale': 'it_IT'},
    'China': {'cc': '+86', 'nsn_length': 11, 'mobile_prefixes': ['13','14','15','16','17','18','19'], 'faker_locale': 'zh_CN'},
    'Japan': {'cc': '+81', 'nsn_length': 10, 'mobile_prefixes': ['70','80','90'], 'faker_locale': 'ja_JP'},
}

# Initialize Faker instances for each country
FAKER_INSTANCES = {country: Faker(locale) for country, details in PHONE_PLANS.items() for locale in [details['faker_locale']]}

# City and province lists for Zimbabwe, Kenya, and Nigeria
COUNTRY_CITIES_PROVINCES = {
    'Zimbabwe': [
        {'city': 'Harare', 'province': 'Harare'},
        {'city': 'Bulawayo', 'province': 'Bulawayo'},
        {'city': 'Mutare', 'province': 'Manicaland'},
        {'city': 'Gweru', 'province': 'Midlands'},
        {'city': 'Masvingo', 'province': 'Masvingo'}
    ],
    'Kenya': [
        {'city': 'Nairobi', 'province': 'Nairobi'},
        {'city': 'Mombasa', 'province': 'Coast'},
        {'city': 'Kisumu', 'province': 'Nyanza'},
        {'city': 'Nakuru', 'province': 'Rift Valley'},
        {'city': 'Eldoret', 'province': 'Rift Valley'}
    ],
    'Nigeria': [
        {'city': 'Lagos', 'province': 'Lagos'},
        {'city': 'Abuja', 'province': 'Federal Capital Territory'},
        {'city': 'Kano', 'province': 'Kano'},
        {'city': 'Ibadan', 'province': 'Oyo'},
        {'city': 'Port Harcourt', 'province': 'Rivers'}
    ]
}

def generate_id_number(nationality, id_type, dob, gender, faker):
    """Generate ID number based on nationality and ID type."""
    if id_type == 'National ID':
        if nationality == 'South Africa':
            dob_str = dob.strftime('%y%m%d')
            seq = f'{random.randint(0, 9999):04d}'
            gender_digit = '0' if gender == 'F' else '1'
            citizenship = random.choice(['0', '1'])
            check_digit = random.randint(0, 9)
            return f'{dob_str}{seq}{gender_digit}{citizenship}{check_digit}'
        else:
            raise ValueError("National ID is only allowed for South Africans")
    elif id_type == 'Passport':
        if nationality == 'South Africa':
            return f'{random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")}{random.randint(10000000, 99999999)}'
        elif nationality in ['United States', 'Canada']:
            return f'{random.randint(100000000, 999999999)}'
        else:
            return f'{random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")}{random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")}{random.randint(1000000, 9999999)}'
    else:  # Driver's License
        if nationality == 'South Africa':
            initials = ''.join([name[0] for name in faker.name().split()[:2]]).upper()
            dob_str = dob.strftime('%y%m%d')
            seq = f'{random.randint(0, 9999):04d}'
            return f'{initials}{dob_str}{seq}'
        elif nationality in ['United States', 'Canada']:
            return f'D{random.randint(10000000, 99999999)}'
        else:
            return f'{random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")}{random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")}{random.randint(10000000, 99999999)}'

def generate_phone_number(nationality):
    """Generate a valid mobile phone number based on nationality."""
    plan = PHONE_PLANS.get(nationality, PHONE_PLANS['United States'])
    cc = plan['cc']
    nsn_length = plan['nsn_length']
    prefix = random.choice(plan['mobile_prefixes'])
    remaining_length = nsn_length - len(prefix)
    digits = ''.join([str(random.randint(0, 9)) for _ in range(remaining_length)])
    return f'{cc}{prefix}{digits}'

def get_city_province(nationality, faker_instance):
    """Get city and province for specific countries, fallback to Faker for others."""
    if nationality in COUNTRY_CITIES_PROVINCES:
        city_province = random.choice(COUNTRY_CITIES_PROVINCES[nationality])
        return city_province['city'], city_province['province']
    else:
        city = faker_instance.city()
        try:
            province = faker_instance.administrative_unit()
        except AttributeError:
            try:
                province = faker_instance.province()
            except AttributeError:
                try:
                    province = faker_instance.state()
                except AttributeError:
                    province = "Unknown"
        return city, province

class Person:
    def __init__(self, client_id, is_main_holder=False):
        """Generate customer information."""
        # Ensure 60% South Africans
        self.nationality = random.choices(
            ['South Africa'] + [c for c in PHONE_PLANS.keys() if c != 'South Africa'],
            weights=[0.6] + [0.4 / (len(PHONE_PLANS) - 1)] * (len(PHONE_PLANS) - 1)
        )[0]
        self.faker = FAKER_INSTANCES[self.nationality]
        self.client_id = client_id
        self.is_main_holder = is_main_holder

        # Basic info
        self.name = self.faker.name()
        min_age = 18 if is_main_holder else 0
        self.dob = self.faker.date_of_birth(minimum_age=min_age, maximum_age=80)
        self.gender = random.choices(['M', 'F', 'Other', 'Prefer not to say'], weights=[0.48, 0.48, 0.02, 0.02])[0]

        # ID details
        if self.nationality == 'South Africa':
            self.id_type = random.choices(['National ID', 'Passport', "Driver's License"], weights=[0.6, 0.3, 0.1])[0]
        else:
            self.id_type = random.choices(['Passport', "Driver's License"], weights=[0.7, 0.3])[0]
        self.id_number = generate_id_number(self.nationality, self.id_type, self.dob, self.gender, self.faker)
        self.travel_document_expiry = self.faker.date_between(start_date=date(TARGET_YEAR, 1, 1), end_date=date(TARGET_YEAR + 10, 12, 31)) if self.id_type == 'Passport' else None

        # Contact details
        email_domain = random.choice(['gmail.com', 'outlook.com', 'yahoo.com', 'hotmail.com'])
        email_name = re.sub(r'[^a-zA-Z0-9]', '', self.name.lower().replace(' ', '.'))
        self.email_address = f'{email_name}@{email_domain}'
        self.phone_number = generate_phone_number(self.nationality)
        self.address = self.faker.street_address()
        
        # Get city and province
        self.city, self.province_state = get_city_province(self.nationality, self.faker)

        self.marketing_consent = random.choices(['Yes', 'No'], weights=[0.7, 0.3])[0]
        self.comm_pref = random.choices(['Email', 'SMS', 'Phone', 'Mail'], weights=[0.4, 0.3, 0.2, 0.1])[0]

        # Registration details
        self.date_of_registration = self.faker.date_between(start_date=date(TARGET_YEAR, 1, 1), end_date=date(TARGET_YEAR, 12, 31))
        self.entry_mode = random.choice(ENTRY_MODES)

def generate_clients():
    """Generate client data with shared client IDs."""
    data = []
    client_counter = 1  # Counter for sequential numbering
    individuals_left = NUM_INDIVIDUALS

    for _ in tqdm(range(NUM_INDIVIDUALS), desc="Generating clients"):
        group_size = random.choices([1, 2, 3, 4, 5], weights=[0.5, 0.3, 0.15, 0.03, 0.02])[0]
        group_size = min(group_size, individuals_left)
        if group_size == 0:
            break

        # Generate client ID in format CL{TARGET_YEAR}0001
        client_id = f"CL{TARGET_YEAR}{client_counter:04d}"
        
        # Main holder (must be over 18)
        data.append(Person(client_id, is_main_holder=True).__dict__)
        # Additional members (can be any age)
        for _ in range(group_size - 1):
            data.append(Person(client_id, is_main_holder=False).__dict__)
        
        client_counter += 1
        individuals_left -= group_size

    # Convert to DataFrame
    df = pd.DataFrame(data)
    # Drop faker object and reorder columns
    df = df[['client_id', 'is_main_holder', 'name', 'dob', 'gender', 'nationality', 'id_type', 'id_number',
             'travel_document_expiry', 'email_address', 'phone_number', 'address', 'city', 'province_state',
             'marketing_consent', 'comm_pref', 'date_of_registration', 'entry_mode']]
    return df

# Generate and save data
os.makedirs('airplane_data', exist_ok=True)
clients_df = generate_clients()
clients_df.to_parquet(f'airplane_data/clients_{TARGET_YEAR}.parquet', index=False)
print(f"Saved {len(clients_df)} records to airplane_data/clients_{TARGET_YEAR}.parquet")

# Verify South African percentage and National ID restriction
sa_count = len(clients_df[clients_df['nationality'] == 'South Africa'])
sa_percentage = (sa_count / len(clients_df)) * 100
national_id_non_sa = len(clients_df[(clients_df['id_type'] == 'National ID') & (clients_df['nationality'] != 'South Africa')])
print(f"South African percentage: {sa_percentage:.2f}%")
print(f"Non-South Africans with National ID: {national_id_non_sa}")
