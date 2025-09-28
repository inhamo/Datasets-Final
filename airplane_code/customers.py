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
TARGET_YEAR = 2021
NUM_INDIVIDUALS = random.randint(random.randint(70000, 200000), random.randint(200000, 500000))
ENTRY_MODES = ['Website', 'Mobile Application', 'Agent', 'Walk-in']

# South African companies with airline partnerships (based on SAA affiliates and partners)
SA_COMPANIES = [
    'South African Express', 'Nedbank', 'Absa Bank', 'Standard Bank',
    'Vodacom', 'MTN South Africa', 'Discovery Vitality', 'Cullinan Holdings', 'Bidvest',
    'Imperial Logistics', 'Superbalist', 'Takealot', 'Woolworths', 'Pick n Pay'
]

# Add team/school related companies
team_companies = ['Cricket South Africa', 'Rugby South Africa', 'Netball South Africa', 'Department of Basic Education']
SA_COMPANIES += team_companies

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

def introduce_typo(text, error_rate=0.05):
    """Introduce random typos in text (e.g., swap letters, replace with similar characters)."""
    if not text or random.random() > error_rate:
        return text
    chars = list(text)
    if len(chars) < 2:
        return text
    idx = random.randint(0, len(chars) - 2)
    chars[idx], chars[idx + 1] = chars[idx + 1], chars[idx]  # Swap adjacent characters
    return ''.join(chars)

def generate_id_number(nationality, id_type, dob, gender, faker):
    """Generate ID number with occasional errors."""
    if id_type == 'National ID':
        if nationality == 'South Africa':
            dob_str = dob.strftime('%y%m%d')
            seq = f'{random.randint(0, 9999):04d}'
            gender_digit = '0' if gender == 'F' else '1'
            citizenship = random.choice(['0', '1'])
            check_digit = random.randint(0, 9)
            id_num = f'{dob_str}{seq}{gender_digit}{citizenship}{check_digit}'
            # Error: Occasionally truncate or add extra digit (1% chance)
            if random.random() < 0.01:
                id_num = id_num[:-1] if random.choice([True, False]) else id_num + str(random.randint(0, 9))
            return id_num
        else:
            # Error: Allow non-South Africans to have National ID in 0.5% of cases
            if random.random() < 0.005:
                dob_str = dob.strftime('%y%m%d')
                seq = f'{random.randint(0, 9999):04d}'
                gender_digit = '0' if gender == 'F' else '1'
                citizenship = random.choice(['0', '1'])
                check_digit = random.randint(0, 9)
                return f'{dob_str}{seq}{gender_digit}{citizenship}{check_digit}'
            raise ValueError("National ID is only allowed for South Africans")
    elif id_type == 'Passport':
        if nationality == 'South Africa':
            passport = f'{random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")}{random.randint(10000000, 99999999)}'
        elif nationality in ['United States', 'Canada']:
            passport = f'{random.randint(100000000, 999999999)}'
        else:
            passport = f'{random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")}{random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")}{random.randint(1000000, 9999999)}'
        # Error: Introduce typos in passport number (2% chance)
        if random.random() < 0.02:
            passport = introduce_typo(passport)
        return passport
    else:  # Driver's License
        if nationality == 'South Africa':
            initials = ''.join([name[0] for name in faker.name().split()[:2]]).upper()
            dob_str = dob.strftime('%y%m%d')
            seq = f'{random.randint(0, 9999):04d}'
            license = f'{initials}{dob_str}{seq}'
        elif nationality in ['United States', 'Canada']:
            license = f'D{random.randint(10000000, 99999999)}'
        else:
            license = f'{random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")}{random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ")}{random.randint(10000000, 99999999)}'
        # Error: Occasionally use lowercase or add spaces (2% chance)
        if random.random() < 0.02:
            license = license.lower() if random.choice([True, False]) else license.replace('', ' ')
        return license

def generate_phone_number(nationality):
    """Generate phone number with occasional invalid formats or errors."""
    plan = PHONE_PLANS.get(nationality, PHONE_PLANS['United States'])
    cc = plan['cc']
    nsn_length = plan['nsn_length']
    prefix = random.choice(plan['mobile_prefixes'])
    remaining_length = nsn_length - len(prefix)
    digits = ''.join([str(random.randint(0, 9)) for _ in range(remaining_length)])
    phone = f'{cc}{prefix}{digits}'
    # Errors: 5% chance of invalid phone number
    if random.random() < 0.05:
        error_type = random.choice(['missing_cc', 'wrong_length', 'non_numeric'])
        if error_type == 'missing_cc':
            phone = f'{prefix}{digits}'  # Missing country code
        elif error_type == 'wrong_length':
            phone = phone[:-random.randint(1, 3)]  # Truncated number
        else:
            phone = phone[:-2] + random.choice(['A', 'B', '#'])  # Non-numeric characters
    return phone

def get_city_province(nationality, faker_instance):
    """Get city and province with occasional mismatches or typos."""
    if nationality in COUNTRY_CITIES_PROVINCES:
        city_province = random.choice(COUNTRY_CITIES_PROVINCES[nationality])
        city, province = city_province['city'], city_province['province']
        # Error: 2% chance of mismatched nationality and city/province
        if random.random() < 0.02:
            other_country = random.choice(list(COUNTRY_CITIES_PROVINCES.keys()))
            city_province = random.choice(COUNTRY_CITIES_PROVINCES[other_country])
            city, province = city_province['city'], city_province['province']
        # Error: 3% chance of typo in city or province
        if random.random() < 0.03:
            city = introduce_typo(city)
        if random.random() < 0.03:
            province = introduce_typo(province)
        return city, province
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
        # Error: 3% chance of typo in city or province
        if random.random() < 0.03:
            city = introduce_typo(city)
        if random.random() < 0.03:
            province = introduce_typo(province)
        return city, province

class Person:
    def __init__(self, client_id, is_main_holder=False, is_team=False, group_type=None, main_info=None):
        """Generate customer information with realistic errors."""
        self.client_id = client_id
        self.is_main_holder = is_main_holder
        self.is_team = is_team
        self.group_type = group_type
        self.main_info = main_info

        if not is_main_holder and main_info:
            self.nationality = main_info['nationality']
            self.faker = main_info['faker']
            self.address = main_info['address']
            self.city = main_info['city']
            self.province_state = main_info['province_state']
            self.marketing_consent = main_info['marketing_consent']
            self.comm_pref = main_info['comm_pref']
            self.date_of_registration = main_info['date_of_registration']
            self.entry_mode = main_info['entry_mode']
            self.partner_company = main_info['partner_company']
        else:
            # Ensure 60% South Africans
            self.nationality = random.choices(
                ['South Africa'] + [c for c in PHONE_PLANS.keys() if c != 'South Africa'],
                weights=[(random.randint(60, 100) / 100) * 1.00] + [((random.randint(20, 40) / 100) * 1.00) / (len(PHONE_PLANS) - 1)] * (len(PHONE_PLANS) - 1)
            )[0]
            self.faker = FAKER_INSTANCES[self.nationality]

            # Address details
            self.address = self.faker.street_address()
            # Error: 3% chance of missing or incomplete address
            if random.random() < 0.03:
                self.address = None if random.choice([True, False]) else self.address.split()[0]

            # Get city and province
            self.city, self.province_state = get_city_province(self.nationality, self.faker)
            # Error: 2% chance of missing city or province
            if random.random() < 0.02:
                self.city = None
            if random.random() < 0.02:
                self.province_state = None

            self.marketing_consent = random.choices(['Yes', 'No'], weights=[0.7, 0.3])[0]
            # Error: 5% chance of missing marketing consent
            if random.random() < 0.05:
                self.marketing_consent = None

            self.comm_pref = random.choices(['Email', 'SMS', 'Phone', 'Mail'], weights=[0.4, 0.3, 0.2, 0.1])[0]
            # Error: 3% chance of missing communication preference
            if random.random() < 0.03:
                self.comm_pref = None

            # Registration details
            self.date_of_registration = self.faker.date_between(start_date=date(TARGET_YEAR, 1, 1), end_date=date(TARGET_YEAR, 12, 31))
            # Error: 1% chance of invalid registration date (e.g., future date)
            if random.random() < 0.01:
                self.date_of_registration = self.faker.date_between(start_date=date(TARGET_YEAR + 1, 1, 1), end_date=date(TARGET_YEAR + 2, 12, 31))

            self.entry_mode = random.choice(ENTRY_MODES)
            # Error: 2% chance of invalid entry mode
            if random.random() < 0.02:
                self.entry_mode = random.choice(['website', 'MOBILE', 'agent', ''])  # Inconsistent or empty

            # Partner company
            self.partner_company = None
            if self.nationality == 'South Africa':
                if self.is_team:
                    self.partner_company = random.choice(team_companies)
                elif random.random() < 0.20:
                    self.partner_company = random.choice(SA_COMPANIES)
                # Error: 2% chance of typo in company name
                if self.partner_company and random.random() < 0.02:
                    self.partner_company = introduce_typo(self.partner_company)

        # Gender
        self.gender = random.choices(['M', 'F', 'Other', 'Prefer not to say'], weights=[0.48, 0.48, 0.02, 0.02])[0]
        # Error: 1% chance of missing gender
        if random.random() < 0.01:
            self.gender = None

        # Name
        if not is_main_holder and main_info and group_type == 'family':
            if self.gender == 'M':
                first_name = self.faker.first_name_male()
            elif self.gender == 'F':
                first_name = self.faker.first_name_female()
            else:
                first_name = self.faker.first_name()
            self.name = first_name + " " + main_info['surname']
        else:
            if self.gender == 'M':
                self.name = self.faker.name_male()
            elif self.gender == 'F':
                self.name = self.faker.name_female()
            else:
                self.name = self.faker.name()
        # Error: 3% chance of typo in name or inconsistent capitalization
        if random.random() < 0.03:
            self.name = introduce_typo(self.name)
        if random.random() < 0.03:
            self.name = self.name.upper() if random.choice([True, False]) else self.name.lower()

        # DOB without errors: respect min_age strictly
        min_age = 18 if is_main_holder else 0
        self.dob = self.faker.date_of_birth(minimum_age=min_age, maximum_age=80)
        # No future DOB or unrealistic ages

        # ID details
        if self.nationality == 'South Africa':
            self.id_type = random.choices(['National ID', 'Passport', "Driver's License"], weights=[0.6, 0.3, 0.1])[0]
        else:
            self.id_type = 'Passport'  # Enforce passport for all non-South Africans
        self.id_number = generate_id_number(self.nationality, self.id_type, self.dob, self.gender, self.faker)
        self.travel_document_expiry = self.faker.date_between(start_date=date(TARGET_YEAR, 1, 1), end_date=date(TARGET_YEAR + 10, 12, 31)) if self.id_type == 'Passport' else None
        # Error: 2% chance of expired passport before registration
        if self.id_type == 'Passport' and random.random() < 0.02:
            self.travel_document_expiry = self.faker.date_between(start_date=date(TARGET_YEAR - 5, 1, 1), end_date=date(TARGET_YEAR - 1, 12, 31))
        # Error: 1% chance of missing travel document expiry
        if self.id_type == 'Passport' and random.random() < 0.01:
            self.travel_document_expiry = None

        # Contact details
        email_domain = random.choice(['gmail.com', 'outlook.com', 'yahoo.com', 'hotmail.com'])
        email_name = re.sub(r'[^a-zA-Z0-9]', '', self.name.lower().replace(' ', '.'))
        if len(email_name) > 20:
            parts = self.name.split()
            if len(parts) > 1:
                initials = ''.join([p[0] for p in parts[:-1]])
                surname = parts[-1]
                email_name = initials.lower() + '.' + surname.lower()
                email_name = re.sub(r'[^a-zA-Z0-9]', '', email_name)
        self.email_address = f'{email_name}@{email_domain}'
        # Error: 5% chance of invalid or missing email
        if random.random() < 0.05:
            error_type = random.choice(['invalid', 'missing'])
            if error_type == 'invalid':
                self.email_address = email_name  # Missing domain
            else:
                self.email_address = None

        self.phone_number = generate_phone_number(self.nationality)
        # Error: 3% chance of missing phone number
        if random.random() < 0.03:
            self.phone_number = None

def generate_clients():
    """Generate client data with shared client IDs and occasional duplicates."""
    data = []
    client_counter = 1
    individuals_left = NUM_INDIVIDUALS

    pbar = tqdm(total=NUM_INDIVIDUALS, desc="Generating clients")
    while individuals_left > 0:
        group_size = random.choices([1, 2, 3, 4, 5, 10, 15, 20, 25, 30], weights=[0.5, 0.3, 0.15, 0.03, 0.02, 0.002, 0.002, 0.002, 0.002, 0.002])[0]
        group_size = min(group_size, individuals_left)
        if group_size == 0:
            break

        is_family = group_size <= 5
        is_team = group_size > 5

        client_id = f"CL{TARGET_YEAR}{client_counter:04d}"
        
        # Main holder
        main_holder = Person(client_id, is_main_holder=True, is_team=is_team)
        data.append(main_holder.__dict__)
        # Error: 1% chance of duplicating main holder with slight variation
        if random.random() < 0.01:
            duplicate = Person(client_id, is_main_holder=True, is_team=is_team)
            duplicate.email_address = f"duplicate_{duplicate.email_address}"
            data.append(duplicate.__dict__)

        # Additional members
        main_info = {
            'surname': main_holder.name.split()[-1] if ' ' in main_holder.name else main_holder.name,
            'nationality': main_holder.nationality,
            'faker': main_holder.faker,
            'address': main_holder.address,
            'city': main_holder.city,
            'province_state': main_holder.province_state,
            'marketing_consent': main_holder.marketing_consent,
            'comm_pref': main_holder.comm_pref,
            'date_of_registration': main_holder.date_of_registration,
            'entry_mode': main_holder.entry_mode,
            'partner_company': main_holder.partner_company
        }
        group_type = 'family' if is_family else 'team'
        for _ in range(group_size - 1):
            member = Person(client_id, is_main_holder=False, group_type=group_type, main_info=main_info)
            data.append(member.__dict__)
        
        client_counter += 1
        individuals_left -= group_size
        pbar.update(group_size)

    pbar.close()

    # Convert to DataFrame
    df = pd.DataFrame(data)
    # Drop faker object and reorder columns, adding partner_company
    columns = ['client_id', 'is_main_holder', 'name', 'dob', 'gender', 'nationality', 'id_type', 'id_number',
               'travel_document_expiry', 'email_address', 'phone_number', 'address', 'city', 'province_state',
               'marketing_consent', 'comm_pref', 'date_of_registration', 'entry_mode', 'partner_company']
    df = df[columns]
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
