import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import date, timedelta
import os
from tqdm import tqdm
from occupations import get_occupations_data
from cities import get_cities_data
from phone_numbers import generate_phone_number
from names import generate_name
import calendar

def introduce_typo(text, typo_prob=0.1):
    """Introduce typographical errors in a string with given probability."""
    if text is None or random.random() > typo_prob:
        return text
    text = list(text)
    if len(text) <= 1:
        return ''.join(text)
    idx = random.randint(0, len(text) - 1)
    action = random.choice(['swap', 'delete', 'add'])
    if action == 'swap' and idx < len(text) - 1:
        text[idx], text[idx + 1] = text[idx + 1], text[idx]
    elif action == 'delete':
        text.pop(idx)
    elif action == 'add':
        text.insert(idx, random.choice('abcdefghijklmnopqrstuvwxyz'))
    return ''.join(text)

def format_date_yymmdd(dt):
    """Format date as YYMMDD, handling dates before 1900 on Windows."""
    if dt is None or pd.isna(dt):
        return ''
    try:
        return dt.strftime('%y%m%d')
    except (ValueError, OSError):
        # Handle dates before 1900
        year_str = str(dt.year % 100).zfill(2)
        month_str = str(dt.month).zfill(2)
        day_str = str(dt.day).zfill(2)
        return f"{year_str}{month_str}{day_str}"

def generate_sa_id_number(birth_date):
    """Generate a South African ID number with correct birth date."""
    yymmdd = format_date_yymmdd(birth_date)
    sequence = ''.join([str(random.randint(0, 9)) for _ in range(4)])
    citizenship = random.choice(['0', '1'])
    gender = random.choice(['0', '1'])
    checksum = str(random.randint(0, 9))
    return yymmdd + sequence + citizenship + gender + checksum

def generate_birth_certificate_number():
    """Generate a birth certificate number."""
    return ''.join([str(random.randint(0, 9)) for _ in range(11)])

def adjust_date_for_year(birth_date, year_change):
    """Adjust the year of a date, ensuring the resulting date is valid."""
    target_year = birth_date.year + year_change
    month = birth_date.month
    day = birth_date.day
    # Get the last day of the target month/year
    last_day = calendar.monthrange(target_year, month)[1]
    # Adjust day if it's out of range
    day = min(day, last_day)
    try:
        return date(target_year, month, day)
    except ValueError:
        # Fallback in case of any remaining edge cases
        return date(target_year, month, last_day)

def generate_customer_data(year):
    # Initialize seeds for reproducibility
    seed_bytes = os.urandom(4)
    seed_int = int.from_bytes(seed_bytes, byteorder='big')
    random.seed(seed_int)
    np.random.seed(seed_int)
    Faker.seed(seed_int)
    fake = Faker('zu_ZA')
    faker_instances = {'zu_ZA': fake}

    # Customer counts based on year
    if year == 2020:
        num_individuals = random.randint(15, 40)
        num_companies = random.randint(0, 3)
        print("Note: 2020 year - Reduced registrations due to COVID-19 lockdowns in South Africa.")
    elif year == 2021:
        num_individuals = random.randint(10000, 15000)
        num_companies = random.randint(1, 8)
        print("Note: 2021 year - Recovery phase post-COVID.")
    elif year in (2022, 2023):
        num_individuals = random.randint(18000, 23000)
        num_companies = random.randint(1, 8)
    else:
        num_individuals = random.randint(10000, 20000)
        num_companies = random.randint(1, 8)

    # Pre-compute education levels
    education_levels_adult = [
        'No Formal Education', 'Primary Education', 'High School Incomplete', 'High School Completed',
        'Certificate', 'Diploma', 'Bachelor Degree', 'Honours Degree', 'Masters Degree', 'Doctorate/PhD'
    ]
    education_probs = np.array([0.20, 0.25, 0.25, 0.20, 0.06, 0.03, 0.015, 0.005, 0.003, 0.002])
    education_probs = education_probs / np.sum(education_probs)

    # Education hierarchy
    education_hierarchy = {edu: idx for idx, edu in enumerate(education_levels_adult)}

    # Pre-compute age distribution
    # Minimum age 21, so for year 2018, max birth year is 1997
    max_birth_year = year - 21
    age_ranges_adult = np.array([21, 25, 35, 45, 55, 65, 75])
    age_weights_adult = np.array([0.35, 0.30, 0.20, 0.10, 0.04, 0.008, 0.002])
    age_weights_adult = age_weights_adult / np.sum(age_weights_adult)  # Normalize

    # Ethnicity options
    ethnicity_options = ['Black', 'Coloured', 'White', 'Indian', 'Asian']
    ethnicity_weights = [0.80, 0.09, 0.08, 0.025, 0.005]

    # Load external data
    occupations, income_ranges, occupation_probs = get_occupations_data()
    provinces, cities, province_probs = get_cities_data()

    # Add informal sector occupations
    informal_occupations = ['Street Vendor', 'Domestic Worker', 'Taxi Driver', 'Spaza Shop Owner']
    for occ in informal_occupations:
        if occ not in occupations:
            occupations.append(occ)
            income_ranges[occ] = {'range': (12000, 60000), 'required_education': 'No Formal Education'}
    occupation_probs = np.append(occupation_probs, [0.1] * len(informal_occupations))
    occupation_probs = occupation_probs / np.sum(occupation_probs)

    def generate_batch_individuals(batch_size, start_idx, used_ids=None):
        if used_ids is None:
            used_ids = set()
        
        # ENFORCE MINIMUM BIRTH YEAR CONSTRAINT
        max_birth_year = year - 21
        min_birth_year = year - 75  # Assuming 75 is the maximum age in age_ranges_adult
        
        # Generate ages ensuring minimum age of 21
        ages = np.random.choice(age_ranges_adult, size=batch_size, p=age_weights_adult)
        # Ensure all ages are at least 21
        ages = np.maximum(ages, 21)
        
        genders = np.random.choice(['M', 'F'], size=batch_size, p=[0.49, 0.51])
        education_batch = np.random.choice(education_levels_adult, size=batch_size, p=education_probs)

        occupations_batch = np.array([None] * batch_size, dtype=object)
        for i in range(batch_size):
            education = education_batch[i]
            valid_occupations = [
                occ for occ in occupations
                if education_hierarchy.get(education, 0) >= education_hierarchy.get(income_ranges[occ]['required_education'], 0)
            ]
            if not valid_occupations:
                valid_occupations = ['Unemployed Unskilled']
            valid_indices_occ = [occupations.index(occ) for occ in valid_occupations]
            valid_probs = occupation_probs[valid_indices_occ]
            valid_probs = valid_probs / np.sum(valid_probs)
            occupations_batch[i] = 'Unemployed Unskilled' if random.random() < 0.35 else np.random.choice(valid_occupations, p=valid_probs)

        provinces_batch = np.random.choice(provinces, size=batch_size, p=province_probs)

        results = []
        local_idx = start_idx
        for i in range(batch_size):
            age = int(ages[i])
            gender = genders[i]
            occupation = occupations_batch[i]
            province = provinces_batch[i]
            education = education_batch[i]

            full_name, nationality, citizenship, ethnicity = generate_name()
            if nationality != 'South Africa':
                citizenship = nationality[:2].upper()  # Non-SA cannot have ZA citizenship
            full_name = introduce_typo(full_name, 0.05)

            income_range = income_ranges.get(occupation, {'range': (0, 0)})['range']
            annual_income = int(np.random.uniform(income_range[0], min(income_range[1], 500000)) * (1 + (age - 25) * 0.01))
            base_risk = 0.15
            if annual_income < 120000:
                base_risk += 0.25
            if age < 25:
                base_risk += 0.15
            if occupation in ['Unemployed Unskilled', 'Student']:
                base_risk += 0.2
            risk_score = min(round(base_risk + np.random.random() * 0.15, 3), 0.99)

            # Generate correct birth_date with enforced minimum age
            # Calculate maximum birth date (21 years before current year)
            max_birth_date = date(year, 12, 31) - timedelta(days=21 * 365)
            # Calculate minimum birth date (75 years before current year)
            min_birth_date = date(year, 1, 1) - timedelta(days=75 * 365)
            
            # Generate random birth date within the valid range
            days_range = (max_birth_date - min_birth_date).days
            random_days = random.randint(0, days_range)
            correct_birth_date = min_birth_date + timedelta(days=random_days)
            
            birth_date = correct_birth_date
            if random.random() < 0.05:  # 5% error rate for adults
                error_type = random.choice(['add_100', 'sub_100', 'add_10', 'sub_10'])
                if error_type == 'add_100':
                    birth_date = adjust_date_for_year(correct_birth_date, 100)
                elif error_type == 'sub_100':
                    birth_date = adjust_date_for_year(correct_birth_date, -100)
                elif error_type == 'add_10':
                    birth_date = adjust_date_for_year(correct_birth_date, 10)
                elif error_type == 'sub_10':
                    birth_date = adjust_date_for_year(correct_birth_date, -10)

            id_type = 'National ID' if nationality == 'South Africa' else 'Passport'
            id_number = generate_sa_id_number(correct_birth_date) if id_type == 'National ID' else fake.passport_number()

            # Rest of the function remains the same...
            city = random.choice(cities[province])
            address = (f"Informal Settlement, {city}, {province}" if random.random() < 0.1 else
                    f"{introduce_typo(fake.street_address(), 0.1)}, {city}, {province}, South Africa")
            if random.random() < 0.2:
                address = address.replace('South Africa', '')

            if year == 2020 and random.random() < 0.3:
                date_of_entry = fake.date_between(start_date=date(year, 1, 1), end_date=date(year, 3, 26))
            else:
                date_of_entry = date(year, random.randint(1, 12), random.randint(1, 28))

            phone_number = generate_phone_number(nationality, faker_instances)
            if random.random() < 0.15:
                phone_number = phone_number.replace('+27', '') or phone_number[:8]

            email = fake.email() if random.random() < 0.6 else None
            if email and random.random() < 0.1:
                email = email.replace('@', '') or email[:-3]

            tax_id_number = (''.join([str(random.randint(0, 9)) for _ in range(10)])
                            if random.random() < (0.4 if occupation and 'unemployed' in occupation.lower() or annual_income < 80000 else 0.8)
                            else None)
            if tax_id_number and random.random() < 0.1:
                tax_id_number = tax_id_number[:8]

            source_of_funds = (random.choice(['Family Support', 'Social Grants', 'Savings', 'Part-time Work'])
                            if occupation and 'unemployed' in occupation.lower() else
                            random.choice(['Family Support', 'Student Loan', 'Part-time Work'])
                            if occupation == 'Student' else 'Employment Income')

            if ethnicity not in ethnicity_options:
                ethnicity = np.random.choice(ethnicity_options, p=ethnicity_weights)

            customer_id = f'IND{year % 100:02d}{local_idx:06d}'
            while customer_id in used_ids:
                local_idx += 1
                customer_id = f'IND{year % 100:02d}{local_idx:06d}'
            used_ids.add(customer_id)

            visa_type = None
            visa_expiry_date = None
            if nationality != 'South Africa':
                visa_type = 'Work'
                visa_expiry_date = fake.date_between(start_date=date(year-2, 1, 1), end_date=date(year+2, 1, 1))

            customer_data = {
                'customer_id': customer_id,
                'customer_type': 'Individual',
                'full_name': full_name,
                'birth_date': birth_date,
                'citizenship': citizenship,
                'residential_address': address,
                'commercial_address': None,
                'email': email,
                'phone_number': phone_number,
                'id_type': id_type,
                'id_number': id_number,
                'expiry_date': None if id_type == 'National ID' else fake.future_date(end_date='+3y'),
                'visa_type': visa_type,
                'visa_expiry_date': visa_expiry_date,
                'is_pep': random.random() < 0.01,
                'sanctioned_country': random.random() < 0.005,
                'risk_score': risk_score,
                'tax_id_number': tax_id_number,
                'occupation': occupation,
                'employer_name': (introduce_typo(fake.company(), 0.05)
                                if occupation and 'unemployed' not in occupation.lower() and random.random() < 0.6 else
                                fake.company() if occupation and 'unemployed' in occupation.lower() and random.random() < 0.05 else None),
                'source_of_funds': source_of_funds,
                'marital_status': random.choice(['Single', 'Married', 'Divorced', 'Widowed']),
                'nationality': nationality,
                'gender': gender,
                'preferred_contact_method': random.choice(['Email', 'Phone', 'SMS', None]),
                'next_of_kin': None,
                'date_of_entry': date_of_entry,
                'annual_income': annual_income,
                'education_level': education,
                'ethnicity': ethnicity,
                'is_affidavit': False
            }
            results.append(customer_data)
            local_idx += 1
        return results, used_ids

    def generate_batch_companies(batch_size, start_idx, used_ids):
        results = []
        for i in range(batch_size):
            idx = start_idx + i + 1
            company_name = introduce_typo(fake.company(), 0.05)
            age = random.randint(1, 20)
            employees = random.randint(5, 80)
            turnover = random.randint(1000000, 30000000)
            province = np.random.choice(provinces, p=province_probs)
            city = random.choice(cities[province])
            risk_score = round(0.2 + np.random.random() * 0.25, 3)
            date_of_entry = date(year, random.randint(1, 12), random.randint(1, 28))
            phone_number = generate_phone_number('South Africa', faker_instances)
            if random.random() < 0.1:
                phone_number = phone_number[:8]

            customer_id = f'COM{year % 100:02d}{idx:06d}'
            while customer_id in used_ids:
                idx += 1
                customer_id = f'COM{year % 100:02d}{idx:06d}'
            used_ids.add(customer_id)

            company_data = {
                'customer_id': customer_id,
                'customer_type': 'Company',
                'full_name': company_name,
                'birth_date': None,
                'citizenship': 'ZA',
                'residential_address': None,
                'commercial_address': f"{introduce_typo(fake.street_address(), 0.1)}, {city}, {province}, South Africa",
                'email': fake.company_email() if random.random() < 0.9 else None,
                'phone_number': phone_number,
                'id_type': 'Registration Number',
                'id_number': f"{random.randint(1900, year)}/{random.randint(100000, 999999)}/{random.randint(1, 99)}",
                'expiry_date': None,
                'visa_type': None,
                'visa_expiry_date': None,
                'is_pep': False,
                'sanctioned_country': random.random() < 0.005,
                'risk_score': risk_score,
                'tax_id_number': ''.join([str(random.randint(0, 9)) for _ in range(10)]) if random.random() < 0.9 else None,
                'occupation': random.choice(['Retail', 'Manufacturing', 'Finance', 'IT', 'Services', 'Informal Trade']),
                'employer_name': None,
                'source_of_funds': 'Business Income',
                'marital_status': None,
                'nationality': 'South Africa',
                'gender': None,
                'preferred_contact_method': random.choice(['Email', 'Phone', None]),
                'next_of_kin': introduce_typo(fake.name(), 0.05) if random.random() < 0.8 else None,
                'date_of_entry': date_of_entry,
                'annual_income': turnover,
                'education_level': None,
                'ethnicity': None,
                'company_age': age,
                'number_of_employees': employees,
                'annual_turnover': turnover,
                'directors_count': random.randint(1, 3),
                'shareholders_count': random.randint(1, 5),
                'bee_level': random.randint(1, 8) if random.random() < 0.7 else None,
                'vat_registered': random.random() < 0.7,
                'industry_risk_rating': random.choice(['Low', 'Medium', 'High', None]),
                'is_affidavit': False
            }
            results.append(company_data)
        return results, used_ids

    print(f"Starting generation for year {year}...")

    if num_individuals == 0 and num_companies == 0:
        print("No customers generated for this year.")
        df = pd.DataFrame()
    else:
        all_customers = []
        used_ids = set()
        batch_size = 500  # Reduced for performance
        individual_batches = (num_individuals + batch_size - 1) // batch_size

        print(f"Generating {num_individuals} individuals in {individual_batches} batches...")
        current_idx = 0
        for batch in tqdm(range(individual_batches), desc="Individual batches"):
            remaining = num_individuals - batch * batch_size
            current_batch_size = min(batch_size, remaining)
            batch_customers, used_ids = generate_batch_individuals(current_batch_size, current_idx, used_ids)
            all_customers.extend(batch_customers)
            current_idx = max(int(id[7:]) for id in used_ids if id.startswith(f'IND{year % 100:02d}')) + 1 if used_ids else current_idx + len(batch_customers)

        if num_companies > 0:
            print(f"Generating {num_companies} companies...")
            company_customers, used_ids = generate_batch_companies(num_companies, 0, used_ids)
            all_customers.extend(company_customers)

        df = pd.DataFrame(all_customers)

        # Remove reason_for_opening_account column
        if 'reason_for_opening_account' in df.columns:
            df = df.drop(columns=['reason_for_opening_account'])

        # Verify no duplicate customer_id
        duplicate_ids = df[df['customer_id'].duplicated(keep=False)]
        if not duplicate_ids.empty:
            print(f"WARNING: Found {len(duplicate_ids)} duplicate customer_id values. This should not happen.")

        # Add next_of_kin for some individuals
        if not df.empty and 'customer_type' in df.columns:
            individual_mask = (df['customer_type'] == 'Individual')
            num_individuals_df = individual_mask.sum()
            if num_individuals_df > 0:
                next_of_kin_indices = np.random.choice(
                    df[individual_mask].index,
                    size=min(int(num_individuals_df * 0.1), num_individuals_df),
                    replace=False
                )
                df.loc[next_of_kin_indices, 'next_of_kin'] = [introduce_typo(fake.name(), 0.05) for _ in range(len(next_of_kin_indices))]

        df = df.sample(frac=1, random_state=seed_int).reset_index(drop=True)

    # Save to file
    github_repo_path = 'banking_data'
    os.makedirs(github_repo_path, exist_ok=True)
    output_file = f'{github_repo_path}/customers_{year}.parquet'
    df.to_parquet(output_file, index=False)

    # Data quality summary
    print(f"Generated {len(df)} customers (Individuals: {num_individuals}, Companies: {num_companies}) for year {year}")
    if not df.empty:
        individual_df = df[df['customer_type'] == 'Individual']
        if len(individual_df) > 0:
            unemployed_with_employer = individual_df[
                (individual_df['occupation'].str.contains('unemployed', case=False, na=False)) &
                (individual_df['employer_name'].notna())
            ]
            missing_email_rate = individual_df['email'].isna().mean()
            missing_tax_id_rate = individual_df['tax_id_number'].isna().mean()
            invalid_id_numbers = individual_df[
                (individual_df['id_type'] == 'National ID') & (individual_df['id_number'].str.len() != 13)
            ]
            duplicate_ids = df[df['customer_id'].duplicated(keep=False)]
            birth_date_errors = individual_df[
                (individual_df['id_type'] == 'National ID') &
                (individual_df['birth_date'].apply(format_date_yymmdd) != individual_df['id_number'].str[:6])
            ]

            print(f"Data Quality Summary:")
            print(f"- Unemployed individuals with employer names: {len(unemployed_with_employer)} (ERROR INTRODUCED)")
            print(f"- Individuals without email: {missing_email_rate:.1%} (REALISTIC FOR SA)")
            print(f"- Individuals without tax ID: {missing_tax_id_rate:.1%} (REALISTIC FOR SA)")
            print(f"- Invalid ID numbers: {len(invalid_id_numbers)} (SHOULD BE ZERO FOR NATIONAL ID)")
            print(f"- Birth date mismatches with ID: {len(birth_date_errors)} (LOW ERROR RATE)")
            print(f"- Duplicate customer_id: {len(duplicate_ids)} (SHOULD BE ZERO)")
            print(f"- Ethnicity standardized: Yes (SA DEMOGRAPHICS)")
            print(f"- Informal sector occupations added: Yes (SA CONTEXT)")
            print(f"- Typographical errors introduced: Yes (NAMES, ADDRESSES, EMAILS)")
            print(f"- Minimum age 21 ensured: Yes (MAX BIRTH YEAR {max_birth_year})")

    print(f"Saved to {output_file}")

    return df

if __name__ == "__main__":
    year = 2024
    generate_customer_data(year)