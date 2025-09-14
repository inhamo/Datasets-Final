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
        num_individuals = random.randint(20, 50)
        num_companies = random.randint(0, 5)
        print("Note: 2020 year - Reduced registrations due to COVID-19 lockdowns in South Africa.")
    elif year == 2021:
        num_individuals = random.randint(13000, 18000)
        num_companies = random.randint(1, 10)
        print("Note: 2021 year - Recovery phase post-COVID.")
    elif year in (2022, 2023):
        num_individuals = random.randint(20000, 25000)
        num_companies = random.randint(1, 10)
    else:
        num_individuals = random.randint(15000, 22000)
        num_companies = random.randint(1, 10)

    # Pre-compute education levels
    education_levels = [
        'No Formal Education', 'Primary Education', 'High School Incomplete', 'High School Completed',
        'Certificate', 'Diploma', 'Bachelor Degree', 'Honours Degree', 'Masters Degree', 'Doctorate/PhD'
    ]
    education_probs = np.array([0.10, 0.15, 0.20, 0.30, 0.10, 0.08, 0.05, 0.015, 0.01, 0.005])
    education_probs = education_probs / np.sum(education_probs)

    # Education hierarchy for comparison
    education_hierarchy = {edu: idx for idx, edu in enumerate(education_levels)}

    # Pre-compute age distribution
    age_ranges = np.array([22, 30, 37, 46, 55, 65, 75])
    age_weights = np.array([0.15, 0.25, 0.22, 0.18, 0.12, 0.06, 0.02])

    # Pre-compute reasons for opening account
    individual_reasons = [
        'Personal Savings', 'Business Transactions', 'Salary Deposit', 'Investment Account',
        'Loan Application', 'Home Purchase', 'Education Funding', 'Travel Expenses'
    ]
    company_reasons = [
        'Business Transactions', 'Payroll Management', 'Supplier Payments', 'Investment Account',
        'Tax Payments', 'Expansion Funding', 'Operational Expenses'
    ]

    # Load external data
    occupations, income_ranges, occupation_probs = get_occupations_data()
    provinces, cities, province_probs = get_cities_data()

    def generate_batch_individuals(batch_size):
        ages = np.random.choice(age_ranges, size=batch_size, p=age_weights)
        genders = np.random.choice(['M', 'F'], size=batch_size, p=[0.49, 0.51])
        education_batch = np.random.choice(education_levels, size=batch_size, p=education_probs)

        # Initialize occupations_batch
        occupations_batch = [None] * batch_size

        # Assign occupations based on education
        for i in range(batch_size):
            education = education_batch[i]
            # Filter occupations where the required education level is met or exceeded
            valid_occupations = [
                occ for occ in occupations
                if education_hierarchy.get(education, 0) >= education_hierarchy.get(income_ranges[occ]['required_education'], 0)
            ]
            if not valid_occupations:
                valid_occupations = ['Unemployed Unskilled']  # Fallback for no valid occupations
            # Use probabilities proportional to original occupation_probs for valid occupations
            valid_indices = [occupations.index(occ) for occ in valid_occupations]
            valid_probs = occupation_probs[valid_indices]
            valid_probs = valid_probs / valid_probs.sum()  # Normalize
            occupations_batch[i] = np.random.choice(valid_occupations, p=valid_probs)

        provinces_batch = np.random.choice(provinces, size=batch_size, p=province_probs)

        results = []
        for i in range(batch_size):
            idx = len(results) + 1
            age = int(ages[i])
            gender = genders[i]
            occupation = occupations_batch[i]
            province = provinces_batch[i]
            education = education_batch[i]

            # Generate name, nationality, citizenship, and ethnicity
            full_name, nationality, citizenship, ethnicity = generate_name()

            # Generate income and risk
            income_range = income_ranges[occupation]['range']
            annual_income = int(np.random.uniform(*income_range) * (1 + (age - 25) * 0.02))
            base_risk = 0.1
            if annual_income < 200000:
                base_risk += 0.2
            if age < 25:
                base_risk += 0.1
            if occupation in ['Unemployed', 'Student']:
                base_risk += 0.15
            risk_score = min(round(base_risk + np.random.random() * 0.1, 3), 0.99)

            # Generate other fields
            birth_date = date.today() - timedelta(days=age*365 + random.randint(0, 364))
            id_number = f"{''.join([str(random.randint(0,9)) for _ in range(13)])}"
            city = random.choice(cities[province])
            address = f"{fake.street_address()}, {city}, {province}, South Africa"

            # Acquisition date
            if year == 2020 and random.random() < 0.15:
                date_of_entry = fake.date_between(start_date=date(year, 1, 1), end_date=date(year, 3, 26))
            else:
                month = random.randint(1, 12)
                day = random.randint(1, 28)
                date_of_entry = date(year, month, day)

            # Generate phone number
            phone_number = generate_phone_number(nationality, faker_instances)

            results.append({
                'customer_id': f'IND{year % 100}{idx:06d}',
                'customer_type': 'Individual',
                'full_name': full_name,
                'birth_date': birth_date,
                'citizenship': citizenship,
                'residential_address': address,
                'commercial_address': None,
                'email': fake.email(),
                'phone_number': phone_number,
                'id_type': 'National ID' if nationality == 'South Africa' else 'Passport',
                'id_number': id_number,
                'expiry_date': None if nationality == 'South Africa' else fake.future_date(end_date='+3y'),
                'visa_type': None if nationality == 'South Africa' else 'Work',
                'visa_expiry_date': None if nationality == 'South Africa' else fake.future_date(end_date='+2y'),
                'is_pep': random.random() < 0.01,
                'sanctioned_country': False,
                'risk_score': risk_score,
                'tax_id_number': ''.join([str(random.randint(0,9)) for _ in range(10)]),
                'occupation': occupation,
                'employer_name': 'Standard Bank' if random.random() < 0.3 else fake.company(),
                'source_of_funds': 'Employment Income' if occupation not in ['Student', 'Unemployed'] else 'Family Support',
                'marital_status': random.choice(['Single', 'Married', 'Divorced']),
                'nationality': nationality,
                'gender': gender,
                'preferred_contact_method': random.choice(['Email', 'Phone', 'SMS']),
                'next_of_kin': None,
                'date_of_entry': date_of_entry,
                'annual_income': annual_income,
                'age': age,
                'education_level': education,
                'ethnicity': ethnicity,
                'reason_for_opening_account': random.choice(individual_reasons)
            })

        return results

    def generate_batch_companies(batch_size):
        results = []
        for i in range(batch_size):
            idx = len(results) + 1
            company_name = fake.company()
            age = random.randint(1, 20)
            employees = random.randint(10, 100)
            turnover = random.randint(5000000, 50000000)
            province = np.random.choice(provinces, p=province_probs)
            city = random.choice(cities[province])
            risk_score = round(0.15 + np.random.random() * 0.2, 3)
            date_of_entry = date(year, random.randint(1, 12), random.randint(1, 28))
            phone_number = generate_phone_number('South Africa', faker_instances)

            results.append({
                'customer_id': f'COM{year % 100}{idx:06d}',
                'customer_type': 'Company',
                'full_name': company_name,
                'birth_date': None,
                'citizenship': 'ZA',
                'residential_address': None,
                'commercial_address': f"{fake.street_address()}, {city}, {province}, South Africa",
                'email': fake.company_email(),
                'phone_number': phone_number,
                'id_type': 'Registration Number',
                'id_number': f"{random.randint(1900, year)}/{random.randint(100000, 999999)}/{random.randint(1, 99)}",
                'expiry_date': None,
                'visa_type': None,
                'visa_expiry_date': None,
                'is_pep': False,
                'sanctioned_country': False,
                'risk_score': risk_score,
                'tax_id_number': ''.join([str(random.randint(0,9)) for _ in range(10)]),
                'occupation': random.choice(['Retail', 'Manufacturing', 'Finance', 'IT']),
                'employer_name': None,
                'source_of_funds': 'Business Income',
                'marital_status': None,
                'nationality': 'South Africa',
                'gender': None,
                'preferred_contact_method': 'Email',
                'next_of_kin': fake.name(),
                'date_of_entry': date_of_entry,
                'annual_income': turnover,
                'age': age,
                'education_level': None,
                'ethnicity': None,
                'reason_for_opening_account': random.choice(company_reasons),
                'company_age': age,
                'number_of_employees': employees,
                'annual_turnover': turnover,
                'directors_count': random.randint(1, 3),
                'shareholders_count': random.randint(1, 5),
                'bee_level': random.randint(1, 8),
                'vat_registered': random.random() < 0.8,
                'industry_risk_rating': random.choice(['Low', 'Medium', 'High'])
            })

        return results

    print(f"Starting generation for year {year}...")

    if num_individuals == 0 and num_companies == 0:
        print("No customers generated for this year.")
        df = pd.DataFrame()
    else:
        all_customers = []
        batch_size = 1000
        individual_batches = (num_individuals + batch_size - 1) // batch_size

        print(f"Generating {num_individuals} individuals in {individual_batches} batches...")
        for batch in tqdm(range(individual_batches), desc="Individual batches"):
            remaining = num_individuals - batch * batch_size
            current_batch_size = min(batch_size, remaining)
            batch_customers = generate_batch_individuals(current_batch_size)
            all_customers.extend(batch_customers)

        if num_companies > 0:
            print(f"Generating {num_companies} companies...")
            company_customers = generate_batch_companies(num_companies)
            all_customers.extend(company_customers)

        df = pd.DataFrame(all_customers)
        if not df.empty and 'customer_type' in df.columns:
            individual_mask = df['customer_type'] == 'Individual'
            num_individuals_df = individual_mask.sum()
            if num_individuals_df > 0:
                next_of_kin_indices = np.random.choice(
                    df[individual_mask].index,
                    size=min(int(num_individuals_df * 0.1), num_individuals_df),
                    replace=False
                )
                df.loc[next_of_kin_indices, 'next_of_kin'] = [fake.name() for _ in next_of_kin_indices]

        df = df.sample(frac=1, random_state=seed_int).reset_index(drop=True)

    # Save to file
    github_repo_path = 'banking_data'
    os.makedirs(github_repo_path, exist_ok=True)
    output_file = f'{github_repo_path}/customers_{year}.parquet'
    df.to_parquet(output_file, index=False)

    print(f"Generated {len(df)} customers (Individuals: {num_individuals}, Companies: {num_companies}) for year {year}")
    print(f"Saved to {output_file}")

    return df

if __name__ == "__main__":
    # Example: Generate for a single year
    year = 2024
    generate_customer_data(year)