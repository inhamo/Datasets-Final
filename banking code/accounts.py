import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import date, timedelta
import os
from tqdm import tqdm
import argparse

# Helper functions defined at module level
def calculate_age(birth_date, target_year):
    # Calculate age based on the target year instead of current date
    return target_year - birth_date.year

def get_income_level(customer_data):
    income = customer_data.get('annual_income', 300000)
    if income < 100000:
        return 'low'
    elif income < 600000:
        return 'medium'
    else:
        return 'high'

def generate_accounts(year):
    # Initialize seeds for reproducibility
    seed_bytes = os.urandom(4)
    seed_int = int.from_bytes(seed_bytes, byteorder='big')
    random.seed(seed_int)
    np.random.seed(seed_int)
    Faker.seed(seed_int)
    fake = Faker('zu_ZA')

    github_repo_path = 'banking_data'
    customer_file = f'{github_repo_path}/customers_{year}.parquet'
    try:
        df_customers = pd.read_parquet(customer_file)
    except FileNotFoundError:
        print(f"Customer file {customer_file} not found. Exiting.")
        return pd.DataFrame()

    # Load previous years' customers (up to 3 years prior) for 3% re-opening
    previous_customers = []
    for prev_year in range(max(2015, year - 3), year):
        try:
            prev_df = pd.read_parquet(f'{github_repo_path}/customers_{prev_year}.parquet')
            # Sample 3% of previous customers
            sample_size = max(1, int(len(prev_df) * 0.03))
            sampled_df = prev_df.sample(n=sample_size, random_state=seed_int)
            previous_customers.append(sampled_df)
        except FileNotFoundError:
            continue
    if previous_customers:
        previous_customers = pd.concat(previous_customers).reset_index(drop=True)
        # Deduplicate previous customers by customer_id to avoid excessive re-openings leading to too many accounts
        previous_customers = previous_customers.drop_duplicates(subset=['customer_id']).reset_index(drop=True)
    else:
        previous_customers = pd.DataFrame()

    # Combine current and previous customers
    df_customers = pd.concat([df_customers, previous_customers]).reset_index(drop=True)

    branch_codes = [f"BR{str(i).zfill(3)}" for i in range(1, 11)]
    account_types_individual = ['savings', 'current', 'cheque', 'aspire', 'easy', 'islamic', 'joint', 'premium', 'gold', 'platinum']
    account_types_company = ['business']
    account_status_options = ['active', 'suspended', 'frozen', 'closed', 'pending_verification', 'dormant', 'restricted']
    currencies = ['ZAR', 'USD', 'EUR']
    account_tiers = ['basic', 'standard', 'premium']

    account_charges = {
        'savings': {'interest_rate': 0.01, 'monthly_charges': 10, 'transactions_rate': 0.02, 'negative_balance_rate': 0.05},
        'current': {'interest_rate': 0.005, 'monthly_charges': 20, 'transactions_rate': 0.01, 'negative_balance_rate': 0.06},
        'cheque': {'interest_rate': 0.007, 'monthly_charges': 15, 'transactions_rate': 0.015, 'negative_balance_rate': 0.04},
        'aspire': {'interest_rate': 0.009, 'monthly_charges': 12, 'transactions_rate': 0.017, 'negative_balance_rate': 0.045},
        'easy': {'interest_rate': 0.006, 'monthly_charges': 8, 'transactions_rate': 0.02, 'negative_balance_rate': 0.05},  # Adjusted to differentiate from cheque
        'islamic': {'interest_rate': 0.0, 'monthly_charges': 8, 'transactions_rate': 0.01, 'negative_balance_rate': 0.0},
        'joint': {'interest_rate': 0.006, 'monthly_charges': 18, 'transactions_rate': 0.012, 'negative_balance_rate': 0.05},
        'premium': {'interest_rate': 0.015, 'monthly_charges': 30, 'transactions_rate': 0.005, 'negative_balance_rate': 0.03},
        'gold': {'interest_rate': 0.012, 'monthly_charges': 25, 'transactions_rate': 0.007, 'negative_balance_rate': 0.035},
        'platinum': {'interest_rate': 0.02, 'monthly_charges': 40, 'transactions_rate': 0.004, 'negative_balance_rate': 0.025},
        'business': {'interest_rate': 0.005, 'monthly_charges': 75, 'transactions_rate': 0.025, 'negative_balance_rate': 0.08}  # Adjusted to be higher/distinct from easy/cheque
    }

    def random_date(start_date, end_date):
        delta = (end_date - start_date).days
        return start_date + timedelta(days=np.random.randint(0, delta + 1))

    def select_realistic_account_type(customer_data, customer_type):
        if customer_type == 'Individual':
            income = customer_data.get('annual_income', 300000)
            if income < 100000:
                return random.choices(['easy', 'savings'], weights=[0.7, 0.3])[0]
            elif income < 300000:
                return random.choices(['savings', 'current', 'cheque'], weights=[0.5, 0.3, 0.2])[0]
            elif income < 600000:
                return random.choices(['current', 'cheque', 'aspire', 'gold'], weights=[0.3, 0.2, 0.3, 0.2])[0]
            elif income < 1000000:
                return random.choices(['gold', 'premium', 'current'], weights=[0.4, 0.4, 0.2])[0]
            else:
                return random.choices(['platinum', 'premium', 'gold'], weights=[0.5, 0.3, 0.2])[0]
        return 'business'

    def determine_account_tier(account_type, income_level):
        if account_type in ['premium', 'gold', 'platinum']:
            return 'premium'
        elif account_type in ['business']:
            return 'standard'
        elif income_level == 'low':
            return 'basic'
        else:
            return 'standard'

    def generate_credit_limit(account_type, income_level):
        if account_type in ['credit_card', 'overdraft_facility', 'business_credit_line']:
            if income_level == 'high':
                return round(random.uniform(50000, 200000), 2)
            elif income_level == 'medium':
                return round(random.uniform(10000, 80000), 2)
            else:
                return round(random.uniform(2000, 30000), 2)
        return 0.0

    def generate_account_requirements(customer_data, account_type):
        requirements = {
            'proof_of_income_provided': False,
            'proof_of_address_provided': True,
            'bank_statements_provided': False,  # Only for cases indicating other banks for address proof
            'employer_letter_provided': False,
            'business_registration_provided': False,
            'tax_certificate_provided': False,
            'minimum_deposit_met': True
        }
        # Adjust bank_statements_provided to be tied to proof_of_address in some cases
        if random.random() < 0.2:  # 20% chance to provide bank statements as additional proof of address
            requirements['bank_statements_provided'] = True
            requirements['proof_of_address_provided'] = True  # Reinforces address proof
        if account_type in ['premium', 'gold', 'platinum']:
            requirements['proof_of_income_provided'] = random.random() < 0.9
        if account_type == 'business':
            requirements['business_registration_provided'] = True
            requirements['tax_certificate_provided'] = random.random() < 0.8
        if customer_data.get('occupation') not in ['Unemployed', 'Student', 'Self-Employed']:
            requirements['employer_letter_provided'] = random.random() < 0.6
            requirements['proof_of_income_provided'] = random.random() < 0.8
        return requirements

    def determine_account_status(opening_date, customer_data, account_requirements, year):
        today = date.today()
        days_since_opening = (today - opening_date).days
        status = 'active'
        status_change_date = None
        status_reason = None

        # No pending_verification for historical years like 2018 (already handled by days_since_opening >30)
        if days_since_opening < 30:
            if not (account_requirements['proof_of_address_provided'] and account_requirements['minimum_deposit_met']):
                if random.random() < 0.3:
                    status = 'pending_verification'
                    status_reason = random.choice(['incomplete_documents', 'address_verification_pending'])
                else:
                    status = 'active'

        risk_score = customer_data.get('risk_score', 0.5)
        if risk_score > 0.8:
            if random.random() < 0.15:
                status = 'frozen'
                status_change_date = opening_date + timedelta(days=random.randint(1, days_since_opening))
                status_reason = 'high_risk_suspicion'
            elif random.random() < 0.25:
                status = 'restricted'
                status_change_date = opening_date + timedelta(days=random.randint(1, days_since_opening))
                status_reason = 'risk_monitoring'
            else:
                status = 'active'
        elif risk_score > 0.6:
            if random.random() < 0.15:
                status = 'restricted'
                status_change_date = opening_date + timedelta(days=random.randint(1, days_since_opening))
                status_reason = 'moderate_risk'
            else:
                status = 'active'

        if days_since_opening > 1095:
            closure_probability = 0.05 + (days_since_opening - 1095) / 10000
            if random.random() < closure_probability:
                status = 'closed'
                # Ensure closure date is after opening and not same day
                min_days = max(1, random.randint(1, days_since_opening - 1))
                status_change_date = opening_date + timedelta(days=min_days)
                status_reason = random.choice(['customer_request', 'non_activity', 'migration_to_other_bank'])

        if status == 'active' and random.random() < 0.08:
            status = 'dormant'
            status_change_date = opening_date + timedelta(days=random.randint(365, days_since_opening))
            status_reason = 'inactivity'

        if status == 'active' and random.random() < 0.02:
            status = 'suspended'
            status_change_date = opening_date + timedelta(days=random.randint(1, days_since_opening))
            status_reason = random.choice(['fraud_suspicion', 'overdue_charges'])

        return status, status_change_date, status_reason

    def generate_bundled_products(primary_account_type, customer_data):
        additional_products = []
        if primary_account_type in ['premium', 'gold', 'platinum']:
            if random.random() < 0.6:
                additional_products.append('investment_account')
            if random.random() < 0.4:
                additional_products.append('credit_card')
            if random.random() < 0.3:
                additional_products.append('overdraft_facility')
        if customer_data.get('customer_type') == 'Individual':
            age = calculate_age(customer_data.get('birth_date', date(1990, 1, 1)), year)
            if age < 25 and customer_data.get('occupation') == 'Student':
                if random.random() < 0.8:
                    additional_products.append('student_card')
        elif customer_data.get('customer_type') == 'Company':
            if random.random() < 0.5:
                additional_products.append('business_credit_line')
            if random.random() < 0.3:
                additional_products.append('merchant_services')
            if random.random() < 0.4:
                additional_products.append('payroll_services')
        return ';'.join(additional_products) if additional_products else None

    def determine_opening_channel_and_details():
        channels = ['branch', 'online', 'mobile_app', 'phone', 'agent']
        weights = [0.85, 0.25, 0.15, 0.10, 0.05]
        weights = [w / sum(weights) for w in weights]
        opening_channel = random.choices(channels, weights=weights)[0]
        # Simplify: only keep opening_channel, extrapolate others if needed but remove derived fields for now
        return {'opening_channel': opening_channel}

    # Set date range for account openings
    if year == 2020:
        opening_start = date(year, 6, 1)
        opening_end = date(year, 12, 31)
    elif year == 2019:
        opening_start = date(year, 1, 1)
        opening_end = date(year, 12, 31)
    else:
        opening_start = date(max(2015, year - 3), 1, 1)
        opening_end = date(year, 12, 31)

    accounts = []
    account_id_counter = 1

    # Track total accounts per customer to enforce realistic limit (max 5 per customer)
    customer_account_counts = {}

    # Process customers (current and previous years)
    df_individuals = df_customers[df_customers['customer_type'] == 'Individual'].copy()
    df_companies = df_customers[df_customers['customer_type'] == 'Company'].copy()
    individual_ids = df_individuals['customer_id'].values
    max_partners = min(len(individual_ids) - 1, 3)

    for _, row in tqdm(df_individuals.iterrows(), total=len(df_individuals), desc="Generating Individual Accounts"):
        customer_id = row['customer_id']
        date_of_entry = row['date_of_entry']
        current_count = customer_account_counts.get(customer_id, 0)
        # Limit total accounts per customer to max 5
        max_accounts = 5 - current_count
        if max_accounts <= 0:
            continue
        num_accounts = min(generate_accounts_with_relationships(row, year), max_accounts)
        income_level = get_income_level(row)

        for _ in range(num_accounts):
            acc_type = select_realistic_account_type(row, 'Individual')
            if acc_type != 'joint':
                opening_date = random_date(max(opening_start, date_of_entry), opening_end)
                requirements = generate_account_requirements(row, acc_type)
                account_status, status_change_date, status_reason = determine_account_status(opening_date, row, requirements, year)
                branch_code = random.choice(branch_codes)
                charges = account_charges[acc_type]
                channel_details = determine_opening_channel_and_details()
                currency = 'ZAR' if random.random() < 0.95 else random.choice(['USD', 'EUR'])
                account_tier = determine_account_tier(acc_type, income_level)
                credit_limit = generate_credit_limit(acc_type, income_level)

                accounts.append({
                    'account_id': f'ACC{year}{account_id_counter:07d}',
                    'customer_id': customer_id,
                    'account_type': acc_type,
                    'opening_date': opening_date,
                    'branch_code': branch_code,
                    'kyc_verified': True,
                    'fica_verified': row['citizenship'] != 'ZA',
                    'expected_amount': round(np.random.lognormal(mean=8.5, sigma=1.2), 2),
                    'account_status': account_status,
                    'status_change_date': status_change_date,
                    'status_reason': status_reason,
                    'linked_joint_accounts': None,
                    'interest_rate': charges['interest_rate'],
                    'monthly_charges': charges['monthly_charges'],
                    'transactions_rate': charges['transactions_rate'],
                    'negative_balance_rate': charges['negative_balance_rate'],
                    'bundled_products': generate_bundled_products(acc_type, row),
                    'currency': currency,
                    'account_tier': account_tier,
                    'credit_limit': credit_limit,
                    **requirements,
                    **channel_details
                })
                account_id_counter += 1
                customer_account_counts[customer_id] = customer_account_counts.get(customer_id, 0) + 1

        # Limit joint accounts similarly, but count towards total
        joint_accounts_to_create = min(random.randint(0, 2) if year != 2020 else 0, max_accounts - num_accounts)
        for _ in range(joint_accounts_to_create):
            partners = np.random.choice([cid for cid in individual_ids if cid != customer_id], 
                                      size=min(random.randint(1, 3), max_partners), replace=False)
            opening_date = random_date(max(opening_start, date_of_entry), opening_end)
            requirements = generate_account_requirements(row, 'joint')
            account_status, status_change_date, status_reason = determine_account_status(opening_date, row, requirements, year)
            branch_code = random.choice(branch_codes)
            charges = account_charges['joint']
            channel_details = determine_opening_channel_and_details()
            currency = 'ZAR' if random.random() < 0.95 else random.choice(['USD', 'EUR'])
            account_tier = determine_account_tier('joint', income_level)
            credit_limit = generate_credit_limit('joint', income_level)

            accounts.append({
                'account_id': f'ACC{year}{account_id_counter:07d}',
                'customer_id': customer_id,
                'account_type': 'joint',
                'opening_date': opening_date,
                'branch_code': branch_code,
                'kyc_verified': True,
                'fica_verified': any(df_individuals[df_individuals['customer_id'].isin([customer_id] + list(partners))]['citizenship'] != 'ZA'),
                'expected_amount': min(round(np.random.lognormal(mean=8.5, sigma=1.2), 2), 100000),
                'account_status': account_status,
                'status_change_date': status_change_date,
                'status_reason': status_reason,
                'linked_joint_accounts': ';'.join(partners),
                'interest_rate': charges['interest_rate'],
                'monthly_charges': charges['monthly_charges'],
                'transactions_rate': charges['transactions_rate'],
                'negative_balance_rate': charges['negative_balance_rate'],
                'bundled_products': generate_bundled_products('joint', row),
                'currency': currency,
                'account_tier': account_tier,
                'credit_limit': credit_limit,
                **requirements,
                **channel_details
            })
            account_id_counter += 1
            customer_account_counts[customer_id] = customer_account_counts.get(customer_id, 0) + 1

    for _, row in tqdm(df_companies.iterrows(), total=len(df_companies), desc="Generating Company Accounts"):
        customer_id = row['customer_id']
        date_of_entry = row['date_of_entry']
        current_count = customer_account_counts.get(customer_id, 0)
        max_accounts = 3 - current_count  # Lower limit for companies, max 3 total
        if max_accounts <= 0:
            continue
        num_accounts = min(random.choices([1, 2], weights=[0.8, 0.2])[0] if year != 2020 else 1, max_accounts)
        income_level = get_income_level(row)

        for _ in range(num_accounts):
            acc_type = 'business'
            opening_date = random_date(max(opening_start, date_of_entry), opening_end)
            requirements = generate_account_requirements(row, acc_type)
            account_status, status_change_date, status_reason = determine_account_status(opening_date, row, requirements, year)
            branch_code = random.choice(branch_codes)
            charges = account_charges[acc_type]
            channel_details = determine_opening_channel_and_details()
            currency = 'ZAR' if random.random() < 0.9 else random.choice(['USD', 'EUR'])
            account_tier = determine_account_tier(acc_type, income_level)
            credit_limit = generate_credit_limit(acc_type, income_level)

            accounts.append({
                'account_id': f'ACC{year}{account_id_counter:07d}',
                'customer_id': customer_id,
                'account_type': acc_type,
                'opening_date': opening_date,
                'branch_code': branch_code,
                'kyc_verified': True,
                'fica_verified': None,
                'expected_amount': round(random.uniform(10000, 1000000), 2),
                'account_status': account_status,
                'status_change_date': status_change_date,
                'status_reason': status_reason,
                'linked_joint_accounts': None,
                'interest_rate': charges['interest_rate'],
                'monthly_charges': charges['monthly_charges'],
                'transactions_rate': charges['transactions_rate'],
                'negative_balance_rate': charges['negative_balance_rate'],
                'bundled_products': generate_bundled_products(acc_type, row),
                'currency': currency,
                'account_tier': account_tier,
                'credit_limit': credit_limit,
                **requirements,
                **channel_details
            })
            account_id_counter += 1
            customer_account_counts[customer_id] = customer_account_counts.get(customer_id, 0) + 1

    df_accounts = pd.DataFrame(accounts)
    os.makedirs(github_repo_path, exist_ok=True)
    output_file = f'{github_repo_path}/accounts_{year}.parquet'
    df_accounts.to_parquet(output_file, index=False)

    print(f"Generated {len(df_accounts)} accounts for year {year}.")
    print(f"Saved to {output_file}")

    return df_accounts

def generate_accounts_with_relationships(customer_data, year):
    customer_type = customer_data['customer_type']
    if customer_type == 'Individual':
        age = calculate_age(customer_data.get('birth_date', date(1990, 1, 1)), year)
        income_level = get_income_level(customer_data)
        if income_level == 'high' and age > 35:
            return random.choices([1, 2, 3], weights=[0.4, 0.4, 0.2])[0]  # Reduced max to 3 to limit total
        elif income_level == 'medium' and age > 25:
            return random.choices([1, 2], weights=[0.6, 0.4])[0]  # Reduced
        return random.choices([1], weights=[1])[0]  # Limit low to 1
    return random.choices([1], weights=[1])[0]  # Limit companies to 1 base

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate account data for a specific year")
    parser.add_argument('--year', type=int, default=2018, help='Year for account data generation')
    args = parser.parse_args()
    generate_accounts(args.year)