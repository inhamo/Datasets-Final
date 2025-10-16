import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import date, timedelta
import os
from tqdm import tqdm
import argparse
from uuid import uuid4
import json

# Assuming cities_branch_codes is available from previous context
from cities_branch_codes import cities_branch_codes

# Helper functions (keeping all existing ones unchanged)
def calculate_age(birth_date, target_year):
    return target_year - birth_date.year

def get_income_level(customer_data):
    income = customer_data.get('annual_income', 300000)
    if income < 100000:
        return 'low'
    elif income < 600000:
        return 'medium'
    else:
        return 'high'

def generate_sa_account_number(branch_code, global_counter):
    """Generate GLOBALLY UNIQUE SA account number: branch(6) + type(2) + sequence(6) + check(1)"""
    branch_num = branch_code
    account_type = random.choice(['01', '02', '03', '27', '28'])
    sequence = str(global_counter % 1000000).zfill(6)
    
    # Simple check digit (Luhn-like)
    base = branch_num + account_type + sequence
    check_sum = sum(int(d) * (2 if i % 2 == 0 else 1) for i, d in enumerate(base))
    check_digit = (10 - (check_sum % 10)) % 10
    
    return base + str(check_digit)

def generate_swift_code(branch_code):
    """Generate SWIFT/BIC code for single bank (Wololo Bank)"""
    base_swift = 'WOLZAJJ'
    if random.random() < 0.3:
        return base_swift + branch_code[-3:]
    return base_swift

def generate_iban(account_number):
    bank_code = '250655'
    check_digits = str(random.randint(10, 99))
    return f"ZA{check_digits}{bank_code}{account_number[:10].zfill(10)}"

def generate_card_number(account_type, year):
    """Generate unique debit/credit card number"""
    if account_type in ['islamic']:
        return None
    
    card_types = {
        'visa_debit': '4',
        'mastercard_debit': '5',
        'visa_credit': '4',
        'mastercard_credit': '5'
    }
    
    is_credit = account_type in ['premium', 'gold', 'platinum', 'business']
    card_type = random.choice(['visa_credit', 'mastercard_credit'] if is_credit else ['visa_debit', 'mastercard_debit'])
    
    card_num = card_types[card_type]
    card_num += ''.join([str(random.randint(0, 9)) for _ in range(14)])
    
    digits = [int(d) for d in card_num]
    check_sum = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        check_sum += d
    check_digit = (10 - (check_sum % 10)) % 10
    
    return card_num + str(check_digit), card_type

def determine_account_purpose(customer_data, account_type):
    """Determine why account was opened"""
    if customer_data.get('customer_type') == 'Company':
        purposes = ['business_operations', 'payroll', 'tax_payments', 'investment', 'trading']
        weights = [0.5, 0.2, 0.15, 0.1, 0.05]
        return random.choices(purposes, weights=weights)[0]
    
    occupation = customer_data.get('occupation', '')
    if 'Student' in occupation:
        return random.choice(['student_savings', 'bursary_account', 'pocket_money'])
    elif 'Unemployed' in occupation:
        return random.choice(['social_grants', 'savings', 'family_support'])
    
    if account_type in ['premium', 'gold', 'platinum']:
        purposes = ['wealth_management', 'investment', 'salary', 'savings']
        weights = [0.3, 0.3, 0.25, 0.15]
    else:
        purposes = ['salary', 'savings', 'daily_transactions', 'emergency_fund', 'side_income']
        weights = [0.4, 0.25, 0.2, 0.1, 0.05]
    
    return random.choices(purposes, weights=weights)[0]

def generate_beneficiaries(customer_data, fake):
    """Generate account beneficiaries"""
    if random.random() < 0.4:
        return None
    
    num_beneficiaries = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
    beneficiaries = []
    
    for _ in range(num_beneficiaries):
        name = fake.name()
        relationship = random.choice(['Spouse', 'Child', 'Parent', 'Sibling', 'Other'])
        percentage = random.randint(10, 100)
        beneficiaries.append(f"{name}|{relationship}|{percentage}%")
    
    return ';'.join(beneficiaries)

def should_reject_application(customer_data, account_type, year):
    """Determine if account application should be rejected"""
    risk_score = customer_data.get('risk_score', 0.5)
    
    rejection_prob = 0.03
    if risk_score > 0.85:
        rejection_prob += 0.15
    elif risk_score > 0.7:
        rejection_prob += 0.08
    
    if account_type in ['premium', 'gold', 'platinum']:
        income = customer_data.get('annual_income', 0)
        if income < 300000:
            rejection_prob += 0.2
    
    if customer_data.get('is_pep') or customer_data.get('sanctioned_country'):
        rejection_prob += 0.1
    
    if pd.isna(customer_data.get('tax_id_number')) and customer_data.get('annual_income', 0) > 500000:
        rejection_prob += 0.15
    
    if random.random() < rejection_prob:
        rejection_reasons = ['high_risk_profile', 'insufficient_documentation', 'failed_credit_check', 
                           'pep_sanctions_concern', 'incomplete_kyc', 'affordability_assessment_failed',
                           'adverse_credit_history', 'employment_verification_failed']
        return True, random.choice(rejection_reasons)
    
    return False, None

bundled_products_available = {
    'savings': ['online_banking', 'debit_card'],
    'current': ['online_banking', 'debit_card', 'overdraft_facility'],
    'cheque': ['online_banking', 'debit_card', 'overdraft_facility'],
    'aspire': ['online_banking', 'debit_card', 'student_card'],
    'easy': ['online_banking', 'debit_card'],
    'islamic': ['online_banking'],
    'joint': ['online_banking', 'debit_card', 'overdraft_facility'],
    'premium': ['online_banking', 'credit_card', 'overdraft_facility', 'investment_account'],
    'gold': ['online_banking', 'credit_card', 'overdraft_facility', 'investment_account'],
    'platinum': ['online_banking', 'credit_card', 'overdraft_facility', 'investment_account', 'wealth_management'],
    'business': ['online_banking', 'business_credit_line', 'merchant_services', 'payroll_services']
}

def get_branch_code(customer_data):
    """Select branch code based on residential or workplace address"""
    residential_address = customer_data.get('residential_address', '')
    if random.random() < 0.5:
        return random.choice(cities_branch_codes['Gauteng']['Johannesburg'])
    
    if not isinstance(residential_address, str) or not residential_address:
        return random.choice(cities_branch_codes['Gauteng']['Johannesburg'])
    
    address_lower = residential_address.lower()
    for province, cities in cities_branch_codes.items():
        for city, codes in cities.items():
            city_lower = city.lower()
            if city_lower in address_lower or any(keyword in address_lower for keyword in [city_lower, 'odendaalsrus']):
                return random.choice(codes)
    
    return random.choice(cities_branch_codes['Gauteng']['Johannesburg'])

class GlobalAccountCounter:
    """Thread-safe, persistent global account counter"""
    
    def __init__(self, github_repo_path):
        self.github_repo_path = github_repo_path
        self.counter_file = f'{github_repo_path}/account_counter.json'
        self.counter = self._load_counter()
    
    def _load_counter(self):
        """Load counter from file, or calculate from existing accounts"""
        # First, check if counter file exists
        if os.path.exists(self.counter_file):
            try:
                with open(self.counter_file, 'r') as f:
                    data = json.load(f)
                    counter = data.get('counter', 0)
                    print(f"Loaded counter from file: {counter}")
                    return counter
            except Exception as e:
                print(f"Warning: Could not load counter file: {e}")
        
        # Otherwise, calculate from existing parquet files
        total_accounts = 0
        for year in range(2015, 2026):
            try:
                file_path = f'{self.github_repo_path}/accounts_{year}.parquet'
                if os.path.exists(file_path):
                    df = pd.read_parquet(file_path)
                    total_accounts += len(df)
                    print(f"Loaded {len(df)} existing accounts from {year}")
            except Exception as e:
                continue
        
        print(f"Total existing accounts across all years: {total_accounts}")
        self._save_counter(total_accounts)
        return total_accounts
    
    def _save_counter(self, value):
        """Persist counter to file"""
        os.makedirs(self.github_repo_path, exist_ok=True)
        try:
            with open(self.counter_file, 'w') as f:
                json.dump({'counter': value, 'timestamp': str(date.today())}, f)
        except Exception as e:
            print(f"Warning: Could not save counter file: {e}")
    
    def get_next(self):
        """Get next unique ID and increment"""
        current = self.counter
        self.counter += 1
        self._save_counter(self.counter)
        return current
    
    def peek(self):
        """View current counter without incrementing"""
        return self.counter

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

    # Load previous years' customers for re-opening
    previous_customers = []
    for prev_year in range(max(2015, year - 3), year):
        try:
            prev_df = pd.read_parquet(f'{github_repo_path}/customers_{prev_year}.parquet')
            sample_size = max(1, int(len(prev_df) * 0.03))
            sampled_df = prev_df.sample(n=sample_size, random_state=seed_int)
            previous_customers.append(sampled_df)
        except FileNotFoundError:
            continue
    if previous_customers:
        previous_customers = pd.concat(previous_customers).reset_index(drop=True)
        previous_customers = previous_customers.drop_duplicates(subset=['customer_id']).reset_index(drop=True)
    else:
        previous_customers = pd.DataFrame()

    df_customers = pd.concat([df_customers, previous_customers]).reset_index(drop=True)

    # *** IMPROVED: Use persistent counter class ***
    counter = GlobalAccountCounter(github_repo_path)
    print(f"Starting generation for year {year} with counter at: {counter.peek()}")

    account_charges = {
        'savings': {'interest_rate': 0.01, 'monthly_charges': 10, 'transactions_rate': 0.02, 'negative_balance_rate': 0.05},
        'current': {'interest_rate': 0.005, 'monthly_charges': 20, 'transactions_rate': 0.01, 'negative_balance_rate': 0.06},
        'cheque': {'interest_rate': 0.007, 'monthly_charges': 15, 'transactions_rate': 0.015, 'negative_balance_rate': 0.04},
        'aspire': {'interest_rate': 0.009, 'monthly_charges': 12, 'transactions_rate': 0.017, 'negative_balance_rate': 0.045},
        'easy': {'interest_rate': 0.006, 'monthly_charges': 8, 'transactions_rate': 0.02, 'negative_balance_rate': 0.05},
        'islamic': {'interest_rate': 0.0, 'monthly_charges': 8, 'transactions_rate': 0.01, 'negative_balance_rate': 0.0},
        'joint': {'interest_rate': 0.006, 'monthly_charges': 18, 'transactions_rate': 0.012, 'negative_balance_rate': 0.05},
        'premium': {'interest_rate': 0.015, 'monthly_charges': 30, 'transactions_rate': 0.005, 'negative_balance_rate': 0.03},
        'gold': {'interest_rate': 0.012, 'monthly_charges': 25, 'transactions_rate': 0.007, 'negative_balance_rate': 0.035},
        'platinum': {'interest_rate': 0.02, 'monthly_charges': 40, 'transactions_rate': 0.004, 'negative_balance_rate': 0.025},
        'business': {'interest_rate': 0.005, 'monthly_charges': 75, 'transactions_rate': 0.025, 'negative_balance_rate': 0.08}
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

    def generate_credit_limit(account_type, income_level, annual_income):
        overdraft_limit = 0.0
        credit_card_limit = 0.0
        
        if account_type in ['current', 'cheque', 'premium', 'gold', 'platinum', 'business']:
            if random.random() < 0.4:
                if income_level == 'high':
                    overdraft_limit = round(random.uniform(10000, min(annual_income * 0.5, 100000)), 2)
                elif income_level == 'medium':
                    overdraft_limit = round(random.uniform(5000, min(annual_income * 0.3, 50000)), 2)
                else:
                    overdraft_limit = round(random.uniform(1000, 10000), 2)
        
        if account_type in ['premium', 'gold', 'platinum']:
            if random.random() < 0.7:
                if income_level == 'high':
                    credit_card_limit = round(random.uniform(50000, 200000), 2)
                elif income_level == 'medium':
                    credit_card_limit = round(random.uniform(10000, 80000), 2)
                else:
                    credit_card_limit = round(random.uniform(5000, 30000), 2)
        elif account_type == 'business':
            if random.random() < 0.5:
                credit_card_limit = round(random.uniform(20000, 150000), 2)
        
        return overdraft_limit, credit_card_limit

    def generate_account_requirements(customer_data, account_type):
        requirements = {
            'proof_of_income_provided': False,
            'proof_of_address_provided': True,
            'bank_statements_provided': False,
            'employer_letter_provided': False,
            'business_registration_provided': False,
            'tax_certificate_provided': False,
            'minimum_deposit_met': True
        }
        
        if random.random() < 0.2:
            requirements['bank_statements_provided'] = True
            requirements['proof_of_address_provided'] = True
        
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
        closure_date = None

        if opening_date.year == year:
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
            elif risk_score > 0.6:
                if random.random() < 0.15:
                    status = 'restricted'
                    status_change_date = opening_date + timedelta(days=random.randint(1, days_since_opening))
                    status_reason = 'moderate_risk'
            
            if status == 'active' and random.random() < 0.02:
                status = 'suspended'
                status_change_date = opening_date + timedelta(days=random.randint(1, days_since_opening))
                status_reason = random.choice(['fraud_suspicion', 'overdue_charges'])
        
        else:
            if days_since_opening < 30:
                if not (account_requirements['proof_of_address_provided'] and account_requirements['minimum_deposit_met']):
                    if random.random() < 0.3:
                        status = 'pending_verification'
                        status_reason = random.choice(['incomplete_documents', 'address_verification_pending'])

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
            elif risk_score > 0.6:
                if random.random() < 0.15:
                    status = 'restricted'
                    status_change_date = opening_date + timedelta(days=random.randint(1, days_since_opening))
                    status_reason = 'moderate_risk'

            if days_since_opening > 1095:
                closure_probability = 0.05 + (days_since_opening - 1095) / 10000
                if random.random() < closure_probability:
                    status = 'closed'
                    min_days = max(1, random.randint(1, days_since_opening - 1))
                    status_change_date = opening_date + timedelta(days=min_days)
                    closure_date = status_change_date
                    status_reason = random.choice(['customer_request', 'non_activity', 'migration_to_other_bank'])

            if status == 'active' and random.random() < 0.08:
                status = 'dormant'
                status_change_date = opening_date + timedelta(days=random.randint(365, days_since_opening))
                status_reason = 'inactivity'

            if status == 'active' and random.random() < 0.02:
                status = 'suspended'
                status_change_date = opening_date + timedelta(days=random.randint(1, days_since_opening))
                status_reason = random.choice(['fraud_suspicion', 'overdue_charges'])

        return status, status_change_date, closure_date, status_reason

    def generate_bundled_products(account_type, customer_data):
        """Select bundled products from available options for the account type"""
        available_products = bundled_products_available.get(account_type, [])
        if not available_products:
            return None
        
        num_products = random.choices([0, 1, 2, 3], weights=[0.2, 0.4, 0.3, 0.1])[0]
        if num_products == 0:
            return None
        
        if customer_data.get('customer_type') == 'Individual':
            age = calculate_age(customer_data.get('birth_date', date(1990, 1, 1)), year)
            if age < 25 and customer_data.get('occupation') == 'Student' and 'student_card' in available_products:
                return 'student_card'
        
        selected_products = random.sample(available_products, min(num_products, len(available_products)))
        return ';'.join(selected_products) if selected_products else None

    def determine_opening_channel_and_details():
        channels = ['branch', 'online', 'mobile_app', 'phone', 'agent']
        weights = [0.50, 0.30, 0.15, 0.03, 0.02]
        opening_channel = random.choices(channels, weights=weights)[0]
        return {'opening_channel': opening_channel}

    def generate_approval_date(opening_date):
        """Generate approval date, 90% on the same day as opening"""
        if random.random() < 0.9:
            return opening_date
        return opening_date + timedelta(days=random.randint(1, 7))

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
    rejected_applications = []
    customer_account_counts = {}
    customer_primary_accounts = {}

    df_individuals = df_customers[df_customers['customer_type'] == 'Individual'].copy()
    df_companies = df_customers[df_customers['customer_type'] == 'Company'].copy()
    individual_ids = df_individuals['customer_id'].values
    max_partners = min(len(individual_ids) - 1, 3)

    for _, row in tqdm(df_individuals.iterrows(), total=len(df_individuals), desc="Generating Individual Accounts"):
        customer_id = row['customer_id']
        date_of_entry = row['date_of_entry']
        current_count = customer_account_counts.get(customer_id, 0)
        max_accounts = 5 - current_count
        if max_accounts <= 0:
            continue
        
        num_accounts = min(generate_accounts_with_relationships(row, year), max_accounts)
        income_level = get_income_level(row)
        annual_income = row.get('annual_income', 300000)

        for account_idx in range(num_accounts):
            acc_type = select_realistic_account_type(row, 'Individual')
            
            is_rejected, rejection_reason = should_reject_application(row, acc_type, year)
            application_date = random_date(max(opening_start, date_of_entry), opening_end)
            if is_rejected:
                global_id = counter.get_next()
                rejected_applications.append({
                    'application_id': f'APP{global_id:07d}',
                    'customer_id': customer_id,
                    'account_type': acc_type,
                    'application_date': application_date,
                    'rejection_reason': rejection_reason,
                    'rejection_date': application_date
                })
                continue
            
            opening_date = application_date
            approval_date = generate_approval_date(opening_date)
            requirements = generate_account_requirements(row, acc_type)
            account_status, status_change_date, closure_date, status_reason = determine_account_status(opening_date, row, requirements, year)
            branch_code = get_branch_code(row)
            charges = account_charges[acc_type]
            channel_details = determine_opening_channel_and_details()
            currency = 'ZAR' if random.random() < 0.95 else random.choice(['USD', 'EUR'])
            account_tier = determine_account_tier(acc_type, income_level)
            overdraft_limit, credit_card_limit = generate_credit_limit(acc_type, income_level, annual_income)
            
            global_id = counter.get_next()
            account_number = generate_sa_account_number(branch_code, global_id)
            swift_code = generate_swift_code(branch_code) if currency != 'ZAR' else None
            iban = generate_iban(account_number) if currency != 'ZAR' else None
            account_purpose = determine_account_purpose(row, acc_type)
            
            is_primary = customer_id not in customer_primary_accounts
            if is_primary:
                customer_primary_accounts[customer_id] = f'ACC{global_id:07d}'
            
            statement_frequency = random.choice(['monthly', 'quarterly', 'annually'])
            online_banking_enabled = (random.random() < 0.85 if channel_details['opening_channel'] in ['online', 'mobile_app'] else random.random() < 0.65)
            online_banking_activation_date = opening_date if online_banking_enabled and channel_details['opening_channel'] in ['online', 'mobile_app'] else (
                opening_date + timedelta(days=random.randint(0, 30)) if online_banking_enabled else None)
            
            card_info = generate_card_number(acc_type, year)
            if card_info:
                card_number, card_type = card_info
                card_expiry_date = date(opening_date.year + random.randint(3, 5), opening_date.month, 1)
                card_issue_date = opening_date if random.random() < 0.8 else opening_date + timedelta(days=random.randint(7, 21))
            else:
                card_number, card_type, card_expiry_date, card_issue_date = None, None, None, None
            
            beneficiaries = generate_beneficiaries(row, fake)
            cross_border_enabled = currency != 'ZAR' or random.random() < 0.3

            accounts.append({
                'account_id': f'ACC{global_id:07d}',
                'account_number': account_number,
                'customer_id': customer_id,
                'account_type': acc_type,
                'account_purpose': account_purpose,
                'is_primary_account': is_primary,
                'opening_date': opening_date,
                'approval_date': approval_date,
                'branch_code': branch_code,
                'kyc_verified': True,
                'fica_verified': row['citizenship'] != 'ZA',
                'expected_amount': round(np.random.lognormal(mean=8.5, sigma=1.2), 2),
                'account_status': account_status,
                'status_change_date': status_change_date,
                'closure_date': closure_date,
                'status_reason': status_reason,
                'linked_joint_accounts': None,
                'interest_rate': charges['interest_rate'],
                'monthly_charges': charges['monthly_charges'],
                'transactions_rate': charges['transactions_rate'],
                'negative_balance_rate': charges['negative_balance_rate'],
                'overdraft_limit': overdraft_limit,
                'credit_card_limit': credit_card_limit,
                'bundled_products': generate_bundled_products(acc_type, row),
                'currency': currency,
                'swift_code': swift_code,
                'iban': iban,
                'account_tier': account_tier,
                'statement_frequency': statement_frequency,
                'online_banking_enabled': online_banking_enabled,
                'online_banking_activation_date': online_banking_activation_date,
                'card_number': card_number,
                'card_type': card_type,
                'card_issue_date': card_issue_date,
                'card_expiry_date': card_expiry_date,
                'beneficiaries': beneficiaries,
                'cross_border_enabled': cross_border_enabled,
                **requirements,
                **channel_details
            })
            customer_account_counts[customer_id] = customer_account_counts.get(customer_id, 0) + 1

        # Joint accounts
        joint_accounts_to_create = min(random.randint(0, 2) if year != 2020 else 0, max_accounts - num_accounts)
        for _ in range(joint_accounts_to_create):
            partners = np.random.choice([cid for cid in individual_ids if cid != customer_id], 
                                      size=min(random.randint(1, 3), max_partners), replace=False)
            opening_date = random_date(max(opening_start, date_of_entry), opening_end)
            approval_date = generate_approval_date(opening_date)
            requirements = generate_account_requirements(row, 'joint')
            account_status, status_change_date, closure_date, status_reason = determine_account_status(opening_date, row, requirements, year)
            branch_code = get_branch_code(row)
            charges = account_charges['joint']
            channel_details = determine_opening_channel_and_details()
            currency = 'ZAR'
            account_tier = determine_account_tier('joint', income_level)
            overdraft_limit, credit_card_limit = generate_credit_limit('joint', income_level, annual_income)
            
            global_id = counter.get_next()
            account_number = generate_sa_account_number(branch_code, global_id)
            account_purpose = 'joint_savings'
            is_primary = False
            statement_frequency = 'monthly'
            online_banking_enabled = (random.random() < 0.85 if channel_details['opening_channel'] in ['online', 'mobile_app'] else random.random() < 0.7)
            online_banking_activation_date = opening_date if online_banking_enabled and channel_details['opening_channel'] in ['online', 'mobile_app'] else (
                opening_date + timedelta(days=random.randint(0, 30)) if online_banking_enabled else None)
            
            card_info = generate_card_number('joint', year)
            if card_info:
                card_number, card_type = card_info
                card_expiry_date = date(opening_date.year + random.randint(3, 5), opening_date.month, 1)
                card_issue_date = opening_date if random.random() < 0.8 else opening_date + timedelta(days=random.randint(7, 21))
            else:
                card_number, card_type, card_expiry_date, card_issue_date = None, None, None, None
            
            beneficiaries = generate_beneficiaries(row, fake)

            accounts.append({
                'account_id': f'ACC{global_id:07d}',
                'account_number': account_number,
                'customer_id': customer_id,
                'account_type': 'joint',
                'account_purpose': account_purpose,
                'is_primary_account': is_primary,
                'opening_date': opening_date,
                'approval_date': approval_date,
                'branch_code': branch_code,
                'kyc_verified': True,
                'fica_verified': any(df_individuals[df_individuals['customer_id'].isin([customer_id] + list(partners))]['citizenship'] != 'ZA'),
                'expected_amount': min(round(np.random.lognormal(mean=8.5, sigma=1.2), 2), 100000),
                'account_status': account_status,
                'status_change_date': status_change_date,
                'closure_date': closure_date,
                'status_reason': status_reason,
                'linked_joint_accounts': ';'.join(partners),
                'interest_rate': charges['interest_rate'],
                'monthly_charges': charges['monthly_charges'],
                'transactions_rate': charges['transactions_rate'],
                'negative_balance_rate': charges['negative_balance_rate'],
                'overdraft_limit': overdraft_limit,
                'credit_card_limit': credit_card_limit,
                'bundled_products': generate_bundled_products('joint', row),
                'currency': currency,
                'swift_code': None,
                'iban': None,
                'account_tier': account_tier,
                'statement_frequency': statement_frequency,
                'online_banking_enabled': online_banking_enabled,
                'online_banking_activation_date': online_banking_activation_date,
                'card_number': card_number,
                'card_type': card_type,
                'card_issue_date': card_issue_date,
                'card_expiry_date': card_expiry_date,
                'beneficiaries': beneficiaries,
                'cross_border_enabled': False,
                **requirements,
                **channel_details
            })
            customer_account_counts[customer_id] = customer_account_counts.get(customer_id, 0) + 1

    # Company accounts
    for _, row in tqdm(df_companies.iterrows(), total=len(df_companies), desc="Generating Company Accounts"):
        customer_id = row['customer_id']
        date_of_entry = row['date_of_entry']
        current_count = customer_account_counts.get(customer_id, 0)
        max_accounts = 3 - current_count
        if max_accounts <= 0:
            continue
        
        num_accounts = min(random.choices([1, 2], weights=[0.8, 0.2])[0] if year != 2020 else 1, max_accounts)
        income_level = get_income_level(row)
        annual_income = row.get('annual_income', 1000000)

        for account_idx in range(num_accounts):
            acc_type = 'business'
            
            is_rejected, rejection_reason = should_reject_application(row, acc_type, year)
            application_date = random_date(max(opening_start, date_of_entry), opening_end)
            if is_rejected:
                global_id = counter.get_next()
                rejected_applications.append({
                    'application_id': f'APP{global_id:07d}',
                    'customer_id': customer_id,
                    'account_type': acc_type,
                    'application_date': application_date,
                    'rejection_reason': rejection_reason,
                    'rejection_date': application_date
                })
                continue
            
            opening_date = application_date
            approval_date = generate_approval_date(opening_date)
            requirements = generate_account_requirements(row, acc_type)
            account_status, status_change_date, closure_date, status_reason = determine_account_status(opening_date, row, requirements, year)
            branch_code = get_branch_code(row)
            charges = account_charges[acc_type]
            channel_details = determine_opening_channel_and_details()
            currency = 'ZAR' if random.random() < 0.9 else random.choice(['USD', 'EUR'])
            account_tier = determine_account_tier(acc_type, income_level)
            overdraft_limit, credit_card_limit = generate_credit_limit(acc_type, income_level, annual_income)
            
            global_id = counter.get_next()
            account_number = generate_sa_account_number(branch_code, global_id)
            swift_code = generate_swift_code(branch_code) if currency != 'ZAR' else None
            iban = generate_iban(account_number) if currency != 'ZAR' else None
            account_purpose = determine_account_purpose(row, acc_type)
            
            is_primary = customer_id not in customer_primary_accounts
            if is_primary:
                customer_primary_accounts[customer_id] = f'ACC{global_id:07d}'
            
            statement_frequency = random.choice(['monthly', 'quarterly'])
            online_banking_enabled = (random.random() < 0.95 if channel_details['opening_channel'] in ['online', 'mobile_app'] else random.random() < 0.95)
            online_banking_activation_date = opening_date if online_banking_enabled and channel_details['opening_channel'] in ['online', 'mobile_app'] else (
                opening_date + timedelta(days=random.randint(0, 14)) if online_banking_enabled else None)
            
            card_info = generate_card_number(acc_type, year)
            if card_info:
                card_number, card_type = card_info
                card_expiry_date = date(opening_date.year + random.randint(3, 5), opening_date.month, 1)
                card_issue_date = opening_date if random.random() < 0.8 else opening_date + timedelta(days=random.randint(7, 21))
            else:
                card_number, card_type, card_expiry_date, card_issue_date = None, None, None, None
            
            beneficiaries = None
            cross_border_enabled = currency != 'ZAR' or random.random() < 0.5

            accounts.append({
                'account_id': f'ACC{global_id:07d}',
                'account_number': account_number,
                'customer_id': customer_id,
                'account_type': acc_type,
                'account_purpose': account_purpose,
                'is_primary_account': is_primary,
                'opening_date': opening_date,
                'approval_date': approval_date,
                'branch_code': branch_code,
                'kyc_verified': True,
                'fica_verified': None,
                'expected_amount': round(random.uniform(10000, 1000000), 2),
                'account_status': account_status,
                'status_change_date': status_change_date,
                'closure_date': closure_date,
                'status_reason': status_reason,
                'linked_joint_accounts': None,
                'interest_rate': charges['interest_rate'],
                'monthly_charges': charges['monthly_charges'],
                'transactions_rate': charges['transactions_rate'],
                'negative_balance_rate': charges['negative_balance_rate'],
                'overdraft_limit': overdraft_limit,
                'credit_card_limit': credit_card_limit,
                'bundled_products': generate_bundled_products(acc_type, row),
                'currency': currency,
                'swift_code': swift_code,
                'iban': iban,
                'account_tier': account_tier,
                'statement_frequency': statement_frequency,
                'online_banking_enabled': online_banking_enabled,
                'online_banking_activation_date': online_banking_activation_date,
                'card_number': card_number,
                'card_type': card_type,
                'card_issue_date': card_issue_date,
                'card_expiry_date': card_expiry_date,
                'beneficiaries': beneficiaries,
                'cross_border_enabled': cross_border_enabled,
                **requirements,
                **channel_details
            })
            customer_account_counts[customer_id] = customer_account_counts.get(customer_id, 0) + 1

    # Create DataFrames and save
    df_accounts = pd.DataFrame(accounts)
    df_rejected = pd.DataFrame(rejected_applications)
    
    os.makedirs(github_repo_path, exist_ok=True)
    output_file = f'{github_repo_path}/accounts_{year}.parquet'
    df_accounts.to_parquet(output_file, index=False)

    if len(df_rejected) > 0:
        rejected_file = f'{github_repo_path}/rejected_applications_{year}.parquet'
        df_rejected.to_parquet(rejected_file, index=False)
        print(f"Generated {len(df_rejected)} rejected applications for year {year}.")
        print(f"Saved to {rejected_file}")

    print(f"Generated {len(df_accounts)} accounts for year {year}.")
    print(f"Final counter: {counter.peek()}")
    print(f"Saved to {output_file}")
    
    if len(df_accounts) > 0:
        print("\nAccount Summary:")
        print(f"- Primary accounts: {df_accounts['is_primary_account'].sum()}")
        print(f"- Accounts with overdraft: {(df_accounts['overdraft_limit'] > 0).sum()}")
        print(f"- Accounts with credit cards: {(df_accounts['credit_card_limit'] > 0).sum()}")
        print(f"- Foreign currency accounts: {(df_accounts['currency'] != 'ZAR').sum()}")
        print(f"- Online banking enabled: {df_accounts['online_banking_enabled'].sum()}")
        print(f"- Accounts with beneficiaries: {df_accounts['beneficiaries'].notna().sum()}")
        print(f"- Cross-border enabled: {df_accounts['cross_border_enabled'].sum()}")
        print(f"- Closed accounts: {(df_accounts['account_status'] == 'closed').sum()}")
        print(f"- Dormant accounts: {(df_accounts['account_status'] == 'dormant').sum()}")

    return df_accounts

def generate_accounts_with_relationships(customer_data, year):
    customer_type = customer_data['customer_type']
    if customer_type == 'Individual':
        age = calculate_age(customer_data.get('birth_date', date(1990, 1, 1)), year)
        income_level = get_income_level(customer_data)
        if income_level == 'high' and age > 35:
            return random.choices([1, 2, 3], weights=[0.4, 0.4, 0.2])[0]
        elif income_level == 'medium' and age > 25:
            return random.choices([1, 2], weights=[0.6, 0.4])[0]
        return random.choices([1], weights=[1])[0]
    return random.choices([1], weights=[1])[0]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate account data for a specific year")
    parser.add_argument('--year', type=int, default=2024, help='Year for account data generation')
    args = parser.parse_args()
    generate_accounts(args.year)