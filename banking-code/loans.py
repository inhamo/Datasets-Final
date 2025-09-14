import pandas as pd
import numpy as np
import random
import math
from faker import Faker
from datetime import date, timedelta, datetime
import os
from tqdm import tqdm
import argparse
import glob

fake = Faker()

def generate_realistic_age():
    age_ranges = [(18, 25), (26, 35), (36, 45), (46, 55), (56, 65), (66, 80)]
    weights = [0.25, 0.30, 0.20, 0.15, 0.07, 0.03]
    age_range = random.choices(age_ranges, weights=weights)[0]
    return random.randint(*age_range)

def calculate_age(birth_date, target_year):
    try:
        if pd.isna(birth_date) or not isinstance(birth_date, date):
            return generate_realistic_age()
        return target_year - birth_date.year
    except (ValueError, TypeError):
        return generate_realistic_age()

def get_income_level(customer_data, target_year):
    income = customer_data.get('annual_income', 300000)
    if pd.isna(income) or income <= 0:
        income = 300000
    # Simulate income reduction in 2020 due to COVID-19
    if target_year == 2020 and random.random() < 0.2:  # 20% chance of income reduction
        income *= random.uniform(0.6, 0.9)  # Reduce income by 10-40%
    if income < 100000:
        return 'low'
    elif income < 600000:
        return 'medium'
    else:
        return 'high'

def calculate_loan_eligibility(customer_data, loan_type, target_year):
    annual_income = customer_data.get('annual_income', 300000)
    if pd.isna(annual_income) or annual_income <= 0:
        annual_income = 300000
    # Simulate income reduction in 2020
    if target_year == 2020 and random.random() < 0.2:
        annual_income *= random.uniform(0.6, 0.9)
    monthly_income = annual_income / 12
    age = calculate_age(customer_data.get('birth_date', date(1990, 1, 1)), target_year) if customer_data.get('customer_type') == 'Individual' else 40
    employment_type = customer_data.get('occupation', 'Unknown')
    
    max_dti_ratios = {
        "Home Loan": 0.28,
        "Personal Loan": 0.15,
        "Business Loan": 0.25,
        "Vehicle Loan": 0.20,
        "Education Loan": 0.10
    }
    
    max_monthly_payment = monthly_income * max_dti_ratios[loan_type]
    
    stability_multiplier = {
        'Doctor': 1.2, 'Lawyer': 1.2, 'Engineer': 1.1, 'Teacher': 1.0,
        'Unemployed': 0.0, 'Self-Employed': 0.8, 'Student': 0.3
    }.get(employment_type, 1.0)
    
    max_monthly_payment *= stability_multiplier
    
    if customer_data.get('customer_type') == 'Individual':
        if age < 25:
            max_monthly_payment *= 0.7
        elif age > 55:
            max_monthly_payment *= 0.9
    
    return max_monthly_payment

def calculate_realistic_loan_amount(customer_data, loan_type, term_months, interest_rate, target_year, financial_distress, application_date):
    max_monthly_payment = calculate_loan_eligibility(customer_data, loan_type, target_year)
    annual_income = customer_data.get('annual_income', 300000)
    if pd.isna(annual_income) or annual_income <= 0:
        annual_income = 300000
    # Simulate income reduction in 2020
    if target_year == 2020 and random.random() < 0.2:
        annual_income *= random.uniform(0.6, 0.9)
    monthly_income = annual_income / 12
    age = calculate_age(customer_data.get('birth_date', date(1990, 1, 1)), target_year) if customer_data.get('customer_type') == 'Individual' else 40
    
    r = interest_rate / 100 / 12
    if r == 0:
        max_principal = max_monthly_payment * term_months
    else:
        max_principal = max_monthly_payment * (math.pow(1 + r, term_months) - 1) / (r * math.pow(1 + r, term_months))
    
    loan_caps = {
        "Home Loan": min(max_principal, annual_income * 4),
        "Personal Loan": min(max_principal, monthly_income * 10),
        "Business Loan": min(max_principal, annual_income * 2),
        "Vehicle Loan": min(max_principal, annual_income * 0.8),
        "Education Loan": min(max_principal, 500000)
    }
    
    base_amount = max(0, loan_caps[loan_type])
    
    # Psychological pricing effects
    psychological_amounts = [50000, 75000, 100000, 150000, 200000, 250000, 500000, 1000000]
    if random.random() < 0.4:
        base_amount = random.choice(psychological_amounts) * np.random.uniform(0.95, 1.05)
    
    # Social media influence for young individuals
    if customer_data['customer_type'] == 'Individual' and age < 35 and random.random() < 0.25:
        base_amount *= np.random.uniform(1.2, 1.8)
    
    # Financial distress behavior
    if financial_distress:
        base_amount = min(base_amount * np.random.uniform(0.8, 1.0), loan_caps[loan_type])
    
    # Family pressure effects
    if customer_data.get('customer_type') == 'Individual' and customer_data.get('marital_status') == 'Married' and customer_data.get('cnt_children', 0) > 2:
        base_amount *= np.random.gamma(1.5, 0.2)
    
    # Seasonal multipliers for companies
    if customer_data['customer_type'] == 'Company':
        month = application_date.month
        seasonal_multipliers = {
            'Agriculture': {3: 2.5, 4: 1.8, 9: 2.0, 10: 1.5},
            'Retail': {10: 2.5, 11: 2.0, 1: 0.4},
            'Construction': {9: 1.8, 10: 1.6, 5: 0.7}
        }
        industry = customer_data.get('occupation', 'Other')  # Using occupation as industry for companies
        if industry in seasonal_multipliers and month in seasonal_multipliers[industry]:
            base_amount *= seasonal_multipliers[industry][month]
    
    return max(0, min(base_amount, loan_caps[loan_type]))

def generate_credit_profile(customer_data, financial_distress, target_year):
    age = calculate_age(customer_data.get('birth_date', date(1990, 1, 1)), target_year) if customer_data.get('customer_type') == 'Individual' else 40
    employment_type = customer_data.get('occupation', 'Unknown')
    
    if financial_distress:
        credit_score = random.randint(300, 580)
    else:
        base_score = 650
        if employment_type in ['Doctor', 'Lawyer', 'Engineer']:
            base_score += 50
        if age > 35:
            base_score += 30
        if age < 25:
            base_score -= 40
        # Simulate credit score impact in 2020
        if target_year == 2020:
            base_score -= random.randint(10, 50)  # Mild credit score reduction
        credit_score = min(850, max(580, int(np.random.normal(base_score, 60))))
    
    credit_history_months = min(age * 12 - 18*12, max(6, int(np.random.normal(max(age * 8, 6), 24)))) if customer_data.get('customer_type') == 'Individual' else random.randint(60, 240)
    
    if credit_score > 740:
        payment_history_pct = random.uniform(0.95, 1.0)
    elif credit_score > 670:
        payment_history_pct = random.uniform(0.85, 0.95)
    elif credit_score > 580:
        payment_history_pct = random.uniform(0.70, 0.85)
    else:
        payment_history_pct = random.uniform(0.50, 0.70)
    
    if financial_distress:
        credit_utilization = random.uniform(0.7, 0.95)
    else:
        credit_utilization = random.uniform(0.05, 0.30)
        # Increase credit utilization in 2020 for some customers
        if target_year == 2020 and random.random() < 0.3:
            credit_utilization += random.uniform(0.1, 0.3)
            credit_utilization = min(credit_utilization, 0.95)
    
    ext_source_rating = payment_history_pct * 0.4 + (1 - credit_utilization) * 0.3 + (credit_score / 850) * 0.3
    
    return {
        'credit_score': credit_score,
        'credit_history_months': credit_history_months,
        'payment_history_pct': round(payment_history_pct, 3),
        'credit_utilization': round(credit_utilization, 3),
        'ext_source_rating': round(ext_source_rating, 2)
    }

def determine_loan_purpose_and_docs(loan_type, customer_data, application_date):
    purposes = {
        "Home Loan": ["Purchase", "Refinance", "Construction", "Home Improvement"],
        "Personal Loan": ["Debt Consolidation", "Medical", "Wedding", "Travel", "Emergency"],
        "Business Loan": ["Equipment", "Working Capital", "Expansion", "Inventory"],
        "Vehicle Loan": ["New Car", "Used Car", "Motorcycle", "Commercial Vehicle"],
        "Education Loan": ["Tuition", "Living Expenses", "Books", "Study Abroad"]
    }
    
    month = application_date.month
    actual_purpose_weights = {
        "Home Loan": [0.4, 0.3, 0.15, 0.15],
        "Personal Loan": [0.6, 0.15, 0.1, 0.1, 0.05],
        "Business Loan": [0.3, 0.4, 0.2, 0.1],
        "Vehicle Loan": [0.5, 0.3, 0.1, 0.1],
        "Education Loan": [0.4, 0.3, 0.2, 0.1]
    }
    if customer_data['customer_type'] == 'Individual' and month in [11, 12]:
        actual_purpose_weights["Personal Loan"] = [0.5, 0.2, 0.15, 0.1, 0.05]
    elif customer_data['customer_type'] == 'Company' and month in [3, 9]:
        actual_purpose_weights["Business Loan"] = [0.2, 0.5, 0.2, 0.1]
    
    purpose = random.choices(purposes[loan_type], weights=actual_purpose_weights[loan_type])[0]
    
    required_docs = {
        'income_proof': True,
        'bank_statements': loan_type in ["Home Loan", "Business Loan"],
        'employment_letter': customer_data.get('occupation') not in ['Self-Employed', 'Unemployed'] and customer_data.get('customer_type') == 'Individual',
        'tax_returns': loan_type == "Business Loan" or (customer_data.get('occupation') == 'Self-Employed' and customer_data.get('customer_type') == 'Individual'),
        'property_valuation': loan_type == "Home Loan",
        'vehicle_valuation': loan_type == "Vehicle Loan",
        'academic_records': loan_type == "Education Loan",
        'business_plan': loan_type == "Business Loan" and purpose == "Expansion"
    }
    
    return purpose, required_docs

def calculate_risk_adjusted_rate(base_rate, credit_profile, loan_type, customer_data, application_date, application_channel, target_year):
    credit_score = credit_profile['credit_score']
    
    if credit_score >= 750:
        risk_adjustment = -1.0
    elif credit_score >= 700:
        risk_adjustment = 0.0
    elif credit_score >= 650:
        risk_adjustment = 1.5
    elif credit_score >= 600:
        risk_adjustment = 3.0
    else:
        risk_adjustment = 5.0
    
    type_adjustments = {
        "Home Loan": -0.5,
        "Vehicle Loan": 0.0,
        "Education Loan": 0.5,
        "Personal Loan": 2.0,
        "Business Loan": 1.0
    }
    
    employment_adjustment = 0.0
    if customer_data.get('customer_type') == 'Individual':
        if customer_data.get('occupation') == 'Self-Employed':
            employment_adjustment = 1.5
        elif customer_data.get('occupation') in ['Doctor', 'Teacher', 'Civil Servant']:
            employment_adjustment = -0.5
    
    credit_shopping_intensity = np.random.poisson(3)
    competitive_discount = np.random.uniform(-0.5, -1.5) if credit_shopping_intensity > 5 else 0
    
    years_with_bank = np.random.exponential(5)
    loyalty_discount = np.random.uniform(-0.25, -0.75) if years_with_bank > 10 and customer_data.get('account_balance', 0) > 100000 else 0
    new_customer_penalty = np.random.uniform(0.25, 1.0) if years_with_bank < 1 else 0
    
    staff_discretion = np.random.normal(0, 0.5) if application_channel == 'branch_manual' else 0
    target_desperation_discount = np.random.uniform(-0.5, -1.0) if application_date.day > 25 else 0
    
    # Economic stress for 2020 and late 2019
    if target_year == 2020:
        economic_stress = 1.2
    elif target_year == 2019 and application_date.month in [11, 12]:
        economic_stress = 1.05  # Slight increase for late 2019 uncertainty
    else:
        economic_stress = 1.0
    
    final_rate = (base_rate + risk_adjustment + type_adjustments[loan_type] + employment_adjustment +
                  competitive_discount + loyalty_discount + new_customer_penalty + staff_discretion +
                  target_desperation_discount) * economic_stress
    return max(5.0, min(25.0, round(final_rate, 2)))

def simulate_approval_process(customer_data, loan_request, credit_profile, target_year, application_date, application_channel):
    meets_minimum_criteria = (
        credit_profile['credit_score'] >= 500 and
        (customer_data.get('occupation') != 'Unemployed' if customer_data.get('customer_type') == 'Individual' else True) and
        loan_request.get('debt_to_income', 0.4) <= 0.4
    )
    
    if not meets_minimum_criteria:
        return {
            'application_status': 'Rejected',
            'rejection_reason': random.choice(['Insufficient income', 'High debt-to-income ratio', 'Poor credit history']),
            'approval_date': None,
            'processing_days': random.randint(1, 3)
        }
    
    processing_days = 1
    if loan_request['amount'] > 1000000:
        processing_days += random.randint(7, 14)
    elif loan_request['amount'] > 500000:
        processing_days += random.randint(3, 7)
    else:
        processing_days += random.randint(1, 3)
    
    if not all(loan_request.get('required_docs', {}).values()):
        processing_days += random.randint(3, 10)
    
    base_approval_prob = 0.95 if credit_profile['credit_score'] >= 720 else \
                        0.80 if credit_profile['credit_score'] >= 650 else \
                        0.60 if credit_profile['credit_score'] >= 580 else 0.30
    # Adjust approval probability for 2020 and late 2019
    if target_year == 2020:
        base_approval_prob *= 0.8
    elif target_year == 2019 and application_date.month in [11, 12]:
        base_approval_prob *= 0.95  # Slight reduction for late 2019
    
    loan_officer_mood = np.random.choice(['good', 'neutral', 'stressed', 'quota_pressure'], p=[0.3, 0.4, 0.2, 0.1])
    officer_bias = np.random.normal(0, 0.1)
    approval_prob = base_approval_prob * (1 + officer_bias)
    if loan_officer_mood == 'stressed':
        approval_prob *= 0.9
    elif loan_officer_mood == 'quota_pressure' and application_date.day > 25:
        approval_prob *= 1.1
    
    channel_modifiers = {
        'walk_in_branch': 1.1,
        'mobile_app': 0.9,
        'broker_channel': 1.2,
        'call_center': 0.85
    }
    approval_prob *= channel_modifiers.get(application_channel, 1.0)
    
    is_approved = random.random() < approval_prob
    status = 'Approved' if is_approved else 'Rejected'
    
    if is_approved and random.random() < 0.05:
        status = 'Approved_Then_Withdrawn'
        rejection_reason = random.choice([
            'Credit_Score_Change', 'Employment_Verification_Failed', 
            'Property_Valuation_Low', 'Bank_Policy_Change', 'Customer_Changed_Mind'
        ])
    else:
        rejection_reason = None if is_approved else random.choice([
            'Insufficient income', 'High debt-to-income ratio', 
            'Poor credit history', 'Incomplete documentation',
            'Loan officer gut feeling', 'Application handwriting illegible'
        ])
    
    return {
        'application_status': status,
        'rejection_reason': rejection_reason,
        'approval_date': application_date + pd.Timedelta(days=processing_days) if is_approved else None,
        'processing_days': processing_days
    }

def amortization_monthly_payment(principal, annual_rate_pct, term_months):
    r = annual_rate_pct / 100 / 12
    if r == 0:
        return principal / term_months
    numerator = r * math.pow(1 + r, term_months)
    denominator = math.pow(1 + r, term_months) - 1
    return principal * numerator / denominator

def credit_bureau_counts(distress, application_date, target_year):
    inquiry_patterns = {
        'stable_customer': {'hour': 0.01, 'day': 0.1, 'week': 0.5, 'month': 1, 'year': 3},
        'rate_shopping': {'hour': 0.8, 'day': 4, 'week': 8, 'month': 12, 'year': 20},
        'financial_distress': {'hour': 2, 'day': 8, 'week': 25, 'month': 60, 'year': 150},
        'credit_repair': {'hour': 0.2, 'day': 1, 'week': 3, 'month': 8, 'year': 15}
    }
    
    # Increase financial distress inquiries in 2020
    pattern = 'financial_distress' if distress or (target_year == 2020 and random.random() < 0.3) else random.choice(['stable_customer', 'rate_shopping', 'credit_repair'])
    counts = {k: max(0, int(np.random.poisson(v))) for k, v in inquiry_patterns[pattern].items()}
    
    hour = application_date.hour
    day = application_date.weekday()
    inquiry_multiplier = 1.5 if hour in [2, 3, 4, 5] else 1.3 if day == 6 else 1.0
    counts = {k: int(v * inquiry_multiplier) for k, v in counts.items()}
    counts['quarter'] = counts['month'] * 3
    
    return counts

def generate_document_flags(customer_data, loan_type, loan_amount, application_date):
    month = application_date.month
    if customer_data['customer_type'] == 'Individual':
        employment_type = customer_data.get('occupation', 'Unknown')
        doc_availability = {
            'Formal Employee': {'bank_statement': 0.8, 'payslip': 0.9, 'employment_letter': 0.6},
            'Self-Employed': {'bank_statement': 0.9, 'tax_returns': 0.4, 'financial_statements': 0.2, 'employment_letter': 0.3},
            'Informal Worker': {'bank_statement': 0.3, 'proof_of_income': 0.1, 'employment_letter': 0.05}
        }
        emp_category = 'Formal Employee' if employment_type in ['Doctor', 'Lawyer', 'Engineer', 'Teacher'] else \
                      'Self-Employed' if employment_type == 'Self-Employed' else 'Informal Worker'
        
        flag_bank_statement = 1 if loan_amount > 100000 or employment_type == 'Self-Employed' else np.random.binomial(1, doc_availability[emp_category]['bank_statement'])
        flag_proof_of_address = np.random.binomial(1, 0.95)
        flag_employment_verification = 0 if employment_type in ['Unemployed', 'Student'] else np.random.binomial(1, doc_availability[emp_category]['employment_letter'] * (0.7 if month in [12, 1] else 1.0))
        flag_tax_returns = np.random.binomial(1, doc_availability[emp_category].get('tax_returns', 0.2))
        flag_car_ownership = np.random.binomial(1, 0.7 if customer_data.get('flag_own_car', 0) else 0.3)
        flag_property_valuation = 1 if loan_type == "Home Loan" else 0
        flag_vehicle_valuation = 1 if loan_type == "Vehicle Loan" else 0
        flag_academic_records = 1 if loan_type == "Education Loan" else 0
        flag_business_registration = 0
        flag_audited_financials = 0
    else:
        company_size = 'Large' if customer_data.get('number_of_employees', 0) > 50 else 'Small'
        company_doc_chaos = {
            'audited_financials': {'Small': 0.3, 'Large': 0.95},
            'tax_compliance_certificate': {'Small': 0.7, 'Large': 0.95}
        }
        flag_bank_statement = 1
        flag_proof_of_address = 1
        flag_employment_verification = 0
        flag_tax_returns = np.random.binomial(1, company_doc_chaos['tax_compliance_certificate'][company_size])
        flag_car_ownership = 0
        flag_property_valuation = 0
        flag_vehicle_valuation = 0
        flag_academic_records = 0
        flag_business_registration = 1
        flag_audited_financials = np.random.binomial(1, company_doc_chaos['audited_financials'][company_size])
    
    return {
        'flag_bank_statement': flag_bank_statement,
        'flag_proof_of_address': flag_proof_of_address,
        'flag_employment_verification': flag_employment_verification,
        'flag_tax_returns': flag_tax_returns,
        'flag_car_ownership': flag_car_ownership,
        'flag_property_valuation': flag_property_valuation,
        'flag_vehicle_valuation': flag_vehicle_valuation,
        'flag_academic_records': flag_academic_records,
        'flag_business_registration': flag_business_registration,
        'flag_audited_financials': flag_audited_financials
    }

def generate_loan_id(system_origin, year, counter, application_date):
    loan_id = {
        'legacy_mainframe': f"LN{year}{random.randint(100000, 999999)}",
        'new_digital': f"DL-{year}-{counter:06d}",
        'branch_manual': f"BR{year}{str(random.randint(1, 10)).zfill(3)}{random.randint(1000, 9999)}",
        'mobile_app': f"MB{year}{application_date.strftime('%y%m%d')}{counter:04d}",
        'broker_channel': f"BK{year}-{random.randint(100, 999)}-{counter:05d}"
    }[system_origin]
    if random.random() < 0.02:
        loan_id += f"-R{random.randint(1, 5)}"
    return loan_id

def assign_loan_grade(credit_score, debt_to_income, ext_source_rating):
    composite_score = (credit_score / 850 * 0.5 + (1 - debt_to_income) * 0.3 + ext_source_rating * 0.2)
    if composite_score >= 0.85: return 'A+'
    elif composite_score >= 0.75: return 'A'
    elif composite_score >= 0.65: return 'B+'
    elif composite_score >= 0.55: return 'B'
    elif composite_score >= 0.45: return 'C+'
    elif composite_score >= 0.35: return 'C'
    else: return 'D'

def can_take_loan(customer_id, loan_history, target_year, max_loans=3, min_gap_days=365):
    if customer_id not in loan_history['customer_id'].values:
        return True
    customer_loans = loan_history[loan_history['customer_id'] == customer_id]
    if len(customer_loans) >= max_loans:
        return False
    # Check if the last loan was at least min_gap_days ago
    last_loan_date = customer_loans['application_date'].max()
    if pd.isna(last_loan_date):
        return True
    days_since_last_loan = (date(target_year, 12, 31) - last_loan_date.date()).days
    return days_since_last_loan >= min_gap_days

def generate_loans(target_year):
    if target_year < 2018:
        print(f"Error: target_year ({target_year}) must be >= 2018.")
        return pd.DataFrame()

    SEED = target_year
    random.seed(SEED)
    np.random.seed(SEED)
    Faker.seed(SEED)

    base_year = 2018
    github_repo_path = 'banking_data'

    # Load customer data from base_year to target_year
    customer_files = [f for f in glob.glob(f'{github_repo_path}/customers_*.parquet') 
                     if base_year <= int(f.split('_')[-1].split('.')[0]) <= target_year]
    if not customer_files:
        print(f"No customer files found for years {base_year}-{target_year}. Exiting.")
        return pd.DataFrame()
    customers_list = [pd.read_parquet(f) for f in customer_files]
    customers_df = pd.concat(customers_list, ignore_index=True).drop_duplicates(subset=['customer_id'])

    # Load account data from base_year to target_year
    account_files = [f for f in glob.glob(f'{github_repo_path}/accounts_*.parquet') 
                     if base_year <= int(f.split('_')[-1].split('.')[0]) <= target_year]
    if not account_files:
        print(f"No account files found for years {base_year}-{target_year}. Exiting.")
        return pd.DataFrame()
    accounts_list = [pd.read_parquet(f) for f in account_files]
    accounts_df = pd.concat(accounts_list, ignore_index=True).drop_duplicates(subset=['account_id'])

    # Load previous loan data to enforce loan limits
    loan_files = [f for f in glob.glob(f'{github_repo_path}/loans_*.parquet') 
                  if base_year <= int(f.split('_')[-1].split('.')[0]) < target_year]
    loan_history = pd.concat([pd.read_parquet(f) for f in loan_files], ignore_index=True) if loan_files else pd.DataFrame(columns=['customer_id', 'application_date'])

    # Merge customers and accounts
    cust_acc_df = accounts_df.merge(customers_df, left_on="customer_id", right_on="customer_id", how="left")

    # FIX: Convert date_of_entry to datetime if it's not already
    if 'date_of_entry' in cust_acc_df.columns:
        cust_acc_df['date_of_entry'] = pd.to_datetime(cust_acc_df['date_of_entry'], errors='coerce')
    else:
        print("Warning: 'date_of_entry' column not found. Creating a dummy column.")
        # Create a dummy date_of_entry column if it doesn't exist
        cust_acc_df['date_of_entry'] = pd.to_datetime(f'{target_year-1}-01-01')

    # Filter to match realistic customer volumes and include 3% of previous customers
    NUM_INDIVIDUALS = random.randint(15000, 25000)
    NUM_COMPANIES = random.randint(500, 1500)
    
    # Sample current year customers
    current_year_customers = cust_acc_df[cust_acc_df['date_of_entry'].dt.year == target_year]
    individuals_df = current_year_customers[current_year_customers['customer_type'] == 'Individual'].sample(n=min(NUM_INDIVIDUALS, len(current_year_customers[current_year_customers['customer_type'] == 'Individual'])), random_state=SEED) if len(current_year_customers[current_year_customers['customer_type'] == 'Individual']) > 0 else pd.DataFrame()
    companies_df = current_year_customers[current_year_customers['customer_type'] == 'Company'].sample(n=min(NUM_COMPANIES, len(current_year_customers[current_year_customers['customer_type'] == 'Company'])), random_state=SEED) if len(current_year_customers[current_year_customers['customer_type'] == 'Company']) > 0 else pd.DataFrame()
    
    # Sample 3% of previous customers
    previous_customers = cust_acc_df[cust_acc_df['date_of_entry'].dt.year < target_year]
    if not previous_customers.empty:
        prev_individuals = previous_customers[previous_customers['customer_type'] == 'Individual'].sample(frac=0.03, random_state=SEED) if len(previous_customers[previous_customers['customer_type'] == 'Individual']) > 0 else pd.DataFrame()
        prev_companies = previous_customers[previous_customers['customer_type'] == 'Company'].sample(frac=0.03, random_state=SEED) if len(previous_customers[previous_customers['customer_type'] == 'Company']) > 0 else pd.DataFrame()
        
        # Combine all dataframes, filtering out empty ones
        dataframes_to_concat = [df for df in [individuals_df, companies_df, prev_individuals, prev_companies] if not df.empty]
        cust_acc_df = pd.concat(dataframes_to_concat, ignore_index=True) if dataframes_to_concat else pd.DataFrame()
    else:
        dataframes_to_concat = [df for df in [individuals_df, companies_df] if not df.empty]
        cust_acc_df = pd.concat(dataframes_to_concat, ignore_index=True) if dataframes_to_concat else pd.DataFrame()

    # Check if we have any customers to process
    if cust_acc_df.empty:
        print(f"Warning: No customers found for year {target_year}. Creating empty loans file.")
        loans_df = pd.DataFrame()
        os.makedirs(github_repo_path, exist_ok=True)
        output_file = f'{github_repo_path}/loans_{target_year}.parquet'
        loans_df.to_parquet(output_file, index=False)
        print(f"Created empty loans file: {output_file}")
        return loans_df

    loan_types = {
        "Home Loan": 7.0,
        "Personal Loan": 12.0,
        "Business Loan": 9.0,
        "Vehicle Loan": 10.5,
        "Education Loan": 8.0
    }
    contract_types = ["Standard", "Revolving"]
    car_brands = ["Toyota", "Ford", "Honda", "BMW", "Mercedes", "Volkswagen", "Audi", "Hyundai", "Nissan"]
    car_models = ["Corolla", "Focus", "Civic", "X5", "C-Class", "Golf", "A4", "Elantra", "Altima"]
    financial_distress_rate = 0.1 if target_year == 2020 else 0.05  # Increased distress in 2020
    system_origins = ['legacy_mainframe', 'new_digital', 'branch_manual', 'mobile_app', 'broker_channel']
    system_weights = [0.2, 0.3, 0.2, 0.2, 0.1]

    loans = []
    loan_id_counter = 1

    customer_groups = cust_acc_df.groupby('customer_id')
    for customer_id, group in tqdm(customer_groups, desc="Generating Loans"):
        if not can_take_loan(customer_id, loan_history, target_year):
            continue
        customer_data = customers_df[customers_df['customer_id'] == customer_id].iloc[0].to_dict()
        age = calculate_age(customer_data.get('birth_date', date(target_year - generate_realistic_age(), 1, 1)), target_year)
        income_level = get_income_level(customer_data, target_year)
        employment_type = customer_data.get('occupation', 'Unknown')
        
        # Determine number of loans
        if customer_data['customer_type'] == 'Individual':
            if target_year == 2020:
                num_loans = random.choices([0, 1], weights=[0.7, 0.3])[0]
            elif target_year == 2019 and random.random() < 0.1:  # Slight reduction in late 2019
                num_loans = random.choices([0, 1], weights=[0.65, 0.35])[0]
            elif age >= 26:
                num_loans = random.choices([1, 2, 3], weights=[0.3, 0.5, 0.2])[0] if income_level == 'high' else random.choices([1, 2], weights=[0.6, 0.4])[0]
            else:
                num_loans = random.choices([0, 1], weights=[0.6, 0.4])[0]
        else:
            num_loans = random.choices([1, 2], weights=[0.8, 0.2])[0] if target_year != 2020 else 1

        financial_distress = random.random() < financial_distress_rate
        credit_profile = generate_credit_profile(customer_data, financial_distress, target_year)
        
        # Account selection logic
        account_ids = group['account_id'].tolist()
        if customer_data['customer_type'] == 'Individual' and len(account_ids) > 1:
            if random.random() < 0.7 and any('joint' in str(aid).lower() for aid in account_ids):
                account_id = random.choice([aid for aid in account_ids if 'joint' in str(aid).lower()])
            elif employment_type == 'Self-Employed' and random.random() < 0.3 and any('business' in str(aid).lower() for aid in account_ids):
                account_id = random.choice([aid for aid in account_ids if 'business' in str(aid).lower()])
            else:
                account_id = random.choice(account_ids)
        else:
            account_id = group['account_id'].iloc[0]

        # Company subsidiary logic
        if customer_data['customer_type'] == 'Company' and customer_data.get('number_of_employees', 0) > 50 and random.random() < 0.4:
            customer_id = f"SUB-{customer_id}-{random.randint(1, 8)}"

        for _ in range(num_loans):
            application_channel = random.choices(system_origins, weights=system_weights)[0]
            application_date = fake.date_time_between(start_date=date(target_year-2, 1, 1), end_date=date(target_year, 11, 1))
            month = application_date.month
            
            # Loan type selection with seasonal and economic effects
            if customer_data['customer_type'] == 'Company':
                possible_loan_types = ["Business Loan"]
                loan_type_weights = {"Business Loan": 1.0}
            else:
                possible_loan_types = ["Home Loan", "Personal Loan", "Vehicle Loan", "Education Loan"]
                if customer_data.get('cnt_children', 0) == 0:
                    possible_loan_types.remove("Education Loan")
                loan_type_weights = {"Home Loan": 0.2, "Personal Loan": 0.35, "Vehicle Loan": 0.25, "Education Loan": 0.15}
                if month in [11, 12]:
                    loan_type_weights['Personal Loan'] *= 1.5
                    loan_type_weights['Vehicle Loan'] *= 1.2
                    if target_year == 2019:  # Early COVID-19 uncertainty in late 2019
                        loan_type_weights['Personal Loan'] *= 1.2
                        loan_type_weights['Home Loan'] *= 0.9
                        loan_type_weights['Vehicle Loan'] *= 0.9
                elif month in [1, 2]:
                    loan_type_weights['Education Loan'] *= 2.0
                    loan_type_weights['Personal Loan'] *= 1.3
                if target_year == 2020:
                    loan_type_weights['Personal Loan'] *= 1.8
                    loan_type_weights['Home Loan'] *= 0.4
                    loan_type_weights['Vehicle Loan'] *= 0.6
                if application_channel == 'broker_channel':
                    loan_type_weights['Personal Loan'] *= 0.7
                    loan_type_weights['Home Loan'] *= 1.4
                # Normalize weights
                total_weight = sum(loan_type_weights.values())
                loan_type_weights = {k: v/total_weight for k, v in loan_type_weights.items() if k in possible_loan_types}
            
            loan_type = random.choices(list(loan_type_weights.keys()), weights=list(loan_type_weights.values()))[0]

            # Term selection
            term_distributions = {
                "Home Loan": random.choice([120, 180, 240, 300, 360]) if customer_data['customer_type'] == 'Individual' and age <= 50 else random.choice([120, 180]),
                "Personal Loan": random.choice([12, 24, 36, 48, 60]),
                "Business Loan": random.choice([6, 12, 18, 24]) if month in [3, 9] else random.choice([24, 36, 48, 60, 84]),
                "Vehicle Loan": random.choice([24, 36, 48, 60, 72]),
                "Education Loan": random.choice([12, 24, 36, 48])
            }
            term_months = term_distributions[loan_type]

            base_rate = loan_types[loan_type]
            interest_rate = calculate_risk_adjusted_rate(base_rate, credit_profile, loan_type, customer_data, application_date, application_channel, target_year)
            principal_amount = calculate_realistic_loan_amount(customer_data, loan_type, term_months, interest_rate, target_year, financial_distress, application_date)
            
            # Amount granted based on creditworthiness
            approval_ratio = np.random.beta(9, 1) if credit_profile['credit_score'] >= 750 else \
                            np.random.beta(7, 3) if credit_profile['credit_score'] >= 650 else \
                            np.random.beta(5, 5)
            amount_granted = principal_amount * approval_ratio
            monthly_installment = round(amortization_monthly_payment(amount_granted, interest_rate, term_months), 2)

            purpose, required_docs = determine_loan_purpose_and_docs(loan_type, customer_data, application_date)
            annual_income = customer_data.get('annual_income', 300000)
            if pd.isna(annual_income) or annual_income <= 0:
                annual_income = 300000
            if target_year == 2020 and random.random() < 0.2:
                annual_income *= random.uniform(0.6, 0.9)
            loan_request = {
                'amount': principal_amount,
                'required_docs': required_docs,
                'debt_to_income': monthly_installment / (annual_income / 12)
            }
            approval_details = simulate_approval_process(customer_data, loan_request, credit_profile, target_year, application_date, application_channel)

            name_contract_type = random.choice(contract_types)
            if loan_type == "Home Loan":
                collateral_description = customer_data.get('residential_address', 'Unknown Address')
            elif loan_type == "Vehicle Loan":
                collateral_description = f"{fake.year()} {random.choice(car_brands)} {random.choice(car_models)}"
            elif loan_type == "Business Loan":
                collateral_description = random.choice(["Equipment", "Inventory", "Accounts Receivable", "None"])
            else:
                collateral_description = random.choice(["None", "Savings Account", "Guarantee", "None"])

            loan_id = generate_loan_id(application_channel, target_year, loan_id_counter, application_date)
            document_flags = generate_document_flags(customer_data, loan_type, principal_amount, application_date)
            credit_counts = credit_bureau_counts(financial_distress, application_date, target_year)
            loan_grade = assign_loan_grade(credit_profile['credit_score'], loan_request['debt_to_income'], credit_profile['ext_source_rating'])

            start_date = approval_details['approval_date'] if approval_details['approval_date'] else application_date + pd.Timedelta(days=approval_details['processing_days'])
            end_date = start_date + pd.DateOffset(months=term_months)

            loans.append({
                "loan_id": loan_id,
                "customer_id": customer_id,
                "account_id": account_id,
                "loan_type": loan_type,
                "principal_amount": round(principal_amount, 2),
                "amount_granted": round(amount_granted, 2),
                "interest_rate": interest_rate,
                "terms_months": term_months,
                "monthly_installment": monthly_installment,
                "collateral_description": collateral_description,
                "loan_grade": loan_grade,
                "name_contract_type": name_contract_type,
                "flag_own_car": int(customer_data.get('flag_own_car', 0)),
                "flag_own_realty": int(customer_data.get('flag_own_realty', 0)),
                "cnt_children": customer_data.get('cnt_children', 0),
                "ext_source_rating": credit_profile['ext_source_rating'],
                "flag_bank_statement": document_flags['flag_bank_statement'],
                "flag_proof_of_address": document_flags['flag_proof_of_address'],
                "flag_employment_verification": document_flags['flag_employment_verification'],
                "flag_tax_returns": document_flags['flag_tax_returns'],
                "flag_car_ownership": document_flags['flag_car_ownership'],
                "flag_property_valuation": document_flags['flag_property_valuation'],
                "flag_vehicle_valuation": document_flags['flag_vehicle_valuation'],
                "flag_academic_records": document_flags['flag_academic_records'],
                "flag_business_registration": document_flags['flag_business_registration'],
                "flag_audited_financials": document_flags['flag_audited_financials'],
                "count_req_credit_bureau_hour": credit_counts["hour"],
                "count_req_credit_bureau_day": credit_counts["day"],
                "count_req_credit_bureau_week": credit_counts["week"],
                "count_req_credit_bureau_month": credit_counts["month"],
                "count_req_credit_bureau_quarter": credit_counts["quarter"],
                "count_req_credit_bureau_year": credit_counts["year"],
                "loan_purpose": purpose,
                "application_status": approval_details['application_status'],
                "rejection_reason": approval_details['rejection_reason'],
                "approval_date": approval_details['approval_date'],
                "application_date": application_date
            })
            loan_id_counter += 1

    loans_df = pd.DataFrame(loans)
    os.makedirs(github_repo_path, exist_ok=True)
    output_file = f'{github_repo_path}/loans_{target_year}.parquet'
    loans_df.to_parquet(output_file, index=False)

    print(f"Generated {len(loans_df)} loans for year {target_year}.")
    print(f"Saved to {output_file}")
    if not loans_df.empty:
        print(loans_df.head(10))

    return loans_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate loan data for a specific year")
    parser.add_argument('--year', type=int, default=2024, help='Year for loan data generation (must be >= 2018)')
    args = parser.parse_args()
    generate_loans(args.year)