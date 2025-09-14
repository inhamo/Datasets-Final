import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import date, timedelta
import os
from tqdm import tqdm
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

# Load all yearly customer and loan files
github_repo_path = 'banking_data'
customer_files = sorted(glob.glob(f'{github_repo_path}/customers_*.parquet'))
loan_files = sorted(glob.glob(f'{github_repo_path}/loans_*.parquet'))

if not customer_files:
    print("No customer files found. Exiting.")
    exit()

customers_list = [pd.read_parquet(f) for f in customer_files]
customers_df = pd.concat(customers_list, ignore_index=True).drop_duplicates(subset=['customer_id'])

if loan_files:
    loans_list = [pd.read_parquet(f) for f in loan_files]
    loans_df = pd.concat(loans_list, ignore_index=True).drop_duplicates(subset=['loan_id'])
else:
    print("No loan files found. Generating defaults table only.")
    loans_df = pd.DataFrame()

# --------- 1. Loan Default Prediction Table ---------

default_prob_base = 0.15

default_records = []

for _, loan in loans_df.iterrows():
    # Adjust default probability based on risk_score from customer
    customer_risk = customers_df[customers_df['customer_id'] == loan['customer_id']]['risk_score'].iloc[0] if not customers_df[customers_df['customer_id'] == loan['customer_id']].empty else 0.5
    default_prob = default_prob_base * (1 + customer_risk * 2)  # Higher risk increases default prob
    
    # Increase default prob in 2020
    if pd.to_datetime(loan["application_date"]).year == 2020:
        default_prob *= 1.5
    
    will_default = random.random() < default_prob
    
    if will_default:
        # Pick a default date between application_date and end of term
        start_date = pd.to_datetime(loan["application_date"])
        term_months = loan.get("terms_months", 36)
        end_date = start_date + pd.DateOffset(months=term_months)
        delta_days = (end_date - start_date).days
        
        if delta_days > 0:
            default_day_offset = random.randint(30, min(delta_days, 365 * 2))  # Default within 2 years
            default_date = start_date + pd.Timedelta(days=default_day_offset)
        else:
            default_date = pd.NaT
    else:
        default_date = pd.NaT
    
    default_records.append({
        "loan_id": loan["loan_id"],
        "will_default": will_default,
        "default_date": default_date
    })

loan_defaults_df = pd.DataFrame(default_records)

# --------- 2. Customer Employment Status Over Time ---------

# Simulate monthly employment status from Jan 2018 to Dec 2024 (84 months)
periods = pd.date_range("2018-01-01", "2024-12-01", freq='MS')  # Month start frequency

employment_records = []

for _, cust in customers_df.iterrows():
    cust_id = cust["customer_id"]
    # Initial employment status from occupation
    occupation = cust.get("occupation", "").lower()
    employed = occupation != "unemployed" and "student" not in occupation.lower()
    
    # Base probabilities for job loss/rehire
    job_loss_prob = 0.02  # 2% monthly chance of losing job
    rehire_prob = 0.15   # 15% monthly chance of getting rehired if unemployed
    
    # Adjust for 2020 COVID impact
    for period in periods:
        year = period.year
        if year == 2020:
            job_loss_prob *= 3  # Triple job loss probability
            rehire_prob *= 0.5  # Halve rehire probability
        
        # Adjust based on age and income
        age = calculate_age(cust.get('birth_date', date(1990, 1, 1)), year)
        income_level = get_income_level(cust, year)
        if age < 25 or age > 60:
            job_loss_prob *= 1.5
        if income_level == 'low':
            job_loss_prob *= 1.2
            rehire_prob *= 0.8
        
        if not employed and random.random() < rehire_prob:
            employed = True
        elif employed and random.random() < job_loss_prob:
            employed = False
        
        employment_records.append({
            "customer_id": cust_id,
            "period": period,
            "is_employed": employed,
            "occupation": occupation if employed else "Unemployed"
        })

customer_employment_df = pd.DataFrame(employment_records)

# Save tables
os.makedirs(github_repo_path, exist_ok=True)
loan_defaults_df.to_parquet(f"{github_repo_path}/loan_defaults.parquet", index=False)
customer_employment_df.to_parquet(f"{github_repo_path}/customer_employment_status.parquet", index=False)

print("Loan Defaults sample:")
print(loan_defaults_df.head(10))
print("\nCustomer Employment Status sample:")
print(customer_employment_df.head(10))