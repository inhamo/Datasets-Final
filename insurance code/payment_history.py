import pandas as pd
import numpy as np
from faker import Faker
from datetime import timedelta
from tqdm import tqdm
import os
import random

# Seed for reproducibility
np.random.seed(42)
random.seed(42)
fake = Faker()

# Load data
try:
    df_policies = pd.read_parquet("insurance_data/insurance_policies.parquet")
    df_customers = pd.read_parquet("insurance_data/insurance_applicants.parquet")
except FileNotFoundError as e:
    print(f"Error: {e}")
    exit(1)

# Ensure datetime
df_policies["Effective_Date"] = pd.to_datetime(df_policies["Effective_Date"])
df_policies["Expiration_Date"] = pd.to_datetime(df_policies["Expiration_Date"])

# Validate columns
required_policy_columns = ["Applicant_ID", "Policy_Number", "Policy_Type", "Premium_Amount", "Payment_Frequency"]
missing_policy_cols = [col for col in required_policy_columns if col not in df_policies.columns]
if missing_policy_cols:
    print(f"Error: Missing columns in df_policies: {missing_policy_cols}")
    exit(1)

# Configurations
payment_methods = {"Debit Order": 0.6, "EFT": 0.25, "Credit Card": 0.1, "Cash": 0.03, "Mobile Payment": 0.02}

# Payment frequency mapping to days
frequency_days = {
    "Monthly": 30,
    "Quarterly": 91,
    "Semi-Annually": 182,
    "Annually": 365,
    "Single Premium": 0
}

# Realistic payment dates (end of month for most frequencies)
def get_payment_date(base_date, frequency, payment_number):
    if frequency == "Monthly":
        # Last business day of month
        next_month = base_date + timedelta(days=32)
        return pd.Timestamp(next_month.year, next_month.month, 1) - timedelta(days=1)
    elif frequency == "Quarterly":
        # End of quarter
        quarter_month = ((base_date.month - 1) // 3) * 3 + 3
        return pd.Timestamp(base_date.year, quarter_month, 1) + timedelta(days=payment_number * 91)
    elif frequency == "Semi-Annually":
        # Middle and end of year
        if base_date.month <= 6:
            return pd.Timestamp(base_date.year, 6, 30)
        else:
            return pd.Timestamp(base_date.year, 12, 31)
    elif frequency == "Annually":
        # Yearly anniversary
        return base_date + timedelta(days=payment_number * 365)
    else:
        return base_date

# Initialize payments list
all_payments = []
payment_id_counter = 1

print("Generating realistic payment records for each policy...")

for _, policy in tqdm(df_policies.iterrows(), total=len(df_policies)):
    policy_number = policy["Policy_Number"]
    base_premium = policy["Premium_Amount"]
    payment_frequency = policy["Payment_Frequency"]
    effective_date = policy["Effective_Date"]
    expiration_date = policy["Expiration_Date"]
    
    # Skip if policy duration is too short
    if (expiration_date - effective_date).days < 1:
        continue
    
    # Single premium policy - one payment only
    if payment_frequency == "Single Premium":
        payment_record = {
            "Payment_ID": f"PAY{payment_id_counter:06d}",
            "Policy_Number": policy_number,
            "Payment_Date": effective_date,
            "Amount_Paid": base_premium,
            "Payment_Method": np.random.choice(list(payment_methods.keys()), p=list(payment_methods.values()))
        }
        all_payments.append(payment_record)
        payment_id_counter += 1
        continue
    
    # Recurring payments - generate all scheduled payments
    current_date = effective_date
    payment_number = 0
    
    while current_date <= expiration_date:
        # Get the scheduled payment date for this frequency
        payment_date = get_payment_date(current_date, payment_frequency, payment_number)
        
        # If payment date is after expiration, stop
        if payment_date > expiration_date:
            break
        
        # For recurring payments, the amount is always the same (as per insurance practice)
        payment_record = {
            "Payment_ID": f"PAY{payment_id_counter:06d}",
            "Policy_Number": policy_number,
            "Payment_Date": payment_date,
            "Amount_Paid": base_premium,
            "Payment_Method": np.random.choice(list(payment_methods.keys()), p=list(payment_methods.values()))
        }
        
        all_payments.append(payment_record)
        payment_id_counter += 1
        payment_number += 1
        
        # Move to next payment period
        if payment_frequency == "Monthly":
            current_date += timedelta(days=30)
        elif payment_frequency == "Quarterly":
            current_date += timedelta(days=91)
        elif payment_frequency == "Semi-Annually":
            current_date += timedelta(days=182)
        elif payment_frequency == "Annually":
            current_date += timedelta(days=365)

# Convert to DataFrame
df_payments = pd.DataFrame(all_payments)

# Save to Parquet
os.makedirs("insurance_data", exist_ok=True)
save_path = "insurance_data/payment_history.parquet"
df_payments.to_parquet(save_path, index=False)

print(f"Saved {len(df_payments)} payment records to {save_path}")
print("\nPayment frequency distribution by policy:")
print(df_payments.groupby("Policy_Number").size().value_counts())
print("\nPayment method distribution:")
print(df_payments["Payment_Method"].value_counts())
print("\nSample payments:")
print(df_payments.head(10))

# Show some examples of different payment frequencies
print("\nExamples of different payment frequencies:")
sample_policies = df_payments["Policy_Number"].unique()[:5]
for policy in sample_policies:
    policy_payments = df_payments[df_payments["Policy_Number"] == policy]
    print(f"\nPolicy {policy}: {len(policy_payments)} payments")
    print(policy_payments[["Payment_Date", "Amount_Paid", "Payment_Method"]].to_string(index=False))