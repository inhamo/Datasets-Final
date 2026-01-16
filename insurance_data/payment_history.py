import pandas as pd
import numpy as np
from faker import Faker
from datetime import timedelta, datetime
from tqdm import tqdm
import os
import random
import calendar

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
required_policy_columns = ["Applicant_ID", "Policy_Number", "Policy_Type", "Premium_Amount", "Payment_Frequency", "Effective_Date"]
required_customer_columns = ["Customer_ID", "Monthly_Income_ZAR"]
missing_policy_cols = [col for col in required_policy_columns if col not in df_policies.columns]
missing_customer_cols = [col for col in required_customer_columns if col not in df_customers.columns]
if missing_policy_cols or missing_customer_cols:
    print(f"Error: Missing columns in df_policies: {missing_policy_cols}")
    print(f"Error: Missing columns in df_customers: {missing_customer_cols}")
    exit(1)

# Enhanced configurations with realism
payment_methods = {
    "Debit Order": 0.55,      # Most common in SA
    "EFT": 0.25,              # Electronic transfer
    "Credit Card": 0.12,      # Less common due to fees
    "Cash": 0.05,             # Declining
    "Mobile Payment": 0.03    # Growing but still small
}

# Customer payment personality types (for realism but not stored)
PAYMENT_PERSONALITY = {
    "Reliable": 0.45,    # Always on time, automated
    "Struggler": 0.30,   # Often late, partial payments
    "Inconsistent": 0.15, # Unpredictable patterns
    "Problematic": 0.08,  # Consistently late/missing
    "High-maintenance": 0.02  # Overpays, disputes
}

# Seasonal adjustments by month
MONTHLY_LATE_ADJUSTMENTS = {
    1: 1.3,   # January - holiday debt
    2: 1.0,   # February
    3: 0.9,   # March
    4: 1.0,   # April
    5: 1.0,   # May
    6: 1.1,   # June - mid-year pressure
    7: 1.2,   # July - tax season
    8: 1.0,   # August
    9: 1.0,   # September
    10: 1.0,  # October
    11: 1.1,  # November - pre-holiday
    12: 1.5   # December - holiday spending
}

def assign_payment_personality(customer):
    """Assign payment personality based on customer characteristics"""
    income = customer["Monthly_Income_ZAR"]
    
    # Adjust probabilities based on income
    if income < 15000:
        adjusted_probs = {
            "Reliable": 0.20,
            "Struggler": 0.45,
            "Inconsistent": 0.25,
            "Problematic": 0.08,
            "High-maintenance": 0.02
        }
    elif income < 40000:
        adjusted_probs = {
            "Reliable": 0.40,
            "Struggler": 0.35,
            "Inconsistent": 0.15,
            "Problematic": 0.07,
            "High-maintenance": 0.03
        }
    else:
        adjusted_probs = {
            "Reliable": 0.60,
            "Struggler": 0.20,
            "Inconsistent": 0.12,
            "Problematic": 0.05,
            "High-maintenance": 0.03
        }
    
    # Normalize probabilities
    total = sum(adjusted_probs.values())
    normalized_probs = {k: v/total for k, v in adjusted_probs.items()}
    
    personality = np.random.choice(list(normalized_probs.keys()), p=list(normalized_probs.values()))
    return personality

def get_payment_due_date(base_date, frequency, payment_number, customer_personality, income):
    """Get the due date for a payment with realistic day clustering"""
    if frequency == "Single Premium":
        return base_date
    
    # Calculate base due date
    if frequency == "Monthly":
        months_offset = payment_number
        new_month = base_date.month + months_offset
        new_year = base_date.year + (new_month - 1) // 12
        new_month = ((new_month - 1) % 12) + 1
        
        # Handle day overflow
        max_day = calendar.monthrange(new_year, new_month)[1]
        base_day = base_date.day
        
        # Realistic payment day based on income and personality
        if income < 15000:
            # Low income: typically after 25th (after grants/salary)
            if customer_personality == "Struggler":
                preferred_day = random.choices([25, 26, 27, 28, 29, 30], weights=[0.3, 0.2, 0.2, 0.15, 0.1, 0.05])[0]
            else:
                preferred_day = random.choices([25, 26, 27, 28], weights=[0.4, 0.3, 0.2, 0.1])[0]
        elif income < 40000:
            # Middle income: 1st-5th
            if customer_personality == "Reliable":
                preferred_day = 1
            else:
                preferred_day = random.choices([1, 2, 3, 4, 5], weights=[0.4, 0.3, 0.15, 0.1, 0.05])[0]
        else:
            # High income: any day, often automated
            if customer_personality == "Reliable":
                preferred_day = 1
            else:
                preferred_day = random.randint(1, 28)
        
        # Adjust for personality
        if customer_personality == "Struggler":
            preferred_day += random.randint(0, 5)  # Later in month
        
        new_day = min(preferred_day, max_day)
        
        due_date = pd.Timestamp(new_year, new_month, new_day)
        
    elif frequency == "Quarterly":
        due_date = base_date + timedelta(days=payment_number * 91)
    elif frequency == "Semi-Annually":
        due_date = base_date + timedelta(days=payment_number * 182)
    elif frequency == "Annually":
        new_year = base_date.year + payment_number
        due_date = pd.Timestamp(new_year, base_date.month, base_date.day)
    else:
        due_date = base_date
    
    # Adjust for weekends
    if due_date.weekday() >= 5:  # Saturday (5) or Sunday (6)
        # Move to next Monday
        due_date += timedelta(days=(7 - due_date.weekday()))
    
    return due_date

def calculate_realistic_payment_date(due_date, customer_personality, payment_method, month, income):
    """Calculate realistic payment date with lateness"""
    
    # Base days late by personality
    base_late_days = {
        "Reliable": random.triangular(0, 0, 3),
        "Struggler": random.triangular(0, 7, 30),
        "Inconsistent": random.triangular(0, 10, 45),
        "Problematic": random.triangular(5, 20, 60),
        "High-maintenance": random.triangular(0, 3, 15)
    }.get(customer_personality, 0)
    
    # Adjust by income
    if income < 10000:
        base_late_days *= 1.4
    elif income < 20000:
        base_late_days *= 1.2
    elif income < 40000:
        base_late_days *= 1.0
    else:
        base_late_days *= 0.7
    
    # Adjust by payment method
    method_multiplier = {
        "Debit Order": 0.3,
        "Credit Card": 0.5,
        "EFT": 1.2,
        "Cash": 1.5,
        "Mobile Payment": 0.8
    }.get(payment_method, 1.0)
    
    # Seasonal adjustment
    seasonal_multiplier = MONTHLY_LATE_ADJUSTMENTS.get(month, 1.0)
    
    # December special - more lateness
    if month == 12 and due_date.day <= 15:
        seasonal_multiplier *= 1.3
    
    # Calculate final days late
    days_late = int(base_late_days * method_multiplier * seasonal_multiplier)
    
    # Add some randomness
    days_late += random.randint(-2, 2)
    days_late = max(0, days_late)
    
    # Special cases
    if customer_personality == "Reliable" and days_late > 7:
        days_late = random.randint(0, 7)
    elif customer_personality == "Problematic" and days_late < 10:
        days_late = random.randint(10, 30)
    
    # Payment method specific processing delays
    if payment_method == "EFT":
        days_late += random.randint(1, 2)  # EFT processing delay
    elif payment_method == "Cash":
        days_late += random.randint(0, 1)  # Cash deposit delay
    
    return due_date + timedelta(days=days_late)

def calculate_realistic_payment_amount(base_premium, customer_personality, payment_method, is_late):
    """Calculate realistic payment amount with variances"""
    
    # Base amount starts at premium
    amount = base_premium
    
    # Adjust by personality
    if customer_personality == "Struggler":
        # Often pays less
        if random.random() < 0.3:  # 30% chance of underpayment
            under_pct = random.triangular(0.05, 0.15, 0.30)
            amount = int(base_premium * (1 - under_pct))
    elif customer_personality == "High-maintenance":
        # Sometimes overpays
        if random.random() < 0.15:  # 15% chance of overpayment
            over_pct = random.triangular(0.01, 0.05, 0.10)
            amount = int(base_premium * (1 + over_pct))
    elif customer_personality == "Inconsistent":
        # Random variations
        if random.random() < 0.2:  # 20% chance of variance
            variance_pct = random.triangular(-0.2, 0, 0.1)
            amount = int(base_premium * (1 + variance_pct))
    
    # Late payments often have issues
    if is_late:
        if random.random() < 0.25:  # 25% chance of payment issues when late
            if customer_personality == "Struggler":
                amount = int(base_premium * random.triangular(0.5, 0.8, 1.0))
            elif customer_personality == "Problematic":
                amount = int(base_premium * random.triangular(0.3, 0.6, 0.9))
    
    # Payment method specific adjustments
    if payment_method == "Cash":
        # Cash payments often rounded
        amount = round(amount / 10) * 10
    elif payment_method in ["Debit Order", "Credit Card"]:
        # Electronic payments are exact
        pass
    elif payment_method == "EFT":
        # EFT sometimes has slight variations
        if random.random() < 0.1:
            amount += random.choice([-1, 1]) * random.randint(1, 5)
    
    # Ensure minimum payment
    amount = max(10, amount)
    
    # Round to nearest rand
    amount = int(round(amount))
    
    return amount

def generate_realistic_payment_method(customer_personality, income, payment_number):
    """Generate realistic payment method"""
    
    # Base probabilities
    base_probs = payment_methods.copy()
    
    # Adjust by personality
    if customer_personality == "Reliable":
        base_probs["Debit Order"] *= 1.5
        base_probs["Cash"] *= 0.3
    elif customer_personality == "Struggler":
        base_probs["Cash"] *= 2.0
        base_probs["Debit Order"] *= 0.7
    elif customer_personality == "High-maintenance":
        base_probs["Credit Card"] *= 2.0
    
    # Adjust by income
    if income > 40000:
        base_probs["Credit Card"] *= 1.5
        base_probs["EFT"] *= 1.2
    elif income < 15000:
        base_probs["Cash"] *= 1.5
        base_probs["Mobile Payment"] *= 1.2
    
    # Normalize
    total = sum(base_probs.values())
    normalized_probs = {k: v/total for k, v in base_probs.items()}
    
    # Customers sometimes change methods
    if payment_number > 0 and random.random() < 0.1:
        # Change method occasionally
        return np.random.choice(list(normalized_probs.keys()), p=list(normalized_probs.values()))
    else:
        # Stick with previous or choose new
        return np.random.choice(list(normalized_probs.keys()), p=list(normalized_probs.values()))

def should_generate_missed_payment(customer_personality, payment_number, income):
    """Determine if a payment should be missed"""
    
    # Base miss probability
    miss_prob = {
        "Reliable": 0.005,
        "Struggler": 0.10,
        "Inconsistent": 0.05,
        "Problematic": 0.25,
        "High-maintenance": 0.02
    }.get(customer_personality, 0.03)
    
    # Adjust by income
    if income < 10000:
        miss_prob *= 1.5
    elif income < 20000:
        miss_prob *= 1.2
    
    # Later payments more likely to be missed
    if payment_number > 6:
        miss_prob *= 1.5
    
    # December - higher missed payments
    if random.random() < miss_prob:
        return True
    return False

# Initialize payments list
all_payments = []
payment_id_counter = 1

print("Generating realistic payment records (same columns, many rows)...")

# Track for realism
customer_personalities = {}
total_payments_generated = 0

# Generate payments for each policy
for _, policy in tqdm(df_policies.iterrows(), total=len(df_policies)):
    policy_number = policy["Policy_Number"]
    base_premium = policy["Premium_Amount"]
    payment_frequency = policy["Payment_Frequency"]
    effective_date = policy["Effective_Date"]
    expiration_date = policy["Expiration_Date"]
    customer_id = policy["Applicant_ID"]
    
    # Get customer info
    customer = df_customers[df_customers["Customer_ID"] == customer_id]
    if customer.empty:
        continue
    customer = customer.iloc[0]
    income = customer["Monthly_Income_ZAR"]
    
    # Assign payment personality if not already assigned
    if customer_id not in customer_personalities:
        customer_personalities[customer_id] = assign_payment_personality(customer)
    
    customer_personality = customer_personalities[customer_id]
    
    # Skip if policy duration is too short
    if (expiration_date - effective_date).days < 1:
        continue
    
    # Single premium policy - one payment only
    if payment_frequency == "Single Premium":
        payment_method = generate_realistic_payment_method(customer_personality, income, 0)
        payment_date = effective_date
        amount_paid = base_premium
        
        # Add some realism for single premiums
        if customer_personality == "Struggler":
            # Might pay in installments
            if random.random() < 0.3:
                # Pay partial
                amount_paid = int(base_premium * random.triangular(0.5, 0.8, 0.9))
                payment_date += timedelta(days=random.randint(0, 15))
        
        payment_record = {
            "Payment_ID": f"PAY{payment_id_counter:06d}",
            "Policy_Number": policy_number,
            "Payment_Date": payment_date,
            "Amount_Paid": amount_paid,
            "Payment_Method": payment_method
        }
        all_payments.append(payment_record)
        payment_id_counter += 1
        total_payments_generated += 1
        continue
    
    # Recurring payments - generate ALL payments for the policy duration
    # This will create many rows
    payment_number = 0
    current_date = effective_date
    previous_payment_method = None
    
    while current_date <= expiration_date:
        # Get the due date
        due_date = get_payment_due_date(
            effective_date, payment_frequency, payment_number,
            customer_personality, income
        )
        
        # If due date is after expiration, stop
        if due_date > expiration_date:
            break
        
        # Check if payment should be missed
        if should_generate_missed_payment(customer_personality, payment_number, income):
            # Skip this payment (no record for missed payments)
            payment_number += 1
            current_date = due_date + timedelta(days=1)
            continue
        
        # Generate payment method
        payment_method = generate_realistic_payment_method(customer_personality, income, payment_number)
        previous_payment_method = payment_method
        
        # Calculate realistic payment date (with lateness)
        payment_date = calculate_realistic_payment_date(
            due_date, customer_personality, payment_method, 
            due_date.month, income
        )
        
        # Ensure payment date is within reasonable bounds
        max_payment_date = expiration_date + timedelta(days=60)  # 60-day grace period
        if payment_date > max_payment_date:
            # Payment too late, treat as missed
            payment_number += 1
            current_date = due_date + timedelta(days=1)
            continue
        
        # Calculate realistic payment amount
        is_late = (payment_date - due_date).days > 7
        amount_paid = calculate_realistic_payment_amount(
            base_premium, customer_personality, payment_method, is_late
        )
        
        # Create payment record
        payment_record = {
            "Payment_ID": f"PAY{payment_id_counter:06d}",
            "Policy_Number": policy_number,
            "Payment_Date": payment_date,
            "Amount_Paid": amount_paid,
            "Payment_Method": payment_method
        }
        
        all_payments.append(payment_record)
        payment_id_counter += 1
        total_payments_generated += 1
        payment_number += 1
        
        # Move to next payment period
        if payment_frequency == "Monthly":
            # Calculate days to next month
            current_month = due_date.month
            current_year = due_date.year
            next_month = current_month + 1
            next_year = current_year
            if next_month > 12:
                next_month = 1
                next_year += 1
            days_in_month = calendar.monthrange(next_year, next_month)[1]
            current_date = due_date + timedelta(days=days_in_month)
        elif payment_frequency == "Quarterly":
            current_date += timedelta(days=91)
        elif payment_frequency == "Semi-Annually":
            current_date += timedelta(days=182)
        elif payment_frequency == "Annually":
            current_date += timedelta(days=365)
    
    # Also generate additional "catch-up" payments for problematic customers
    # This adds more rows and realism
    if customer_personality in ["Struggler", "Problematic"] and random.random() < 0.3:
        # Generate additional partial payments
        num_extra_payments = random.randint(1, 3)
        for _ in range(num_extra_payments):
            extra_date = effective_date + timedelta(days=random.randint(30, 365))
            if extra_date > expiration_date:
                break
            
            extra_amount = int(base_premium * random.triangular(0.1, 0.3, 0.5))
            extra_method = random.choice(["Cash", "EFT"])
            
            payment_record = {
                "Payment_ID": f"PAY{payment_id_counter:06d}",
                "Policy_Number": policy_number,
                "Payment_Date": extra_date,
                "Amount_Paid": extra_amount,
                "Payment_Method": extra_method
            }
            all_payments.append(payment_record)
            payment_id_counter += 1
            total_payments_generated += 1
    
    # Generate refunds/credits for some policies (adds more rows)
    if customer_personality == "High-maintenance" and random.random() < 0.1:
        # Generate a refund payment
        refund_date = effective_date + timedelta(days=random.randint(60, 180))
        if refund_date <= expiration_date:
            refund_amount = int(-base_premium * random.triangular(0.05, 0.15, 0.25))
            refund_method = random.choice(["EFT", "Credit Card Refund"])
            
            payment_record = {
                "Payment_ID": f"PAY{payment_id_counter:06d}",
                "Policy_Number": policy_number,
                "Payment_Date": refund_date,
                "Amount_Paid": refund_amount,  # Negative amount for refund
                "Payment_Method": refund_method
            }
            all_payments.append(payment_record)
            payment_id_counter += 1
            total_payments_generated += 1

# Convert to DataFrame
df_payments = pd.DataFrame(all_payments)

# Ensure correct data types
df_payments["Payment_Date"] = pd.to_datetime(df_payments["Payment_Date"])
df_payments["Amount_Paid"] = df_payments["Amount_Paid"].astype(int)

# Add some duplicate payments for realism (bank errors, double charges)
if len(df_payments) > 1000:
    num_duplicates = int(len(df_payments) * 0.01)  # 1% duplicates
    duplicate_indices = random.sample(range(len(df_payments)), num_duplicates)
    
    duplicates = []
    for idx in duplicate_indices:
        original = df_payments.iloc[idx].copy()
        # Create duplicate with slight variations
        duplicate = original.copy()
        duplicate["Payment_ID"] = f"PAY{payment_id_counter:06d}"
        duplicate["Amount_Paid"] = original["Amount_Paid"] + random.choice([-1, 0, 1])
        duplicate["Payment_Date"] = original["Payment_Date"] + timedelta(days=random.randint(0, 2))
        payment_id_counter += 1
        duplicates.append(duplicate)
    
    if duplicates:
        df_duplicates = pd.DataFrame(duplicates)
        df_payments = pd.concat([df_payments, df_duplicates], ignore_index=True)

# Shuffle the payments for realism
df_payments = df_payments.sample(frac=1, random_state=42).reset_index(drop=True)

# Save to Parquet
os.makedirs("insurance_data", exist_ok=True)
save_path = "insurance_data/payment_history.parquet"
df_payments.to_parquet(save_path, index=False)

print(f"\nSaved {len(df_payments)} payment records to {save_path}")
print(f"   (Generated {total_payments_generated} base payments + {len(df_payments) - total_payments_generated} additional/duplicate records)")

# Calculate statistics
print("\n=== PAYMENT STATISTICS ===")

# Merge with policy data for analysis
df_analysis = df_payments.merge(
    df_policies[["Policy_Number", "Premium_Amount", "Payment_Frequency"]], 
    on="Policy_Number", 
    how="left"
)

print(f"\n1. Total Payments: {len(df_payments):,}")
print(f"   Unique Policies: {df_payments['Policy_Number'].nunique():,}")
print(f"   Average payments per policy: {len(df_payments) / df_payments['Policy_Number'].nunique():.1f}")

print(f"\n2. Payment Method Distribution:")
method_counts = df_payments["Payment_Method"].value_counts()
for method, count in method_counts.items():
    pct = count / len(df_payments) * 100
    print(f"   {method}: {count:,} payments ({pct:.1f}%)")

print(f"\n3. Payment Amount Analysis:")
print(f"   Total Amount Paid: R{df_payments['Amount_Paid'].sum():,.2f}")
print(f"   Average Payment: R{df_payments['Amount_Paid'].mean():,.2f}")
print(f"   Median Payment: R{df_payments['Amount_Paid'].median():,.2f}")
print(f"   Min Payment: R{df_payments['Amount_Paid'].min():,.2f}")
print(f"   Max Payment: R{df_payments['Amount_Paid'].max():,.2f}")

# Payment vs Premium analysis
if 'Premium_Amount' in df_analysis.columns:
    df_analysis['Variance'] = df_analysis['Amount_Paid'] - df_analysis['Premium_Amount']
    df_analysis['Variance_Pct'] = (df_analysis['Variance'] / df_analysis['Premium_Amount'] * 100)
    
    print(f"\n4. Payment vs Premium Analysis:")
    print(f"   Exact payments: {(df_analysis['Variance'] == 0).sum():,} ({(df_analysis['Variance'] == 0).sum() / len(df_analysis) * 100:.1f}%)")
    print(f"   Underpayments: {(df_analysis['Variance'] < 0).sum():,} ({(df_analysis['Variance'] < 0).sum() / len(df_analysis) * 100:.1f}%)")
    print(f"   Overpayments: {(df_analysis['Variance'] > 0).sum():,} ({(df_analysis['Variance'] > 0).sum() / len(df_analysis) * 100:.1f}%)")
    print(f"   Average variance: R{df_analysis['Variance'].mean():,.2f}")
    print(f"   Average % variance: {df_analysis['Variance_Pct'].mean():.1f}%")

print(f"\n5. Payment Date Analysis:")
df_payments['Payment_Year'] = df_payments['Payment_Date'].dt.year
df_payments['Payment_Month'] = df_payments['Payment_Date'].dt.month
df_payments['Payment_Day'] = df_payments['Payment_Date'].dt.day

print(f"   Date range: {df_payments['Payment_Date'].min().strftime('%Y-%m-%d')} to {df_payments['Payment_Date'].max().strftime('%Y-%m-%d')}")
print(f"   Years covered: {sorted(df_payments['Payment_Year'].unique())}")

print(f"\n6. Top Payment Days of Month:")
day_counts = df_payments['Payment_Day'].value_counts().head(10)
print("   Day  Count    %")
for day, count in day_counts.items():
    pct = count / len(df_payments) * 100
    print(f"   {day:2d}   {count:6,}   {pct:.1f}%")

print(f"\n7. Monthly Payment Pattern:")
month_counts = df_payments['Payment_Month'].value_counts().sort_index()
print("   Month  Count    %")
for month, count in month_counts.items():
    month_name = datetime(2020, month, 1).strftime('%b')
    pct = count / len(df_payments) * 100
    print(f"   {month_name:3s}   {count:6,}   {pct:.1f}%")

print(f"\n8. Payment Frequency Impact:")
if 'Payment_Frequency' in df_analysis.columns:
    freq_stats = df_analysis.groupby('Payment_Frequency').agg({
        'Amount_Paid': ['count', 'mean', 'sum']
    }).round(2)
    freq_stats.columns = ['Count', 'Avg_Amount', 'Total_Amount']
    print(freq_stats)

# Identify refunds (negative amounts)
refunds = df_payments[df_payments['Amount_Paid'] < 0]
if len(refunds) > 0:
    print(f"\n9. Refunds Generated: {len(refunds):,}")
    print(f"   Total refund amount: R{refunds['Amount_Paid'].sum():,.2f}")
    print(f"   Average refund: R{refunds['Amount_Paid'].mean():,.2f}")

print(f"\n10. Sample Payments (first 20):")
print(df_payments.head(20).to_string())

# Generate summary report
summary_report = f"""
Payment History Summary Report
=============================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Total Payments Generated: {len(df_payments):,}
Unique Policies: {df_payments['Policy_Number'].nunique():,}
Date Range: {df_payments['Payment_Date'].min().strftime('%Y-%m-%d')} to {df_payments['Payment_Date'].max().strftime('%Y-%m-%d')}

Payment Method Distribution:
{method_counts.to_string()}

Monthly Payment Pattern:
{month_counts.to_string()}

Key Statistics:
- Total Amount: R{df_payments['Amount_Paid'].sum():,.2f}
- Average Payment: R{df_payments['Amount_Paid'].mean():,.2f}
- Median Payment: R{df_payments['Amount_Paid'].median():,.2f}
- Most Common Day: {day_counts.index[0]} (of month)
- Busiest Month: {month_counts.idxmax()} ({month_counts.max():,} payments)

Realism Features Included:
1. Income-based payment patterns
2. Customer personality types
3. Seasonal adjustments (December/January effects)
4. Payment method preferences
5. Late payments with realistic distributions
6. Under/over payments
7. Refunds and adjustments
8. Duplicate payments (bank errors)
9. Catch-up payments for struggling customers
"""

with open("insurance_data/payment_summary_report.txt", "w") as f:
    f.write(summary_report)

print(f"\n📊 Detailed summary saved to: insurance_data/payment_summary_report.txt")
print(f"\n✅ Done! Generated {len(df_payments):,} realistic payment records with the same 5 columns.")