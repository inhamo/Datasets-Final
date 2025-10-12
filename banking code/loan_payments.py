import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os
import glob
from tqdm import tqdm
import warnings

# Set the default start year
START_YEAR = 2024

def generate_loan_payment_transactions_for_year(target_year, min_year=START_YEAR):
    """
    Generate realistic loan payment transactions for a specific year, loading loans from min_year to target_year.
    All payments are automated.
    
    Parameters:
    target_year (int): The year for which to generate transactions
    min_year (int): Minimum year from which to load loans (default: START_YEAR)
    """
    
    # Initialize Faker
    fake = Faker()
    
    # File paths
    github_repo_path = 'banking_data'
    
    # Load all loan files from min_year to target_year
    all_loans = []
    for year in range(min_year, target_year + 1):
        loan_file = f'{github_repo_path}/loans_{year}.parquet'
        try:
            df = pd.read_parquet(loan_file)
            all_loans.append(df)
            print(f"Loaded loans for {year}: {len(df)} records")
        except FileNotFoundError:
            print(f"Loan file for {year} not found: {loan_file}")
    
    if not all_loans:
        print(f"No loan files found from {min_year} to {target_year}. Please generate loan data first.")
        return pd.DataFrame()
    
    # Combine all loans
    loans_df = pd.concat(all_loans, ignore_index=True)
    print(f"Total loans loaded from {min_year} to {target_year}: {len(loans_df)}")
    print(f"Available columns: {list(loans_df.columns)}")
    
    # Load loan default data (if available)
    try:
        default_file = f'{github_repo_path}/loan_defaults.parquet'
        defaults_df = pd.read_parquet(default_file)
        print(f"Loaded loan defaults: {len(defaults_df)} records")
        default_map = dict(zip(defaults_df['loan_id'], zip(defaults_df['will_default'], defaults_df['default_date'])))
    except FileNotFoundError:
        print(f"Loan defaults file not found: {default_file}. Assuming no defaults.")
        default_map = {}
    
    # Load accounts data to get transaction costs (if available)
    try:
        account_files = sorted(glob.glob(f'{github_repo_path}/accounts_*.parquet'))
        if account_files:
            accounts_list = [pd.read_parquet(f) for f in account_files]
            accounts_df = pd.concat(accounts_list, ignore_index=True).drop_duplicates(subset=['account_id'])
            if "transaction_cost" in accounts_df.columns:
                account_cost_map = dict(zip(accounts_df["account_id"], accounts_df["transaction_cost"]))
            else:
                account_cost_map = {acc: 5.0 for acc in accounts_df["account_id"]}
        else:
            account_cost_map = {}
    except Exception as e:
        print(f"Could not load accounts data: {e}")
        account_cost_map = {}
    
    # Filter approved loans only and clean data
    approved_loans = clean_loan_data(loans_df[loans_df["application_status"] == "Approved"].copy())
    print(f"Approved loans after cleaning: {len(approved_loans)}")
    
    # Convert date columns to datetime
    approved_loans["approval_date"] = pd.to_datetime(approved_loans["approval_date"])
    approved_loans["application_date"] = pd.to_datetime(approved_loans["application_date"])
    approved_loans["disbursement_date"] = pd.to_datetime(approved_loans.get("disbursement_date"))
    
    # Set all loans to automatic payment
    approved_loans['payment_automation'] = 'automatic'
    
    # Transaction channels for automatic payments
    automatic_channels = {
        'Automated': 0.85,
        'Online': 0.10,
        'Mobile': 0.05
    }
    
    # Transaction statuses for automatic payments
    auto_status_weights = [0.96, 0.03, 0.01]  # Completed, Failed, Cancelled
    transaction_statuses = ["Completed", "Failed", "Cancelled"]
    
    transactions = []
    txn_counter = 1
    
    print(f"Generating loan payment transactions for {target_year}...")
    
    # Process each approved loan
    for _, loan in tqdm(approved_loans.iterrows(), total=len(approved_loans), desc="Processing loans"):
        loan_transactions = generate_loan_payment_schedule(
            loan, default_map, account_cost_map,
            automatic_channels, auto_status_weights, 
            transaction_statuses, txn_counter, target_year, loan_id_col=None
        )
        transactions.extend(loan_transactions)
        txn_counter += len(loan_transactions)
    
    # Create DataFrame
    print(f"Creating transactions DataFrame for {target_year}...")
    transactions_df = pd.DataFrame(transactions)
    
    if len(transactions_df) == 0:
        print(f"No transactions generated for {target_year}. Please check your loan data.")
        return pd.DataFrame()
    
    # Introduce realistic data errors for cleaning exercises
    transactions_df = introduce_data_errors(transactions_df)
    
    # Save to both CSV (with errors) and clean Parquet
    csv_output_file = f'{github_repo_path}/loan_payment_transactions_{target_year}.csv'
    parquet_output_file = f'{github_repo_path}/loan_payment_transactions_{target_year}.parquet'
    
    # Save CSV with errors (for data cleaning exercises)
    transactions_df.to_csv(csv_output_file, index=False)
    
    # Create clean version and save as Parquet (for production use)
    transactions_df = pd.DataFrame(transactions)  # Original clean data
    transactions_df.to_parquet(parquet_output_file, index=False)
    
    os.makedirs(github_repo_path, exist_ok=True)
    
    # Summary statistics
    print_summary_statistics(transactions_df, target_year, csv_output_file, parquet_output_file)
    
    return transactions_df

def clean_loan_data(loans_df):
    """Clean and validate loan data before processing"""
    print("Cleaning loan data...")
    print(f"Available columns: {list(loans_df.columns)}")
    original_count = len(loans_df)
    
    # Check for common loan amount column names
    amount_columns = ['loan_amount', 'amount', 'principal_amount', 'loan_value']
    loan_amount_col = None
    for col in amount_columns:
        if col in loans_df.columns:
            loan_amount_col = col
            break
    
    if loan_amount_col:
        # Remove loans with invalid amounts
        loans_df = loans_df[loans_df[loan_amount_col] > 0]
        print(f"Using '{loan_amount_col}' as loan amount column")
    else:
        print("Warning: No loan amount column found, skipping amount validation")
    
    # Check for monthly installment column
    installment_columns = ['monthly_installment', 'installment_amount', 'monthly_payment', 'payment_amount', 'reduced_installment']
    installment_col = None
    for col in installment_columns:
        if col in loans_df.columns:
            installment_col = col
            break
    
    if installment_col:
        loans_df = loans_df[loans_df[installment_col] > 0]
        print(f"Using '{installment_col}' as monthly installment column")
    else:
        print("Warning: No monthly installment column found!")
        return pd.DataFrame()
    
    # Check for terms column
    terms_columns = ['terms_months', 'term_months', 'loan_term', 'duration_months']
    terms_col = None
    for col in terms_columns:
        if col in loans_df.columns:
            terms_col = col
            break
    
    if terms_col:
        # Remove loans with invalid terms
        loans_df = loans_df[(loans_df[terms_col] >= 1) & (loans_df[terms_col] <= 600)]  # Max 50 years
        print(f"Using '{terms_col}' as loan terms column")
    else:
        print("Warning: No loan terms column found, skipping term validation")
    
    # Remove loans with missing critical fields
    required_fields = ['loan_id', 'account_id', 'customer_id', 'approval_date', 'loan_type']
    available_required_fields = [field for field in required_fields if field in loans_df.columns]
    
    if not available_required_fields:
        print("Error: None of the required fields found in loan data")
        return pd.DataFrame()
    
    for field in available_required_fields:
        loans_df = loans_df[loans_df[field].notna()]
    
    # Remove duplicates based on available loan ID column
    id_columns = ['loan_id', 'id', 'loan_reference']
    loan_id_col = None
    for col in id_columns:
        if col in loans_df.columns:
            loan_id_col = col
            break
    
    if loan_id_col:
        loans_df = loans_df.drop_duplicates(subset=[loan_id_col])
        print(f"Using '{loan_id_col}' as loan ID column")
    
    cleaned_count = len(loans_df)
    print(f"Removed {original_count - cleaned_count} invalid loan records")
    
    return loans_df

def generate_loan_payment_schedule(loan, default_map, account_cost_map,
                                 automatic_channels, auto_status_weights, 
                                 transaction_statuses, start_txn_counter, target_year, loan_id_col=None):
    """Generate payment schedule for a single loan, only for the target year"""
    loan_transactions = []
    txn_counter = start_txn_counter
    
    # Get column names dynamically
    installment_columns = ['monthly_installment', 'installment_amount', 'monthly_payment', 'payment_amount', 'reduced_installment']
    terms_columns = ['terms_months', 'term_months', 'loan_term', 'duration_months']
    loan_id_columns = ['loan_id', 'id', 'loan_reference']
    
    # Find the correct column names
    monthly_payment_col = 'reduced_installment' if loan.get('under_debt_review', False) and 'reduced_installment' in loan.index else next((col for col in installment_columns if col in loan.index), None)
    terms_col = next((col for col in terms_columns if col in loan.index), None)
    if loan_id_col is None:
        loan_id_col = next((col for col in loan_id_columns if col in loan.index), None)
    
    if not monthly_payment_col or not terms_col or not loan_id_col:
        print(f"Warning: Missing required columns for loan {loan.get(loan_id_col, 'UNKNOWN')}")
        return []
    
    # Calculate actual payment dates based on disbursement date and payment_day
    disbursement_date = pd.to_datetime(loan.get("disbursement_date", loan["approval_date"]))
    terms_months = loan[terms_col]
    monthly_payment = loan[monthly_payment_col]
    loan_id = loan[loan_id_col]
    payment_day = loan.get("payment_day", 1)  # Default to 1st if not specified
    
    # First payment is on payment_day of the next month after disbursement
    first_payment_date = (disbursement_date + pd.DateOffset(months=1)).replace(day=payment_day)
    if first_payment_date.day != payment_day:  # Adjust for months with fewer days
        first_payment_date = first_payment_date.replace(day=min(payment_day, pd.Timestamp(first_payment_date).days_in_month))
    
    # Check if loan defaults
    will_default = False
    default_date = None
    recovery_attempts = 0
    if loan_id in default_map:
        will_default, default_date = default_map[loan_id]
        if will_default and pd.notnull(default_date):
            default_date = pd.to_datetime(default_date)
            recovery_attempts = random.randint(2, 6)  # 2-6 recovery payment attempts
    
    # Generate each monthly payment
    for month in range(int(terms_months)):
        payment_date = first_payment_date + pd.DateOffset(months=month)
        if payment_date.day != payment_day:
            payment_date = payment_date.replace(day=min(payment_day, pd.Timestamp(payment_date).days_in_month))
        
        # Only generate for target year
        if payment_date.year != target_year:
            continue
        
        # Determine if payment should occur based on default status
        should_process_payment = True
        is_recovery_attempt = False
        
        if will_default and payment_date >= default_date:
            if recovery_attempts > 0:
                if random.random() < 0.3:  # 30% chance of recovery attempt
                    should_process_payment = True
                    is_recovery_attempt = True
                    recovery_attempts -= 1
                else:
                    should_process_payment = False
            else:
                should_process_payment = False
        
        if not should_process_payment:
            continue
        
        # Generate payment variations (partial, late fees, early payments)
        payment_variations = generate_payment_variations(
            monthly_payment, payment_date, is_recovery_attempt
        )
        
        for variation in payment_variations:
            transaction = create_transaction_record(
                loan, payment_date, variation, automatic_channels,
                auto_status_weights, transaction_statuses,
                account_cost_map, txn_counter, target_year, is_recovery_attempt, loan_id_col
            )
            loan_transactions.append(transaction)
            txn_counter += 1
    
    return loan_transactions

def generate_payment_variations(monthly_payment, payment_date, is_recovery_attempt):
    """Generate variations in payment amounts (late, partial, extra)"""
    variations = []
    base_amount = round(float(monthly_payment), 2)
    
    if is_recovery_attempt:
        # Recovery payments may be partial
        recovery_amount = round(base_amount * random.gauss(0.6, 0.1), 2)
        variations.append({
            'amount': max(recovery_amount, 0),
            'type': 'recovery_payment',
            'description_suffix': '- Recovery Payment'
        })
        return variations
    
    # Normal payment variations
    rand = random.random()
    if rand < 0.92:  # 92% standard payments
        variations.append({
            'amount': base_amount,
            'type': 'standard_payment',
            'description_suffix': ''
        })
    elif rand < 0.96:  # 4% late payments
        late_fee = round(base_amount * 0.05, 2)  # 5% late fee
        variations.append({
            'amount': base_amount + late_fee,
            'type': 'late_payment',
            'description_suffix': f'- Late Payment (Fee: R{late_fee})'
        })
    elif rand < 0.98:  # 2% partial payments
        partial_amount = round(base_amount * random.gauss(0.7, 0.1), 2)
        variations.append({
            'amount': max(partial_amount, 0),
            'type': 'partial_payment',
            'description_suffix': '- Partial Payment'
        })
    else:  # 2% extra payments
        extra_amount = round(base_amount * random.gauss(1.5, 0.2), 2)
        variations.append({
            'amount': max(extra_amount, 0),
            'type': 'extra_payment',
            'description_suffix': '- Extra Principal Payment'
        })
    
    return variations

def create_transaction_record(loan, payment_date, variation, automatic_channels,
                            auto_status_weights, transaction_statuses,
                            account_cost_map, txn_counter, target_year, is_recovery_attempt, loan_id_col):
    """Create a single transaction record"""
    
    loan_id = loan[loan_id_col]
    
    # Automatic payment channel and status
    channel = np.random.choice(list(automatic_channels.keys()), p=list(automatic_channels.values()))
    status = np.random.choice(transaction_statuses, p=auto_status_weights)
    
    # Automated payments occur early morning
    if channel == 'Automated':
        txn_hour = random.choice([2, 3, 4])  # 2-4 AM processing
        txn_minute = random.randint(0, 59)
    else:
        txn_hour = random.randint(1, 6)  # Online/Mobile early but varied
        txn_minute = random.randint(0, 59)
    
    txn_second = random.randint(0, 59)
    txn_time = f"{txn_hour:02d}:{txn_minute:02d}:{txn_second:02d}"
    
    # Determine if immediate payment (higher cost)
    immediate_payment = channel in ['Online', 'Mobile'] and random.random() < 0.08
    
    # Calculate transaction cost
    account_id = loan.get("account_id", "UNKNOWN")
    base_cost = account_cost_map.get(account_id, 5.0)
    if immediate_payment:
        trans_cost = base_cost * 2  # Double cost for immediate
    else:
        trans_cost = base_cost * 0.5  # Lower cost for digital channels
    
    # Create transaction record
    transaction = {
        "transaction_id": f"TXNL{target_year}{txn_counter:07d}",
        "account_id": account_id,
        "transaction_date": payment_date.strftime("%Y-%m-%d"),
        "transaction_time": txn_time,
        "amount": variation['amount'],
        "debit_credit": "Debit",
        "status": status,
        "description": f"Loan Payment - {loan_id}{variation['description_suffix']}",
        "immediate_payment": immediate_payment,
        "receiving_account": None,
        "transaction_cost": round(trans_cost, 2),
        "ewallet_number": None,
        "channel": channel,
        "loan_id": loan_id,
        "customer_id": loan.get("customer_id", "UNKNOWN"),
        "loan_type": loan.get("loan_type", "UNKNOWN"),
        "payment_type": variation['type'],
        "is_recovery_attempt": is_recovery_attempt
    }
    
    return transaction

def introduce_data_errors(df):
    """Introduce realistic data quality issues for cleaning exercises"""
    print("Introducing data quality issues for cleaning exercises...")
    
    df_with_errors = df.copy()
    n_rows = len(df_with_errors)
    
    # Convert to object dtype for columns we'll be modifying to handle mixed types
    df_with_errors = df_with_errors.astype({
        'amount': 'object',
        'transaction_cost': 'object',
        'transaction_time': 'object',
        'transaction_date': 'object',
        'channel': 'object',
        'status': 'object'
    })
    
    # 1. Missing values (2-5% across different columns)
    missing_cols = ['transaction_time', 'description', 'channel']
    for col in missing_cols:
        if col in df_with_errors.columns:
            missing_indices = np.random.choice(n_rows, int(n_rows * 0.02), replace=False)
            df_with_errors.loc[missing_indices, col] = None
    
    # 2. Duplicate transaction IDs (1% duplicates)
    duplicate_indices = np.random.choice(n_rows, int(n_rows * 0.01), replace=False)
    for idx in duplicate_indices:
        if idx > 0:
            df_with_errors.loc[idx, 'transaction_id'] = df_with_errors.loc[idx-1, 'transaction_id']
    
    # 3. Invalid amounts (negative or zero - 0.5%)
    invalid_amount_indices = np.random.choice(n_rows, int(n_rows * 0.005), replace=False)
    for idx in invalid_amount_indices:
        original_amount = df_with_errors.loc[idx, 'amount']
        df_with_errors.loc[idx, 'amount'] = random.choice([0, -abs(float(original_amount))])
    
    # 4. Invalid dates (future dates or malformed - 0.3%)
    invalid_date_indices = np.random.choice(n_rows, int(n_rows * 0.003), replace=False)
    for idx in invalid_date_indices[:len(invalid_date_indices)//2]:
        df_with_errors.loc[idx, 'transaction_date'] = '2030-12-31'  # Future date
    for idx in invalid_date_indices[len(invalid_date_indices)//2:]:
        df_with_errors.loc[idx, 'transaction_date'] = '2024-13-45'  # Invalid date
    
    # 5. Invalid time formats (1%)
    invalid_time_indices = np.random.choice(n_rows, int(n_rows * 0.01), replace=False)
    invalid_times = ['25:30:00', '12:65:30', '12:30:70', 'Invalid Time']
    for idx in invalid_time_indices:
        df_with_errors.loc[idx, 'transaction_time'] = random.choice(invalid_times)
    
    # 6. Inconsistent channel values (0.8%)
    invalid_channel_indices = np.random.choice(n_rows, int(n_rows * 0.008), replace=False)
    invalid_channels = ['ONLINE', 'mobile', 'Automated System', '']
    for idx in invalid_channel_indices:
        df_with_errors.loc[idx, 'channel'] = random.choice(invalid_channels)
    
    # 7. Inconsistent status values (0.5%)
    invalid_status_indices = np.random.choice(n_rows, int(n_rows * 0.005), replace=False)
    invalid_statuses = ['COMPLETED', 'failed', 'Success', 'Pending', '']
    for idx in invalid_status_indices:
        df_with_errors.loc[idx, 'status'] = random.choice(invalid_statuses)
    
    # 8. Mixed data types in numeric columns
    mixed_type_indices = np.random.choice(n_rows, int(n_rows * 0.003), replace=False)
    for idx in mixed_type_indices:
        original_amount = df_with_errors.loc[idx, 'amount']
        df_with_errors.loc[idx, 'amount'] = f"R{original_amount}"
    
    # 8b. Mixed data types in transaction_cost
    mixed_cost_indices = np.random.choice(n_rows, int(n_rows * 0.002), replace=False)
    for idx in mixed_cost_indices:
        original_cost = df_with_errors.loc[idx, 'transaction_cost']
        df_with_errors.loc[idx, 'transaction_cost'] = f"${original_cost}"
    
    # 9. Whitespace issues (2%)
    whitespace_indices = np.random.choice(n_rows, int(n_rows * 0.02), replace=False)
    for idx in whitespace_indices:
        if pd.notna(df_with_errors.loc[idx, 'description']):
            df_with_errors.loc[idx, 'description'] = f"  {df_with_errors.loc[idx, 'description']}  "
    
    # 10. Invalid loan_id references (0.2%)
    invalid_loan_indices = np.random.choice(n_rows, int(n_rows * 0.002), replace=False)
    for idx in invalid_loan_indices:
        df_with_errors.loc[idx, 'loan_id'] = f"INVALID_LOAN_{random.randint(1000, 9999)}"
    
    # 11. Inconsistent boolean values (for immediate_payment)
    if 'immediate_payment' in df_with_errors.columns:
        bool_indices = np.random.choice(n_rows, int(n_rows * 0.01), replace=False)
        bool_values = ['TRUE', 'FALSE', 'Yes', 'No', '1', '0', 'true', 'false']
        for idx in bool_indices:
            df_with_errors.loc[idx, 'immediate_payment'] = random.choice(bool_values)
    
    # 12. Extra leading/trailing characters in IDs
    id_corruption_indices = np.random.choice(n_rows, int(n_rows * 0.005), replace=False)
    for idx in id_corruption_indices:
        original_id = str(df_with_errors.loc[idx, 'transaction_id'])
        df_with_errors.loc[idx, 'transaction_id'] = f" {original_id} "
    
    print(f"Introduced data quality issues in {len(df_with_errors)} transactions")
    return df_with_errors

def print_summary_statistics(transactions_df, target_year, csv_file, parquet_file):
    """Print comprehensive summary statistics"""
    print(f"\nGenerated {len(transactions_df)} loan payment transactions for {target_year}")
    print(f"Saved to:")
    print(f"  - CSV (with data quality issues): {csv_file}")
    print(f"  - Parquet (clean): {parquet_file}")
    print(f"\nTransaction Summary for {target_year}:")
    print(f"- Loans from previous years up to {target_year}")
    print(f"- Total transactions: {len(transactions_df):,}")
    
    # Calculate total amount (handle mixed data types)
    try:
        numeric_amounts = pd.to_numeric(transactions_df['amount'], errors='coerce')
        total_amount = numeric_amounts.sum()
        print(f"- Total amount (excluding corrupted values): R{total_amount:,.2f}")
    except:
        print("- Total amount: Cannot calculate due to data quality issues")
    
    print(f"\nStatus distribution:")
    print(transactions_df['status'].value_counts())
    
    print(f"\nChannel distribution:")
    print(transactions_df['channel'].value_counts())
    
    print(f"\nPayment type distribution:")
    if 'payment_type' in transactions_df.columns:
        print(transactions_df['payment_type'].value_counts())
    
    print(f"\nLoan type distribution:")
    print(transactions_df['loan_type'].value_counts().head(10))
    
    print(f"\nRecovery attempts: {len(transactions_df[transactions_df.get('is_recovery_attempt', False) == True])}")
    
    # Show data quality issues
    print(f"\n=== DATA QUALITY ISSUES INTRODUCED ===")
    print(f"Missing values:")
    missing_counts = transactions_df.isnull().sum()
    for col, count in missing_counts[missing_counts > 0].items():
        print(f"  - {col}: {count} missing values")
    
    # Check for duplicated transaction IDs
    duplicate_count = transactions_df['transaction_id'].duplicated().sum()
    print(f"Duplicate transaction IDs: {duplicate_count}")
    
    # Check for invalid amounts (non-numeric)
    try:
        invalid_amounts = pd.to_numeric(transactions_df['amount'], errors='coerce').isnull().sum()
        print(f"Invalid amount formats: {invalid_amounts}")
    except:
        pass
    
    # Show sample
    print(f"\nSample transactions (with potential data quality issues):")
    display_cols = ['transaction_id', 'transaction_date', 'amount', 'status', 'channel']
    available_cols = [col for col in display_cols if col in transactions_df.columns]
    if 'payment_type' in transactions_df.columns:
        available_cols.append('payment_type')
    
    print(transactions_df.head(10)[available_cols].to_string())

def generate_loan_payments_for_specific_year(year):
    """Generate loan payment transactions for a specific year only"""
    return generate_loan_payment_transactions_for_year(year)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate realistic loan payment transactions")
    parser.add_argument('--start_year', type=int, default=START_YEAR, help='Starting year for transaction generation')
    parser.add_argument('--end_year', type=int, default=2024, help='Ending year for transaction generation')
    
    args = parser.parse_args()

    # Validate years
    if args.start_year > args.end_year:
        print("Error: start_year cannot be greater than end_year")
        exit(1)
    
    if args.start_year < 2018:
        print("Warning: start_year is before 2018, some data dependencies might not be available")
    
    min_year = args.start_year
    for year in range(args.start_year, args.end_year + 1):
        generate_loan_payment_transactions_for_year(year, min_year=min_year)