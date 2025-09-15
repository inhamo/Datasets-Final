import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os
import glob
from tqdm import tqdm
import warnings

# Set the default start year here (change this to your desired year)
START_YEAR = 2018

def generate_loan_payment_transactions(start_year=START_YEAR, end_year=2024):
    """
    Generate realistic loan payment transactions from start_year to end_year.
    
    Parameters:
    start_year (int): Starting year for transaction generation (default: START_YEAR)
    end_year (int): Ending year for transaction generation (default: 2024)
    """
    
    # Initialize Faker
    fake = Faker()
    
    # File paths
    github_repo_path = 'banking_data'
    
    # Load all loan files for the year range
    all_loans = []
    for year in range(start_year, end_year + 1):
        loan_file = f'{github_repo_path}/loans_{year}.parquet'
        try:
            df = pd.read_parquet(loan_file)
            all_loans.append(df)
            print(f"Loaded loans for {year}: {len(df)} records")
        except FileNotFoundError:
            print(f"Loan file for {year} not found: {loan_file}")
    
    if not all_loans:
        print("No loan files found. Please generate loan data first.")
        return pd.DataFrame()
    
    # Combine all loans
    loans_df = pd.concat(all_loans, ignore_index=True)
    print(f"Total loans loaded: {len(loans_df)}")
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
    
    # Add payment automation preferences (realistic distribution)
    # Most loans start as automatic but some customers prefer manual payments
    loan_automation_types = np.random.choice(
        ['automatic', 'manual', 'mixed'], 
        size=len(approved_loans),
        p=[0.65, 0.25, 0.10]  # 65% automatic, 25% manual, 10% mixed
    )
    approved_loans['payment_automation'] = loan_automation_types
    
    # Transaction channels with realistic weights for different payment types
    automatic_channels = {
        'Automated': 0.85,
        'Online': 0.10,
        'Mobile': 0.05
    }
    
    manual_channels = {
        'Online': 0.40,
        'Mobile': 0.25,
        'Branch': 0.15,
        'ATM': 0.12,
        'Automated': 0.08  # Some manual payers still use auto-pay occasionally
    }
    
    # Transaction statuses with different weights for automatic vs manual
    auto_status_weights = [0.96, 0.03, 0.01]  # Higher success rate for automatic
    manual_status_weights = [0.88, 0.08, 0.04]  # Lower success rate for manual
    transaction_statuses = ["Completed", "Failed", "Cancelled"]
    
    transactions = []
    txn_counter = 1
    
    print("Generating loan payment transactions...")
    
    # Process each approved loan
    for _, loan in tqdm(approved_loans.iterrows(), total=len(approved_loans), desc="Processing loans"):
        loan_transactions = generate_loan_payment_schedule(
            loan, default_map, account_cost_map,
            automatic_channels, manual_channels,
            auto_status_weights, manual_status_weights, 
            transaction_statuses, txn_counter, start_year, end_year
        )
        transactions.extend(loan_transactions)
        txn_counter += len(loan_transactions)
    
    # Create DataFrame
    print("Creating transactions DataFrame...")
    transactions_df = pd.DataFrame(transactions)
    
    if len(transactions_df) == 0:
        print("No transactions generated. Please check your loan data.")
        return pd.DataFrame()
    
    # Introduce realistic data errors for cleaning exercises
    transactions_df = introduce_data_errors(transactions_df)
    
    # Save to both CSV (for mixed data types) and clean Parquet (without errors)
    csv_output_file = f'{github_repo_path}/loan_payment_transactions_{start_year}_{end_year}_with_errors.csv'
    parquet_output_file = f'{github_repo_path}/loan_payment_transactions_{start_year}_{end_year}_clean.parquet'
    
    # Save CSV with errors (for data cleaning exercises)
    transactions_df.to_csv(csv_output_file, index=False)
    
    # Create clean version and save as Parquet (for production use)
    clean_df = pd.DataFrame(transactions)  # Original clean data
    clean_df.to_parquet(parquet_output_file, index=False)
    
    os.makedirs(github_repo_path, exist_ok=True)
    
    # Summary statistics
    print_summary_statistics(transactions_df, start_year, end_year, csv_output_file, parquet_output_file)
    
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
    installment_columns = ['monthly_installment', 'installment_amount', 'monthly_payment', 'payment_amount']
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
        return pd.DataFrame()  # Return empty if no installment column
    
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
                                 automatic_channels, manual_channels,
                                 auto_status_weights, manual_status_weights,
                                 transaction_statuses, start_txn_counter, start_year, end_year):
    """Generate payment schedule for a single loan"""
    loan_transactions = []
    txn_counter = start_txn_counter
    
    # Get column names dynamically
    installment_columns = ['monthly_installment', 'installment_amount', 'monthly_payment', 'payment_amount']
    terms_columns = ['terms_months', 'term_months', 'loan_term', 'duration_months']
    loan_id_columns = ['loan_id', 'id', 'loan_reference']
    
    # Find the correct column names
    monthly_payment_col = next((col for col in installment_columns if col in loan.index), None)
    terms_col = next((col for col in terms_columns if col in loan.index), None)
    loan_id_col = next((col for col in loan_id_columns if col in loan.index), None)
    
    if not monthly_payment_col or not terms_col or not loan_id_col:
        print(f"Warning: Missing required columns for loan processing")
        return []
    
    # Calculate actual payment dates based on approval date
    approval_date = loan["approval_date"]
    terms_months = loan[terms_col]
    monthly_payment = loan[monthly_payment_col]
    loan_id = loan[loan_id_col]
    payment_automation = loan.get('payment_automation', 'automatic')
    
    # First payment is typically 30 days after approval
    first_payment_date = approval_date + timedelta(days=30)
    # Adjust to first of month for simplicity
    first_payment_date = first_payment_date.replace(day=1)
    
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
        
        # Skip if outside our generation range
        if payment_date.year < start_year or payment_date.year > end_year:
            continue
        
        # Determine if payment should occur based on default status
        should_process_payment = True
        is_recovery_attempt = False
        
        if will_default and payment_date >= default_date:
            # After default, some recovery attempts may occur
            if recovery_attempts > 0:
                # Recovery attempts happen sporadically
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
        
        # Generate payment variations (partial, late fees, early payments, etc.)
        payment_variations = generate_payment_variations(
            monthly_payment, payment_date, is_recovery_attempt, payment_automation
        )
        
        for variation in payment_variations:
            transaction = create_transaction_record(
                loan, payment_date, variation, automatic_channels, manual_channels,
                auto_status_weights, manual_status_weights, transaction_statuses,
                account_cost_map, txn_counter, is_recovery_attempt, loan_id_col
            )
            loan_transactions.append(transaction)
            txn_counter += 1
    
    return loan_transactions

def generate_payment_variations(base_amount, payment_date, is_recovery_attempt, payment_automation):
    """Generate realistic payment variations"""
    variations = []
    
    if is_recovery_attempt:
        # Recovery attempts often partial payments
        if random.random() < 0.6:  # 60% partial payments during recovery
            partial_amount = round(base_amount * random.uniform(0.2, 0.8), 2)
            variations.append({
                'amount': partial_amount,
                'type': 'partial_payment',
                'description_suffix': '- Partial Payment'
            })
        else:
            variations.append({
                'amount': base_amount,
                'type': 'regular_payment',
                'description_suffix': '- Recovery Payment'
            })
    else:
        # Regular payment scenarios
        rand = random.random()
        
        if rand < 0.85:  # 85% regular payments
            variations.append({
                'amount': base_amount,
                'type': 'regular_payment',
                'description_suffix': ''
            })
        elif rand < 0.92:  # 7% late payments with fees
            late_fee = round(base_amount * random.uniform(0.02, 0.05), 2)  # 2-5% late fee
            variations.append({
                'amount': base_amount + late_fee,
                'type': 'late_payment',
                'description_suffix': f'- Late Payment (Fee: R{late_fee})'
            })
        elif rand < 0.96:  # 4% partial payments
            partial_amount = round(base_amount * random.uniform(0.5, 0.95), 2)
            variations.append({
                'amount': partial_amount,
                'type': 'partial_payment',
                'description_suffix': '- Partial Payment'
            })
        else:  # 4% early/extra payments
            extra_amount = round(base_amount * random.uniform(1.1, 2.0), 2)
            variations.append({
                'amount': extra_amount,
                'type': 'extra_payment',
                'description_suffix': '- Extra Principal Payment'
            })
    
    return variations

def create_transaction_record(loan, payment_date, variation, automatic_channels, manual_channels,
                            auto_status_weights, manual_status_weights, transaction_statuses,
                            account_cost_map, txn_counter, is_recovery_attempt, loan_id_col):
    """Create a single transaction record"""
    
    payment_automation = loan.get('payment_automation', 'automatic')
    loan_id = loan[loan_id_col]
    
    # Determine channel and timing based on automation type
    if payment_automation == 'automatic' or (payment_automation == 'mixed' and random.random() < 0.7):
        # Automatic payments
        channel = np.random.choice(list(automatic_channels.keys()), p=list(automatic_channels.values()))
        status = np.random.choice(transaction_statuses, p=auto_status_weights)
        
        if channel == 'Automated':
            # Automated payments occur at consistent times (early morning)
            txn_hour = random.choice([2, 3, 4])  # 2-4 AM processing
            txn_minute = random.randint(0, 59)
        else:
            # Online/Mobile automatic still early but more varied
            txn_hour = random.randint(1, 6)
            txn_minute = random.randint(0, 59)
    else:
        # Manual payments
        channel = np.random.choice(list(manual_channels.keys()), p=list(manual_channels.values()))
        status = np.random.choice(transaction_statuses, p=manual_status_weights)
        
        # Manual payments during business hours or evening
        if channel == 'Branch':
            txn_hour = random.randint(9, 16)  # Branch hours
        elif channel == 'ATM':
            txn_hour = random.randint(8, 22)  # Extended ATM hours
        else:  # Online/Mobile
            txn_hour = random.randint(7, 23)  # Most flexible timing
        
        txn_minute = random.randint(0, 59)
    
    txn_second = random.randint(0, 59)
    txn_time = f"{txn_hour:02d}:{txn_minute:02d}:{txn_second:02d}"
    
    # Determine if immediate payment (higher cost)
    immediate_payment = False
    if channel in ['Online', 'Mobile'] and random.random() < 0.08:  # 8% immediate
        immediate_payment = True
    
    # Calculate transaction cost
    account_id = loan.get("account_id", "UNKNOWN")
    base_cost = account_cost_map.get(account_id, 5.0)
    if immediate_payment:
        trans_cost = base_cost * 2  # Double cost for immediate
    elif channel == 'Branch':
        trans_cost = base_cost * 1.5  # Higher cost for branch
    elif channel == 'ATM':
        trans_cost = base_cost
    else:
        trans_cost = base_cost * 0.5  # Lower cost for digital channels
    
    # Create transaction record
    transaction = {
        "transaction_id": f"TXNL{txn_counter:08d}",
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
    invalid_channels = ['ONLINE', 'mobile', 'Branch Office', 'atm', '']
    for idx in invalid_channel_indices:
        df_with_errors.loc[idx, 'channel'] = random.choice(invalid_channels)
    
    # 7. Inconsistent status values (0.5%)
    invalid_status_indices = np.random.choice(n_rows, int(n_rows * 0.005), replace=False)
    invalid_statuses = ['COMPLETED', 'failed', 'Success', 'Pending', '']
    for idx in invalid_status_indices:
        df_with_errors.loc[idx, 'status'] = random.choice(invalid_statuses)
    
    # 8. Mixed data types in numeric columns (create string representations)
    mixed_type_indices = np.random.choice(n_rows, int(n_rows * 0.003), replace=False)
    for idx in mixed_type_indices:
        original_amount = df_with_errors.loc[idx, 'amount']
        df_with_errors.loc[idx, 'amount'] = f"R{original_amount}"
    
    # 8b. Mixed data types in transaction_cost column
    mixed_cost_indices = np.random.choice(n_rows, int(n_rows * 0.002), replace=False)
    for idx in mixed_cost_indices:
        original_cost = df_with_errors.loc[idx, 'transaction_cost']
        df_with_errors.loc[idx, 'transaction_cost'] = f"${original_cost}"
    
    # 9. Whitespace issues (2%)
    whitespace_indices = np.random.choice(n_rows, int(n_rows * 0.02), replace=False)
    for idx in whitespace_indices:
        if pd.notna(df_with_errors.loc[idx, 'description']):
            df_with_errors.loc[idx, 'description'] = f"  {df_with_errors.loc[idx, 'description']}  "
    
    # 10. Invalid loan_id references (0.2% - reference non-existent loans)
    invalid_loan_indices = np.random.choice(n_rows, int(n_rows * 0.002), replace=False)
    for idx in invalid_loan_indices:
        df_with_errors.loc[idx, 'loan_id'] = f"INVALID_LOAN_{random.randint(1000, 9999)}"
    
    # 11. Inconsistent boolean values (for immediate_payment column)
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

def print_summary_statistics(transactions_df, start_year, end_year, csv_file, parquet_file):
    """Print comprehensive summary statistics"""
    print(f"\nGenerated {len(transactions_df)} loan payment transactions")
    print(f"Saved to:")
    print(f"  - CSV (with data quality issues): {csv_file}")
    print(f"  - Parquet (clean): {parquet_file}")
    print(f"\nTransaction Summary:")
    print(f"- Date range: {start_year} to {end_year}")
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
    return generate_loan_payment_transactions(year, year)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate realistic loan payment transactions")
    parser.add_argument('--start_year', type=int, default=START_YEAR, help='Starting year for transaction generation')
    parser.add_argument('--end_year', type=int, default=2024, help='Ending year for transaction generation (default: 2024)')
    
    args = parser.parse_args()
    
    # Validate years
    if args.start_year > args.end_year:
        print("Error: start_year cannot be greater than end_year")
        exit(1)
    
    if args.start_year < 2018:
        print("Warning: start_year is before 2018, some data dependencies might not be available")
    
    generate_loan_payment_transactions(args.start_year, args.end_year)