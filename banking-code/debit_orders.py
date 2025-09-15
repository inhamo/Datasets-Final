```python
import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os
import glob
from tqdm import tqdm

# Set the default start year here (change this to your desired year)
START_YEAR = 2018

def generate_loan_payment_transactions(start_year=START_YEAR, end_year=2024):
    """
    Generate loan payment transactions from start_year to end_year.
    
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
    
    # Filter approved loans only
    approved_loans = loans_df[loans_df["application_status"] == "Approved"].copy()
    print(f"Approved loans: {len(approved_loans)}")
    
    # Convert date columns to datetime
    approved_loans["approval_date"] = pd.to_datetime(approved_loans["approval_date"])
    approved_loans["application_date"] = pd.to_datetime(approved_loans["application_date"])
    
    # Transaction channels
    channels = ["Online", "Mobile", "ATM", "Branch", "Automated"]
    
    # Transaction statuses with weights
    transaction_statuses = ["Completed", "Failed", "Cancelled"]
    status_weights = [0.92, 0.06, 0.02]  # Most payments complete successfully
    
    transactions = []
    txn_counter = 1
    
    # Generate date range for all years
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    date_range = pd.date_range(start_date, end_date, freq='MS')  # Monthly start dates
    
    def loan_payment_occurs_on(loan, date):
        """Check if a loan payment should occur on a specific date"""
        approval_date = loan["approval_date"]
        terms_months = loan["terms_months"]
        loan_id = loan["loan_id"]
        
        # Calculate payment date (first of each month after approval)
        months_since_approval = (date.year - approval_date.year) * 12 + date.month - approval_date.month
        if months_since_approval < 0 or months_since_approval >= terms_months:
            return False
        
        # Check if loan defaulted
        if loan_id in default_map:
            will_default, default_date = default_map[loan_id]
            if will_default and pd.notnull(default_date):
                default_date = pd.to_datetime(default_date)
                if date >= default_date:
                    return False
        
        # Payment occurs on the first of the month
        return date.day == 1
    
    print("Generating loan payment transactions...")
    
    # Process each date in the range
    for single_date in tqdm(date_range, desc="Processing dates"):
        day_transactions = []
        
        # Check each approved loan
        for _, loan in approved_loans.iterrows():
            if loan_payment_occurs_on(loan, single_date):
                
                # Determine transaction status
                status = np.random.choice(transaction_statuses, p=status_weights)
                
                # Generate transaction time (typically early morning)
                if random.random() < 0.7:  # 70% process between 06:00-09:00
                    txn_hour = random.randint(6, 8)
                else:  # 30% process throughout business hours
                    txn_hour = random.randint(9, 17)
                txn_minute = random.randint(0, 59)
                txn_second = random.randint(0, 59)
                txn_time = f"{txn_hour:02d}:{txn_minute:02d}:{txn_second:02d}"
                
                # Determine if it's an immediate payment (rare)
                immediate_payment = random.random() < 0.05  # 5% chance
                
                # Calculate transaction cost
                trans_cost = account_cost_map.get(loan["account_id"], 5.0) if immediate_payment else 0.0
                
                # Create transaction record
                transaction = {
                    "transaction_id": f"TXN{txn_counter:08d}",
                    "account_id": loan["account_id"],
                    "transaction_date": single_date.strftime("%Y-%m-%d"),
                    "transaction_time": txn_time,
                    "amount": loan["monthly_installment"],
                    "debit_credit": "Debit",  # Loan payments are debits
                    "status": status,
                    "description": f"Loan Payment - {loan['loan_id']}",
                    "immediate_payment": immediate_payment,
                    "receiving_account": None,  # Loan payments typically go to the bank
                    "transaction_cost": trans_cost,
                    "ewallet_number": None,  # Not applicable for loan payments
                    "channel": np.random.choice(channels, p=[0.05, 0.10, 0.02, 0.03, 0.80]),  # Heavily weighted to Automated
                    "loan_id": loan["loan_id"],
                    "customer_id": loan["customer_id"],
                    "loan_type": loan["loan_type"]
                }
                
                day_transactions.append(transaction)
                txn_counter += 1
        
        # Add all transactions for this day
        transactions.extend(day_transactions)
    
    # Create DataFrame
    print("Creating transactions DataFrame...")
    transactions_df = pd.DataFrame(transactions)
    
    if len(transactions_df) == 0:
        print("No transactions generated. Please check your loan data.")
        return pd.DataFrame()
    
    # Save to file
    output_file = f'{github_repo_path}/loan_payment_transactions_{start_year}_{end_year}.parquet'
    os.makedirs(github_repo_path, exist_ok=True)
    transactions_df.to_parquet(output_file, index=False)
    
    # Summary statistics
    print(f"\nGenerated {len(transactions_df)} loan payment transactions")
    print(f"Saved to: {output_file}")
    print(f"\nTransaction Summary:")
    print(f"- Date range: {start_year} to {end_year}")
    print(f"- Total transactions: {len(transactions_df):,}")
    print(f"- Total amount: R{transactions_df['amount'].sum():,.2f}")
    print(f"\nStatus distribution:")
    print(transactions_df['status'].value_counts())
    print(f"\nTransaction type distribution:")
    print(transactions_df['loan_type'].value_counts().head(10))
    
    # Show sample
    print(f"\nSample transactions:")
    print(transactions_df.head(10).to_string())
    
    return transactions_df


def generate_loan_payments_for_specific_year(year):
    """Generate loan payment transactions for a specific year only"""
    return generate_loan_payment_transactions(year, year)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate loan payment transactions")
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
```

### How to Use
1. **Set the Start Year**:
   - The script is already set to start from 2018 (`START_YEAR = 2018`).
   - To change it, edit the `START_YEAR` variable at the top of the script:
     ```python
     START_YEAR = 2018  # Already set to 2018
     ```

2. **Run the Script**:
   - Save the script as `generate_loan_payment_transactions.py`.
   - Ensure the `banking_data` directory contains:
     - `loans_<year>.parquet` files for 2018 to 2024 (or the specified range).
     - `loan_defaults.parquet` (optional, for default information).
     - `accounts_*.parquet` (optional, for transaction costs).
   - Install dependencies: `pip install pandas numpy faker tqdm`.
   - Run from the terminal:
     ```bash
     python generate_loan_payment_transactions.py
     ```
     This generates transactions from 2018 to 2024.

3. **Override via Command Line**:
   - To use a different start year or range without editing the script:
     ```bash
     python generate_loan_payment_transactions.py --start_year 2018 --end_year 2022
     ```

4. **Run in Python Environment**:
   - Call the function directly:
     ```python
     from generate_loan_payment_transactions import generate_loan_payment_transactions
     transactions_df = generate_loan_payment_transactions(start_year=2018)
     ```

### Key Features
- **Monthly Payments**: Generates transactions on the 1st of each month based on `approval_date` and `terms_months`.
- **Default Handling**: Stops payments if a loan defaults (based on `default_date` from `loan_defaults.parquet`).
- **Transaction Details**: Includes `transaction_id`, `account_id`, `customer_id`, `loan_id`, `loan_type`, `amount` (from `monthly_installment`), `status`, etc.
- **Output**: Saves to `banking_data/loan_payment_transactions_2018_2024.parquet` and prints summary statistics (total transactions, amount, status distribution, etc.).
- **Error Handling**: Checks for missing files and filters only approved loans.

### Notes
- **Missing Files**: If `loans_<year>.parquet` or `loan_defaults.parquet` are missing, the script will notify you and may return an empty DataFrame.
- **Data Schema**: Ensure the loan data matches the provided schema (e.g., contains `monthly_installment`, `terms_months`, `approval_date`).
- **Performance**: Uses `tqdm` for progress tracking and processes only the 1st of each month (`freq='MS'`) to optimize speed compared to daily checks.

If you need help creating sample input files or encounter issues running the script, let me know!