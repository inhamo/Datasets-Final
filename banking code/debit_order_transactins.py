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

def generate_debit_order_transactions_for_year(target_year, min_year=START_YEAR):
    """
    Generate transactions for debit orders for a specific year, loading debit orders from min_year to target_year.
    
    Parameters:
    target_year (int): The year for which to generate transactions
    min_year (int): Minimum year from which to load debit orders (default: START_YEAR)
    """
    
    # Initialize Faker
    fake = Faker()
    
    # File paths
    github_repo_path = 'banking_data'
    
    # Load all debit orders files from min_year to target_year
    all_debit_orders = []
    for year in range(min_year, target_year + 1):
        debit_file = f'{github_repo_path}/debit_orders_{year}.parquet'
        try:
            df = pd.read_parquet(debit_file)
            all_debit_orders.append(df)
            print(f"Loaded debit orders for {year}: {len(df)} records")
        except FileNotFoundError:
            print(f"Debit orders file for {year} not found: {debit_file}")
    
    if not all_debit_orders:
        print(f"No debit order files found from {min_year} to {target_year}. Please generate debit orders first.")
        return pd.DataFrame()
    
    # Combine all debit orders
    debit_orders_df = pd.concat(all_debit_orders, ignore_index=True)
    print(f"Total debit orders loaded from {min_year} to {target_year}: {len(debit_orders_df)}")
    
    # Load accounts data to get transaction costs (if available)
    try:
        account_files = sorted(glob.glob(f'{github_repo_path}/accounts_*.parquet'))
        if account_files:
            accounts_list = [pd.read_parquet(f) for f in account_files]
            accounts_df = pd.concat(accounts_list, ignore_index=True).drop_duplicates(subset=['account_id'])
            
            # Create transaction cost mapping (default to 5.0 if not available)
            if "transaction_cost" in accounts_df.columns:
                account_cost_map = dict(zip(accounts_df["account_id"], accounts_df["transaction_cost"]))
            else:
                account_cost_map = {acc: 5.0 for acc in accounts_df["account_id"]}
        else:
            account_cost_map = {}
    except Exception as e:
        print(f"Could not load accounts data: {e}")
        account_cost_map = {}
    
    # Filter active debit orders only
    active_debit_orders = debit_orders_df[debit_orders_df["status"] == "Active"].copy()
    print(f"Active debit orders: {len(active_debit_orders)}")
    
    # Convert date columns to datetime, handling missing columns
    active_debit_orders["start_date"] = pd.to_datetime(active_debit_orders["start_date"])
    if "end_date" in active_debit_orders.columns:
        active_debit_orders["end_date"] = pd.to_datetime(active_debit_orders["end_date"])
    else:
        active_debit_orders["end_date"] = pd.NaT
    if "cancellation_date" in active_debit_orders.columns:
        active_debit_orders["cancellation_date"] = pd.to_datetime(active_debit_orders["cancellation_date"])
    else:
        active_debit_orders["cancellation_date"] = pd.NaT
    
    # Transaction channels for debit orders
    channels = ["Online", "Mobile", "ATM", "Branch", "Automated"]
    
    # Transaction statuses with weights
    transaction_statuses = ["Completed", "Failed", "Cancelled"]
    status_weights = [0.92, 0.06, 0.02]  # Most debit orders complete successfully
    
    transactions = []
    txn_counter = 1
    
    # Generate date range for the specific year only
    start_date = datetime(target_year, 1, 1)
    end_date = datetime(target_year, 12, 31)
    date_range = pd.date_range(start_date, end_date)
    
    def debit_order_occurs_on(debit_order, date):
        """Check if a debit order should occur on a specific date"""
        freq = debit_order["frequency"]
        start = debit_order["start_date"]
        end = debit_order["end_date"] if pd.notnull(debit_order["end_date"]) else pd.Timestamp("2025-12-31")
        
        # Check if date is within the active period
        if not (start <= date <= end):
            return False
            
        # Check if order was cancelled before this date
        if pd.notnull(debit_order["cancellation_date"]) and date >= debit_order["cancellation_date"]:
            return False
        
        # Check frequency patterns
        if freq == "Monthly":
            # Monthly on the same day of month as start date
            return date.day == start.day or (start.day > 28 and date.day == min(28, date.days_in_month))
        elif freq == "Weekly":
            # Weekly on the same weekday
            return date.weekday() == start.weekday()
        elif freq == "Quarterly":
            # Quarterly (every 3 months on same day)
            months_diff = (date.year - start.year) * 12 + (date.month - start.month)
            return months_diff % 3 == 0 and (date.day == start.day or (start.day > 28 and date.day == min(28, date.days_in_month)))
        elif freq == "Annually":
            # Annually on same date
            return date.month == start.month and (date.day == start.day or (start.day > 28 and date.day == min(28, date.days_in_month)))
        else:
            return False
    
    print(f"Generating debit order transactions for {target_year}...")
    
    # Process each date in the year
    for single_date in tqdm(date_range, desc="Processing dates"):
        day_transactions = []
        
        # Check each active debit order
        for _, debit_order in active_debit_orders.iterrows():
            if debit_order_occurs_on(debit_order, single_date):
                
                # Determine transaction status
                status = np.random.choice(transaction_statuses, p=status_weights)
                
                # Generate transaction time (debit orders typically process early morning)
                if random.random() < 0.7:  # 70% process between 06:00-09:00
                    txn_hour = random.randint(6, 8)
                else:  # 30% process throughout business hours
                    txn_hour = random.randint(9, 17)
                txn_minute = random.randint(0, 59)
                txn_second = random.randint(0, 59)
                txn_time = f"{txn_hour:02d}:{txn_minute:02d}:{txn_second:02d}"
                
                # Determine if it's an immediate payment (rarely for debit orders)
                immediate_payment = random.random() < 0.05  # 5% chance
                
                # Calculate transaction cost
                trans_cost = account_cost_map.get(debit_order["account_id"], 5.0) if immediate_payment else 0.0
                
                # Determine receiving bank (empty if internal account)
                receiving_bank = ""
                if not pd.isna(debit_order["account_to"]) and len(str(debit_order["account_to"])) > 20:
                    # External account (IBAN-like format)
                    receiving_bank = "External Bank"
                
                # EWallet number (only for certain transaction types)
                ewallet_number = None
                if "ewallet" in str(debit_order["description"]).lower() or "mobile" in str(debit_order["description"]).lower():
                    ewallet_number = f"27{random.randint(600000000, 899999999)}"  # SA mobile format
                
                # Select channel (debit orders are typically automated)
                channel_weights = [0.05, 0.10, 0.02, 0.03, 0.80]  # Heavily weighted towards "Automated"
                channel = np.random.choice(channels, p=channel_weights)
                
                # Create transaction record
                transaction = {
                    "transaction_id": f"TXN{target_year}{txn_counter:07d}",
                    "account_id": debit_order["account_id"],
                    "transaction_date": single_date.strftime("%Y-%m-%d"),
                    "transaction_time": txn_time,
                    "amount": debit_order["amount"],
                    "debit_credit": "Debit",  # Debit orders are always debits
                    "status": status,
                    "description": debit_order["description"] if pd.notnull(debit_order["description"]) else f"{debit_order['debit_order_type']} - {debit_order['debit_order_id']}",
                    "immediate_payment": immediate_payment,
                    "receiving_account": debit_order["account_to"],
                    "transaction_cost": trans_cost,
                    "ewallet_number": ewallet_number,
                    "channel": channel,
                    # Additional debit order specific fields
                    "debit_order_id": debit_order["debit_order_id"],
                    "debit_order_type": debit_order["debit_order_type"],
                    "customer_id": debit_order["customer_id"]
                }
                
                day_transactions.append(transaction)
                txn_counter += 1
        
        # Add all transactions for this day
        transactions.extend(day_transactions)
    
    # Create DataFrame
    print(f"Creating transactions DataFrame for {target_year}...")
    transactions_df = pd.DataFrame(transactions)
    
    if len(transactions_df) == 0:
        print(f"No transactions generated for {target_year}. Please check your debit orders data.")
        return pd.DataFrame()
    
    # Save to file per year
    output_file = f'{github_repo_path}/debit_order_transactions_{target_year}.parquet'
    os.makedirs(github_repo_path, exist_ok=True)
    transactions_df.to_parquet(output_file, index=False)
    
    # Summary statistics
    print(f"\nGenerated {len(transactions_df)} debit order transactions for {target_year}")
    print(f"Saved to: {output_file}")
    print(f"\nTransaction Summary for {target_year}:")
    print(f"- Debit orders from: {min_year} to {target_year}")
    print(f"- Total transactions: {len(transactions_df):,}")
    print(f"- Total amount: R{transactions_df['amount'].sum():,.2f}")
    print(f"\nStatus distribution:")
    print(transactions_df['status'].value_counts())
    print(f"\nTransaction type distribution:")
    print(transactions_df['debit_order_type'].value_counts().head(10))
    
    # Show sample
    print(f"\nSample transactions for {target_year}:")
    print(transactions_df.head(10).to_string())
    
    return transactions_df


def generate_transactions_for_specific_year(year):
    """Generate transactions for a specific year only"""
    return generate_debit_order_transactions_for_year(year)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate debit order transactions")
    parser.add_argument('--start_year', type=int, default=START_YEAR, help='Starting year for transaction generation')
    parser.add_argument('--end_year', type=int, default=2024, help='Ending year for transaction generation (default: 2024)')
    
    args = parser.parse_args()
    
    # Validate years
    if args.start_year > args.end_year:
        print("Error: start_year cannot be greater than end_year")
        exit(1)
    
    if args.start_year < 2018:
        print("Warning: start_year is before 2018, some data dependencies might not be available")
    
    min_year = args.start_year
    for year in range(args.start_year, args.end_year + 1):
        generate_debit_order_transactions_for_year(year, min_year=min_year)