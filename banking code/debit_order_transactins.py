import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os
import glob
from tqdm import tqdm
from pandas.tseries.holiday import Holiday, AbstractHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

# Define South African public holidays dynamically
class SouthAfricanCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('New Year\'s Day', month=1, day=1),
        Holiday('Human Rights Day', month=3, day=21),
        Holiday('Good Friday', month=4, day=19, year=2019),  # Varies, hardcoded for 2019
        Holiday('Family Day', month=4, day=22, year=2019),   # Varies, hardcoded for 2019
        Holiday('Freedom Day', month=4, day=27),
        Holiday('Workers\' Day', month=5, day=1),
        Holiday('Youth Day', month=6, day=16),
        Holiday('National Women\'s Day', month=8, day=9),
        Holiday('Heritage Day', month=9, day=24),
        Holiday('Day of Reconciliation', month=12, day=16),
        Holiday('Christmas Day', month=12, day=25),
        Holiday('Day of Goodwill', month=12, day=26)
    ]

# Set the default start year
START_YEAR = 2024

def generate_debit_order_transactions_for_year(target_year, min_year=START_YEAR):
    """
    Generate transactions for debit orders for a specific year, loading debit orders from min_year to target_year.
    
    Parameters:
    target_year (int): The year for which to generate transactions
    min_year (int): Minimum year from which to load debit orders (default: START_YEAR)
    """
    
    # Initialize Faker
    fake = Faker()
    Faker.seed(target_year)  # Seed for reproducibility
    
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
    
    # Load accounts data to get transaction costs and status
    try:
        account_files = sorted(glob.glob(f'{github_repo_path}/accounts_*.parquet'))
        if account_files:
            accounts_list = [pd.read_parquet(f) for f in account_files]
            accounts_df = pd.concat(accounts_list, ignore_index=True).drop_duplicates(subset=['account_id'])
            account_cost_map = dict(zip(accounts_df["account_id"], accounts_df.get("transaction_cost", 5.0)))
            account_status_map = dict(zip(accounts_df["account_id"], accounts_df.get("status", "active")))
        else:
            account_cost_map = {}
            account_status_map = {}
    except Exception as e:
        print(f"Could not load accounts data: {e}")
        account_cost_map = {}
        account_status_map = {}
    
    # Filter active debit orders only
    active_debit_orders = debit_orders_df[debit_orders_df["status"] == "Active"].copy()
    print(f"Active debit orders: {len(active_debit_orders)}")
    
    # Convert date columns to datetime, handling missing columns
    active_debit_orders["start_date"] = pd.to_datetime(active_debit_orders["start_date"])
    active_debit_orders["end_date"] = pd.to_datetime(active_debit_orders.get("end_date", pd.NaT))
    active_debit_orders["cancellation_date"] = pd.to_datetime(active_debit_orders.get("cancellation_date", pd.NaT))
    active_debit_orders["suspension_date"] = pd.to_datetime(active_debit_orders.get("suspension_date", pd.NaT))
    
    # Transaction statuses with weights
    transaction_statuses = ["Completed", "Failed", "Cancelled"]
    status_weights_base = [0.92, 0.06, 0.02]
    
    # SA bank names for external transfers
    sa_banks = ['Standard Bank', 'FNB', 'ABSA', 'Nedbank', 'Capitec', 'TymeBank', 'African Bank']
    
    transactions = []
    txn_counter = 1
    
    # Generate business day calendar for the target year
    sa_calendar = SouthAfricanCalendar()
    bday = CustomBusinessDay(calendar=sa_calendar)
    date_range = pd.date_range(start=f"{target_year}-01-01", end=f"{target_year}-12-31", freq=bday)
    
    def debit_order_occurs_on(debit_order, date):
        """Check if a debit order should occur on a specific date"""
        freq = debit_order["frequency"]
        start = debit_order["start_date"]
        end = debit_order["end_date"] if pd.notnull(debit_order["end_date"]) else pd.Timestamp("2025-12-31")
        suspension_date = debit_order["suspension_date"] if pd.notnull(debit_order["suspension_date"]) else pd.NaT
        
        # Check if date is within the active period
        if not (start <= date <= end):
            return False
            
        # Check if order was cancelled or suspended before this date
        if pd.notnull(debit_order["cancellation_date"]) and date >= debit_order["cancellation_date"]:
            return False
        if pd.notnull(suspension_date) and date >= suspension_date:
            return False
        
        # Special case for School Fees (Jan, Apr, Jul, Oct)
        if debit_order["debit_order_type"] == "School Fees":
            return date.month in [1, 4, 7, 10] and (date.day == start.day or (start.day > 28 and date.day == min(28, date.days_in_month)))
        
        # Special case for Insurance Premium (annual in specific month)
        if debit_order["debit_order_type"] == "Insurance Premium" and freq == "Annually":
            return date.month == start.month and (date.day == start.day or (start.day > 28 and date.day == min(28, date.days_in_month)))
        
        # Standard frequency patterns
        if freq == "Monthly":
            return date.day == start.day or (start.day > 28 and date.day == min(28, date.days_in_month))
        elif freq == "Weekly":
            return date.weekday() == start.weekday()
        elif freq == "Quarterly":
            months_diff = (date.year - start.year) * 12 + (date.month - start.month)
            return months_diff % 3 == 0 and (date.day == start.day or (start.day > 28 and date.day == min(28, date.days_in_month)))
        elif freq == "Annually":
            return date.month == start.month and (date.day == start.day or (start.day > 28 and date.day == min(28, date.days_in_month)))
        return False
    
    print(f"Generating debit order transactions for {target_year}...")
    
    # Process each business day in the year
    for single_date in tqdm(date_range, desc="Processing dates"):
        day_transactions = []
        
        for _, debit_order in active_debit_orders.iterrows():
            if debit_order_occurs_on(debit_order, single_date):
                # Check account status
                account_id = debit_order["account_id"]
                account_status = account_status_map.get(account_id, "active")
                if account_status in ["closed", "frozen"]:
                    status = "Failed"
                    description = f"{debit_order['description']} - Failed: account_not_operational"
                    amount = 0.0
                else:
                    # Determine transaction status
                    status_weights = status_weights_base.copy()
                    if debit_order.get("suspension_reason") == "insufficient_funds":
                        status_weights = [0.7, 0.25, 0.05]
                    status = np.random.choice(transaction_statuses, p=status_weights)
                    description = debit_order["description"]
                    
                    # Adjust amount for utilities if not fixed
                    amount = debit_order["amount"]
                    is_fixed_amount = debit_order.get("is_fixed_amount", True)
                    if debit_order["debit_order_type"] == "Utility Bill" and not is_fixed_amount:
                        amount = round(amount * random.uniform(0.8, 1.2), 2)
                
                # Determine transaction time based on type
                if debit_order["debit_order_type"] in ["Salary Payment", "Payroll"]:
                    txn_hour, txn_minute, txn_second = 6, 0, 0  # 06:00:00
                elif debit_order["debit_order_type"] in ["Loan Repayment", "Business Loan Repayment"]:
                    if random.random() < 0.95:
                        txn_hour = random.randint(6, 7)
                        txn_minute = random.randint(0, 59)
                        txn_second = random.randint(0, 59)
                    else:
                        txn_hour = random.randint(8, 17)
                        txn_minute = random.randint(0, 59)
                        txn_second = random.randint(0, 59)
                elif debit_order["debit_order_type"] == "Utility Bill":
                    time_peaks = [(8, 0.4), (12, 0.35), (16, 0.25)]  # Peaks at 08:00, 12:00, 16:00, summing to 1
                    peak_probs = [p for _, p in time_peaks]
                    if random.random() < 0.7:
                        txn_hour = np.random.choice([h for h, _ in time_peaks], p=peak_probs)
                        txn_minute = random.randint(0, 15)
                    else:
                        txn_hour = random.randint(6, 17)
                        txn_minute = random.randint(0, 59)
                    txn_second = random.randint(0, 59)
                else:
                    txn_hour = random.randint(6, 17)
                    txn_minute = random.randint(0, 59)
                    txn_second = random.randint(0, 59)
                
                txn_time = f"{txn_hour:02d}:{txn_minute:02d}:{txn_second:02d}"
                
                # No immediate payments for debit orders
                immediate_payment = False
                trans_cost = account_cost_map.get(account_id, 5.0) if immediate_payment else 0.0
                
                # Determine receiving bank
                receiving_bank = ""
                if debit_order.get("beneficiary_bank_name"):
                    receiving_bank = debit_order["beneficiary_bank_name"]
                elif pd.notna(debit_order["account_to"]) and debit_order["account_to"] not in accounts_df["account_id"].values:
                    receiving_bank = random.choice(sa_banks)
                
                # Create transaction record
                transaction = {
                    "transaction_id": f"TXN{target_year}{txn_counter:07d}",
                    "account_id": account_id,
                    "transaction_date": single_date.strftime("%Y-%m-%d"),
                    "transaction_time": txn_time,
                    "amount": amount,
                    "debit_credit": "Debit",
                    "status": status,
                    "description": description,
                    "immediate_payment": immediate_payment,
                    "receiving_account": debit_order["account_to"],
                    "transaction_cost": trans_cost,
                    "channel": "Automated",  # Debit orders are always automated
                    "debit_order_id": debit_order["debit_order_id"],
                    "debit_order_type": debit_order["debit_order_type"],
                    "customer_id": debit_order["customer_id"]
                }
                
                day_transactions.append(transaction)
                txn_counter += 1
        
        transactions.extend(day_transactions)
    
    # Create DataFrame
    print(f"Creating transactions DataFrame for {target_year}...")
    transactions_df = pd.DataFrame(transactions)
    
    if len(transactions_df) == 0:
        print(f"No transactions generated for {target_year}. Please check your debit orders data.")
        return pd.DataFrame()
    
    # Save to file
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
        generate_debit_order_transactions_for_year(year, min_year=min_year)