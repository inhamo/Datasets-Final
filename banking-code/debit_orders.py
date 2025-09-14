import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import date, timedelta
import os
import glob
from tqdm import tqdm

def generate_debit_orders(target_year):
    # Initialize seeds for reproducibility
    seed_bytes = os.urandom(4)
    seed_int = int.from_bytes(seed_bytes, byteorder='big')
    random.seed(seed_int)
    np.random.seed(seed_int)
    Faker.seed(seed_int)
    fake = Faker()

    # File paths
    github_repo_path = 'banking_data'
    customer_files = sorted(glob.glob(f'{github_repo_path}/customers_*.parquet'))
    account_files = sorted(glob.glob(f'{github_repo_path}/accounts_*.parquet'))
    employment_file = f'{github_repo_path}/customer_employment_status.parquet'
    default_file = f'{github_repo_path}/loan_defaults.parquet'

    # Load data
    if not customer_files or not account_files:
        print("Customer or account files missing. Exiting.")
        return pd.DataFrame()

    print("Loading data files...")
    customers_list = [pd.read_parquet(f) for f in customer_files]
    customers_df = pd.concat(customers_list, ignore_index=True).drop_duplicates(subset=['customer_id'])

    accounts_list = [pd.read_parquet(f) for f in account_files]
    accounts_df = pd.concat(accounts_list, ignore_index=True).drop_duplicates(subset=['account_id'])

    # OPTIMIZATION 1: Create indexed lookups for faster access
    customers_dict = customers_df.set_index('customer_id').to_dict('index')
    
    try:
        employment_df = pd.read_parquet(employment_file)
        employment_df['period'] = pd.to_datetime(employment_df['period'])
        # OPTIMIZATION 2: Pre-group employment data by customer_id
        employment_grouped = employment_df.groupby('customer_id')
    except FileNotFoundError:
        print("Employment status file not found. Proceeding with default employed assumption.")
        employment_df = pd.DataFrame()
        employment_grouped = None

    try:
        loan_defaults_df = pd.read_parquet(default_file)
    except FileNotFoundError:
        print("Loan defaults file not found. Proceeding without default data.")
        loan_defaults_df = pd.DataFrame()

    # Load loans data for linking loan defaults
    loan_files = sorted(glob.glob(f'{github_repo_path}/loans_*.parquet'))
    if loan_files:
        loans_list = [pd.read_parquet(f) for f in loan_files]
        loans_df = pd.concat(loans_list, ignore_index=True).drop_duplicates(subset=['loan_id'])
        # OPTIMIZATION 3: Create customer to loans mapping
        customer_loans = loans_df.groupby('customer_id')['loan_id'].apply(list).to_dict()
    else:
        loans_df = pd.DataFrame()
        customer_loans = {}

    # OPTIMIZATION 4: Pre-filter and group loan defaults
    if not loan_defaults_df.empty:
        defaults_by_loan = loan_defaults_df.set_index('loan_id').to_dict('index')
    else:
        defaults_by_loan = {}

    # Parameters
    num_debit_orders = random.randint(3219, 8329)
    print(f"Generating {num_debit_orders} debit orders...")

    # Pre-define constants to avoid repeated lookups
    personal_debit_types = [
        "Salary Payment", "Utility Bill", "Loan Repayment",
        "Subscription", "Insurance Premium", "Mortgage",
        "School Fees", "Credit Card Payment", "Membership Fee", "Donation"
    ]
    business_debit_types = [
        "Payroll", "Supplier Payment", "Business Loan Repayment",
        "Office Lease", "Utility Bill", "Insurance Premium",
        "Software Subscription", "Corporate Credit Card Payment"
    ]
    frequencies = ["Monthly", "Weekly", "Quarterly", "Annually"]
    freq_weights = [0.7, 0.15, 0.1, 0.05]
    statuses = ["Active", "Suspended", "Cancelled"]
    status_weights_base = [0.82, 0.09, 0.09]

    # OPTIMIZATION 5: Pre-sample all random choices to avoid repeated calls
    print("Pre-generating random values...")
    sampled_accounts = accounts_df.sample(num_debit_orders, replace=True, random_state=seed_int)
    target_accounts = accounts_df.sample(num_debit_orders, replace=True, random_state=seed_int + 1)
    
    # Pre-generate all random numbers needed
    start_years = np.random.randint(max(2018, target_year - 3), target_year + 1, num_debit_orders)
    frequencies_selected = np.random.choice(frequencies, size=num_debit_orders, p=freq_weights)
    end_date_randoms = np.random.random(num_debit_orders)
    internal_external_randoms = np.random.random(num_debit_orders)
    
    # Pre-generate fake data in batches
    fake_dates_start = []
    fake_dates_next = []
    fake_uuids = []
    fake_ibans = []
    fake_companies = []
    
    print("Pre-generating fake data...")
    for i in range(num_debit_orders):
        fake_uuids.append(fake.uuid4())
        fake_ibans.append(fake.iban()[:16])
        fake_companies.append(fake.company())
        
        # Generate start dates - fix date range issue
        account = sampled_accounts.iloc[i]
        min_start = pd.Timestamp(account["opening_date"])
        date_start = max(min_start, pd.Timestamp(f"{start_years[i]}-01-01"))
        date_end = pd.Timestamp(f"{start_years[i]}-12-31")
        
        # Ensure date_start is not after date_end
        if date_start > date_end:
            date_start = pd.Timestamp(f"{start_years[i]}-01-01")
            if date_start > date_end:
                # If account opening date is after the target year, use account opening date as both start and end
                date_start = min_start
                date_end = max(date_start, pd.Timestamp(f"{start_years[i]}-12-31"))
        
        start_date = pd.to_datetime(fake.date_between_dates(date_start=date_start, date_end=date_end))
        fake_dates_start.append(start_date)
        
        # Generate next debit dates
        next_debit_date = fake.date_between_dates(
            date_start=pd.Timestamp(f"{target_year}-01-01"),
            date_end=pd.Timestamp(f"{target_year}-12-31")
        )
        fake_dates_next.append(next_debit_date)

    debit_orders = []

    print("Generating debit orders...")
    for i in tqdm(range(num_debit_orders), desc="Processing"):
        # Get pre-sampled account and customer
        account = sampled_accounts.iloc[i]
        customer_id = account["customer_id"]
        account_id = account["account_id"]
        
        # OPTIMIZATION 6: Direct dictionary lookup instead of DataFrame filtering
        if customer_id not in customers_dict:
            continue
        customer = customers_dict[customer_id]
        is_business = customer["customer_type"] == "Company"

        start_date = fake_dates_start[i]

        # OPTIMIZATION 7: Faster employment lookup using pre-grouped data
        is_employed = True
        if employment_grouped is not None and customer_id in employment_grouped.groups:
            cust_employment = employment_grouped.get_group(customer_id)
            relevant_employment = cust_employment[cust_employment['period'] <= start_date]
            if not relevant_employment.empty:
                is_employed = relevant_employment.sort_values('period').iloc[-1]['is_employed']
            else:
                is_employed = cust_employment.sort_values('period').iloc[0]['is_employed']

        # OPTIMIZATION 8: Faster default lookup using pre-built mappings
        has_default = False
        default_date = pd.NaT
        if customer_id in customer_loans:
            for loan_id in customer_loans[customer_id]:
                if loan_id in defaults_by_loan:
                    default_info = defaults_by_loan[loan_id]
                    if default_info.get('will_default', False):
                        has_default = True
                        default_date = default_info.get('default_date')
                        break

        # Choose debit type
        debit_types = business_debit_types if is_business else personal_debit_types
        debit_type_weights = {dt: 1.0 for dt in debit_types}
        
        if not is_employed:
            if not is_business:
                debit_type_weights.update({
                    "Loan Repayment": debit_type_weights.get("Loan Repayment", 1.0) * 0.5,
                    "Mortgage": debit_type_weights.get("Mortgage", 1.0) * 0.5,
                    "School Fees": debit_type_weights.get("School Fees", 1.0) * 0.7,
                    "Utility Bill": debit_type_weights.get("Utility Bill", 1.0) * 1.2,
                    "Subscription": debit_type_weights.get("Subscription", 1.0) * 1.2
                })
        
        if has_default:
            if "Loan Repayment" in debit_type_weights:
                debit_type_weights["Loan Repayment"] *= 0.3
            if "Business Loan Repayment" in debit_type_weights:
                debit_type_weights["Business Loan Repayment"] *= 0.3

        # Normalize and select
        total_weight = sum(debit_type_weights.values())
        normalized_weights = [debit_type_weights[dt] / total_weight for dt in debit_types]
        debit_type = np.random.choice(debit_types, p=normalized_weights)

        # Amount calculation (vectorized where possible)
        if debit_type in ["Salary Payment", "Payroll"]:
            amount = max(np.random.normal(25000 if is_business else 18000, 8000), 4000)
            amount *= (0.5 if not is_employed and not is_business else 1.0)
        elif debit_type in ["Mortgage", "Business Loan Repayment", "Loan Repayment"]:
            amount = max(np.random.normal(9000, 3000), 2000)
            amount *= (0.7 if not is_employed or has_default else 1.0)
        elif debit_type in ["Utility Bill", "Software Subscription", "Subscription", "Membership Fee"]:
            amount = max(np.random.normal(1200, 500), 200)
        elif debit_type in ["Supplier Payment", "Office Lease"]:
            amount = max(np.random.normal(30000, 15000), 5000)
        else:
            amount = max(np.random.exponential(2000), 100)
        
        amount = round(amount, 2)

        # Use pre-selected frequency
        frequency = frequencies_selected[i]

        # Status calculation
        status_weights = status_weights_base.copy()
        if not is_employed:
            status_weights = [0.6, 0.2, 0.2]
        if has_default and debit_type in ["Loan Repayment", "Business Loan Repayment"] and pd.notna(default_date):
            if start_date >= pd.to_datetime(default_date):
                status_weights = [0.1, 0.45, 0.45]
        
        status = np.random.choice(statuses, p=status_weights)

        # End date
        end_date = None
        if end_date_randoms[i] < (0.3 if not is_employed or has_default else 0.15):
            end_date = fake.date_between_dates(
                date_start=start_date,
                date_end=pd.Timestamp(f"{target_year}-12-31")
            )
            if has_default and pd.notna(default_date) and debit_type in ["Loan Repayment", "Business Loan Repayment"]:
                end_date = max(end_date, pd.to_datetime(default_date).date())

        # Internal vs External using pre-generated random
        if internal_external_randoms[i] < 0.65:
            target_account = target_accounts.iloc[i]
            # Ensure it's not the same account
            while target_account["account_id"] == account_id:
                target_account = accounts_df.sample(1, random_state=seed_int + i).iloc[0]
            account_to = target_account["account_id"]
            description = f"Transfer to {target_account['account_type']} - {target_account['customer_id']}"
        else:
            account_to = fake_ibans[i]
            description = fake_companies[i]

        debit_orders.append({
            "debit_order_id": f"DBT{target_year}{str(i+1).zfill(6)}",
            "account_id": account_id,
            "customer_id": customer_id,
            "debit_order_type": debit_type,
            "amount": amount,
            "frequency": frequency,
            "start_date": start_date,
            "end_date": end_date,
            "status": status,
            "account_to": account_to,
            "description": description
        })

    # Create and save DataFrame
    print("Creating final DataFrame...")
    debit_orders_df = pd.DataFrame(debit_orders)
    os.makedirs(github_repo_path, exist_ok=True)
    output_file = f'{github_repo_path}/debit_orders_{target_year}.parquet'
    debit_orders_df.to_parquet(output_file, index=False)

    print(f"Generated {len(debit_orders_df)} debit orders for year {target_year}.")
    print(f"Saved to {output_file}")
    print("Debit Orders sample:")
    print(debit_orders_df.head(10))

    return debit_orders_df

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate debit order data for a specific year")
    parser.add_argument('--year', type=int, default=2019, help='Year for debit order data generation (must be >= 2018)')
    args = parser.parse_args()
    generate_debit_orders(args.year)