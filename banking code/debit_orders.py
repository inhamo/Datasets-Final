import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import date, timedelta
import os
import glob
from tqdm import tqdm

SA_CREDITORS = {
    "Utility Bill": {
        "Eskom": {"abbrev": "ESKOM", "creditor_id": "ESK001"},
        "City Power": {"abbrev": "CITYPWR", "creditor_id": "CTP002"},
        "Rand Water": {"abbrev": "RANDWATER", "creditor_id": "RWA003"}
    },
    "Insurance Premium": {
        "Discovery Life": {"abbrev": "DISCOVERY", "creditor_id": "DSC100"},
        "Old Mutual": {"abbrev": "OLDMUTUAL", "creditor_id": "OM101"},
        "Sanlam": {"abbrev": "SANLAM", "creditor_id": "SNL102"}
    },
    "Subscription": {
        "DSTV": {"abbrev": "MULTICHOICE", "creditor_id": "DTV200"},
        "Netflix": {"abbrev": "NETFLIX", "creditor_id": "NFX201"},
        "Showmax": {"abbrev": "SHOWMAX", "creditor_id": "SHM202"}
    },
    "School Fees": {
        "Crawford College": {"abbrev": "CRAWFORD", "creditor_id": "CRW300"},
        "Reddam House": {"abbrev": "REDDAM", "creditor_id": "RDM301"}
    }
}

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

    # Create indexed lookups for faster access
    customers_dict = customers_df.set_index('customer_id').to_dict('index')
    accounts_dict = accounts_df.set_index('account_id').to_dict('index')
    
    try:
        employment_df = pd.read_parquet(employment_file)
        employment_df['period'] = pd.to_datetime(employment_df['period'])
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

    # Load loans data for linking loan defaults and repayments
    loan_files = sorted(glob.glob(f'{github_repo_path}/loans_*.parquet'))
    if loan_files:
        loans_list = [pd.read_parquet(f) for f in loan_files]
        loans_df = pd.concat(loans_list, ignore_index=True).drop_duplicates(subset=['loan_id'])
        customer_loans = loans_df.groupby('customer_id')['loan_id'].apply(list).to_dict()
        loans_dict = loans_df.set_index('loan_id').to_dict('index')
    else:
        loans_df = pd.DataFrame()
        customer_loans = {}
        loans_dict = {}

    # Pre-filter and group loan defaults
    if not loan_defaults_df.empty:
        defaults_by_loan = loan_defaults_df.set_index('loan_id').to_dict('index')
    else:
        defaults_by_loan = {}

    # Parameters
    num_debit_orders = random.randint(3219, 8329)
    print(f"Generating {num_debit_orders} debit orders...")

    # Pre-define constants
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
    suspension_reasons = ["insufficient_funds", "customer_request", "fraud_suspected", "dispute"]
    cancellation_reasons = ["customer_request", "contract_ended", "account_closed", "creditor_request"]
    notification_methods = ["sms", "email", "app_notification", "none"]
    bank_names = ["Standard Bank", "ABSA", "Nedbank", "FNB", "Capitec"]
    account_types = ["savings", "current", "transmission"]

    # Pre-sample accounts and generate random values
    print("Pre-generating random values...")
    sampled_accounts = accounts_df.sample(num_debit_orders, replace=True, random_state=seed_int)
    
    # Pre-generate random numbers and fake data
    start_years = np.random.randint(max(2018, target_year - 3), target_year + 1, num_debit_orders)
    frequencies_selected = np.random.choice(frequencies, size=num_debit_orders, p=freq_weights)
    end_date_randoms = np.random.random(num_debit_orders)
    internal_external_randoms = np.random.random(num_debit_orders)
    suspension_randoms = np.random.random(num_debit_orders)
    notification_randoms = np.random.random(num_debit_orders)
    
    fake_dates_start = []
    fake_dates_next = []
    fake_uuids = []
    fake_beneficiary_accounts = []
    fake_branch_codes = []
    fake_companies = []
    
    print("Pre-generating fake data...")
    for i in range(num_debit_orders):
        fake_uuids.append(fake.uuid4())
        fake_beneficiary_accounts.append(str(fake.random_int(1000000000, 9999999999)))
        fake_branch_codes.append(str(fake.random_int(100000, 999999)))
        fake_companies.append(fake.company())
        
        account = sampled_accounts.iloc[i]
        opening_date = pd.Timestamp(account["opening_date"])
        effective_start_year = max(start_years[i], opening_date.year)
        date_start = max(opening_date, pd.Timestamp(f"{effective_start_year}-01-01"))
        date_end = pd.Timestamp(f"{effective_start_year}-12-31")
        
        start_date = pd.to_datetime(fake.date_between_dates(date_start=date_start, date_end=date_end))
        fake_dates_start.append(start_date)
        
        next_debit_date = fake.date_between_dates(
            date_start=pd.Timestamp(f"{target_year}-01-01"),
            date_end=pd.Timestamp(f"{target_year}-12-31")
        )
        fake_dates_next.append(next_debit_date)

    debit_orders = []
    existing_debit_orders = set()  # Track creditor-customer pairs to avoid duplicates

    print("Generating debit orders...")
    for i in tqdm(range(num_debit_orders), desc="Processing"):
        account = sampled_accounts.iloc[i]
        customer_id = account["customer_id"]
        account_id = account["account_id"]
        
        # Validation checks
        if customer_id not in customers_dict:
            continue
        customer = customers_dict[customer_id]
        
        # Check account status and age
        account_info = accounts_dict[account_id]
        if account_info.get("status") in ["closed", "frozen"]:
            continue
        account_opening = pd.Timestamp(account_info["opening_date"])
        if (pd.Timestamp(f"{target_year}-12-31") - account_opening).days < 90:
            continue
        if customer.get("debt_review", False):
            continue

        is_business = customer["customer_type"] == "Company"
        start_date = fake_dates_start[i]

        # Employment status
        is_employed = True
        if employment_grouped is not None and customer_id in employment_grouped.groups:
            cust_employment = employment_grouped.get_group(customer_id)
            relevant_employment = cust_employment[cust_employment['period'] <= start_date]
            if not relevant_employment.empty:
                is_employed = relevant_employment.sort_values('period').iloc[-1]['is_employed']
            else:
                is_employed = cust_employment.sort_values('period').iloc[0]['is_employed']

        # Default status
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
        
        if not is_employed and not is_business:
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

        total_weight = sum(debit_type_weights.values())
        normalized_weights = [debit_type_weights[dt] / total_weight for dt in debit_types]
        debit_type = np.random.choice(debit_types, p=normalized_weights)

        # Check for duplicate debit orders
        creditor_key = f"{customer_id}_{debit_type}"
        if creditor_key in existing_debit_orders:
            continue
        existing_debit_orders.add(creditor_key)

        # Amount calculation
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

        # Frequency and status
        frequency = frequencies_selected[i]
        status_weights = status_weights_base.copy()
        if not is_employed:
            status_weights = [0.6, 0.2, 0.2]
        if has_default and debit_type in ["Loan Repayment", "Business Loan Repayment"] and pd.notna(default_date):
            if start_date >= pd.to_datetime(default_date):
                status_weights = [0.1, 0.45, 0.45]
        
        status = np.random.choice(statuses, p=status_weights)

        # Suspension and cancellation details
        suspension_date = None
        suspension_reason = None
        suspension_initiated_by = None
        cancellation_date = None
        cancellation_reason = None
        can_be_reactivated = True

        if status == "Suspended" and suspension_randoms[i] < 0.5:
            suspension_date = fake.date_between_dates(
                date_start=start_date,
                date_end=pd.Timestamp(f"{target_year}-12-31")
            )
            suspension_reason = np.random.choice(suspension_reasons)
            suspension_initiated_by = np.random.choice(["customer", "bank", "creditor", "system"])

        # End date and cancellation
        end_date = None
        if status == "Cancelled" or end_date_randoms[i] < (0.3 if not is_employed or has_default else 0.15):
            end_date = fake.date_between_dates(
                date_start=start_date,
                date_end=pd.Timestamp(f"{target_year}-12-31")
            )
            if status == "Cancelled":
                cancellation_date = end_date
                cancellation_reason = np.random.choice(cancellation_reasons)
                can_be_reactivated = cancellation_reason != "account_closed"

        # Notification settings
        notification_required = debit_type in ["Utility Bill", "Insurance Premium", "Subscription", "School Fees"]
        notification_days_before = np.random.randint(5, 11) if notification_required else 0
        notification_method = np.random.choice(notification_methods) if notification_required else "none"

        # Internal vs External transfer
        linked_loan_id = None
        linked_policy_number = None
        linked_subscription_id = None
        linked_account_internal = None
        account_to = None
        description = None
        beneficiary_account_number = None
        beneficiary_branch_code = None
        beneficiary_bank_name = None
        beneficiary_account_type = None
        beneficiary_name = None
        creditor_id = None

        if internal_external_randoms[i] < 0.65:
            # Internal transfer logic
            if debit_type in ["Salary Payment", "Payroll"]:
                # Prefer savings/investment accounts for salary
                target_accounts = accounts_df[
                    (accounts_df['customer_id'] == customer_id) & 
                    (accounts_df['account_type'].isin(['savings', 'investment'])) &
                    (accounts_df['account_id'] != account_id)
                ]
                description = "Monthly savings transfer"
            elif debit_type in ["Loan Repayment", "Business Loan Repayment"]:
                # Link to loan accounts
                if customer_id in customer_loans and customer_loans[customer_id]:
                    linked_loan_id = random.choice(customer_loans[customer_id])
                    target_accounts = accounts_df[accounts_df['account_id'].isin([loans_dict[linked_loan_id]['account_id']])]
                    description = f"Loan repayment - {linked_loan_id}"
                else:
                    continue  # Skip if no valid loan found
            else:
                # Other internal transfers
                target_accounts = accounts_df[accounts_df['account_id'] != account_id]
                description = f"Transfer to {debit_type.lower()}"

            if not target_accounts.empty:
                target_account = target_accounts.sample(1, random_state=seed_int + i).iloc[0]
                linked_account_internal = target_account['account_id']
                account_to = linked_account_internal
            else:
                continue  # Skip if no valid target account
        else:
            # External transfer
            if debit_type in SA_CREDITORS:
                creditor = random.choice(list(SA_CREDITORS[debit_type].keys()))
                creditor_info = SA_CREDITORS[debit_type][creditor]
                creditor_id = creditor_info['creditor_id']
                beneficiary_name = creditor
                description = f"{debit_type} - {creditor}"
                if debit_type == "Insurance Premium":
                    linked_policy_number = f"POL-{target_year}-{fake.random_int(1000, 9999)}"
                elif debit_type == "Subscription":
                    linked_subscription_id = f"SUB-{creditor_info['abbrev']}-{fake.random_int(100, 999)}"
            else:
                beneficiary_name = fake_companies[i]
                description = f"{debit_type} - {beneficiary_name}"
            
            account_to = fake_beneficiary_accounts[i]
            beneficiary_account_number = account_to
            beneficiary_branch_code = fake_branch_codes[i]
            beneficiary_bank_name = np.random.choice(bank_names)
            beneficiary_account_type = np.random.choice(account_types)

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
            "suspension_date": suspension_date,
            "suspension_reason": suspension_reason,
            "suspension_initiated_by": suspension_initiated_by,
            "cancellation_date": cancellation_date,
            "cancellation_reason": cancellation_reason,
            "can_be_reactivated": can_be_reactivated,
            "notification_required": notification_required,
            "notification_days_before": notification_days_before,
            "notification_method": notification_method,
            "account_to": account_to,
            "beneficiary_account_number": beneficiary_account_number,
            "beneficiary_branch_code": beneficiary_branch_code,
            "beneficiary_bank_name": beneficiary_bank_name,
            "beneficiary_account_type": beneficiary_account_type,
            "beneficiary_name": beneficiary_name,
            "creditor_id": creditor_id,
            "linked_loan_id": linked_loan_id,
            "linked_policy_number": linked_policy_number,
            "linked_subscription_id": linked_subscription_id,
            "linked_account_internal": linked_account_internal,
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
    parser.add_argument('--year', type=int, default=2024, help='Year for debit order data generation (must be >= 2018)')
    args = parser.parse_args()
    generate_debit_orders(args.year)