import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os
from tqdm import tqdm
import logging
from sa_merchant import (SA_COMPANIES, STATUS_WEIGHTS, PEAK_HOURS, WEEKEND_MULTIPLIERS,
                         PAYDAY_MULTIPLIERS, AGE_SPENDING_PREFERENCES, INCOME_SPENDING_MULTIPLIERS)

# Initialize constants
TRANSACTION_STATUSES = ['completed', 'failed', 'cancelled', 'pending']
CHANNELS = ['pos', 'online banking', 'mobile banking app', 'branch']

# Initialize faker with South African locale
fake = Faker('zu_ZA')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BalanceTracker:
    """Track running balances for all accounts considering all transaction types"""
    
    def __init__(self):
        self.account_balances = {}
        self.overdraft_limits = {
            'Premium Banking': -10000,
            'Gold Banking': -5000,
            'Standard Banking': -2000,
            'Basic Banking': -500,
            'Student Banking': -200,
            'Business Banking': -20000
        }
    
    def initialize_account_balance(self, account_id, account_type, initial_deposit=0):
        """Initialize account with starting balance"""
        self.account_balances[account_id] = {
            'balance': initial_deposit,
            'account_type': account_type,
            'overdraft_limit': self.overdraft_limits.get(account_type, -1000)
        }
        logger.debug(f"Initialized account {account_id} with balance {initial_deposit}")
    
    def can_transact(self, account_id, amount):
        """Check if account can handle the transaction"""
        if account_id not in self.account_balances:
            logger.warning(f"Account {account_id} not found in balance tracker")
            return False
        
        current_balance = self.account_balances[account_id]['balance']
        overdraft_limit = self.account_balances[account_id]['overdraft_limit']
        return (current_balance - amount) >= overdraft_limit
    
    def process_transaction(self, account_id, amount, transaction_type='debit'):
        """Process transaction and update balance"""
        if account_id not in self.account_balances:
            logger.warning(f"Cannot process transaction for unknown account {account_id}")
            return False
        
        if transaction_type == 'debit':
            if not self.can_transact(account_id, amount):
                logger.debug(f"Transaction failed: Insufficient funds for {account_id}, amount: {amount}")
                return False
            self.account_balances[account_id]['balance'] -= amount
        else:  # credit
            self.account_balances[account_id]['balance'] += amount
        
        logger.debug(f"Processed {transaction_type} transaction for {account_id}, amount: {amount}, new balance: {self.account_balances[account_id]['balance']}")
        return True
    
    def get_balance(self, account_id):
        """Get current balance for account"""
        if account_id not in self.account_balances:
            logger.warning(f"Account {account_id} not found, returning balance 0")
            return 0
        return self.account_balances[account_id]['balance']

def load_banking_data_for_year(year):
    """Load banking data files for all years up to the specified year"""
    accounts_dfs = []
    customers_dfs = []
    
    for y in range(2018, year + 1):
        try:
            accounts_df = pd.read_parquet(f"banking_data/accounts_{y}.parquet")
            customers_df = pd.read_parquet(f"banking_data/customers_{y}.parquet")
            # Rename columns if necessary
            customers_df = customers_df.rename(columns={'CustomerID': 'customer_id'})
            # Generate synthetic data for missing customer columns
            if 'age' not in customers_df.columns:
                customers_df['age'] = np.random.randint(18, 80, size=len(customers_df))
                logger.warning(f"Generated synthetic 'age' column for {y}")
            if 'income' not in customers_df.columns:
                customers_df['income'] = np.random.normal(25000, 8000, size=len(customers_df)).clip(min=4000)
                logger.warning(f"Generated synthetic 'income' column for {y}")
            if 'occupation' not in customers_df.columns:
                customers_df['occupation'] = np.random.choice(['Employed', 'Self-Employed', 'Unemployed', 'Student'], size=len(customers_df))
                logger.warning(f"Generated synthetic 'occupation' column for {y}")
            accounts_dfs.append(accounts_df)
            customers_dfs.append(customers_df)
            logger.info(f"Loaded data for year {y}: {len(accounts_df)} accounts, {len(customers_df)} customers")
        except FileNotFoundError as e:
            logger.warning(f"Could not load data for {y}: {e}")
            continue
    
    if accounts_dfs:
        accounts_df = pd.concat(accounts_dfs, ignore_index=True)
        accounts_df = accounts_df.sort_values(by="account_id").drop_duplicates(subset="account_id", keep="last")
        logger.info(f"Total unique accounts loaded: {len(accounts_df)}")
    else:
        accounts_df = pd.DataFrame(columns=['account_id', 'customer_id', 'account_type'])
        logger.warning("No account data found, using empty DataFrame")
    
    if customers_dfs:
        customers_df = pd.concat(customers_dfs, ignore_index=True)
        customers_df = customers_df.sort_values(by="customer_id").drop_duplicates(subset="customer_id", keep="last")
        logger.info(f"Total unique customers loaded: {len(customers_df)}")
    else:
        customers_df = pd.DataFrame(columns=['customer_id', 'age', 'income', 'occupation'])
        logger.warning("No customer data found, using empty DataFrame")
    
    # Validate required columns
    required_account_cols = ['account_id', 'customer_id']
    if not all(col in accounts_df.columns for col in required_account_cols):
        logger.error("Missing required columns in accounts data")
        return accounts_df, customers_df
    
    required_customer_cols = ['customer_id', 'age', 'income', 'occupation']
    if not all(col in customers_df.columns for col in required_customer_cols):
        logger.warning("Missing required columns in customers data, using synthetic data")
        if 'customer_id' not in customers_df.columns and not accounts_df.empty:
            customers_df = pd.DataFrame({
                'customer_id': accounts_df['customer_id'].unique(),
                'age': np.random.randint(18, 80, size=len(accounts_df['customer_id'].unique())),
                'income': np.random.normal(25000, 8000, size=len(accounts_df['customer_id'].unique())).clip(min=4000),
                'occupation': np.random.choice(['Employed', 'Self-Employed', 'Unemployed', 'Student'], size=len(accounts_df['customer_id'].unique()))
            })
    
    # Add distress_level
    if 'distress_level' not in customers_df.columns:
        customers_df['distress_level'] = customers_df['income'].apply(
            lambda inc: np.random.choice([0.0, 0.5, 0.8], p=[0.6, 0.3, 0.1]) if inc < 15000 else 
                        np.random.choice([0.0, 0.5, 0.8], p=[0.8, 0.15, 0.05]) if inc > 40000 else 
                        np.random.choice([0.0, 0.5, 0.8], p=[0.7, 0.25, 0.05])
        )
        logger.warning(f"Generated synthetic 'distress_level' column for {year}")
    
    return accounts_df, customers_df

def load_existing_transactions(year, accounts_df):
    """Load existing loan and debit order transactions for a specific year, filtered by valid accounts"""
    try:
        loan_transactions_df = pd.read_parquet("banking_data/loan_payment_transactions_2018_2024.parquet", engine="fastparquet")
        debit_order_transactions_df = pd.read_parquet("banking_data/debit_order_transactions_2018_2024.parquet", engine="fastparquet")
        
        # Filter by year and valid account_ids
        valid_account_ids = set(accounts_df['account_id'])
        loan_transactions_df = loan_transactions_df[
            (loan_transactions_df['transaction_date'].str.startswith(str(year))) &
            (loan_transactions_df['account_id'].isin(valid_account_ids))
        ]
        debit_order_transactions_df = debit_order_transactions_df[
            (debit_order_transactions_df['transaction_date'].str.startswith(str(year))) &
            (debit_order_transactions_df['account_id'].isin(valid_account_ids))
        ]
        
        # Log any invalid account_ids
        if not loan_transactions_df.empty:
            invalid_loan_accounts = set(loan_transactions_df['account_id'].unique()) - valid_account_ids
            if invalid_loan_accounts:
                logger.warning(f"Found {len(invalid_loan_accounts)} invalid account IDs in loan transactions: {list(invalid_loan_accounts)[:10]}")
        
        if not debit_order_transactions_df.empty:
            invalid_debit_accounts = set(debit_order_transactions_df['account_id'].unique()) - valid_account_ids
            if invalid_debit_accounts:
                logger.warning(f"Found {len(invalid_debit_accounts)} invalid account IDs in debit order transactions: {list(invalid_debit_accounts)[:10]}")
        
        logger.info(f"Loaded {len(loan_transactions_df)} loan transactions and {len(debit_order_transactions_df)} debit order transactions for {year}")
        return loan_transactions_df, debit_order_transactions_df
    except FileNotFoundError as e:
        logger.warning(f"Could not load existing transactions: {e}")
        return pd.DataFrame(), pd.DataFrame()

def preprocess_scheduled_transactions(balance_tracker, loan_transactions_df, debit_order_transactions_df):
    """Pre-process all scheduled transactions to update balances chronologically"""
    logger.info("Pre-processing scheduled transactions for balance tracking...")
    
    all_scheduled = []
    
    if not loan_transactions_df.empty:
        loan_copy = loan_transactions_df.copy()
        loan_copy['transaction_datetime'] = pd.to_datetime(
            loan_copy['transaction_date'] + ' ' + 
            loan_copy.get('transaction_time', '09:00:00')
        )
        all_scheduled.append(loan_copy[['account_id', 'amount', 'debit_credit', 'transaction_datetime']])
    
    if not debit_order_transactions_df.empty:
        debit_copy = debit_order_transactions_df.copy()
        debit_copy['transaction_datetime'] = pd.to_datetime(
            debit_copy['transaction_date'] + ' ' + 
            debit_copy.get('transaction_time', '06:00:00')
        )
        all_scheduled.append(debit_copy[['account_id', 'amount', 'debit_credit', 'transaction_datetime']])
    
    if all_scheduled:
        combined_scheduled = pd.concat(all_scheduled, ignore_index=True)
        combined_scheduled = combined_scheduled.sort_values('transaction_datetime')
        
        # Log unique account IDs
        logger.info(f"Processing {len(combined_scheduled)} scheduled transactions for {len(combined_scheduled['account_id'].unique())} unique accounts")
        
        for _, row in combined_scheduled.iterrows():
            if row['account_id'] not in balance_tracker.account_balances:
                logger.warning(f"Skipping transaction for uninitialized account {row['account_id']}")
                continue
            if row['debit_credit'] == 'debit':
                balance_tracker.process_transaction(row['account_id'], row['amount'], 'debit')
            else:
                balance_tracker.process_transaction(row['account_id'], row['amount'], 'credit')

def generate_sa_phone_number(size=1):
    """Generate South African phone numbers efficiently"""
    prefixes = np.array(['082', '083', '084', '072', '073', '074', '076', '078', '079', '081', '071'])
    prefixes_selected = np.random.choice(prefixes, size=size)
    numbers = np.random.randint(0, 10000000, size=size)
    numbers_str = np.char.zfill(numbers.astype(str), 7)
    return np.char.add(prefixes_selected, numbers_str)

def get_merchant_info(category, size=1):
    """Get merchant information based on category"""
    if category.lower() not in SA_COMPANIES:
        logger.warning(f"Category {category} not found, using default merchant")
        return [{"name": fake.company(), "size": "medium", "avg_transaction": 200, "std_deviation": 60, "hours": {"open": 8, "close": 20}}] * size
    
    merchants = SA_COMPANIES[category.lower()]
    selected = np.random.choice(len(merchants), size=size, replace=True)
    return [merchants[i] for i in selected]

def get_age_group(age):
    """Categorize customer by age group"""
    if pd.isna(age):
        return '26-35'  # Default to middle age group
    age = int(age)
    if age < 26:
        return '18-25'
    elif age < 36:
        return '26-35'
    elif age < 51:
        return '36-50'
    elif age < 66:
        return '51-65'
    else:
        return '65+'

def get_income_category(income):
    """Categorize customer by income level"""
    if pd.isna(income):
        return 'medium'  # Default to medium income
    income = float(income)
    if income < 15000:
        return 'low'
    elif income < 40000:
        return 'medium'
    elif income < 80000:
        return 'high'
    else:
        return 'premium'

def calculate_transaction_amount(merchant_info, customer_age, customer_income, category, distress_level=0.0):
    """Calculate realistic transaction amount based on merchant, customer profile"""
    base_amount = np.random.normal(
        merchant_info['avg_transaction'], 
        merchant_info.get('std_deviation', merchant_info['avg_transaction'] * 0.3)
    )
    
    age_group = get_age_group(customer_age)
    age_multiplier = AGE_SPENDING_PREFERENCES.get(age_group, {}).get(category, 1.0)
    
    income_category = get_income_category(customer_income)
    income_multipliers = INCOME_SPENDING_MULTIPLIERS.get(income_category, {'normal': 1.0, 'distressed': 0.6, 'categories': {}})
    
    distress_weight = distress_level
    normal_weight = 1.0 - distress_weight
    income_multiplier = normal_weight * income_multipliers['normal'] + distress_weight * income_multipliers['distressed']
    
    category_multiplier = income_multipliers['categories'].get(category, income_multiplier)
    
    final_amount = max(10, base_amount * age_multiplier * category_multiplier)
    return round(final_amount, 2)

def get_transaction_status_weighted(merchant_size, date_obj, distress_level=0.0):
    """Get transaction status based on merchant size and date"""
    weights = STATUS_WEIGHTS.get(merchant_size, STATUS_WEIGHTS['medium']).copy()
    
    if distress_level > 0.5:
        weights['failed'] = min(0.1, weights['failed'] * (1 + distress_level))
        weights['cancelled'] = min(0.1, weights['cancelled'] * (1 + distress_level))
        weights['completed'] = 1 - (weights['cancelled'] + weights['failed'] + weights['pending'])
    
    if date_obj.weekday() >= 5:  # Weekend
        weights['failed'] = min(0.08, weights['failed'] * 1.5)
        weights['completed'] = 1 - (weights['cancelled'] + weights['failed'] + weights['pending'])
    
    statuses = list(weights.keys())
    probabilities = list(weights.values())
    return np.random.choice(statuses, p=probabilities)

def is_merchant_open(merchant_info, hour):
    """Check if merchant is open at given hour"""
    hours = merchant_info.get('hours', {'open': 8, 'close': 20})
    open_hour = hours['open']
    close_hour = hours['close']
    
    if close_hour == 24 or close_hour == 0:
        return True
    elif close_hour < open_hour:
        return hour >= open_hour or hour <= close_hour
    else:
        return open_hour <= hour < close_hour

def generate_realistic_time(category, date_obj):
    """Generate realistic transaction time based on category artiste day"""
    peak_hours = PEAK_HOURS.get(category, [12, 13, 17, 18])
    
    if date_obj.weekday() >= 5 and category in ['entertainment', 'restaurants', 'alcohol']:
        peak_hours = [min(h + 2, 23) for h in peak_hours]
    
    hour = np.random.choice(peak_hours) if random.random() < 0.6 else np.random.randint(8, 22)
    minute = np.random.randint(0, 60)
    second = np.random.randint(0, 60)
    
    return f"{hour:02d}:{minute:02d}:{second:02d}"

def generate_alcohol_transactions(eligible_accounts, customers_df, balance_tracker, 
                                 single_date, txn_counter, year):
    """Generate alcohol purchases on drinking days (Thu-Sun)"""
    if single_date.weekday() not in [3, 4, 5, 6]:
        return pd.DataFrame(), txn_counter
    
    acc_cust = eligible_accounts.merge(customers_df, on="customer_id", how="left")
    
    drinking_customers = acc_cust[
        (acc_cust['age'] >= 18) & 
        (acc_cust['occupation'] != 'Student') &
        (acc_cust['age'] <= 65)
    ]
    
    if drinking_customers.empty:
        logger.debug(f"No eligible customers for alcohol transactions on {single_date}")
        return pd.DataFrame(), txn_counter
    
    if single_date.weekday() in [5, 6]:
        drink_probability = 0.18
        time_range = (16, 23)
    elif single_date.weekday() == 4:
        drink_probability = 0.15
        time_range = (17, 22)
    else:
        drink_probability = 0.08
        time_range = (18, 21)
    
    n_drinkers = int(len(drinking_customers) * drink_probability)
    if n_drinkers == 0:
        return pd.DataFrame(), txn_counter
    
    selected_customers = drinking_customers.sample(n=n_drinkers)
    
    transactions = []
    successful_transactions = 0
    
    for _, customer in selected_customers.iterrows():
        distress_level = customer.get('distress_level', 0.0)
        if distress_level > 0.5 and random.random() < 0.5:
            continue  # Skip some discretionary spending for distressed
        
        merchant_info = get_merchant_info('alcohol', 1)[0]
        
        amount = calculate_transaction_amount(
            merchant_info, customer.get('age', 30), 
            max(0, customer.get('income', 25000)), 'alcohol', distress_level
        )
        
        if not balance_tracker.can_transact(customer['account_id'], amount):
            current_balance = balance_tracker.get_balance(customer['account_id'])
            overdraft_limit = balance_tracker.account_balances[customer['account_id']]['overdraft_limit']
            max_amount = current_balance - overdraft_limit - 10
            
            if max_amount < 50:
                continue
            amount = min(amount, max_amount)
        
        if balance_tracker.process_transaction(customer['account_id'], amount, 'debit'):
            hour = np.random.randint(time_range[0], time_range[1] + 1)
            minute = np.random.randint(0, 60)
            time_str = f"{hour:02d}:{minute:02d}:00"
            
            transactions.append({
                "transaction_id": f"TXN{year}{txn_counter + successful_transactions:06d}",
                "account_id": customer['account_id'],
                "transaction_date": single_date.strftime("%Y-%m-%d"),
                "transaction_time": time_str,
                "amount": amount,
                "debit_credit": "debit",
                "category": "alcohol",
                "status": get_transaction_status_weighted(merchant_info['size'], single_date, distress_level),
                "description": f"Purchase at {merchant_info['name']}",
                "immediate_payment": False,
                "receiving_account": "",
                "receiving_bank": "",
                "transaction_cost": 0.0,
                "ewallet_number": None,
                "channel": "pos",
                "merchant_name": merchant_info['name']
            })
            successful_transactions += 1
    
    if transactions:
        logger.debug(f"Generated {len(transactions)} alcohol transactions for {single_date}")
        return pd.DataFrame(transactions), txn_counter + successful_transactions
    return pd.DataFrame(), txn_counter

def create_initial_deposits(eligible_accounts, customers_df, balance_tracker, date, txn_counter, year):
    """Create initial deposit transactions and update balance tracker"""
    n_accounts = len(eligible_accounts)
    deposit_amounts = np.maximum(100, np.random.uniform(200, 8000, n_accounts)).round(2)
    
    transactions = []
    
    for i, (_, account) in enumerate(eligible_accounts.iterrows()):
        amount = deposit_amounts[i]
        
        balance_tracker.initialize_account_balance(
            account['account_id'], 
            account.get('account_type', 'Standard Banking'),
            amount
        )
        
        transactions.append({
            "transaction_id": f"TXN{year}{txn_counter + i:06d}",
            "account_id": account['account_id'],
            "transaction_date": date,
            "transaction_time": fake.time(),
            "amount": amount,
            "debit_credit": "credit",
            "category": "initial deposit",
            "status": "completed",
            "description": "Account opening deposit",
            "immediate_payment": False,
            "receiving_account": account['account_id'],
            "receiving_bank": "",
            "transaction_cost": 0.0,
            "ewallet_number": None,
            "channel": np.random.choice(["branch", "online banking"]),
            "merchant_name": ""
        })
    
    logger.info(f"Created {len(transactions)} initial deposits for {year}")
    return pd.DataFrame(transactions), txn_counter + n_accounts

def generate_category_transactions(category, eligible_accounts, customers_df, balance_tracker,
                                 single_date, base_count, txn_counter, year):
    """Generate transactions for a specific category with balance checking"""
    weekend_mult = WEEKEND_MULTIPLIERS.get(category, 1.0) if single_date.weekday() >= 5 else 1.0
    payday_mult = PAYDAY_MULTIPLIERS.get(category, 1.0) if 25 <= single_date.day <= 28 else 1.0
    cash_flow_mult = 0.6 if 1 <= single_date.day <= 7 and category not in ['utilities', 'medical'] else 1.0
    
    final_count = int(base_count * weekend_mult * payday_mult * cash_flow_mult)
    
    if final_count == 0:
        return pd.DataFrame(), txn_counter
    
    acc_cust = eligible_accounts.merge(customers_df, on="customer_id", how="left")
    
    transactions = []
    successful_transactions = 0
    
    for _ in range(final_count):
        customer = acc_cust.sample(n=1).iloc[0]
        distress_level = customer.get('distress_level', 0.0)
        
        if distress_level > 0.5 and category in ['alcohol', 'entertainment', 'clothing', 'restaurants'] and random.random() < 0.5:
            continue  # Skip some discretionary for distressed
        
        merchant_info = get_merchant_info(category, 1)[0]
        
        hour = int(generate_realistic_time(category, single_date).split(':')[0])
        if not is_merchant_open(merchant_info, hour):
            continue
        
        amount = calculate_transaction_amount(
            merchant_info, customer.get('age', 30),
            max(0, customer.get('income', 25000)), category, distress_level
        )
        
        if not balance_tracker.can_transact(customer['account_id'], amount):
            current_balance = balance_tracker.get_balance(customer['account_id'])
            overdraft_limit = balance_tracker.account_balances[customer['account_id']]['overdraft_limit']
            max_amount = current_balance - overdraft_limit - 20
            
            if max_amount < 10:
                continue
            amount = min(amount, max_amount)
        
        if balance_tracker.process_transaction(customer['account_id'], amount, 'debit'):
            transactions.append({
                "transaction_id": f"TXN{year}{txn_counter + successful_transactions:06d}",
                "account_id": customer['account_id'],
                "transaction_date": single_date.strftime("%Y-%m-%d"),
                "transaction_time": generate_realistic_time(category, single_date),
                "amount": amount,
                "debit_credit": "debit",
                "category": category,
                "status": get_transaction_status_weighted(merchant_info['size'], single_date, distress_level),
                "description": f"Purchase at {merchant_info['name']}",
                "immediate_payment": False,
                "receiving_account": "",
                "receiving_bank": "",
                "transaction_cost": 0.0,
                "ewallet_number": None,
                "channel": "pos" if category in ['groceries', 'clothing', 'fuel'] else np.random.choice(["pos", "online banking"]),
                "merchant_name": merchant_info['name']
            })
            successful_transactions += 1
    
    if transactions:
        logger.debug(f"Generated {len(transactions)} {category} transactions for {single_date}")
        return pd.DataFrame(transactions), txn_counter + successful_transactions
    return pd.DataFrame(), txn_counter

def generate_salary_payments(eligible_accounts, customers_df, balance_tracker,
                           single_date, txn_counter, year):
    """Generate salary payments with balance updates"""
    if not (23 <= single_date.day <= 28):
        return pd.DataFrame(), txn_counter
    
    acc_cust = eligible_accounts.merge(customers_df, on="customer_id", how="left")
    working_customers = acc_cust[acc_cust["occupation"] != "Unemployed"]
    
    if working_customers.empty:
        logger.debug(f"No working customers for salary payments on {single_date}")
        return pd.DataFrame(), txn_counter
    
    n_salary = int(len(working_customers) * 0.8)
    selected_customers = working_customers.sample(n=n_salary)
    
    transactions = []
    
    for i, (_, customer) in enumerate(selected_customers.iterrows()):
        base_salary = customer.get('income', np.random.normal(25000, 8000))
        base_salary = max(4000, abs(base_salary))
        monthly_salary = max(4000, np.random.normal(base_salary, base_salary * 0.2))
        
        balance_tracker.process_transaction(customer['account_id'], monthly_salary, 'credit')
        
        transactions.append({
            "transaction_id": f"TXN{year}{txn_counter + i:06d}",
            "account_id": customer['account_id'],
            "transaction_date": single_date.strftime("%Y-%m-%d"),
            "transaction_time": f"{np.random.randint(6, 10):02d}:00:00",
            "amount": round(monthly_salary, 2),
            "debit_credit": "credit",
            "category": "salary payment",
            "status": "completed",
            "description": "Monthly salary",
            "immediate_payment": False,
            "receiving_account": customer['account_id'],
            "receiving_bank": "",
            "transaction_cost": 0.0,
            "ewallet_number": None,
            "channel": np.random.choice(["online banking", "mobile banking app"]),
            "merchant_name": ""
        })
    
    logger.debug(f"Generated {len(transactions)} salary payments for {single_date}")
    return pd.DataFrame(transactions), txn_counter + len(transactions)

def generate_transactions_for_month(year, month, eligible_accounts, customers_df, 
                                   balance_tracker, txn_counter, loan_transactions_df, debit_order_transactions_df):
    """Generate transactions for a specific month with balance tracking"""
    transactions = []
    
    start_date = datetime(year, month, 1)
    end_date = (start_date + pd.offsets.MonthEnd(0)).date()
    date_range = pd.date_range(start_date, end_date)
    
    for single_date in tqdm(date_range, desc=f"Processing {year}-{month:02d}"):
        day_transactions = []
        
        date_str = single_date.strftime("%Y-%m-%d")
        
        # Include loan and debit order transactions for the day
        daily_loans = loan_transactions_df[loan_transactions_df['transaction_date'] == date_str]
        if not daily_loans.empty:
            day_transactions.append(daily_loans)
            for _, loan in daily_loans.iterrows():
                balance_tracker.process_transaction(loan['account_id'], loan['amount'], loan['debit_credit'])
        
        daily_debit_orders = debit_order_transactions_df[debit_order_transactions_df['transaction_date'] == date_str]
        if not daily_debit_orders.empty:
            day_transactions.append(daily_debit_orders)
            for _, debit_order in daily_debit_orders.iterrows():
                balance_tracker.process_transaction(debit_order['account_id'], debit_order['amount'], debit_order['debit_credit'])
        
        salary_txns, txn_counter = generate_salary_payments(
            eligible_accounts, customers_df, balance_tracker, single_date, txn_counter, year
        )
        if not salary_txns.empty:
            day_transactions.append(salary_txns)
        
        if 1 <= single_date.day <= 5:
            acc_cust = eligible_accounts.merge(customers_df, on="customer_id", how="left")
            unemployed_customers = acc_cust[acc_cust["occupation"] == "Unemployed"]
            
            if not unemployed_customers.empty:
                for _, customer in unemployed_customers.iterrows():
                    balance_tracker.process_transaction(customer['account_id'], 350, 'credit')
                
                grants_df = pd.DataFrame({
                    "transaction_id": [f"TXN{year}{i:06d}" for i in range(txn_counter, txn_counter + len(unemployed_customers))],
                    "account_id": unemployed_customers['account_id'].values,
                    "transaction_date": date_str,
                    "transaction_time": "08:00:00",
                    "amount": 350.00,
                    "debit_credit": "credit",
                    "category": "government grant",
                    "status": "completed",
                    "description": "Monthly social grant",
                    "immediate_payment": False,
                    "receiving_account": unemployed_customers['account_id'].values,
                    "receiving_bank": "",
                    "transaction_cost": 0.0,
                    "ewallet_number": None,
                    "channel": "branch",
                    "merchant_name": ""
                })
                day_transactions.append(grants_df)
                txn_counter += len(unemployed_customers)
        
        alcohol_txns, txn_counter = generate_alcohol_transactions(
            eligible_accounts, customers_df, balance_tracker, single_date, txn_counter, year
        )
        if not alcohol_txns.empty:
            day_transactions.append(alcohol_txns)
        
        categories_and_counts = [
            ('groceries', random.randint(80, 150)),
            ('clothing', random.randint(8, 20)),
            ('fuel', random.randint(15, 25)),
            ('restaurants', random.randint(10, 25)),
            ('retail', random.randint(5, 15)),
            ('transport', random.randint(20, 40)),
            ('entertainment', random.randint(3, 10)),
            ('medical', random.randint(2, 8)),
            ('utilities', random.randint(5, 12)),
            ('airtime', random.randint(15, 30))
        ]
        
        for category, base_count in categories_and_counts:
            cat_txns, txn_counter = generate_category_transactions(
                category, eligible_accounts, customers_df, balance_tracker,
                single_date, base_count, txn_counter, year
            )
            if not cat_txns.empty:
                day_transactions.append(cat_txns)
        
        if day_transactions:
            transactions.extend(day_transactions)
    
    if transactions:
        combined = pd.concat(transactions, ignore_index=True)
        logger.info(f"Generated {len(combined)} transactions for {year}-{month:02d}")
        return combined, txn_counter
    logger.debug(f"No transactions generated for {year}-{month:02d}")
    return pd.DataFrame(), txn_counter

def merge_monthly_transactions(month_transactions, loan_transactions_df, debit_order_transactions_df, year, month):
    """Merge base, loan, and debit order transactions for a specific month"""
    def standardize_columns(df):
        df = df.copy()
        df.columns = df.columns.str.lower()
        return df
    
    month_transactions = standardize_columns(month_transactions)
    monthly_loan_transactions = loan_transactions_df[
        loan_transactions_df['transaction_date'].str.startswith(f"{year}-{month:02d}")
    ]
    monthly_debit_order_transactions = debit_order_transactions_df[
        debit_order_transactions_df['transaction_date'].str.startswith(f"{year}-{month:02d}")
    ]
    
    monthly_loan_transactions = standardize_columns(monthly_loan_transactions)
    monthly_debit_order_transactions = standardize_columns(monthly_debit_order_transactions)
    
    all_columns = set(month_transactions.columns) | set(monthly_loan_transactions.columns) | set(monthly_debit_order_transactions.columns)
    
    for col in all_columns:
        if col not in month_transactions.columns:
            month_transactions[col] = None
        if col not in monthly_loan_transactions.columns:
            monthly_loan_transactions[col] = None if col in ['loan_id', 'debit_order_id', 'customer_id'] else ""
        if col not in monthly_debit_order_transactions.columns:
            monthly_debit_order_transactions[col] = None if col in ['loan_id', 'debit_order_id', 'customer_id'] else ""
    
    columns_order = sorted(all_columns)
    combined = pd.concat([
        month_transactions[columns_order],
        monthly_loan_transactions[columns_order],
        monthly_debit_order_transactions[columns_order]
    ], ignore_index=True)
    
    logger.info(f"Merged {len(combined)} transactions for {year}-{month:02d}")
    return combined

def generate_transactions_for_year(year):
    """Generate transactions for a specific year with enhanced balance tracking"""
    logger.info(f"Generating enhanced transactions for {year}...")
    
    accounts_df, customers_df = load_banking_data_for_year(year)
    eligible_accounts = accounts_df.copy()
    logger.info(f"Loaded {len(accounts_df)} accounts and {len(customers_df)} customers for {year}")
    
    if eligible_accounts.empty:
        logger.error(f"No accounts available for {year}. Skipping transaction generation.")
        return
    
    loan_transactions_df, debit_order_transactions_df = load_existing_transactions(year, accounts_df)
    
    balance_tracker = BalanceTracker()
    
    temp_dir = f"banking_data/transactions_by_year/temp_{year}"
    try:
        os.makedirs(temp_dir, exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to create temp directory {temp_dir}: {e}")
        return
    
    txn_counter = 1
    monthly_files = []
    
    new_accounts = eligible_accounts[eligible_accounts["account_id"].str.contains(str(year))]
    if not new_accounts.empty:
        initial_deposits, txn_counter = create_initial_deposits(
            new_accounts, customers_df, balance_tracker, f"{year}-01-01", txn_counter, year
        )
        initial_file = f"{temp_dir}/initial_deposits.parquet"
        initial_deposits.to_parquet(initial_file, index=False, engine="fastparquet")
        monthly_files.append(initial_file)
    
    existing_accounts = eligible_accounts[~eligible_accounts["account_id"].str.contains(str(year))]
    for _, account in existing_accounts.iterrows():
        estimated_balance = np.random.uniform(500, 15000)
        balance_tracker.initialize_account_balance(
            account['account_id'],
            account.get('account_type', 'Standard Banking'),
            estimated_balance
        )
    
    preprocess_scheduled_transactions(balance_tracker, loan_transactions_df, debit_order_transactions_df)
    
    for month in range(1, 13):
        month_transactions, txn_counter = generate_transactions_for_month(
            year, month, eligible_accounts, customers_df, balance_tracker, 
            txn_counter, loan_transactions_df, debit_order_transactions_df
        )
        
        if not month_transactions.empty:
            # Merge with loan and debit order transactions for the month
            combined_month = merge_monthly_transactions(
                month_transactions, loan_transactions_df, debit_order_transactions_df, year, month
            )
            month_file = f"banking_data/transactions_by_year/transactions_{year}_{month:02d}.parquet"
            combined_month.to_parquet(month_file, index=False, engine="fastparquet")
            monthly_files.append(month_file)
            logger.info(f"Saved {len(combined_month)} transactions to {month_file}")
    
    # Clean up temporary files
    for f in monthly_files:
        if os.path.exists(f) and "temp_" in f:
            try:
                os.remove(f)
            except OSError as e:
                logger.warning(f"Failed to remove temp file {f}: {e}")
    try:
        os.rmdir(temp_dir)
    except OSError as e:
        logger.warning(f"Failed to remove temp directory {temp_dir}: {e}")
    
    logger.info(f"Generated transactions for {year}, saved as monthly files in banking_data/transactions_by_year/")

def main():
    """Main function to generate enhanced transaction data"""
    try:
        year = int(input("Enter the year to process: "))
        if year < 2000:
            logger.error("Year must be 2000 or later.")
            return None
    except ValueError:
        logger.error("Please enter a valid integer year.")
        return None
    
    try:
        os.makedirs("banking_data/transactions_by_year", exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to create output directory: {e}")
        return None
    
    generate_transactions_for_year(year)
    
    logger.info(f"\nTransaction files generated for {year}:")
    for month in range(1, 13):
        month_file = f"banking_data/transactions_by_year/transactions_{year}_{month:02d}.parquet"
        if os.path.exists(month_file):
            file_size = os.path.getsize(month_file) / (1024 * 1024)  # Size in MB
            logger.info(f"{month_file}: {file_size:.2f} MB")
    
    return None

if __name__ == "__main__":
    main()