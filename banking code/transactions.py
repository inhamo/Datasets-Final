import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os
from tqdm import tqdm
import logging
from collections import defaultdict
from sa_merchant import (SA_COMPANIES, STATUS_WEIGHTS, PEAK_HOURS, WEEKEND_MULTIPLIERS,
                         PAYDAY_MULTIPLIERS, AGE_SPENDING_PREFERENCES, INCOME_SPENDING_MULTIPLIERS)

# Constants
TRANSACTION_STATUSES = ['completed', 'failed', 'cancelled', 'pending']
CHANNELS = ['pos', 'branch', 'atm', 'online banking', 'mobile banking app']
FAKER = Faker('zu_ZA')

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)

class AccountBalanceManager:
    """
    Manages account balances, transaction history, and activation rules.
    Enforces 7-day grace period after first credit before debits are allowed.
    Handles account statuses: active (full), dormant/frozen (credits only), closed (none).
    Tracks whether first transaction has occurred to control branch usage.
    """
    def __init__(self):
        self.balances = {}
        self.histories = {}
        self.first_credits = {}
        self.pending_deposits = {}
        self.first_transaction_done = {}  # Track if first transaction has occurred
        self.overdraft_limits = {
            'Premium Banking': -10000,
            'Gold Banking': -5000,
            'Standard Banking': -2000,
            'Basic Banking': -500,
            'Student Banking': -200,
            'Business Banking': -20000
        }

    def setup_account(self, account_id, account_type, initial_balance=0, open_date=None, status='active', status_change_date=None):
        """Initialize an account with zero or initial balance."""
        if open_date is None:
            open_date = datetime(1900, 1, 1).date()
        if status_change_date is None or pd.isna(status_change_date):
            status_change_date = None
        else:
            status_change_date = pd.to_datetime(status_change_date).date()
        self.balances[account_id] = {
            'balance': initial_balance,
            'type': account_type,
            'overdraft': self.overdraft_limits.get(account_type, -1000),
            'last_tx_time': datetime(1900, 1, 1),
            'open_date': open_date,
            'status': status,
            'status_change_date': status_change_date,
            'activated': initial_balance > 0
        }
        self.histories[account_id] = []
        self.first_transaction_done[account_id] = initial_balance > 0  # First tx done if initial balance exists
        if initial_balance > 0 and open_date:
            self.first_credits[account_id] = open_date
        LOGGER.debug(f"Set up {account_id} with balance {initial_balance}, status {status}")

    def get_status_at_date(self, account_id, check_date):
        """Determine account status at a given date."""
        if account_id not in self.balances:
            return 'closed'
        info = self.balances[account_id]
        change_date = info['status_change_date']
        if change_date and check_date >= change_date:
            return info['status']
        return 'active'

    def is_transaction_allowed(self, account_id, is_debit, tx_date):
        """Check if a transaction (debit or credit) is permissible based on status."""
        status = self.get_status_at_date(account_id, tx_date)
        if status == 'closed':
            return False
        if is_debit and status in ['frozen', 'dormant']:
            return False
        return True

    def is_debit_allowed(self, account_id, amount, tx_date=None):
        """Check if a debit transaction is permissible."""
        if not self.is_transaction_allowed(account_id, True, tx_date):
            return False
        if account_id not in self.balances:
            return False
        info = self.balances[account_id]
        if not info['activated']:
            return False
        if account_id in self.first_credits and tx_date:
            days_passed = (tx_date - self.first_credits[account_id]).days
            if days_passed < 7:
                return False
        projected_balance = info['balance'] - amount
        return projected_balance >= info['overdraft']

    def execute_tx(self, account_id, amount, is_debit=True, tx_time=None):
        """Process a transaction and update balance."""
        if account_id not in self.balances:
            return False
        if tx_time is None:
            check_date = datetime.now().date()
        else:
            check_date = tx_time.date()
        if not self.is_transaction_allowed(account_id, is_debit, check_date):
            return False
        info = self.balances[account_id]
        tx_date = check_date
        if is_debit:
            if not self.is_debit_allowed(account_id, amount, tx_date):
                return False
            info['balance'] -= amount
        else:
            info['balance'] += amount
            if not info['activated']:
                info['activated'] = True
                if tx_time and account_id not in self.first_credits:
                    self.first_credits[account_id] = tx_time.date()
        if tx_time:
            info['last_tx_time'] = tx_time
        self.histories[account_id].append({
            'time': tx_time,
            'direction': 'debit' if is_debit else 'credit',
            'amount': amount
        })
        self.first_transaction_done[account_id] = True  # Mark first transaction done
        LOGGER.debug(f"Executed {'debit' if is_debit else 'credit'} of {amount} for {account_id}; balance now {info['balance']}")
        return True

    def current_balance(self, account_id):
        """Retrieve current balance for an account."""
        return self.balances.get(account_id, {}).get('balance', 0)

class FraudInjector:
    """
    Handles selection and injection of fraud patterns into transactions.
    """
    def __init__(self, year):
        self.year = year
        self.suspect_accounts = set()
        self.smurfing_accounts = []
        self.high_velocity_accounts = []
        self.cycle_pairs = []
        self.reactivated_dormants = []

    def pick_suspects(self, accounts_df, customers_df):
        """Randomly select accounts for fraud types."""
        merged = accounts_df.merge(customers_df, on='customer_id', how='left')
        merged = merged[~(merged['status'] == 'closed') | merged['status_change_date'].isna()]
        total_accounts = len(merged)

        smurf_count = max(3, int(total_accounts * 0.025))
        self.smurfing_accounts = merged.sample(n=smurf_count)['account_id'].tolist()

        velocity_count = max(2, int(total_accounts * 0.01))
        remaining = merged[~merged['account_id'].isin(self.smurfing_accounts)]
        if len(remaining) >= velocity_count:
            self.high_velocity_accounts = remaining.sample(n=velocity_count)['account_id'].tolist()

        pair_count = max(2, int(total_accounts * 0.008))
        further_remaining = remaining[~remaining['account_id'].isin(self.high_velocity_accounts)]
        if len(further_remaining) >= pair_count * 2:
            sampled = further_remaining.sample(n=pair_count * 2)
            self.cycle_pairs = [(sampled.iloc[i]['account_id'], sampled.iloc[i + 1]['account_id'])
                                for i in range(0, len(sampled) - 1, 2)]

        dormant_count = max(2, int(total_accounts * 0.005))
        final_remaining = further_remaining[~further_remaining['account_id'].isin(
            [acc for pair in self.cycle_pairs for acc in pair]
        )]
        if len(final_remaining) >= dormant_count:
            self.reactivated_dormants = final_remaining.sample(n=dormant_count)['account_id'].tolist()

        self.suspect_accounts = set(
            self.smurfing_accounts + self.high_velocity_accounts +
            [acc for pair in self.cycle_pairs for acc in pair] + self.reactivated_dormants
        )
        LOGGER.info(f"Fraud selection: Smurfing {len(self.smurfing_accounts)}, Velocity {len(self.high_velocity_accounts)}, "
                    f"Pairs {len(self.cycle_pairs)}, Dormants {len(self.reactivated_dormants)}")

def fetch_data_up_to_year(target_year):
    """Load and merge accounts and customers data from 2018 to target_year."""
    account_frames = []
    customer_frames = []
    for yr in range(2018, target_year + 1):
        try:
            acc_df = pd.read_parquet(f"banking_data/accounts_{yr}.parquet")
            cust_df = pd.read_parquet(f"banking_data/customers_{yr}.parquet")
            cust_df.rename(columns={'CustomerID': 'customer_id'}, inplace=True)
            if 'age' not in cust_df:
                cust_df['age'] = np.random.randint(18, 80, len(cust_df))
            if 'income' not in cust_df:
                cust_df['income'] = np.maximum(4000, np.random.normal(25000, 8000, len(cust_df)))
            if 'occupation' not in cust_df:
                cust_df['occupation'] = np.random.choice(['Employed', 'Self-Employed', 'Unemployed', 'Student'], len(cust_df))
            if 'distress_level' not in cust_df:
                def distress_calc(inc):
                    if inc < 15000:
                        return np.random.choice([0.0, 0.5, 0.8], p=[0.6, 0.3, 0.1])
                    elif inc > 40000:
                        return np.random.choice([0.0, 0.5, 0.8], p=[0.8, 0.15, 0.05])
                    else:
                        return np.random.choice([0.0, 0.5, 0.8], p=[0.7, 0.25, 0.05])
                cust_df['distress_level'] = cust_df['income'].apply(distress_calc)
            if 'online_banking_enabled' not in acc_df:
                acc_df['online_banking_enabled'] = np.random.choice([0, 1], len(acc_df), p=[0.3, 0.7])
            account_frames.append(acc_df)
            customer_frames.append(cust_df)
            LOGGER.info(f"Loaded {len(acc_df)} accounts and {len(cust_df)} customers for {yr}")
        except FileNotFoundError:
            LOGGER.warning(f"Missing data for {yr}")
            continue

    if account_frames:
        accounts = pd.concat(account_frames, ignore_index=True).sort_values('account_id').drop_duplicates('account_id', keep='last')
    else:
        accounts = pd.DataFrame(columns=['account_id', 'customer_id', 'account_type', 'online_banking_enabled'])

    if customer_frames:
        customers = pd.concat(customer_frames, ignore_index=True).sort_values('customer_id').drop_duplicates('customer_id', keep='last')
    else:
        customers = pd.DataFrame(columns=['customer_id', 'age', 'income', 'occupation', 'distress_level'])

    if 'status' not in accounts.columns:
        accounts['status'] = np.random.choice(['active', 'dormant', 'frozen', 'closed'], len(accounts), p=[0.85, 0.05, 0.03, 0.07])
    if 'status_change_date' not in accounts.columns:
        accounts['status_change_date'] = pd.NaT
        mask = accounts['status'] != 'active'
        if mask.sum() > 0:
            for idx in accounts[mask].index:
                open_d = accounts.at[idx, 'opening_date']
                if pd.isna(open_d):
                    continue
                open_d = pd.to_datetime(open_d, errors='coerce').date()
                months_after = random.randint(1, 24)
                change_d = open_d + pd.DateOffset(months=months_after)
                accounts.at[idx, 'status_change_date'] = change_d
    accounts['opening_date'] = pd.to_datetime(accounts['opening_date'], errors='coerce')
    accounts['status_change_date'] = pd.to_datetime(accounts['status_change_date'], errors='coerce')

    return accounts, customers

def load_scheduled_txs(year, accounts):
    """Load loan payments and debit orders for the year."""
    try:
        loans = pd.read_parquet(f"banking_data/loan_payment_transactions_{year}.parquet", engine='fastparquet')
        debits = pd.read_parquet(f"banking_data/debit_order_transactions_{year}.parquet", engine='fastparquet')
        loans.drop(columns=['loan_type'], errors='ignore', inplace=True)
        debits.drop(columns=['payment_type'], errors='ignore', inplace=True)
        valid_ids = set(accounts['account_id'])
        loans = loans[loans['account_id'].isin(valid_ids)]
        debits = debits[debits['account_id'].isin(valid_ids)]
        LOGGER.info(f"Loaded {len(loans)} loans and {len(debits)} debits for {year}")
        return loans, debits
    except FileNotFoundError:
        LOGGER.warning(f"No scheduled transactions found for {year}")
        return pd.DataFrame(), pd.DataFrame()

def pick_merchant(cat, count=1):
    """Select merchant details for a category."""
    cat_lower = cat.lower()
    if cat_lower not in SA_COMPANIES:
        base_merch = {'name': FAKER.company(), 'size': 'medium', 'avg_transaction': 200, 'std_deviation': 60,
                      'hours': {'open': 8, 'close': 20}}
        return [base_merch] * count
    merchants = SA_COMPANIES[cat_lower]
    indices = np.random.choice(len(merchants), count, replace=True)
    return [merchants[i] for i in indices]

def age_bucket(age):
    """Map age to spending preference group."""
    if pd.isna(age):
        return '26-35'
    age_int = int(age)
    if age_int < 26: return '18-25'
    if age_int < 36: return '26-35'
    if age_int < 51: return '36-50'
    if age_int < 66: return '51-65'
    return '65+'

def income_tier(inc):
    """Map income to spending tier."""
    if pd.isna(inc):
        return 'medium'
    inc_float = float(inc)
    if inc_float < 15000: return 'low'
    if inc_float < 40000: return 'medium'
    if inc_float < 80000: return 'high'
    return 'premium'

def compute_amount(merchant, age, income, cat, distress=0.0):
    """Generate realistic transaction amount based on factors."""
    base = np.random.normal(merchant['avg_transaction'], merchant.get('std_deviation', merchant['avg_transaction'] * 0.3))
    age_grp = age_bucket(age)
    age_mult = AGE_SPENDING_PREFERENCES.get(age_grp, {}).get(cat, 1.0)
    inc_grp = income_tier(income)
    inc_mults = INCOME_SPENDING_MULTIPLIERS.get(inc_grp, {'normal': 1.0, 'distressed': 0.6, 'categories': {}})
    norm_wt = 1.0 - distress
    base_inc_mult = norm_wt * inc_mults['normal'] + distress * inc_mults['distressed']
    cat_mult = inc_mults['categories'].get(cat, base_inc_mult)
    final = max(10, base * age_mult * cat_mult)
    return round(final, 2)

def add_errors(tx_dict):
    """Occasionally introduce data quality issues."""
    if random.random() < 0.02:
        err_type = random.choice(['missing_field', 'wrong_status', 'timestamp_error'])
        if err_type == 'missing_field':
            if random.random() < 0.5:
                tx_dict['merchant_name'] = None
            else:
                tx_dict['description'] = None
        elif err_type == 'wrong_status':
            if tx_dict['status'] == 'completed':
                tx_dict['status'] = 'pending'
        elif err_type == 'timestamp_error':
            tx_dict['transaction_time'] = '00:00:00'
    return tx_dict

def get_available_channels(account_id, accounts_df):
    """Determine available channels based on online_banking_enabled."""
    acc = accounts_df[accounts_df['account_id'] == account_id]
    if acc.empty or acc['online_banking_enabled'].iloc[0] == 0:
        return ['pos', 'branch', 'atm']
    return CHANNELS

def create_smurfing_txs(account_id, base_date, tx_id_start, year, manager, accounts_df):
    """Generate multiple cash deposits with varied amounts below reporting threshold."""
    txs = []
    num_txs = random.randint(1, 3)
    threshold = 50000
    available_channels = get_available_channels(account_id, accounts_df)
    for i in range(num_txs):
        offset_days = random.randint(-7, 7)
        dep_date = base_date + timedelta(days=offset_days)
        hr = random.randint(9, 16)
        mn = random.randint(0, 59)
        tme = f"{hr:02d}:{mn:02d}:00"
        amt = round(random.uniform(threshold * 0.50, threshold * 0.95), 2)
        channel = 'branch' if not manager.first_transaction_done.get(account_id, False) else random.choice(available_channels)
        tx_cost = round(random.uniform(10, 50), 2) if channel in ['branch', 'atm'] else 0.0
        tx = {
            'transaction_id': f"TXN{year}{tx_id_start + i:06d}",
            'account_id': account_id,
            'transaction_date': dep_date.strftime('%Y-%m-%d'),
            'transaction_time': tme,
            'amount': amt,
            'debit_credit': 'credit',
            'category': 'cash deposit',
            'status': 'completed',
            'description': 'Cash deposit',
            'immediate_payment': False,
            'receiving_account': account_id,
            'receiving_bank': 'Same Bank',
            'transaction_cost': tx_cost,
            'channel': channel,
            'merchant_name': ''
        }
        dt = pd.to_datetime(dep_date.strftime('%Y-%m-%d') + ' ' + tme)
        if manager.execute_tx(account_id, amt, False, dt):
            txs.append(add_errors(tx))
            if random.random() < 0.5:
                merch = pick_merchant('retail')[0]
                debit_amt = compute_amount(merch, 30, 25000, 'retail', 0.0)
                debit_time = f"{hr + 1:02d}:{random.randint(0, 59):02d}:00"
                debit_dt = pd.to_datetime(dep_date.strftime('%Y-%m-%d') + ' ' + debit_time)
                if manager.is_debit_allowed(account_id, debit_amt, debit_dt.date()):
                    manager.execute_tx(account_id, debit_amt, True, debit_dt)
                    debit_tx = {
                        'transaction_id': f"TXN{year}{tx_id_start + i + 1:06d}",
                        'account_id': account_id,
                        'transaction_date': dep_date.strftime('%Y-%m-%d'),
                        'transaction_time': debit_time,
                        'amount': debit_amt,
                        'debit_credit': 'debit',
                        'category': 'retail',
                        'status': 'completed',
                        'description': f"Purchase at {merch['name']}",
                        'immediate_payment': False,
                        'receiving_account': '',
                        'receiving_bank': '',
                        'transaction_cost': 0.0,
                        'channel': random.choice(available_channels),
                        'merchant_name': merch['name']
                    }
                    txs.append(add_errors(debit_tx))
                    i += 1
    return txs, len(txs)

def create_velocity_burst(account_id, base_date, manager, tx_id_start, year, accounts_df):
    """Generate rapid-fire transactions."""
    txs = []
    num_txs = random.randint(8, 15)
    start_hr = random.randint(10, 18)
    available_channels = get_available_channels(account_id, accounts_df)
    for i in range(num_txs):
        mins = random.randint(0, 119)
        hr = start_hr + (mins // 60)
        mnt = mins % 60
        sec = random.randint(0, 59)
        tme = f"{hr:02d}:{mnt:02d}:{sec:02d}"
        amt = round(random.uniform(500, 2500), 2)
        merch = pick_merchant(random.choice(['retail', 'electronics']))[0]
        dt = pd.to_datetime(base_date.strftime('%Y-%m-%d') + ' ' + tme)
        status = 'failed'
        if manager.is_debit_allowed(account_id, amt, dt.date()):
            status = random.choices(['completed', 'failed'], weights=[0.75, 0.25])[0]
            if status == 'completed':
                manager.execute_tx(account_id, amt, True, dt)
        channel = random.choice(available_channels)
        tx = {
            'transaction_id': f"TXN{year}{tx_id_start + i:06d}",
            'account_id': account_id,
            'transaction_date': base_date.strftime('%Y-%m-%d'),
            'transaction_time': tme,
            'amount': amt,
            'debit_credit': 'debit',
            'category': 'retail',
            'status': status,
            'description': f"Purchase at {merch['name']}",
            'immediate_payment': False,
            'receiving_account': '',
            'receiving_bank': '',
            'transaction_cost': 0.0,
            'channel': channel,
            'merchant_name': merch['name']
        }
        txs.append(add_errors(tx))
    return txs, len(txs)

def create_cycle_txs(pair, base_date, manager, tx_id_start, year, accounts_df):
    """Generate back-and-forth transfers between accounts."""
    acc1, acc2 = pair
    txs = []
    amt = round(random.uniform(25000, 75000), 2)
    tme1 = f"{random.randint(9, 12):02d}:{random.randint(0, 59):02d}:00"
    dt1 = pd.to_datetime(base_date.strftime('%Y-%m-%d') + ' ' + tme1)
    available_channels_acc1 = get_available_channels(acc1, accounts_df)
    if manager.is_debit_allowed(acc1, amt, dt1.date()):
        manager.execute_tx(acc1, amt, True, dt1)
        manager.execute_tx(acc2, amt, False, dt1)
        channel = random.choice(available_channels_acc1)
        tx1 = {
            'transaction_id': f"TXN{year}{tx_id_start:06d}",
            'account_id': acc1,
            'transaction_date': base_date.strftime('%Y-%m-%d'),
            'transaction_time': tme1,
            'amount': amt,
            'debit_credit': 'debit',
            'category': 'transfer',
            'status': 'completed',
            'description': 'Transfer to account',
            'immediate_payment': True,
            'receiving_account': acc2,
            'receiving_bank': 'Same Bank',
            'transaction_cost': 5.50,
            'channel': channel,
            'merchant_name': ''
        }
        txs.append(add_errors(tx1))
    offset = random.randint(1, 2)
    ret_date = base_date + timedelta(days=offset)
    tme2 = f"{random.randint(13, 17):02d}:{random.randint(0, 59):02d}:00"
    ret_amt = round(amt - random.uniform(50, 200), 2)
    dt2 = pd.to_datetime(ret_date.strftime('%Y-%m-%d') + ' ' + tme2)
    available_channels_acc2 = get_available_channels(acc2, accounts_df)
    if manager.is_debit_allowed(acc2, ret_amt, dt2.date()):
        manager.execute_tx(acc2, ret_amt, True, dt2)
        manager.execute_tx(acc1, ret_amt, False, dt2)
        channel = random.choice(available_channels_acc2)
        tx2 = {
            'transaction_id': f"TXN{year}{tx_id_start + 1:06d}",
            'account_id': acc2,
            'transaction_date': ret_date.strftime('%Y-%m-%d'),
            'transaction_time': tme2,
            'amount': ret_amt,
            'debit_credit': 'debit',
            'category': 'transfer',
            'status': 'completed',
            'description': 'Transfer to account',
            'immediate_payment': True,
            'receiving_account': acc1,
            'receiving_bank': 'Same Bank',
            'transaction_cost': 5.50,
            'channel': channel,
            'merchant_name': ''
        }
        txs.append(add_errors(tx2))
    return txs, len(txs)

def create_dormant_spike(account_id, base_date, manager, tx_id_start, year, accounts_df):
    """Simulate sudden activity on inactive account."""
    txs = []
    cred_amt = round(random.uniform(50000, 150000), 2)
    tme_cred = f"{random.randint(9, 11):02d}:{random.randint(0, 59):02d}:00"
    dt_cred = pd.to_datetime(base_date.strftime('%Y-%m-%d') + ' ' + tme_cred)
    available_channels = get_available_channels(account_id, accounts_df)
    channel = 'branch' if not manager.first_transaction_done.get(account_id, False) else random.choice(available_channels)
    tx_cost = round(random.uniform(10, 50), 2) if channel in ['branch', 'atm'] else 0.0
    if manager.execute_tx(account_id, cred_amt, False, dt_cred):
        tx_cred = {
            'transaction_id': f"TXN{year}{tx_id_start:06d}",
            'account_id': account_id,
            'transaction_date': base_date.strftime('%Y-%m-%d'),
            'transaction_time': tme_cred,
            'amount': cred_amt,
            'debit_credit': 'credit',
            'category': 'transfer',
            'status': 'completed',
            'description': 'Transfer from another account',
            'immediate_payment': True,
            'receiving_account': account_id,
            'receiving_bank': 'Same Bank',
            'transaction_cost': tx_cost,
            'channel': channel,
            'merchant_name': ''
        }
        txs.append(add_errors(tx_cred))
    num_spends = random.randint(3, 5)
    for i in range(num_spends):
        spend_amt = round(random.uniform(1000, 10000), 2)
        tme_spend = f"{random.randint(12, 18):02d}:{random.randint(0, 59):02d}:00"
        dt_spend = pd.to_datetime(base_date.strftime('%Y-%m-%d') + ' ' + tme_spend)
        status = 'failed'
        if manager.is_debit_allowed(account_id, spend_amt, dt_spend.date()):
            status = 'completed'
            manager.execute_tx(account_id, spend_amt, True, dt_spend)
        merch = pick_merchant(random.choice(['retail', 'electronics']))[0]
        channel = random.choice(available_channels)
        tx_spend = {
            'transaction_id': f"TXN{year}{tx_id_start + 1 + i:06d}",
            'account_id': account_id,
            'transaction_date': base_date.strftime('%Y-%m-%d'),
            'transaction_time': tme_spend,
            'amount': spend_amt,
            'debit_credit': 'debit',
            'category': 'retail',
            'status': status,
            'description': f"Purchase at {merch['name']}",
            'immediate_payment': False,
            'receiving_account': '',
            'receiving_bank': '',
            'transaction_cost': 0.0,
            'channel': channel,
            'merchant_name': merch['name']
        }
        txs.append(add_errors(tx_spend))
    return txs, len(txs)

def get_account_info(accounts_df, acc_id):
    """Extract account info with defaults."""
    acc = accounts_df[accounts_df['account_id'] == acc_id]
    if acc.empty:
        return 'Standard Banking', pd.to_datetime('1900-01-01'), 'active', pd.NaT, 0
    row = acc.iloc[0]
    atype = row.get('account_type', 'Standard Banking')
    odate = pd.to_datetime(row.get('opening_date', '1900-01-01'), errors='coerce')
    status = row.get('status', 'active')
    scdate = pd.to_datetime(row.get('status_change_date'), errors='coerce')
    online_enabled = row.get('online_banking_enabled', 0)
    return atype, odate.date() if not pd.isna(odate) else datetime(1900, 1, 1).date(), status, scdate, online_enabled

def produce_category_txs(cat, active_accs, custs, manager, date, count, tx_id, year, fraudster, accs):
    """Generate transactions for a specific category."""
    tx_list = []
    success_count = 0
    is_weekend = date.weekday() >= 5
    is_payday = date.day in [25, 26, 27, 28]
    merged = active_accs.merge(custs, on='customer_id', how='left')
    if merged.empty:
        return pd.DataFrame(), tx_id
    sample_size = min(count, len(merged))
    if sample_size == 0:
        return pd.DataFrame(), tx_id
    sel_accs = merged.sample(sample_size)
    for _, row in sel_accs.iterrows():
        acc_id = row['account_id']
        age = row.get('age', 30)
        income = row.get('income', 25000)
        distress = row.get('distress_level', 0.0)
        if acc_id not in manager.balances:
            atype, odate, status, scdate, _ = get_account_info(accs, acc_id)
            manager.setup_account(acc_id, atype, 0, odate, status, scdate)
        merch = pick_merchant(cat)[0]
        
        # Get peak hours for this category, or use default business hours
        peak_hours = PEAK_HOURS.get(cat, list(range(9, 18)))
        
        # Sample an hour from the peak hours for this category
        hr = random.choice(peak_hours)
        
        tme = f"{int(hr):02d}:{random.randint(0, 59):02d}:00"
        amt = compute_amount(merch, age, income, cat, distress)
        mult = 1.0
        if is_weekend:
            mult *= WEEKEND_MULTIPLIERS.get(cat, 1.0)
        if is_payday:
            mult *= PAYDAY_MULTIPLIERS.get(cat, 1.0)
        amt = round(amt * mult, 2)
        imm = random.random() < 0.3
        status = random.choices(TRANSACTION_STATUSES, weights=STATUS_WEIGHTS.get(cat, [0.9, 0.05, 0.03, 0.02]))[0]
        available_channels = get_available_channels(acc_id, accs)
        channel = 'branch' if not manager.first_transaction_done.get(acc_id, False) else random.choice(available_channels)
        tx_cost = round(random.uniform(10, 50), 2) if channel in ['branch', 'atm'] else 0.0
        dt = pd.to_datetime(date.strftime('%Y-%m-%d') + ' ' + tme)
        if status == 'completed' and manager.is_debit_allowed(acc_id, amt, dt.date()):
            manager.execute_tx(acc_id, amt, True, dt)
        else:
            status = 'failed'
        tx = {
            'transaction_id': f"TXN{year}{tx_id + success_count:06d}",
            'account_id': acc_id,
            'transaction_date': date.strftime('%Y-%m-%d'),
            'transaction_time': tme,
            'amount': amt,
            'debit_credit': 'debit',
            'category': cat,
            'status': status,
            'description': f"Purchase at {merch['name']}",
            'immediate_payment': imm,
            'receiving_account': '',
            'receiving_bank': '',
            'transaction_cost': tx_cost,
            'channel': channel,
            'merchant_name': merch['name']
        }
        tx_list.append(add_errors(tx))
        success_count += 1
    if tx_list:
        df = pd.DataFrame(tx_list)
        return df, tx_id + success_count
    return pd.DataFrame(), tx_id

def generate_salaries(active_accs, custs, manager, date, tx_id, year, accs):
    """Create salary credits on the 25th."""
    if date.day != 25:
        return pd.DataFrame(), tx_id
    merged = active_accs.merge(custs, on='customer_id', how='left')
    if merged.empty:
        return pd.DataFrame(), tx_id
    workers = merged[merged['occupation'] != 'Unemployed']
    if workers.empty:
        return pd.DataFrame(), tx_id
    num_sal = int(len(workers) * 0.8)
    sel_workers = workers.sample(num_sal)
    tx_list = []
    for i, (_, worker) in enumerate(sel_workers.iterrows()):
        base_sal = max(4000, abs(worker.get('income', np.random.normal(25000, 8000))))
        mon_sal = max(4000, np.random.normal(base_sal, base_sal * 0.2))
        acc_id = worker['account_id']
        if acc_id not in manager.balances:
            atype, odate, status, scdate, _ = get_account_info(accs, acc_id)
            manager.setup_account(acc_id, atype, 0, odate, status, scdate)
        tme = f"{random.randint(6, 10):02d}:00:00"
        dt = pd.to_datetime(date.strftime('%Y-%m-%d') + ' ' + tme)
        available_channels = get_available_channels(acc_id, accs)
        channel = random.choice(available_channels)
        tx_cost = round(random.uniform(10, 50), 2) if channel in ['branch', 'atm'] else 0.0
        if manager.execute_tx(acc_id, mon_sal, False, dt):
            tx = {
                'transaction_id': f"TXN{year}{tx_id + i:06d}",
                'account_id': acc_id,
                'transaction_date': date.strftime('%Y-%m-%d'),
                'transaction_time': tme,
                'amount': round(mon_sal, 2),
                'debit_credit': 'credit',
                'category': 'salary payment',
                'status': 'completed',
                'description': 'Monthly salary',
                'immediate_payment': False,
                'receiving_account': acc_id,
                'receiving_bank': 'Same Bank',
                'transaction_cost': tx_cost,
                'channel': channel,
                'merchant_name': ''
            }
            tx_list.append(add_errors(tx))
    return pd.DataFrame(tx_list), tx_id + len(tx_list)

def generate_corp_payrolls(active_accs, custs, manager, date, tx_id, year, accs):
    """Handle business payroll debits on the 25th."""
    if date.day != 25:
        return pd.DataFrame(), tx_id
    corps = active_accs[
        active_accs['customer_id'].str.startswith(('COM', 'SUB-COM'))
    ]
    if corps.empty:
        return pd.DataFrame(), tx_id
    tx_list = []
    emp_offset = 0
    for _, corp in corps.iterrows():
        emp_count = random.randint(10, 250)
        corp_acc = corp['account_id']
        if corp_acc not in manager.balances:
            atype, odate, status, scdate, _ = get_account_info(accs, corp_acc)
            manager.setup_account(corp_acc, atype, 10000, odate, status, scdate)
        available_channels = get_available_channels(corp_acc, accs)
        for emp in range(emp_count):
            sal_amt = round(random.uniform(3500, 45000), 2)
            tme = f"{random.randint(6, 10):02d}:{random.randint(0, 59):02d}:00"
            dt = pd.to_datetime(date.strftime('%Y-%m-%d') + ' ' + tme)
            status = 'failed'
            if manager.is_debit_allowed(corp_acc, sal_amt, dt.date()):
                status = 'completed'
                manager.execute_tx(corp_acc, sal_amt, True, dt)
            channel = random.choice(available_channels)
            tx_cost = round(random.uniform(10, 50), 2) if channel in ['branch', 'atm'] else 0.0
            tx = {
                'transaction_id': f"TXN{year}{tx_id + emp_offset + emp:06d}",
                'account_id': corp_acc,
                'transaction_date': date.strftime('%Y-%m-%d'),
                'transaction_time': tme,
                'amount': sal_amt,
                'debit_credit': 'debit',
                'category': 'salary payment',
                'status': status,
                'description': f"Employee salary payment #{emp + 1}",
                'immediate_payment': True,
                'receiving_account': f"EMP{random.randint(100000, 999999)}",
                'receiving_bank': 'Same Bank',
                'transaction_cost': tx_cost,
                'channel': channel,
                'merchant_name': ''
            }
            tx_list.append(add_errors(tx))
        emp_offset += emp_count
    if tx_list:
        df = pd.DataFrame(tx_list)
        return df, tx_id + len(tx_list)
    return pd.DataFrame(), tx_id

def process_monthly_txs(year, month, accs, custs, manager, start_id, loans, debits, fraudster):
    """Generate all transactions for a given month."""
    all_txs = []
    month_start = datetime(year, month, 1)
    month_end = month_start + pd.offsets.MonthEnd(0)
    dates = pd.date_range(month_start, month_end)

    for day in tqdm(dates, desc=f"Generating {year}-{month:02d}"):
        if day.month == 1 and day.day == 1:  # Skip January 1 (public holiday)
            continue
        daily_txs = []
        day_str = day.strftime('%Y-%m-%d')
        day_dt = pd.to_datetime(day_str)

        active_mask = (
            (accs['opening_date'] <= day_dt) &
            (
                (accs['status'] != 'closed') |
                (accs['status_change_date'] > day_dt) |
                accs['status_change_date'].isna()
            ) &
            (
                (accs['status'] == 'active') |
                ((accs['status'].isin(['dormant', 'frozen'])) & (accs['status_change_date'] > day_dt) & ~accs['status_change_date'].isna())
            )
        )
        active_day = accs[active_mask].copy()
        if active_day.empty:
            continue

        due_accs = [aid for aid, info in manager.pending_deposits.items()
                    if info['due_date'].strftime('%Y-%m-%d') == day_str and manager.get_status_at_date(aid, day_dt) != 'closed']
        pend_txs = []
        for aid in due_accs:
            info = manager.pending_deposits.pop(aid)
            amt = info['amount']
            hr = random.randint(9, 15)
            mn = random.randint(0, 59)
            tme = f"{hr:02d}:{mn:02d}:00"
            dt = pd.to_datetime(day_str + ' ' + tme)
            channel = 'branch'
            tx_cost = round(random.uniform(10, 50), 2)
            if manager.execute_tx(aid, amt, False, dt):
                tx = {
                    'transaction_id': f"TXN{year}{start_id:06d}",
                    'account_id': aid,
                    'transaction_date': day_str,
                    'transaction_time': tme,
                    'amount': amt,
                    'debit_credit': 'credit',
                    'category': 'initial deposit',
                    'status': 'completed',
                    'description': 'Account opening deposit',
                    'immediate_payment': False,
                    'receiving_account': aid,
                    'receiving_bank': 'Same Bank',
                    'transaction_cost': tx_cost,
                    'channel': channel,
                    'merchant_name': ''
                }
                pend_txs.append(add_errors(tx))
                start_id += 1
        if pend_txs:
            daily_txs.append(pd.DataFrame(pend_txs))

        day_loans = loans[loans['transaction_date'] == day_str].copy()
        day_loans = day_loans[day_loans['account_id'].isin(active_day['account_id'])]
        if not day_loans.empty:
            if 'transaction_id' not in day_loans:
                day_loans['transaction_id'] = [f"TXN{year}{start_id + i:06d}" for i in range(len(day_loans))]
                start_id += len(day_loans)
            for col in ['description', 'immediate_payment', 'receiving_account', 'receiving_bank', 'transaction_cost',
                        'channel', 'merchant_name', 'status', 'debit_credit', 'category']:
                if col not in day_loans:
                    if col == 'status':
                        day_loans[col] = 'completed'
                    elif col == 'category':
                        day_loans[col] = 'loan payment'
                    elif col == 'immediate_payment':
                        day_loans[col] = False
                    elif col == 'transaction_cost':
                        day_loans[col] = round(random.uniform(10, 50), 2)
                    elif col == 'channel':
                        day_loans[col] = 'branch'
                    elif col == 'debit_credit':
                        day_loans[col] = 'debit'
                    else:
                        day_loans[col] = ''
            if 'transaction_time' not in day_loans:
                day_loans['transaction_time'] = '09:00:00'
            processed_loans = []
            for _, loan in day_loans.iterrows():
                aid = loan['account_id']
                if aid not in manager.balances:
                    atype, odate, status, scdate, _ = get_account_info(accs, aid)
                    manager.setup_account(aid, atype, 0, odate, status, scdate)
                dir_debit = loan.get('debit_credit', 'debit') == 'debit'
                amt = loan['amount']
                dt_loan = pd.to_datetime(loan['transaction_date'] + ' ' + loan['transaction_time'])
                status = loan['status']
                if dir_debit:
                    if not manager.is_debit_allowed(aid, amt, dt_loan.date()):
                        status = 'failed'
                    else:
                        manager.execute_tx(aid, amt, True, dt_loan)
                else:
                    if not manager.is_transaction_allowed(aid, False, dt_loan.date()):
                        status = 'failed'
                    else:
                        manager.execute_tx(aid, amt, False, dt_loan)
                loan = loan.copy()
                loan['status'] = status
                processed_loans.append(loan)
            day_loans = pd.DataFrame(processed_loans)
            daily_txs.append(day_loans)

        day_debits = debits[debits['transaction_date'] == day_str].copy()
        day_debits = day_debits[day_debits['account_id'].isin(active_day['account_id'])]
        if not day_debits.empty:
            if 'transaction_id' not in day_debits:
                day_debits['transaction_id'] = [f"TXN{year}{start_id + i:06d}" for i in range(len(day_debits))]
                start_id += len(day_debits)
            for col in ['description', 'immediate_payment', 'receiving_account', 'receiving_bank', 'transaction_cost',
                        'channel', 'merchant_name', 'status', 'category', 'debit_credit']:
                if col not in day_debits:
                    if col == 'status':
                        day_debits[col] = 'completed'
                    elif col == 'category':
                        day_debits[col] = 'debit order'
                    elif col == 'immediate_payment':
                        day_debits[col] = False
                    elif col == 'transaction_cost':
                        day_debits[col] = round(random.uniform(10, 50), 2)
                    elif col == 'channel':
                        day_debits[col] = 'branch'
                    elif col == 'debit_credit':
                        day_debits[col] = 'debit'
                    else:
                        day_debits[col] = ''
            if 'transaction_time' not in day_debits:
                day_debits['transaction_time'] = '06:00:00'
            processed_debits = []
            for _, debit in day_debits.iterrows():
                aid = debit['account_id']
                if aid not in manager.balances:
                    atype, odate, status, scdate, _ = get_account_info(accs, aid)
                    manager.setup_account(aid, atype, 0, odate, status, scdate)
                dir_debit = debit.get('debit_credit', 'debit') == 'debit'
                amt = debit['amount']
                dt_debit = pd.to_datetime(debit['transaction_date'] + ' ' + debit['transaction_time'])
                status = debit['status']
                if dir_debit:
                    if not manager.is_debit_allowed(aid, amt, dt_debit.date()):
                        status = 'failed'
                    else:
                        manager.execute_tx(aid, amt, True, dt_debit)
                else:
                    if not manager.is_transaction_allowed(aid, False, dt_debit.date()):
                        status = 'failed'
                    else:
                        manager.execute_tx(aid, amt, False, dt_debit)
                debit = debit.copy()
                debit['status'] = status
                processed_debits.append(debit)
            day_debits = pd.DataFrame(processed_debits)
            daily_txs.append(day_debits)

        sal_txs, start_id = generate_salaries(active_day, custs, manager, day, start_id, year, accs)
        if not sal_txs.empty:
            daily_txs.append(sal_txs)

        if day.day == 1:
            merged = active_day.merge(custs, on='customer_id', how='left')
            unemp = merged[merged['occupation'] == 'Unemployed']
            if not unemp.empty:
                grant_txs = []
                for i, (_, unem) in enumerate(unemp.iterrows()):
                    aid = unem['account_id']
                    if aid not in manager.balances:
                        atype, odate, status, scdate, _ = get_account_info(accs, aid)
                        manager.setup_account(aid, atype, 0, odate, status, scdate)
                    dt_grant = pd.to_datetime(day_str + ' 08:00:00')
                    available_channels = get_available_channels(aid, accs)
                    channel = random.choice(available_channels)
                    tx_cost = round(random.uniform(10, 50), 2) if channel in ['branch', 'atm'] else 0.0
                    if manager.execute_tx(aid, 350, False, dt_grant):
                        grant_tx = {
                            'transaction_id': f"TXN{year}{start_id + i:06d}",
                            'account_id': aid,
                            'transaction_date': day_str,
                            'transaction_time': '08:00:00',
                            'amount': 350.00,
                            'debit_credit': 'credit',
                            'category': 'government grant',
                            'status': 'completed',
                            'description': 'Monthly social grant',
                            'immediate_payment': False,
                            'receiving_account': aid,
                            'receiving_bank': 'Same Bank',
                            'transaction_cost': tx_cost,
                            'channel': channel,
                            'merchant_name': ''
                        }
                        grant_txs.append(add_errors(grant_tx))
                if grant_txs:
                    daily_txs.append(pd.DataFrame(grant_txs))
                    start_id += len(grant_txs)

        pay_txs, start_id = generate_corp_payrolls(active_day, custs, manager, day, start_id, year, accs)
        if not pay_txs.empty:
            daily_txs.append(pay_txs)

        for pair in fraudster.cycle_pairs:
            if random.random() < 0.02 and all(acc in active_day['account_id'].values for acc in pair):
                cyc_txs, cnt = create_cycle_txs(pair, day, manager, start_id, year, accs)
                if cyc_txs:
                    daily_txs.append(pd.DataFrame(cyc_txs))
                    start_id += cnt
                    LOGGER.debug(f"Injected cycle for {pair} on {day}")

        if not (day.month == 1 and day.day == 1):
            cats_counts = [
                ('groceries', random.randint(80, 150)),
                ('clothing', random.randint(8, 20)),
                ('fuel', random.randint(15, 25)),
                ('restaurants', random.randint(10, 25)),
                ('retail', random.randint(5, 15)),
                ('transport', random.randint(20, 40)),
                ('entertainment', random.randint(3, 10)),
                ('medical', random.randint(2, 8)),
                ('utilities', random.randint(5, 12)),
                ('airtime', random.randint(15, 30)),
                ('electronics', random.randint(3, 8))
            ]
            for cat, cnt in cats_counts:
                cat_txs, start_id = produce_category_txs(cat, active_day, custs, manager, day, cnt, start_id, year, fraudster, accs)
                if not cat_txs.empty:
                    daily_txs.append(cat_txs)

        if daily_txs:
            all_txs.extend(daily_txs)

    if all_txs:
        combined = pd.concat(all_txs, ignore_index=True)
        if not combined.empty:
            combined['dt'] = pd.to_datetime(combined['transaction_date'] + ' ' + combined['transaction_time'])
            combined = combined.sort_values(['dt', 'debit_credit'], ascending=[True, True])
            combined.drop('dt', axis=1, inplace=True)
        LOGGER.info(f"Month {month:02d} complete: {len(combined)} txs")
        return combined, start_id
    return pd.DataFrame(), start_id

def generate_initial_deposits(year, accs, custs, manager):
    """Generate initial deposit transactions for all accounts based on opening_date + 0-7 days."""
    year_start = datetime(year, 1, 1).date()
    pending = {}
    for _, acc in accs.iterrows():
        aid = acc['account_id']
        atype = acc.get('account_type', 'Standard Banking')
        odate = acc.get('opening_date')
        status = acc.get('status', 'active')
        scdate = acc.get('status_change_date')
        if pd.isna(odate):
            odate = pd.to_datetime(f"{year}-01-01")
        odate = odate.date()
        if pd.isna(scdate):
            scdate = None
        else:
            scdate = scdate.date()
        if scdate and odate < year_start and scdate <= year_start and status == 'closed':
            initial_balance = 0
        elif odate < year_start:
            if status == 'closed' and scdate and scdate <= year_start:
                initial_balance = 0
            else:
                initial_balance = round(np.random.lognormal(9, 1.5), 2)
        else:
            initial_balance = 0
        manager.setup_account(aid, atype, initial_balance, odate, status, scdate)
        if initial_balance > 0:
            manager.first_credits[aid] = odate
        if odate >= year_start and status != 'closed' and (scdate is None or odate < scdate):
            delay = random.randint(0, 7)
            due_dt = pd.to_datetime(odate + timedelta(days=delay))
            if due_dt.date() < year_start:
                due_dt = datetime.combine(year_start, datetime.min.time())
            amt = round(np.random.uniform(500, 10000), 2)
            pending[aid] = {'due_date': due_dt.date(), 'amount': amt}

    manager.pending_deposits = pending

    due_dates = sorted(set(info['due_date'] for info in pending.values() if info['due_date'] >= year_start))
    initial_txs = []
    tx_id = 1

    for due in due_dates:
        if due.month == 1 and due.day == 1:  # Skip January 1
            continue
        day_str = due.strftime('%Y-%m-%d')
        due_accs = [aid for aid, info in manager.pending_deposits.items() if info['due_date'].strftime('%Y-%m-%d') == day_str]
        for aid in due_accs:
            if manager.get_status_at_date(aid, due) == 'closed':
                manager.pending_deposits.pop(aid)
                continue
            info = manager.pending_deposits.pop(aid)
            amt = info['amount']
            hr = random.randint(9, 15)
            mn = random.randint(0, 59)
            tme = f"{hr:02d}:{mn:02d}:00"
            dt = pd.to_datetime(day_str + ' ' + tme)
            channel = 'branch'
            tx_cost = round(random.uniform(10, 50), 2)
            if manager.execute_tx(aid, amt, False, dt):
                tx = {
                    'transaction_id': f"TXN{year}{tx_id:06d}",
                    'account_id': aid,
                    'transaction_date': day_str,
                    'transaction_time': tme,
                    'amount': amt,
                    'debit_credit': 'credit',
                    'category': 'cash deposit',
                    'status': 'completed',
                    'description': 'Initial cash deposit',
                    'immediate_payment': False,
                    'receiving_account': aid,
                    'receiving_bank': 'Same Bank',
                    'transaction_cost': tx_cost,
                    'channel': channel,
                    'merchant_name': ''
                }
                initial_txs.append(add_errors(tx))
                tx_id += 1

    if initial_txs:
        df = pd.DataFrame(initial_txs)
        if not df.empty:
            df['dt'] = pd.to_datetime(df['transaction_date'] + ' ' + df['transaction_time'])
            df = df.sort_values('dt')
            df.drop('dt', axis=1, inplace=True)
        fname = f"banking_data/transactions_by_year/transactions_{year}_00.parquet"
        df.to_parquet(fname, index=False, engine='fastparquet')
        LOGGER.info(f"Saved {len(df)} initial deposits to {fname}")
        return tx_id
    return 1

def orchestrate_year(year):
    """Main orchestration for generating a year's transactions."""
    LOGGER.info(f"Starting transaction synthesis for {year}")
    accs, custs = fetch_data_up_to_year(year)
    if accs.empty:
        LOGGER.error("No accounts found")
        return

    fraudster = FraudInjector(year)
    fraudster.pick_suspects(accs, custs)

    loans, debits = load_scheduled_txs(year, accs)
    manager = AccountBalanceManager()

    os.makedirs("banking_data/transactions_by_year", exist_ok=True)

    schema_cols = [
        'transaction_id', 'account_id', 'transaction_date', 'transaction_time', 'amount',
        'debit_credit', 'category', 'status', 'description', 'immediate_payment',
        'receiving_account', 'receiving_bank', 'transaction_cost', 'channel', 'merchant_name'
    ]
    pd.DataFrame(columns=schema_cols).to_parquet(f"banking_data/transactions_by_year/transactions_{year}_00.parquet",
                                                 index=False, engine='fastparquet')
    LOGGER.info("Schema file created")

    tx_counter = generate_initial_deposits(year, accs, custs, manager)

    for mon in range(1, 13):
        mon_txs, tx_counter = process_monthly_txs(year, mon, accs, custs, manager, tx_counter, loans, debits, fraudster)
        if not mon_txs.empty:
            if random.random() < 0.001:
                dup_cnt = random.randint(1, 3)
                dups = mon_txs.sample(min(dup_cnt, len(mon_txs)))
                mon_txs = pd.concat([mon_txs, dups], ignore_index=True)
            fname = f"banking_data/transactions_by_year/transactions_{year}_{mon:02d}.parquet"
            mon_txs.to_parquet(fname, index=False, engine='fastparquet')
            LOGGER.info(f"Saved {len(mon_txs)} txs to {fname}")

    LOGGER.info("=" * 60)
    LOGGER.info(f"Year {year} finalized with fraud injection and status handling")
    LOGGER.info("Output in banking_data/transactions_by_year/")
    LOGGER.info("=" * 60)

def run_generator():
    """Entry point for the generator."""
    try:
        yr = int(input("Enter year: "))
        if yr < 2000:
            raise ValueError("Year too early")
    except ValueError as e:
        LOGGER.error(f"Invalid year: {e}")
        return
    orchestrate_year(yr)
    LOGGER.info(f"\nYear {yr} files:")
    for m in range(0, 13):
        fname = f"banking_data/transactions_by_year/transactions_{yr}_{m:02d}.parquet"
        if os.path.exists(fname):
            size_mb = os.path.getsize(fname) / (1024 * 1024)
            LOGGER.info(f"  {fname}: {size_mb:.2f} MB")

if __name__ == "__main__":
    run_generator()