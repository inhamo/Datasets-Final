from datetime import datetime
import logging
from constants import FAKER
import pandas as pd

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
        self.first_transaction_done = {}
        self.overdraft_limits = {
            'Premium Banking': -10000,
            'Gold Banking': -5000,
            'Standard Banking': -2000,
            'Basic Banking': -500,
            'Student Banking': -200,
            'Business Banking': -20000
        }

    def setup_account(self, account_id, account_type, initial_balance=0, open_date=None, status='active', status_change_date=None):
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
        self.first_transaction_done[account_id] = initial_balance > 0
        if initial_balance > 0 and open_date:
            self.first_credits[account_id] = open_date
        LOGGER.debug(f"Set up {account_id} with balance {initial_balance}, status {status}")

    def get_status_at_date(self, account_id, check_date):
        if account_id not in self.balances:
            return 'closed'
        info = self.balances[account_id]
        change_date = info['status_change_date']
        if change_date and check_date >= change_date:
            return info['status']
        return 'active'

    def is_transaction_allowed(self, account_id, is_debit, tx_date):
        status = self.get_status_at_date(account_id, tx_date)
        if status == 'closed':
            return False
        if is_debit and status in ['frozen', 'dormant']:
            return False
        return True

    def is_debit_allowed(self, account_id, amount, tx_date=None):
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
        self.first_transaction_done[account_id] = True
        LOGGER.debug(f"Executed {'debit' if is_debit else 'credit'} of {amount} for {account_id}; balance now {info['balance']}")
        return True

    def current_balance(self, account_id):
        return self.balances.get(account_id, {}).get('balance', 0)