import pandas as pd
import os
import logging
from account_manager import AccountBalanceManager
from fraud_injector import FraudInjector
from transaction_generator import (fetch_data_up_to_year, load_scheduled_txs, process_monthly_txs,
                                  generate_initial_deposits)
from sa_merchant import (SA_COMPANIES, STATUS_WEIGHTS, PEAK_HOURS, WEEKEND_MULTIPLIERS,
                         PAYDAY_MULTIPLIERS, AGE_SPENDING_PREFERENCES, INCOME_SPENDING_MULTIPLIERS,
                         TRANSACTION_STATUSES, CHANNELS)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)

def orchestrate_year(year):
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
            fname = f"banking_data/transactions_by_year/transactions_{year}_{mon:02d}.parquet"
            mon_txs.to_parquet(fname, index=False, engine='fastparquet')
            LOGGER.info(f"Saved {len(mon_txs)} transactions for {year}-{mon:02d} to {fname}")
        else:
            LOGGER.info(f"No transactions generated for {year}-{mon:02d}")

def run_generator():
    try:
        year = int(input("Enter year: "))
        if year < 2018:
            LOGGER.error("Year must be 2018 or later")
            return
        orchestrate_year(year)
    except ValueError:
        LOGGER.error("Invalid year input. Please enter a valid year (e.g., 2023)")
    except Exception as e:
        LOGGER.error(f"Error during transaction generation: {str(e)}")

if __name__ == "__main__":
    run_generator()