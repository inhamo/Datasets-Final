import pandas as pd
import numpy as np
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)

class FraudInjector:
    def __init__(self, year):
        self.year = year
        self.suspect_accounts = set()
        self.smurfing_accounts = []
        self.high_velocity_accounts = []
        self.cycle_pairs = []
        self.reactivated_dormants = []

    def pick_suspects(self, accounts_df, customers_df):
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
            [acc for pair in self.cycle_pairs for acc in pair])]
        if len(final_remaining) >= dormant_count:
            self.reactivated_dormants = final_remaining.sample(n=dormant_count)['account_id'].tolist()

        self.suspect_accounts = set(
            self.smurfing_accounts + self.high_velocity_accounts +
            [acc for pair in self.cycle_pairs for acc in pair] + self.reactivated_dormants
        )
        LOGGER.info(f"Fraud selection: Smurfing {len(self.smurfing_accounts)}, Velocity {len(self.high_velocity_accounts)}, "
                    f"Pairs {len(self.cycle_pairs)}, Dormants {len(self.reactivated_dormants)}")