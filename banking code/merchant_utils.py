import json
import random
import numpy as np
import pandas as pd
import logging
from constants import FAKER, BANK_CODES, SA_COMPANIES

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)

def generate_and_add_merchant(category, existing_merchants=None):
    """Generate a new merchant and append to merchants_accounts.json."""
    # Load existing merchants
    if existing_merchants is None:
        try:
            with open('merchants_accounts.json', 'r') as f:
                existing_merchants = json.load(f)
        except FileNotFoundError:
            existing_merchants = {}
    
    # Generate unique merchant name
    base_name = FAKER.first_name()
    suffix = random.choice(['Spaza', 'Tuck Shop', 'Corner Shop', 'Street Stall', 'Salon', 'Repair'])
    merchant_name = f"{base_name}'s {suffix}"
    while merchant_name in existing_merchants:
        base_name = FAKER.first_name()
        merchant_name = f"{base_name}'s {suffix}"
    
    # Assign bank and account number
    bank_code = random.choice(list(BANK_CODES.keys()))
    bank_name = "" if bank_code == '632005' else BANK_CODES[bank_code]
    
    # Find the highest account number for the selected bank
    max_acc_num = 0
    for m in existing_merchants.values():
        if m['bank_code'] == bank_code:
            acc_num = int(m['account_number'])
            if acc_num > max_acc_num:
                max_acc_num = acc_num
    account_number = str(max_acc_num + 1) if max_acc_num else f"{bank_code}{random.randint(1000000, 9999999)}"
    
    # Map suffix to category if not provided
    if not category:
        category_map = {
            'Spaza': 'groceries', 'Tuck Shop': 'groceries', 'Corner Shop': 'groceries',
            'Street Stall': 'restaurants', 'Salon': 'services', 'Repair': 'services'
        }
        category = category_map.get(suffix, 'groceries')
    
    # Create merchant entry
    new_merchant = {
        'account_number': account_number,
        'bank_code': bank_code,
        'bank_name': bank_name,
        'category': category
    }
    
    # Update JSON file
    existing_merchants[merchant_name] = new_merchant
    with open('merchants_accounts.json', 'w') as f:
        json.dump(existing_merchants, f, indent=4)
    
    LOGGER.info(f"Added new merchant {merchant_name} to merchants_accounts.json")
    return {'name': merchant_name, **new_merchant}

def pick_merchant(cat, count=1, existing_merchants=None):
    """Select merchant details for a category from merchants_accounts.json."""
    if existing_merchants is None:
        try:
            with open('merchants_accounts.json', 'r') as f:
                existing_merchants = json.load(f)
        except FileNotFoundError:
            existing_merchants = {}
    
    cat_lower = cat.lower()
    cat_merchants = [m for m, details in existing_merchants.items() if details['category'].lower() == cat_lower]
    
    if not cat_merchants:
        # Generate new merchants if none exist for the category
        merchants = [generate_and_add_merchant(cat_lower, existing_merchants) for _ in range(count)]
    else:
        indices = np.random.choice(len(cat_merchants), count, replace=True)
        merchants = [{'name': cat_merchants[i], **existing_merchants[cat_merchants[i]]} for i in indices]
    
    # Add default transaction parameters
    for m in merchants:
        m['size'] = 'medium'
        m['avg_transaction'] = SA_COMPANIES.get(cat_lower, [{'avg_transaction': 200}])[0]['avg_transaction']
        m['std_deviation'] = m['avg_transaction'] * 0.3
        m['hours'] = {'open': 8, 'close': 20}
    
    return merchants