import numpy as np
import pandas as pd
from faker import Faker

# Constants
TRANSACTION_STATUSES = ['completed', 'failed', 'cancelled', 'pending']
CHANNELS = ['pos', 'branch', 'atm', 'online banking', 'mobile banking app', 'ewallet']
FAKER = Faker('zu_ZA')

# Bank Codes
BANK_CODES = {
    '632005': 'OUR_BANK',
    '470010': 'Capitec Bank',
    '250655': 'Standard Bank',
    '198765': 'FNB',
    '580105': 'ABSA',
    '410506': 'Nedbank',
    '430000': 'Investec',
    '462005': 'African Bank',
    '450105': 'TymeBank',
    '678910': 'Discovery Bank'
}

# Transaction Rules
TRANSACTION_RULES = {
    'atm': {
        'allowed_types': ['withdrawal', 'deposit'],
        'description_formats': {
            'withdrawal': 'ATM WITHDRAWAL - {location}',
            'deposit': 'ATM DEPOSIT - {location}'
        },
        'needs_receiving_account': False,
        'needs_receiving_bank': False,
        'max_withdrawal': 5000,
        'notes': 'ATM transactions are instant, location-based'
    },
    'branch': {
        'allowed_types': ['deposit', 'payment'],
        'description_formats': {
            'deposit': 'BRANCH DEPOSIT - {branch_name}',
            'payment': 'BRANCH PAYMENT - {beneficiary}'
        },
        'needs_receiving_account': False,
        'needs_receiving_bank': False,
        'notes': 'Branch deposits and payments into own or other accounts'
    },
    'online_banking': {
        'allowed_types': [
            'transfer', 'payment', 'debit_order_payment', 'card_purchase', 'bill_payment'
        ],
        'description_formats': {
            'transfer': 'ONLINE TRANSFER - {reference}',
            'payment': 'ONLINE PAYMENT - {beneficiary}',
            'debit_order_payment': 'DEBIT ORDER - {merchant} - {debit_order_ref}',
            'card_purchase': 'CARD PURCHASE - {merchant}',
            'bill_payment': 'BILL PAYMENT - {service_provider}'
        },
        'needs_receiving_account': True,
        'needs_receiving_bank': 'if_external',
        'services': ['Gautrain', 'Municipal Bills', 'Insurance', 'School Fees'],
        'notes': 'Can do immediate_payment and scheduled payments'
    },
    'mobile_banking_app': {
        'allowed_types': [
            'transfer', 'payment', 'immediate_payment', 'debit_order_payment',
            'card_purchase', 'bill_payment', 'airtime_purchase', 'data_purchase'
        ],
        'description_formats': {
            'transfer': 'MOBILE TRANSFER - {reference}',
            'payment': 'MOBILE PAYMENT - {beneficiary}',
            'immediate_payment': 'INSTANT PAY - {beneficiary}',
            'debit_order_payment': 'DEBIT ORDER - {merchant} - {debit_order_ref}',
            'card_purchase': 'CARD PURCHASE - {merchant}',
            'bill_payment': 'BILL PAYMENT - {service_provider}',
            'airtime_purchase': 'AIRTIME - {network}',
            'data_purchase': 'DATA BUNDLE - {network}'
        },
        'needs_receiving_account': True,
        'needs_receiving_bank': 'if_external',
        'services': ['Bolt', 'Uber', 'Netflix', 'DSTV', 'Electricity'],
        'notes': 'High failure rate due to network issues and insufficient balance'
    },
    'ewallet': {
        'allowed_types': ['ewallet_transfer', 'merchant_payment', 'airtime_purchase', 'bill_payment'],
        'description_formats': {
            'ewallet_transfer': 'EWALLET SEND - {cellphone}',
            'merchant_payment': 'EWALLET PAY - {merchant}',
            'airtime_purchase': 'EWALLET AIRTIME - {network}',
            'bill_payment': 'EWALLET BILL - {service}'
        },
        'needs_receiving_account': False,
        'needs_cellphone': True,
        'needs_receiving_bank': False,
        'max_amount': 3000,
        'merchants': ['Shoprite', 'Pick n Pay', 'Boxer', 'Spar', 'Checkers'],
        'notes': 'Uses cellphone numbers, not account numbers. Max R3000 per transaction'
    },
    'pos': {
        'allowed_types': ['card_purchase', 'card_refund'],
        'description_formats': {
            'card_purchase': 'CARD PURCHASE - {merchant}',
            'card_refund': 'CARD REFUND - {merchant}'
        },
        'needs_receiving_account': False,
        'needs_merchant_id': True,
        'needs_receiving_bank': False,
        'notes': 'Point of sale transactions at physical stores'
    }
}

ATM_LOCATIONS = [
    'Menlyn Mall ATM', 'Sandton City ATM', 'Gateway ATM', 'Canal Walk ATM',
    'Cresta ATM', 'Rosebank ATM', 'Brooklyn Mall ATM', 'Centurion Mall ATM'
]

BRANCH_NAMES = [
    'Sandton Branch', 'Pretoria CBD Branch', 'Johannesburg Main Branch',
    'Cape Town Central Branch', 'Durban Branch', 'Rosebank Branch'
]

# Transaction status weights for different categories
STATUS_WEIGHTS = {
    'groceries': [0.95, 0.03, 0.01, 0.01],  # [completed, failed, cancelled, pending]
    'clothing': [0.90, 0.05, 0.03, 0.02],
    'fuel': [0.97, 0.02, 0.005, 0.005],
    'restaurants': [0.92, 0.04, 0.02, 0.02],
    'retail': [0.93, 0.04, 0.02, 0.01],
    'transport': [0.94, 0.03, 0.02, 0.01],
    'entertainment': [0.90, 0.05, 0.03, 0.02],
    'medical': [0.96, 0.02, 0.01, 0.01],
    'utilities': [0.98, 0.01, 0.005, 0.005],
    'airtime': [0.97, 0.02, 0.005, 0.005],
    'electronics': [0.89, 0.06, 0.03, 0.02],
    'default': [0.90, 0.05, 0.03, 0.02]
}

# Peak hours for transaction categories
PEAK_HOURS = {
    'groceries': [8, 9, 10, 11, 12, 16, 17, 18, 19],
    'clothing': [10, 11, 12, 13, 14, 15, 16],
    'fuel': [6, 7, 8, 16, 17, 18],
    'restaurants': [12, 13, 18, 19, 20],
    'retail': [10, 11, 12, 13, 14, 15, 16],
    'transport': [6, 7, 8, 16, 17, 18],
    'entertainment': [18, 19, 20, 21],
    'medical': [8, 9, 10, 11, 12],
    'utilities': [9, 10, 11],
    'airtime': [8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
    'electronics': [10, 11, 12, 13, 14, 15]
}

# Multipliers for weekend transactions
WEEKEND_MULTIPLIERS = {
    'groceries': 1.2,
    'clothing': 1.5,
    'fuel': 1.1,
    'restaurants': 1.8,
    'retail': 1.4,
    'transport': 0.9,
    'entertainment': 2.0,
    'medical': 0.8,
    'utilities': 1.0,
    'airtime': 1.1,
    'electronics': 1.3
}

# Multipliers for payday transactions
PAYDAY_MULTIPLIERS = {
    'groceries': 1.3,
    'clothing': 1.6,
    'fuel': 1.1,
    'restaurants': 1.5,
    'retail': 1.7,
    'transport': 1.0,
    'entertainment': 1.8,
    'medical': 1.0,
    'utilities': 1.2,
    'airtime': 1.2,
    'electronics': 1.5
}

# Spending preferences by age group
AGE_SPENDING_PREFERENCES = {
    '18-25': {
        'groceries': 0.8,
        'clothing': 1.5,
        'fuel': 0.9,
        'restaurants': 1.4,
        'retail': 1.2,
        'transport': 1.3,
        'entertainment': 1.6,
        'medical': 0.7,
        'utilities': 0.8,
        'airtime': 1.5,
        'electronics': 1.4
    },
    '26-35': {
        'groceries': 1.0,
        'clothing': 1.2,
        'fuel': 1.1,
        'restaurants': 1.2,
        'retail': 1.0,
        'transport': 1.1,
        'entertainment': 1.2,
        'medical': 0.9,
        'utilities': 1.0,
        'airtime': 1.2,
        'electronics': 1.1
    },
    '36-50': {
        'groceries': 1.2,
        'clothing': 1.0,
        'fuel': 1.2,
        'restaurants': 1.0,
        'retail': 0.9,
        'transport': 1.0,
        'entertainment': 0.9,
        'medical': 1.1,
        'utilities': 1.2,
        'airtime': 1.0,
        'electronics': 0.9
    },
    '51-65': {
        'groceries': 1.1,
        'clothing': 0.8,
        'fuel': 1.0,
        'restaurants': 0.8,
        'retail': 0.7,
        'transport': 0.8,
        'entertainment': 0.7,
        'medical': 1.3,
        'utilities': 1.1,
        'airtime': 0.8,
        'electronics': 0.7
    },
    '65+': {
        'groceries': 1.0,
        'clothing': 0.6,
        'fuel': 0.7,
        'restaurants': 0.6,
        'retail': 0.6,
        'transport': 0.7,
        'entertainment': 0.5,
        'medical': 1.5,
        'utilities': 1.0,
        'airtime': 0.6,
        'electronics': 0.5
    }
}

# Spending multipliers by income tier
INCOME_SPENDING_MULTIPLIERS = {
    'low': {
        'normal': 0.8,
        'distressed': 0.5,
        'categories': {
            'groceries': 1.0,
            'clothing': 0.6,
            'fuel': 0.8,
            'restaurants': 0.5,
            'retail': 0.6,
            'transport': 0.9,
            'entertainment': 0.5,
            'medical': 1.0,
            'utilities': 0.9,
            'airtime': 0.9,
            'electronics': 0.6
        }
    },
    'medium': {
        'normal': 1.0,
        'distressed': 0.6,
        'categories': {
            'groceries': 1.0,
            'clothing': 1.0,
            'fuel': 1.0,
            'restaurants': 1.0,
            'retail': 1.0,
            'transport': 1.0,
            'entertainment': 1.0,
            'medical': 1.0,
            'utilities': 1.0,
            'airtime': 1.0,
            'electronics': 1.0
        }
    },
    'high': {
        'normal': 1.3,
        'distressed': 0.8,
        'categories': {
            'groceries': 1.1,
            'clothing': 1.3,
            'fuel': 1.2,
            'restaurants': 1.3,
            'retail': 1.2,
            'transport': 1.1,
            'entertainment': 1.3,
            'medical': 1.0,
            'utilities': 1.0,
            'airtime': 1.0,
            'electronics': 1.3
        }
    },
    'premium': {
        'normal': 1.5,
        'distressed': 0.9,
        'categories': {
            'groceries': 1.2,
            'clothing': 1.5,
            'fuel': 1.3,
            'restaurants': 1.5,
            'retail': 1.4,
            'transport': 1.2,
            'entertainment': 1.5,
            'medical': 1.0,
            'utilities': 1.0,
            'airtime': 1.0,
            'electronics': 1.5
        }
    }
}

# South African companies by category for merchant generation
SA_COMPANIES = {
    'groceries': [
        {'name': 'Shoprite', 'avg_transaction': 200, 'std_deviation': 60},
        {'name': 'Pick n Pay', 'avg_transaction': 250, 'std_deviation': 75},
        {'name': 'Checkers', 'avg_transaction': 300, 'std_deviation': 90},
        {'name': 'Spar', 'avg_transaction': 220, 'std_deviation': 66},
        {'name': 'Boxer', 'avg_transaction': 180, 'std_deviation': 54}
    ],
    'clothing': [
        {'name': 'Mr Price', 'avg_transaction': 350, 'std_deviation': 105},
        {'name': 'Woolworths', 'avg_transaction': 500, 'std_deviation': 150},
        {'name': 'Truworths', 'avg_transaction': 400, 'std_deviation': 120},
        {'name': 'Foschini', 'avg_transaction': 450, 'std_deviation': 135}
    ],
    'fuel': [
        {'name': 'Engen', 'avg_transaction': 600, 'std_deviation': 180},
        {'name': 'Shell', 'avg_transaction': 650, 'std_deviation': 195},
        {'name': 'BP', 'avg_transaction': 620, 'std_deviation': 186},
        {'name': 'Total', 'avg_transaction': 610, 'std_deviation': 183}
    ],
    'restaurants': [
        {'name': 'Nando\'s', 'avg_transaction': 150, 'std_deviation': 45},
        {'name': 'Spur', 'avg_transaction': 200, 'std_deviation': 60},
        {'name': 'Ocean Basket', 'avg_transaction': 250, 'std_deviation': 75},
        {'name': 'Mugg & Bean', 'avg_transaction': 180, 'std_deviation': 54}
    ],
    'retail': [
        {'name': 'Game', 'avg_transaction': 300, 'std_deviation': 90},
        {'name': 'Makro', 'avg_transaction': 500, 'std_deviation': 150},
        {'name': 'CNA', 'avg_transaction': 100, 'std_deviation': 30}
    ],
    'transport': [
        {'name': 'Gautrain', 'avg_transaction': 50, 'std_deviation': 15},
        {'name': 'Bolt', 'avg_transaction': 80, 'std_deviation': 24},
        {'name': 'Uber', 'avg_transaction': 90, 'std_deviation': 27}
    ],
    'entertainment': [
        {'name': 'Ster-Kinekor', 'avg_transaction': 120, 'std_deviation': 36},
        {'name': 'Nu Metro', 'avg_transaction': 130, 'std_deviation': 39},
        {'name': 'DSTV', 'avg_transaction': 200, 'std_deviation': 60}
    ],
    'medical': [
        {'name': 'Dis-Chem', 'avg_transaction': 150, 'std_deviation': 45},
        {'name': 'Clicks', 'avg_transaction': 140, 'std_deviation': 42},
        {'name': 'MediRite', 'avg_transaction': 160, 'std_deviation': 48}
    ],
    'utilities': [
        {'name': 'Eskom', 'avg_transaction': 500, 'std_deviation': 150},
        {'name': 'City Power', 'avg_transaction': 450, 'std_deviation': 135},
        {'name': 'Telkom', 'avg_transaction': 300, 'std_deviation': 90}
    ],
    'airtime': [
        {'name': 'MTN', 'avg_transaction': 50, 'std_deviation': 15},
        {'name': 'Vodacom', 'avg_transaction': 60, 'std_deviation': 18},
        {'name': 'Cell C', 'avg_transaction': 40, 'std_deviation': 12}
    ],
    'electronics': [
        {'name': 'Incredible Connection', 'avg_transaction': 1000, 'std_deviation': 300},
        {'name': 'Dion Wired', 'avg_transaction': 1200, 'std_deviation': 360},
        {'name': 'HiFi Corp', 'avg_transaction': 900, 'std_deviation': 270}
    ],
    'services': [
        {'name': 'Generic Salon', 'avg_transaction': 200, 'std_deviation': 60},
        {'name': 'Generic Repair', 'avg_transaction': 300, 'std_deviation': 90}
    ]
}