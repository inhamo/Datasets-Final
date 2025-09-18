# sa_merchants.py
# Enhanced South African merchant data with realistic transaction patterns

SA_COMPANIES = {
    'groceries': [
        {"name": "Checkers", "size": "large", "avg_transaction": 450, "std_deviation": 200, "hours": {"open": 7, "close": 21}},
        {"name": "Shoprite", "size": "large", "avg_transaction": 380, "std_deviation": 180, "hours": {"open": 8, "close": 20}},
        {"name": "Pick n Pay", "size": "large", "avg_transaction": 420, "std_deviation": 190, "hours": {"open": 7, "close": 22}},
        {"name": "Woolworths", "size": "premium", "avg_transaction": 650, "std_deviation": 250, "hours": {"open": 8, "close": 21}},
        {"name": "Spar", "size": "medium", "avg_transaction": 350, "std_deviation": 160, "hours": {"open": 7, "close": 20}},
        {"name": "Food Lovers Market", "size": "medium", "avg_transaction": 400, "std_deviation": 180, "hours": {"open": 8, "close": 21}},
        {"name": "Game", "size": "large", "avg_transaction": 750, "std_deviation": 400, "hours": {"open": 9, "close": 21}},
        {"name": "Makro", "size": "large", "avg_transaction": 1200, "std_deviation": 600, "hours": {"open": 7, "close": 19}},
        {"name": "Cambridge Foods", "size": "small", "avg_transaction": 180, "std_deviation": 80, "hours": {"open": 7, "close": 19}},
        {"name": "Fruit & Veg City", "size": "medium", "avg_transaction": 220, "std_deviation": 100, "hours": {"open": 7, "close": 20}}
    ],
    
    'clothing': [
        {"name": "Edgars", "size": "large", "avg_transaction": 850, "std_deviation": 400, "hours": {"open": 9, "close": 21}},
        {"name": "Mr Price", "size": "medium", "avg_transaction": 450, "std_deviation": 200, "hours": {"open": 9, "close": 21}},
        {"name": "Truworths", "size": "medium", "avg_transaction": 750, "std_deviation": 350, "hours": {"open": 9, "close": 21}},
        {"name": "Foschini", "size": "medium", "avg_transaction": 800, "std_deviation": 380, "hours": {"open": 9, "close": 21}},
        {"name": "Jet", "size": "medium", "avg_transaction": 500, "std_deviation": 250, "hours": {"open": 9, "close": 21}},
        {"name": "Ackermans", "size": "medium", "avg_transaction": 350, "std_deviation": 180, "hours": {"open": 8, "close": 20}},
        {"name": "PEP", "size": "small", "avg_transaction": 250, "std_deviation": 120, "hours": {"open": 8, "close": 19}},
        {"name": "Cotton On", "size": "medium", "avg_transaction": 450, "std_deviation": 200, "hours": {"open": 10, "close": 21}},
        {"name": "Zara", "size": "premium", "avg_transaction": 1200, "std_deviation": 500, "hours": {"open": 10, "close": 21}},
        {"name": "H&M", "size": "medium", "avg_transaction": 600, "std_deviation": 280, "hours": {"open": 10, "close": 21}},
        {"name": "Markham", "size": "medium", "avg_transaction": 650, "std_deviation": 300, "hours": {"open": 9, "close": 21}},
        {"name": "Identity", "size": "medium", "avg_transaction": 550, "std_deviation": 250, "hours": {"open": 9, "close": 21}}
    ],
    
    'fuel': [
        {"name": "Shell", "size": "large", "avg_transaction": 650, "std_deviation": 300, "hours": {"open": 0, "close": 24}},
        {"name": "BP", "size": "large", "avg_transaction": 680, "std_deviation": 320, "hours": {"open": 0, "close": 24}},
        {"name": "Engen", "size": "large", "avg_transaction": 620, "std_deviation": 280, "hours": {"open": 0, "close": 24}},
        {"name": "Sasol", "size": "large", "avg_transaction": 700, "std_deviation": 350, "hours": {"open": 0, "close": 24}},
        {"name": "Caltex", "size": "medium", "avg_transaction": 580, "std_deviation": 250, "hours": {"open": 5, "close": 23}},
        {"name": "Total", "size": "medium", "avg_transaction": 640, "std_deviation": 290, "hours": {"open": 5, "close": 23}}
    ],
    
    'restaurants': [
        {"name": "Nandos", "size": "medium", "avg_transaction": 180, "std_deviation": 80, "hours": {"open": 11, "close": 22}},
        {"name": "KFC", "size": "large", "avg_transaction": 120, "std_deviation": 60, "hours": {"open": 10, "close": 23}},
        {"name": "McDonald's", "size": "large", "avg_transaction": 95, "std_deviation": 45, "hours": {"open": 6, "close": 23}},
        {"name": "Steers", "size": "medium", "avg_transaction": 110, "std_deviation": 50, "hours": {"open": 10, "close": 22}},
        {"name": "Wimpy", "size": "medium", "avg_transaction": 140, "std_deviation": 70, "hours": {"open": 7, "close": 22}},
        {"name": "Debonairs Pizza", "size": "medium", "avg_transaction": 220, "std_deviation": 100, "hours": {"open": 11, "close": 23}},
        {"name": "Roman's Pizza", "size": "small", "avg_transaction": 180, "std_deviation": 80, "hours": {"open": 11, "close": 23}},
        {"name": "Ocean Basket", "size": "medium", "avg_transaction": 320, "std_deviation": 150, "hours": {"open": 11, "close": 22}},
        {"name": "Spur", "size": "medium", "avg_transaction": 280, "std_deviation": 120, "hours": {"open": 11, "close": 22}},
        {"name": "Mugg & Bean", "size": "medium", "avg_transaction": 160, "std_deviation": 80, "hours": {"open": 7, "close": 21}}
    ],
    
    'alcohol': [
        {"name": "Liquor City", "size": "large", "avg_transaction": 350, "std_deviation": 200, "hours": {"open": 10, "close": 21}},
        {"name": "Tops Liquor Store", "size": "large", "avg_transaction": 320, "std_deviation": 180, "hours": {"open": 10, "close": 20}},
        {"name": "Ultra Liquor", "size": "medium", "avg_transaction": 280, "std_deviation": 150, "hours": {"open": 10, "close": 20}},
        {"name": "Pick n Pay Liquor", "size": "large", "avg_transaction": 400, "std_deviation": 220, "hours": {"open": 10, "close": 20}},
        {"name": "Makro Liquor", "size": "large", "avg_transaction": 600, "std_deviation": 350, "hours": {"open": 9, "close": 19}},
        {"name": "Norman Goodfellows", "size": "premium", "avg_transaction": 450, "std_deviation": 250, "hours": {"open": 10, "close": 21}},
        {"name": "Checkers Liquor Shop", "size": "medium", "avg_transaction": 380, "std_deviation": 200, "hours": {"open": 10, "close": 20}},
        {"name": "Bottle Store Express", "size": "small", "avg_transaction": 180, "std_deviation": 90, "hours": {"open": 10, "close": 19}},
        {"name": "Woolworths Wine", "size": "premium", "avg_transaction": 650, "std_deviation": 300, "hours": {"open": 10, "close": 20}}
    ],
    
    'retail': [
        {"name": "Clicks", "size": "large", "avg_transaction": 220, "std_deviation": 120, "hours": {"open": 8, "close": 21}},
        {"name": "Dis-Chem", "size": "large", "avg_transaction": 280, "std_deviation": 150, "hours": {"open": 8, "close": 21}},
        {"name": "CNA", "size": "medium", "avg_transaction": 150, "std_deviation": 80, "hours": {"open": 8, "close": 20}},
        {"name": "Incredible Connection", "size": "medium", "avg_transaction": 1500, "std_deviation": 800, "hours": {"open": 9, "close": 20}},
        {"name": "Takealot", "size": "large", "avg_transaction": 650, "std_deviation": 400, "hours": {"open": 0, "close": 24}},
        {"name": "Musica", "size": "medium", "avg_transaction": 450, "std_deviation": 250, "hours": {"open": 9, "close": 21}},
        {"name": "Hi-Fi Corp", "size": "medium", "avg_transaction": 2200, "std_deviation": 1200, "hours": {"open": 9, "close": 20}},
        {"name": "PNA", "size": "small", "avg_transaction": 120, "std_deviation": 60, "hours": {"open": 8, "close": 18}},
        {"name": "Sportsmans Warehouse", "size": "medium", "avg_transaction": 850, "std_deviation": 400, "hours": {"open": 9, "close": 20}}
    ],
    
    'pharmacies': [
        {"name": "Clicks Pharmacy", "size": "large", "avg_transaction": 180, "std_deviation": 100, "hours": {"open": 8, "close": 21}},
        {"name": "Dis-Chem Pharmacy", "size": "large", "avg_transaction": 220, "std_deviation": 120, "hours": {"open": 8, "close": 21}},
        {"name": "Alpha Pharm", "size": "small", "avg_transaction": 150, "std_deviation": 80, "hours": {"open": 8, "close": 19}},
        {"name": "MediRite", "size": "small", "avg_transaction": 140, "std_deviation": 75, "hours": {"open": 8, "close": 18}},
        {"name": "Link Pharmacy", "size": "small", "avg_transaction": 160, "std_deviation": 85, "hours": {"open": 8, "close": 19}}
    ],
    
    'transport': [
        {"name": "Uber", "size": "large", "avg_transaction": 85, "std_deviation": 40, "hours": {"open": 0, "close": 24}},
        {"name": "Bolt", "size": "medium", "avg_transaction": 75, "std_deviation": 35, "hours": {"open": 0, "close": 24}},
        {"name": "Gautrain", "size": "medium", "avg_transaction": 45, "std_deviation": 15, "hours": {"open": 5, "close": 23}},
        {"name": "Metrobus", "size": "medium", "avg_transaction": 20, "std_deviation": 5, "hours": {"open": 5, "close": 22}},
        {"name": "Taxi Fare", "size": "small", "avg_transaction": 25, "std_deviation": 10, "hours": {"open": 5, "close": 22}},
        {"name": "Rea Vaya", "size": "medium", "avg_transaction": 18, "std_deviation": 5, "hours": {"open": 5, "close": 22}}
    ],
    
    'entertainment': [
        {"name": "Ster Kinekor", "size": "large", "avg_transaction": 180, "std_deviation": 80, "hours": {"open": 10, "close": 23}},
        {"name": "Nu Metro", "size": "medium", "avg_transaction": 170, "std_deviation": 75, "hours": {"open": 10, "close": 23}},
        {"name": "Emperors Palace", "size": "large", "avg_transaction": 450, "std_deviation": 300, "hours": {"open": 10, "close": 2}},
        {"name": "Gold Reef City", "size": "large", "avg_transaction": 320, "std_deviation": 150, "hours": {"open": 9, "close": 22}},
        {"name": "Canal Walk", "size": "large", "avg_transaction": 280, "std_deviation": 140, "hours": {"open": 9, "close": 21}}
    ],
    
    'utilities': [
        {"name": "Eskom Prepaid", "size": "large", "avg_transaction": 250, "std_deviation": 150, "hours": {"open": 0, "close": 24}},
        {"name": "City Power Prepaid", "size": "large", "avg_transaction": 200, "std_deviation": 120, "hours": {"open": 0, "close": 24}},
        {"name": "Joburg Water", "size": "large", "avg_transaction": 180, "std_deviation": 100, "hours": {"open": 0, "close": 24}},
        {"name": "Tshwane Municipality", "size": "large", "avg_transaction": 220, "std_deviation": 130, "hours": {"open": 0, "close": 24}},
        {"name": "Telkom", "size": "large", "avg_transaction": 320, "std_deviation": 150, "hours": {"open": 0, "close": 24}},
        {"name": "DSTV", "size": "large", "avg_transaction": 450, "std_deviation": 200, "hours": {"open": 0, "close": 24}}
    ],
    
    'airtime': [
        {"name": "MTN Airtime", "size": "large", "avg_transaction": 50, "std_deviation": 30, "hours": {"open": 0, "close": 24}},
        {"name": "Vodacom Airtime", "size": "large", "avg_transaction": 50, "std_deviation": 30, "hours": {"open": 0, "close": 24}},
        {"name": "Cell C Airtime", "size": "medium", "avg_transaction": 45, "std_deviation": 25, "hours": {"open": 0, "close": 24}},
        {"name": "Telkom Mobile", "size": "medium", "avg_transaction": 40, "std_deviation": 20, "hours": {"open": 0, "close": 24}},
        {"name": "Rain Mobile", "size": "small", "avg_transaction": 35, "std_deviation": 18, "hours": {"open": 0, "close": 24}}
    ],
    
    'medical': [
        {"name": "Netcare Hospital", "size": "large", "avg_transaction": 2500, "std_deviation": 1500, "hours": {"open": 0, "close": 24}},
        {"name": "Life Healthcare", "size": "large", "avg_transaction": 2200, "std_deviation": 1300, "hours": {"open": 0, "close": 24}},
        {"name": "Mediclinic", "size": "large", "avg_transaction": 2800, "std_deviation": 1600, "hours": {"open": 0, "close": 24}},
        {"name": "GP Consultation", "size": "medium", "avg_transaction": 650, "std_deviation": 200, "hours": {"open": 8, "close": 17}},
        {"name": "Dental Practice", "size": "medium", "avg_transaction": 1200, "std_deviation": 600, "hours": {"open": 8, "close": 17}},
        {"name": "Optometrist", "size": "small", "avg_transaction": 850, "std_deviation": 400, "hours": {"open": 9, "close": 17}}
    ]
}

# Transaction status weights by merchant size and category
STATUS_WEIGHTS = {
    'large': {'completed': 0.95, 'cancelled': 0.03, 'failed': 0.015, 'pending': 0.005},
    'medium': {'completed': 0.93, 'cancelled': 0.04, 'failed': 0.025, 'pending': 0.005},
    'small': {'completed': 0.90, 'cancelled': 0.05, 'failed': 0.04, 'pending': 0.01},
    'premium': {'completed': 0.97, 'cancelled': 0.02, 'failed': 0.008, 'pending': 0.002}
}

# Peak shopping hours by category
PEAK_HOURS = {
    'groceries': [17, 18, 19],  # After work
    'clothing': [14, 15, 16, 19, 20],  # Afternoon and evening
    'fuel': [7, 8, 17, 18],  # Rush hours
    'restaurants': [12, 13, 19, 20],  # Lunch and dinner
    'alcohol': [17, 18, 19, 20],  # Evening
    'retail': [14, 15, 16],  # Afternoon
    'transport': [7, 8, 17, 18, 19],  # Rush hours
    'entertainment': [19, 20, 21],  # Evening
    'medical': [9, 10, 11, 14, 15]  # Business hours
}

# Weekend multipliers by category
WEEKEND_MULTIPLIERS = {
    'groceries': 1.3,
    'clothing': 1.8,
    'fuel': 0.8,
    'restaurants': 2.2,
    'alcohol': 3.5,
    'retail': 1.6,
    'transport': 0.6,
    'entertainment': 2.5,
    'medical': 0.3,
    'utilities': 1.0,
    'airtime': 1.1
}

# Payday multipliers (25th-28th of month)
PAYDAY_MULTIPLIERS = {
    'groceries': 1.6,
    'clothing': 2.2,
    'fuel': 1.4,
    'restaurants': 1.8,
    'alcohol': 2.0,
    'retail': 2.5,
    'entertainment': 2.8,
    'utilities': 1.2,
    'airtime': 1.3
}

TRANSACTION_STATUSES = ["completed", "cancelled", "failed", "pending"]
CHANNELS = ["atm", "mobile banking app", "pos", "online banking", "branch"]

# Age-based spending preferences
AGE_SPENDING_PREFERENCES = {
    '18-25': {
        'alcohol': 1.8, 'entertainment': 2.2, 'clothing': 1.6, 'transport': 1.4,
        'restaurants': 1.5, 'groceries': 0.8, 'medical': 0.5
    },
    '26-35': {
        'alcohol': 1.3, 'entertainment': 1.5, 'clothing': 1.3, 'groceries': 1.2,
        'restaurants': 1.3, 'utilities': 1.1, 'medical': 0.8
    },
    '36-50': {
        'groceries': 1.4, 'utilities': 1.3, 'medical': 1.2, 'fuel': 1.2,
        'alcohol': 1.0, 'entertainment': 1.0, 'clothing': 1.0
    },
    '51-65': {
        'medical': 1.8, 'groceries': 1.2, 'utilities': 1.1, 'pharmacies': 1.5,
        'alcohol': 0.7, 'entertainment': 0.8, 'transport': 0.9
    },
    '65+': {
        'medical': 2.5, 'pharmacies': 2.0, 'groceries': 1.0, 'utilities': 1.0,
        'alcohol': 0.4, 'entertainment': 0.6, 'transport': 0.7
    }
}

# Income-based spending multipliers
INCOME_SPENDING_MULTIPLIERS = {
    'low': {
        'normal': 0.7,
        'distressed': 0.4,
        'categories': {
            'groceries': 0.6,
            'utilities': 0.6,
            'medical': 0.5,
            'alcohol': 0.1,
            'entertainment': 0.1,
            'clothing': 0.2,
            'restaurants': 0.3,
            'retail': 0.4,
            'transport': 0.5,
            'fuel': 0.5,
            'airtime': 0.5
        }
    },
    'medium': {
        'normal': 1.0,
        'distressed': 0.6,
        'categories': {
            'groceries': 0.8,
            'utilities': 0.7,
            'medical': 0.6,
            'alcohol': 0.2,
            'entertainment': 0.2,
            'clothing': 0.3,
            'restaurants': 0.4,
            'retail': 0.5,
            'transport': 0.6,
            'fuel': 0.6,
            'airtime': 0.6
        }
    },
    'high': {
        'normal': 1.2,
        'distressed': 0.8,
        'categories': {
            'groceries': 1.0,
            'utilities': 0.9,
            'medical': 0.8,
            'alcohol': 0.3,
            'entertainment': 0.3,
            'clothing': 0.4,
            'restaurants': 0.5,
            'retail': 0.6,
            'transport': 0.7,
            'fuel': 0.7,
            'airtime': 0.7
        }
    },
    'premium': {
        'normal': 1.5,
        'distressed': 1.0,
        'categories': {
            'groceries': 1.2,
            'utilities': 1.0,
            'medical': 0.9,
            'alcohol': 0.4,
            'entertainment': 0.4,
            'clothing': 0.5,
            'restaurants': 0.6,
            'retail': 0.7,
            'transport': 0.8,
            'fuel': 0.8,
            'airtime': 0.8
        }
    }
}