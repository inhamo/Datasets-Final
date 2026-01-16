import pandas as pd
import random
import numpy as np
from faker import Faker
from datetime import date, timedelta
import os
from tqdm import tqdm
from scipy.stats import norm, beta, gamma, expon, poisson, lognorm

# Set seeds for reproducibility
random.seed(23)
np.random.seed(23)

fake = Faker()

# Load existing insurance applicants
applicants_path = "insurance_data/insurance_applicants.parquet"
if not os.path.exists(applicants_path):
    raise FileNotFoundError(f"{applicants_path} not found.")
df_customers = pd.read_parquet(applicants_path)

# Validate required customer columns
required_customer_columns = ["Customer_ID", "Age", "Monthly_Income_ZAR", "Assets_Value_ZAR", "Address", "Is_Smoker", "Long_Term_Medication", "Alcohol_Use", "Job_Title", "Credit_Score"]
missing_customer_cols = [col for col in required_customer_columns if col not in df_customers.columns]
if missing_customer_cols:
    raise ValueError(f"Missing columns in df_customers: {missing_customer_cols}")

# REALISTIC coverage factors
POLICY_TYPES = {
    "Life": {
        "coverage_multiplier": 4,
        "min_coverage": 100000,
        "max_coverage": 10000000,
        "base_premium_rate": 0.008,
        "has_xol": True,
        "reinsurance_threshold": 5000000,
        "renewal_rate": 0.75,
        "age_restrictions": (18, 75),
        "medical_exam_threshold": 2000000,
        "admin_fee": 100
    },
    "Health": {
        "coverage_multiplier": 2,
        "min_coverage": 50000,
        "max_coverage": 2000000,
        "base_premium_rate": 0.06,
        "has_xol": True,
        "reinsurance_threshold": 1000000,
        "renewal_rate": 0.92,
        "age_restrictions": (18, 80),
        "medical_exam_threshold": 500000,
        "admin_fee": 150
    },
    "Car": {
        "coverage_multiplier": 0.8,
        "min_coverage": 50000,
        "max_coverage": 1500000,
        "base_premium_rate": 0.05,
        "has_xol": False,
        "reinsurance_threshold": 1000000,
        "renewal_rate": 0.85,
        "age_restrictions": (18, 90),
        "medical_exam_threshold": None,
        "admin_fee": 75
    },
    "Home": {
        "coverage_multiplier": 3,
        "min_coverage": 500000,
        "max_coverage": 15000000,
        "base_premium_rate": 0.004,
        "has_xol": True,
        "reinsurance_threshold": 5000000,
        "renewal_rate": 0.88,
        "age_restrictions": (18, 90),
        "medical_exam_threshold": None,
        "admin_fee": 80
    },
    "Travel": {
        "coverage_multiplier": 0.05,
        "min_coverage": 50000,
        "max_coverage": 500000,
        "base_premium_rate": 0.02,
        "has_xol": False,
        "reinsurance_threshold": 300000,
        "renewal_rate": 0.45,
        "age_restrictions": (18, 80),
        "medical_exam_threshold": None,
        "admin_fee": 50
    },
    "Commercial": {
        "coverage_multiplier": 5,
        "min_coverage": 1000000,
        "max_coverage": 50000000,
        "base_premium_rate": 0.015,
        "has_xol": True,
        "reinsurance_threshold": 10000000,
        "renewal_rate": 0.90,
        "age_restrictions": (21, 75),
        "medical_exam_threshold": None,
        "admin_fee": 200
    }
}

# Seasonal policy peaks
SEASONAL_PATTERNS = {
    "Life": [1.2, 0.9, 0.9, 1.0, 1.0, 0.9, 1.0, 0.9, 1.1, 1.0, 1.0, 0.9],  # Peak Jan, Sep
    "Health": [1.3, 1.0, 0.9, 0.9, 1.0, 1.0, 1.0, 0.9, 1.1, 1.0, 1.0, 0.9],  # Peak Jan
    "Car": [1.1, 1.0, 1.0, 0.9, 1.0, 1.0, 1.0, 1.0, 1.2, 1.0, 1.0, 1.1],  # Peak Sep, Dec
    "Home": [1.1, 1.0, 1.0, 0.9, 1.0, 1.0, 1.0, 1.0, 1.1, 1.0, 1.0, 1.0],
    "Travel": [0.9, 0.9, 1.0, 1.1, 1.0, 1.1, 1.3, 1.2, 1.0, 1.0, 1.0, 1.4],  # Peak Jul, Dec
    "Commercial": [1.0, 1.0, 1.2, 1.0, 1.0, 1.1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.2]  # Peak Mar, Jun, Dec
}

# Geographic risk factors by province
GEOGRAPHIC_RISK = {
    "Gauteng": {"Car": 1.25, "Home": 1.1, "Life": 1.0},  # High crime
    "Western Cape": {"Car": 1.15, "Home": 1.2, "Life": 1.0},  # Coastal flooding
    "KwaZulu-Natal": {"Car": 1.1, "Home": 1.15, "Life": 1.0},
    "Eastern Cape": {"Car": 1.05, "Home": 1.0, "Life": 1.0},
    "Free State": {"Car": 1.0, "Home": 1.0, "Life": 1.0},
    "Mpumalanga": {"Car": 1.05, "Home": 1.05, "Life": 1.0},
    "Limpopo": {"Car": 1.05, "Home": 1.0, "Life": 1.0},
    "North West": {"Car": 1.0, "Home": 1.0, "Life": 1.0},
    "Northern Cape": {"Car": 0.95, "Home": 0.95, "Life": 1.0}
}

# Policy subtypes with weights
POLICY_SUBTYPES = {
    "Life": {
        "options": ["Term Life", "Whole Life", "Universal Life", "Funeral Cover", "Group Life"],
        "weights": [0.4, 0.3, 0.2, 0.05, 0.05]
    },
    "Health": {
        "options": ["Comprehensive", "Hospital Plan", "Primary Care", "Gap Cover", "Executive Health"],
        "weights": [0.5, 0.3, 0.1, 0.05, 0.05]
    },
    "Car": {
        "options": ["Comprehensive", "Third Party", "Third Party Fire & Theft", "Fleet"],
        "weights": [0.6, 0.2, 0.15, 0.05]
    },
    "Home": {
        "options": ["Building", "Contents", "Building & Contents", "Landlord", "High-Net-Worth"],
        "weights": [0.25, 0.25, 0.4, 0.05, 0.05]
    },
    "Travel": {
        "options": ["Single Trip", "Annual Multi-Trip", "Backpacker", "Business", "Adventure"],
        "weights": [0.5, 0.3, 0.1, 0.05, 0.05]
    },
    "Commercial": {
        "options": ["Property", "Liability", "Professional Indemnity", "Directors & Officers", "Cyber"],
        "weights": [0.4, 0.3, 0.15, 0.1, 0.05]
    }
}

# XOL applicable subtypes - VERY RARE
XOL_APPLICABLE_SUBTYPES = ["Group Life", "Executive Health", "Fleet", "High-Net-Worth", "Directors & Officers", "Cyber"]

# Specific reinsurance - PORTFOLIO LEVEL (very rare for individual policies)
REINSURANCE_COMPANIES = [
    "Munich Re", "Swiss Re", "Hannover Re", "SCOR", "Africa Re", "RGA Re Africa", "Sanlam Re"
]

# Payment frequency with realistic channel alignment
PAYMENT_FREQ = {
    "options": ["Monthly", "Quarterly", "Annually", "Single Premium"],
    "weights": [0.65, 0.15, 0.15, 0.05]
}

# Agents by region
AGENTS_BY_REGION = {
    "Gauteng": ["Thabo Mokoena", "Sarah Nkosi", "David Pretorius", "Emma Botha", "Lerato Sithole"],
    "Western Cape": ["Michael de Vries", "Aisha Patel", "James Bothma", "Nadine Cloete"],
    "KwaZulu-Natal": ["Zanele Dlamini", "Richard Naidoo", "Kathy Govender", "Sibusiso Mkhize"],
    "Eastern Cape": ["Nomvula Ngece", "Andrew Smith", "Lungile Mbatha", "Tanya Adams"],
    "Free State": ["Rethabile Moloi", "Jacques du Plessis", "Nthabiseng Tshabalala"],
    "Mpumalanga": ["Siphiwe Ngwenya", "Annelize Kruger", "Themba Mahlangu"],
    "Limpopo": ["Mpho Makgoba", "Rendani Netshifhefhe", "Tshepo Malatji"],
    "North West": ["Kagiso Modise", "Amelia Fourie", "Tumelo Moagi"],
    "Northern Cape": ["Karabo van Niekerk", "Hendrik Coetzee", "Fatima Abrahams"]
}

CHANNELS = ["Online", "Walk-in", "Phone", "Broker", "Corporate"]

# Channel pricing adjustments
CHANNEL_ADJUSTMENTS = {
    "Online": 0.88,      # 12% discount
    "Walk-in": 1.0,      # Base
    "Phone": 1.0,        # Base
    "Broker": 1.08,      # 8% commission markup
    "Corporate": 0.75    # 25% group discount
}

# Deductible options
DEDUCTIBLE_OPTIONS = {
    "Life": {"options": [0], "weights": [1.0]},
    "Health": {"options": [0, 5000, 10000, 15000, 20000], "weights": [0.3, 0.25, 0.25, 0.15, 0.05]},
    "Car": {"options": [2500, 5000, 7500, 10000, 15000], "weights": [0.15, 0.3, 0.25, 0.2, 0.1]},
    "Home": {"options": [0, 5000, 10000, 15000, 25000], "weights": [0.2, 0.25, 0.25, 0.2, 0.1]},
    "Travel": {"options": [0, 500, 1000, 2000], "weights": [0.3, 0.3, 0.25, 0.15]},
    "Commercial": {"options": [10000, 25000, 50000, 100000], "weights": [0.2, 0.35, 0.3, 0.15]}
}

def generate_policy_number(policy_type, effective_date, sequence):
    """Generate realistic policy number format"""
    year = effective_date.year
    prefix = {"Life": "L", "Health": "H", "Car": "A", "Home": "P", "Travel": "T", "Commercial": "C"}
    return f"ZA{prefix[policy_type]}{year}{str(sequence).zfill(6)}"

def calculate_risk_factor(customer, policy_type, province):
    """Calculate comprehensive risk factor"""
    risk_factor = 1.0
    
    # Age risk
    if policy_type in ["Life", "Health"]:
        if customer["Age"] < 25:
            age_risk = 0.95
        elif customer["Age"] < 35:
            age_risk = 1.0
        elif customer["Age"] < 45:
            age_risk = 1.1
        elif customer["Age"] < 55:
            age_risk = 1.25
        elif customer["Age"] < 65:
            age_risk = 1.5
        else:
            age_risk = 2.0
        risk_factor *= age_risk
    
    # Health factors
    if customer["Is_Smoker"] == "Yes" and policy_type in ["Life", "Health"]:
        risk_factor *= 1.5
    if customer["Long_Term_Medication"] == "Yes" and policy_type in ["Life", "Health"]:
        risk_factor *= 1.3
    if customer["Alcohol_Use"] == "High" and policy_type in ["Life", "Health"]:
        risk_factor *= 1.2
    
    # Credit score impact (15-40% loading for poor credit)
    credit_score = customer["Credit_Score"]
    if credit_score < 550:
        risk_factor *= 1.35
    elif credit_score < 600:
        risk_factor *= 1.25
    elif credit_score < 650:
        risk_factor *= 1.15
    elif credit_score > 750:
        risk_factor *= 0.95  # Good credit discount
    
    # Occupation risk
    high_risk_jobs = ["Security Guard", "Construction Worker", "Miner", "Electrician", "Mechanic", "Pilot", "Driver"]
    if customer["Job_Title"] in high_risk_jobs:
        if policy_type in ["Life", "Health"]:
            risk_factor *= 1.3
        elif policy_type == "Car":
            risk_factor *= 1.15
    
    # Geographic risk
    geo_factor = GEOGRAPHIC_RISK.get(province, {}).get(policy_type, 1.0)
    risk_factor *= geo_factor
    
    return round(max(0.7, min(risk_factor, 3.5)), 2)

def calculate_realistic_coverage(policy_type, customer, policy_config):
    """Calculate realistic coverage with income constraints"""
    annual_income = customer["Monthly_Income_ZAR"] * 12
    
    # MAX coverage = 10-15x annual income (debt-to-income ratio)
    max_affordable = annual_income * random.uniform(8, 12)
    
    if policy_type == "Life":
        base_coverage = annual_income * policy_config["coverage_multiplier"]
        coverage = int(base_coverage * random.uniform(0.8, 1.2))
        
    elif policy_type == "Health":
        base_coverage = annual_income * policy_config["coverage_multiplier"]
        coverage = int(base_coverage * random.uniform(0.7, 1.3))
        
    elif policy_type == "Car":
        car_value = annual_income * random.uniform(0.5, 1.5)
        coverage = int(car_value)
        
    elif policy_type == "Home":
        if customer["Assets_Value_ZAR"] > 500000:
            property_value = customer["Assets_Value_ZAR"] * random.uniform(0.6, 0.9)
        else:
            property_value = annual_income * policy_config["coverage_multiplier"] * random.uniform(0.8, 1.2)
        coverage = int(property_value)
        
    elif policy_type == "Travel":
        coverage_options = [50000, 100000, 200000, 300000, 500000]
        coverage = random.choice(coverage_options)
        
    elif policy_type == "Commercial":
        estimated_turnover = annual_income * random.uniform(8, 15)
        coverage = int(estimated_turnover * policy_config["coverage_multiplier"])
    
    # Apply income constraint
    coverage = min(coverage, max_affordable)
    
    # Ensure within min/max bounds
    coverage = max(policy_config["min_coverage"], min(coverage, policy_config["max_coverage"]))
    coverage = round(coverage / 10000) * 10000
    
    return coverage

def calculate_realistic_premium(policy_type, coverage, risk_factor, payment_frequency, policy_config, channel, num_policies, years_with_company):
    """Calculate realistic premium with all adjustments"""
    # Base annual premium
    base_annual_premium = coverage * policy_config["base_premium_rate"] * risk_factor
    
    min_premiums = {
        "Life": 500, "Health": 800, "Car": 600, "Home": 400, "Travel": 200, "Commercial": 2000
    }
    base_annual_premium = max(base_annual_premium, min_premiums[policy_type])
    
    # Channel adjustment
    channel_factor = CHANNEL_ADJUSTMENTS.get(channel, 1.0)
    base_annual_premium *= channel_factor
    
    # Multi-policy discount (10-20%)
    if num_policies > 1:
        bundle_discount = min(0.20, 0.08 + (num_policies - 1) * 0.04)
        base_annual_premium *= (1 - bundle_discount)
    
    # Loyalty discount (5-15% after 3+ years)
    if years_with_company >= 3:
        loyalty_discount = min(0.15, 0.05 + (years_with_company - 3) * 0.02)
        base_annual_premium *= (1 - loyalty_discount)
    
    # Convert to payment frequency
    if payment_frequency == "Monthly":
        premium = int((base_annual_premium / 12) * 1.05)  # 5% admin markup
    elif payment_frequency == "Quarterly":
        premium = int((base_annual_premium / 4) * 1.02)  # 2% admin markup
    elif payment_frequency == "Annually":
        premium = int(base_annual_premium * 0.95)  # 5% discount
    elif payment_frequency == "Single Premium":
        if policy_type == "Travel":
            premium = int(base_annual_premium * 0.3)
        else:
            premium = int(base_annual_premium)
    
    # Add admin fee
    premium += policy_config["admin_fee"]
    
    # Round to nearest 10
    premium = round(premium / 10) * 10
    
    return max(premium, 100)

def determine_xol_and_reinsurance(policy_type, subtype, coverage_amount, policy_config):
    """REALISTIC reinsurance - only 5-10% of policies, mainly portfolio-level"""
    has_xol = False
    is_reinsured = False
    reinsurance_company = None
    reinsurance_share = None
    xol_retention_amount = None
    reinsurance_type = None
    
    # STRICT threshold - must be WAY above threshold
    if coverage_amount < policy_config["reinsurance_threshold"] * 1.5:
        return has_xol, is_reinsured, reinsurance_company, reinsurance_share, xol_retention_amount, reinsurance_type
    
    # Only 5-10% probability even if above threshold
    coverage_ratio = coverage_amount / policy_config["reinsurance_threshold"]
    reinsurance_prob = min(0.15, 0.03 + (coverage_ratio - 1.5) * 0.08)
    
    # Special products more likely
    if subtype in XOL_APPLICABLE_SUBTYPES:
        reinsurance_prob *= 2.5
    
    is_reinsured = random.random() < reinsurance_prob
    
    if is_reinsured:
        reinsurance_company = random.choice(REINSURANCE_COMPANIES)
        
        if subtype in XOL_APPLICABLE_SUBTYPES and policy_config["has_xol"]:
            reinsurance_type = 'XOL' if random.random() > 0.3 else 'Proportional'
        else:
            reinsurance_type = 'Proportional' if random.random() > 0.8 else 'XOL'
        
        if reinsurance_type == 'XOL' and policy_config["has_xol"]:
            has_xol = True
            xol_retention_amount = int(policy_config["reinsurance_threshold"] * random.uniform(0.85, 0.95))
            reinsurance_share = None
        else:
            has_xol = False
            reinsurance_share = round(random.uniform(0.4, 0.7), 2)  # Lower share
            xol_retention_amount = None
    
    return has_xol, is_reinsured, reinsurance_company, reinsurance_share, xol_retention_amount, reinsurance_type

def should_renew_policy(policy_type, policy_config, has_claims, income, credit_score):
    """Determine renewal with multiple factors"""
    base_renewal_rate = policy_config["renewal_rate"]
    
    if has_claims:
        base_renewal_rate *= 0.92
    
    # Income impact
    if income < 15000:
        base_renewal_rate *= 0.80
    elif income < 30000:
        base_renewal_rate *= 0.90
    
    # Credit score impact
    if credit_score < 600:
        base_renewal_rate *= 0.85
    
    return random.random() < base_renewal_rate

def get_seasonal_adjustment(policy_type, month):
    """Get seasonal pattern adjustment"""
    return SEASONAL_PATTERNS[policy_type][month - 1]

def generate_policies_with_renewals(customer, sequence_start, customer_policy_history):
    """Generate policies with comprehensive realism"""
    policies = []
    age = customer["Age"]
    income = customer["Monthly_Income_ZAR"]
    assets = customer["Assets_Value_ZAR"]
    credit_score = customer["Credit_Score"]
    
    is_commercial_customer = customer["Job_Title"] in ["Business Owner", "Director", "Executive", "Manager"]
    
    # Extract province
    address_parts = customer["Address"].split(",")
    province = address_parts[-2].strip() if len(address_parts) >= 2 else "Gauteng"
    agents_region = AGENTS_BY_REGION.get(province, AGENTS_BY_REGION["Gauteng"])

    # Base policy selection
    base_policies = []
    
    if income < 15000:
        base_policies = random.choice([["Health"], ["Car"]])
    elif income < 30000:
        choices = [["Health", "Car"], ["Health"], ["Car"]]
        base_policies = random.choice(choices)
    elif income < 50000:
        choices = [["Health", "Car"], ["Health", "Car", "Home"], ["Life", "Health"]]
        base_policies = random.choice(choices)
    else:
        choices = [["Health", "Car", "Home"], ["Life", "Health", "Car"], ["Life", "Health", "Home"]]
        base_policies = random.choice(choices)
    
    if is_commercial_customer and random.random() > 0.5:
        base_policies.append("Commercial")
    
    if income > 40000 and random.random() > 0.7:
        base_policies.append("Travel")
    
    base_policies = list(set(base_policies))
    if not base_policies:
        base_policies = ["Health"]

    # Track years with company
    years_with_company = 0
    policy_counter = 0
    
    for policy_type in base_policies:
        policy_config = POLICY_TYPES[policy_type]
        
        # Age restrictions
        age_min, age_max = policy_config["age_restrictions"]
        if age < age_min or age > age_max:
            continue
        
        # Subtype
        subtype_info = POLICY_SUBTYPES[policy_type]
        subtype = np.random.choice(subtype_info["options"], p=subtype_info["weights"])

        # Coverage
        coverage = calculate_realistic_coverage(policy_type, customer, policy_config)

        # Risk factor
        risk_factor = calculate_risk_factor(customer, policy_type, province)
        
        # Payment frequency
        if policy_type == "Travel":
            payment_freq = np.random.choice(["Single Premium", "Annually"], p=[0.7, 0.3])
        elif income < 20000:
            payment_freq = np.random.choice(PAYMENT_FREQ["options"], p=[0.85, 0.1, 0.04, 0.01])
        else:
            payment_freq = np.random.choice(PAYMENT_FREQ["options"], p=PAYMENT_FREQ["weights"])

        # Deductible
        deductible_info = DEDUCTIBLE_OPTIONS[policy_type]
        deductible = np.random.choice(deductible_info["options"], p=deductible_info["weights"])

        # Channel
        if customer["Job_Title"] == "Student":
            channel_weights = [0.6, 0.2, 0.15, 0.03, 0.02]
        elif is_commercial_customer:
            channel_weights = [0.15, 0.1, 0.15, 0.35, 0.25]
        else:
            channel_weights = [0.45, 0.25, 0.2, 0.07, 0.03]
        
        channel = np.random.choice(CHANNELS, p=channel_weights)
        
        if channel == "Walk-in":
            agent_name = random.choice(agents_region)
        elif channel == "Broker":
            agent_name = "Broker Channel"
        elif channel == "Corporate":
            agent_name = "Corporate Account Manager"
        else:
            agent_name = random.choice(agents_region + ["Online Agent"])

        # INITIAL POLICY - seasonal patterns
        start_date = date(2015, 1, 1)
        initial_start_range = (date(2017, 12, 31) - start_date).days
        
        # Try multiple times to get seasonal-appropriate month
        for _ in range(10):
            days_offset = random.randint(0, initial_start_range)
            effective_date = start_date + timedelta(days=days_offset)
            seasonal_factor = get_seasonal_adjustment(policy_type, effective_date.month)
            if random.random() < seasonal_factor:
                break
        
        # Round to 1st of month (80%)
        if random.random() > 0.2:
            effective_date = effective_date.replace(day=1)

        # Discount
        discount = round(random.triangular(0, 5, 10), 1)

        # Reinsurance - VERY RARE
        has_xol, is_reinsured, reinsurance_company, reinsurance_share, xol_retention_amount, reinsurance_type = determine_xol_and_reinsurance(
            policy_type, subtype, coverage, policy_config
        )

        # Generate renewals
        has_claims = False
        current_start = effective_date
        num_policies_for_customer = len(base_policies)
        
        while current_start <= date(2020, 12, 31):
            # Duration
            if policy_type == "Travel" and payment_freq == "Single Premium":
                duration_days = random.randint(7, 28)
            else:
                duration_days = 365
            
            expiration_date = current_start + timedelta(days=duration_days)
            expiration_date = min(expiration_date, date(2020, 12, 31))
            
            if (expiration_date - current_start).days < 7:
                break

            # Renewal notice
            renewal_days = random.randint(30, 60)
            renewal_notice = max(current_start, min(expiration_date - timedelta(days=renewal_days), date(2020, 12, 31)))

            # Calculate premium with all adjustments
            premium = calculate_realistic_premium(
                policy_type, coverage, risk_factor, payment_freq, policy_config,
                channel, num_policies_for_customer, years_with_company
            )

            # Add policy
            policies.append({
                "Policy_Number": generate_policy_number(policy_type, current_start, sequence_start + policy_counter + 1),
                "Applicant_ID": customer["Customer_ID"],
                "Policy_Type": policy_type,
                "Policy_Subtype": subtype,
                "Coverage_Amount": coverage,
                "Premium_Amount": premium,
                "Deductible_Amount": deductible,
                "Has_XOL": has_xol,
                "Is_Reinsured": is_reinsured,
                "Reinsurance_Company": reinsurance_company,
                "Reinsurance_Share": reinsurance_share,
                "XOL_Retention_Amount": xol_retention_amount,
                "Reinsurance_Type": reinsurance_type,
                "Risk_Factor": risk_factor,
                "Effective_Date": current_start,
                "Expiration_Date": expiration_date,
                "Renewal_Notice_Date": renewal_notice,
                "Payment_Frequency": payment_freq,
                "Discount_Percentage": discount,
                "Agent_Name": agent_name,
                "Channel": channel
            })
            
            policy_counter += 1
            
            if expiration_date >= date(2020, 12, 31):
                break
            
            # Renewal decision
            if not should_renew_policy(policy_type, policy_config, has_claims, income, credit_score):
                break
            
            current_start = expiration_date + timedelta(days=1)
            years_with_company = (current_start - effective_date).days / 365
            
            # Annual inflation increase (5-10%)
            if random.random() < 0.7:
                inflation_rate = random.uniform(1.05, 1.10)
                coverage = int(coverage * inflation_rate)
                coverage = max(policy_config["min_coverage"], min(coverage, policy_config["max_coverage"]))
            
            # Claims impact
            if random.random() < 0.15:
                has_claims = True
    
    return policies

# Generate policies for all customers
all_policies = []
sequence_start = 1
customer_policy_history = {}

for _, customer in tqdm(df_customers.iterrows(), total=len(df_customers), desc="Generating Realistic Policies"):
    customer_policies = generate_policies_with_renewals(customer, sequence_start, customer_policy_history)
    all_policies.extend(customer_policies)
    sequence_start += len(customer_policies)
    customer_policy_history[customer["Customer_ID"]] = len(customer_policies)

df_policies = pd.DataFrame(all_policies)

# Ensure correct data types
df_policies["Coverage_Amount"] = df_policies["Coverage_Amount"].astype(int)
df_policies["Premium_Amount"] = df_policies["Premium_Amount"].astype(int)
df_policies["Deductible_Amount"] = df_policies["Deductible_Amount"].astype(int)
df_policies["Risk_Factor"] = df_policies["Risk_Factor"].astype(float)
df_policies["Discount_Percentage"] = df_policies["Discount_Percentage"].astype(float)
df_policies["Effective_Date"] = pd.to_datetime(df_policies["Effective_Date"])
df_policies["Expiration_Date"] = pd.to_datetime(df_policies["Expiration_Date"])
df_policies["Renewal_Notice_Date"] = pd.to_datetime(df_policies["Renewal_Notice_Date"])

# Save to parquet
os.makedirs("insurance_data", exist_ok=True)
save_path = "insurance_data/insurance_policies.parquet"
df_policies.to_parquet(save_path, index=False)

print(f"Saved {len(df_policies)} policies to {save_path}")
print(f"Total unique customers: {df_policies['Applicant_ID'].nunique()}")
print(f"Average policies per customer: {len(df_policies) / df_policies['Applicant_ID'].nunique():.2f}")

print("\n=== POLICY DISTRIBUTION ===")
print(df_policies["Policy_Type"].value_counts())
print("\n=== PAYMENT FREQUENCY ===")
print(df_policies["Payment_Frequency"].value_counts())
print("\n=== CHANNEL DISTRIBUTION ===")
print(df_policies["Channel"].value_counts())

print("\n=== REINSURANCE ANALYSIS (Should be ~5-10%) ===")
reinsured_pct = (df_policies["Is_Reinsured"].sum() / len(df_policies)) * 100
print(f"Reinsured policies: {df_policies['Is_Reinsured'].sum()} ({reinsured_pct:.1f}%)")
print(df_policies[df_policies["Is_Reinsured"]]["Reinsurance_Type"].value_counts())

print("\n=== RENEWAL ANALYSIS ===")
for policy_type in df_policies["Policy_Type"].unique():
    type_policies = df_policies[df_policies["Policy_Type"] == policy_type]
    customers_with_type = type_policies.groupby("Applicant_ID").size()
    renewed_customers = (customers_with_type > 1).sum()
    total_customers = len(customers_with_type)
    if total_customers > 0:
        print(f"{policy_type}: {renewed_customers}/{total_customers} renewed ({renewed_customers/total_customers*100:.1f}%)")

print("\n=== COVERAGE & PREMIUM STATS ===")
print(df_policies.groupby("Policy_Type")[["Coverage_Amount", "Premium_Amount"]].describe())