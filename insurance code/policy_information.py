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
required_customer_columns = ["Customer_ID", "Age", "Monthly_Income_ZAR", "Assets_Value_ZAR", "Address", "Is_Smoker", "Long_Term_Medication", "Alcohol_Use", "Job_Title"]
missing_customer_cols = [col for col in required_customer_columns if col not in df_customers.columns]
if missing_customer_cols:
    raise ValueError(f"Missing columns in df_customers: {missing_customer_cols}")

# Define policy types with XOL eligibility and reinsurance flags
POLICY_TYPES = {
    "Life": {
        "coverage_factor": 10, 
        "base_premium": 2000,
        "has_xol": True,
        "reinsurance_threshold": 1000000
    },
    "Health": {
        "coverage_factor": 5, 
        "base_premium": 1500,
        "has_xol": True,
        "reinsurance_threshold": 500000
    },
    "Car": {
        "coverage_factor": 0.5, 
        "base_premium": 1000,
        "has_xol": False,
        "reinsurance_threshold": 250000
    },
    "Home": {
        "coverage_factor": 1, 
        "base_premium": 1200,
        "has_xol": True,
        "reinsurance_threshold": 750000
    },
    "Travel": {
        "coverage_factor": 0.1, 
        "base_premium": 500,
        "has_xol": False,
        "reinsurance_threshold": 100000
    },
    "Commercial": {
        "coverage_factor": 2,
        "base_premium": 3000,
        "has_xol": True,
        "reinsurance_threshold": 500000
    }
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
        "weights": [0.4, 0.3, 0.2, 0.05, 0.05]
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

# XOL applicable subtypes
XOL_APPLICABLE_SUBTYPES = [
    "Group Life", "Executive Health", "Fleet", "High-Net-Worth", 
    "Commercial", "Property", "Liability", "Professional Indemnity",
    "Directors & Officers", "Cyber"
]

# Specific reinsurance assignments
SPECIFIC_REINSURANCE = {
    ("Home", "Landlord"): {"type": "Proportional", "company": "Sanlam Re"},
    ("Life", "Group Life"): {"type": "XOL", "company": "Swiss Re Africa"},
    ("Health", "Executive Health"): {"type": "XOL", "company": "RGA Re Africa"},
    ("Car", "Fleet"): {"type": "Proportional", "company": "Hollard Re"},
    ("Commercial", "Cyber"): {"type": "XOL", "company": "Munich Re"},
    ("Commercial", "Property"): {"type": "Proportional", "company": "Africa Re"},
}

# Reinsurance companies
REINSURANCE_COMPANIES = [
    "Munich Re", "Swiss Re", "Hannover Re", "SCOR", "Lloyd's of London",
    "Berkshire Hathaway Re", "Everest Re", "PartnerRe", "AXIS Re", "RenaissanceRe",
    "Transatlantic Re", "Arch Re", "Aspen Re", "Validus Re", "XL Catlin Re",
    "Africa Re", "Munich Re Africa", "Swiss Re Africa", "Santam Re", "Guardrisk Re",
    "Hollard Re", "Old Mutual Re", "Gen Re Africa", "RGA Re Africa", "Absa Re",
    "Discovery Re", "Liberty Re", "Momentum Re", "Sanlam Re", "OUTsurance Re"
]

# Payment frequency options with weights
PAYMENT_FREQ = {
    "options": ["Monthly", "Quarterly", "Annually", "Single Premium"],
    "weights": [0.6, 0.2, 0.15, 0.05]
}

# Agents by region
AGENTS_BY_REGION = {
    "Gauteng": ["Thabo Mokoena", "Sarah Nkosi", "David Pretorius", "Emma Botha", "Lerato Sithole", "Pieter van Zyl", "Grace Mabaso"],
    "Western Cape": ["Michael de Vries", "Aisha Patel", "James Bothma", "Nadine Cloete", "Sipho Ndlovu"],
    "KwaZulu-Natal": ["Zanele Dlamini", "Richard Naidoo", "Kathy Govender", "Sibusiso Mkhize", "Claire Pillay"],
    "Eastern Cape": ["Nomvula Ngece", "Andrew Smith", "Lungile Mbatha", "Tanya Adams", "Bongani Xaba"],
    "Free State": ["Rethabile Moloi", "Jacques du Plessis", "Nthabiseng Tshabalala", "Willem Steyn"],
    "Mpumalanga": ["Siphiwe Ngwenya", "Annelize Kruger", "Themba Mahlangu", "Vicky Nkuna"],
    "Limpopo": ["Mpho Makgoba", "Rendani Netshifhefhe", "Tshepo Malatji", "Lindiwe Phalane"],
    "North West": ["Kagiso Modise", "Amelia Fourie", "Tumelo Moagi", "Boitumelo Seema"],
    "Northern Cape": ["Karabo van Niekerk", "Hendrik Coetzee", "Fatima Abrahams", "John Kgoele"]
}

CHANNELS = ["Online", "Walk-in", "Phone", "Broker", "Corporate"]

# Coverage limits by policy type
COVERAGE_LIMITS = {
    "Life": {"min": 100000, "max": 10000000},
    "Health": {"min": 50000, "max": 5000000},
    "Car": {"min": 50000, "max": 2000000},
    "Home": {"min": 200000, "max": 10000000},
    "Travel": {"min": 10000, "max": 1000000},
    "Commercial": {"min": 500000, "max": 50000000}
}

# Deductible options by policy type
DEDUCTIBLE_OPTIONS = {
    "Life": {
        "options": [0, 5000, 10000, 25000],
        "weights": [0.1, 0.2, 0.3, 0.4]
    },
    "Health": {
        "options": [1000, 2000, 5000, 10000, 20000],
        "weights": [0.1, 0.2, 0.3, 0.25, 0.15]
    },
    "Car": {
        "options": [2500, 5000, 7500, 10000, 15000],
        "weights": [0.1, 0.2, 0.3, 0.25, 0.15]
    },
    "Home": {
        "options": [5000, 10000, 15000, 25000, 50000],
        "weights": [0.1, 0.2, 0.3, 0.25, 0.15]
    },
    "Travel": {
        "options": [500, 1000, 2000, 5000],
        "weights": [0.1, 0.2, 0.3, 0.4]
    },
    "Commercial": {
        "options": [10000, 25000, 50000, 100000, 250000],
        "weights": [0.1, 0.2, 0.3, 0.25, 0.15]
    }
}

def generate_policy_number(policy_type, effective_date, sequence):
    """Generate realistic policy number format"""
    year = effective_date.year
    prefix = {"Life": "L", "Health": "H", "Car": "A", "Home": "P", "Travel": "T", "Commercial": "C"}
    return f"ZA{prefix[policy_type]}{year}{str(sequence).zfill(6)}"

def calculate_risk_factor(customer, policy_type):
    """Calculate risk factor based on customer profile using statistical distributions"""
    risk_factor = 1.0
    
    # Age risk
    if policy_type in ["Life", "Health"]:
        age_diff = max(0, customer["Age"] - 25)
        age_risk = 1.0 + norm.rvs(loc=0, scale=0.015, size=1)[0] * age_diff
        risk_factor *= max(1.0, age_risk)
    
    # Smoking increases risk
    if customer["Is_Smoker"] == "Yes" and policy_type in ["Life", "Health"]:
        smoking_factor = 1.0 + beta.rvs(a=2, b=3, size=1)[0] * 1.5
        risk_factor *= smoking_factor
    
    # Health conditions
    if customer["Long_Term_Medication"] == "Yes" and policy_type in ["Life", "Health"]:
        health_factor = 1.0 + gamma.rvs(a=1.5, scale=0.3, size=1)[0]
        risk_factor *= health_factor
    
    # High alcohol consumption
    if customer["Alcohol_Use"] == "High" and policy_type in ["Life", "Health"]:
        alcohol_factor = 1.0 + expon.rvs(scale=0.25, size=1)[0]
        risk_factor *= alcohol_factor
    
    # High-risk occupations
    high_risk_jobs = ["Security Guard", "Construction Worker", "Miner", "Electrician", "Mechanic", "Pilot"]
    if customer["Job_Title"] in high_risk_jobs and policy_type in ["Life", "Health"]:
        job_risk = 1.0 + norm.rvs(loc=0.4, scale=0.2, size=1)[0]
        risk_factor *= max(1.0, job_risk)
    
    return round(max(1.0, risk_factor), 2)

def determine_xol_and_reinsurance(policy_type, subtype, coverage_amount, policy_config):
    """Determine reinsurance details, distinguishing proportional and XOL"""
    has_xol = False
    is_reinsured = False
    reinsurance_company = None
    reinsurance_share = None
    xol_retention_amount = None
    reinsurance_type = None
    
    key = (policy_type, subtype)
    specific = SPECIFIC_REINSURANCE.get(key, None)
    
    coverage_ratio = coverage_amount / policy_config["reinsurance_threshold"]
    if coverage_ratio > 0.8:
        reinsurance_prob = min(0.95, 0.3 + coverage_ratio * 0.7)
        if specific:
            reinsurance_prob = 1.0
        is_reinsured = beta.rvs(a=2, b=2, size=1)[0] < reinsurance_prob
    
    if is_reinsured:
        if specific:
            reinsurance_type = specific["type"]
            reinsurance_company = specific["company"]
        else:
            if subtype in XOL_APPLICABLE_SUBTYPES and policy_config["has_xol"]:
                xol_prob = beta.rvs(a=4, b=1.5, size=1)[0]
                reinsurance_type = 'XOL' if xol_prob > 0.3 else 'Proportional'
            else:
                xol_prob = beta.rvs(a=2, b=4, size=1)[0]
                reinsurance_type = 'XOL' if xol_prob > 0.6 and policy_config["has_xol"] else 'Proportional'
            reinsurance_company = random.choice(REINSURANCE_COMPANIES)
        
        if reinsurance_type == 'XOL':
            has_xol = True
            xol_retention_amount = int(norm.rvs(
                loc=policy_config["reinsurance_threshold"] * 0.85, 
                scale=policy_config["reinsurance_threshold"] * 0.1, 
                size=1
            )[0])
            reinsurance_share = None
        elif reinsurance_type == 'Proportional':
            has_xol = False
            reinsurance_share = beta.rvs(a=2, b=1.5, loc=0.4, scale=0.5, size=1)[0]
            reinsurance_share = round(min(0.9, reinsurance_share), 2)
            xol_retention_amount = None
    
    return has_xol, is_reinsured, reinsurance_company, reinsurance_share, xol_retention_amount, reinsurance_type

def get_related_policies(existing_policies, customer):
    """Suggest related policies based on existing ones and customer profile"""
    related = []
    
    # Business customers might need commercial policies
    if customer["Job_Title"] in ["Manager", "Director", "Business Owner", "Executive"]:
        commercial_prob = beta.rvs(a=2, b=3, size=1)[0]
        if commercial_prob > 0.6:
            related.append("Commercial")
    
    # Home owners might need additional coverage
    if "Home" in existing_policies and customer["Assets_Value_ZAR"] > 500000:
        home_prob = beta.rvs(a=2, b=4, size=1)[0]
        if home_prob > 0.7:
            related.append("Home")
    
    # High-income individuals might need life insurance
    if customer["Monthly_Income_ZAR"] > 75000 and "Life" not in existing_policies:
        life_prob = beta.rvs(a=3, b=3, size=1)[0]
        if life_prob > 0.5:
            related.append("Life")
    
    return list(set(related))

def generate_policies(customer, sequence_start):
    """Generate policies for a customer, ensuring at least one policy"""
    policies = []
    age = customer["Age"]
    income = customer["Monthly_Income_ZAR"]
    assets = customer["Assets_Value_ZAR"]
    
    # Determine if commercial customer
    is_commercial_customer = customer["Job_Title"] in ["Business Owner", "Director", "Executive", "Manager"]
    
    # Number of policies (minimum 1)
    if is_commercial_customer:
        num_policies = max(1, poisson.rvs(mu=2.5, size=1)[0])
    else:
        num_policies = max(1, poisson.rvs(mu=1.5, size=1)[0])
    num_policies = min(num_policies, 5)  # Cap at 5 policies

    # Extract province from address
    address_parts = customer["Address"].split(",")
    customer_region = address_parts[-2].strip() if len(address_parts) >= 2 else "Unknown"
    agents_region = AGENTS_BY_REGION.get(customer_region, ["Online Agent"])

    # Base policy selection
    base_policies = []
    if is_commercial_customer:
        base_policies = ["Commercial", "Life", "Health"]
    elif age < 26:
        base_policies = ["Travel", "Car"]
    elif age <= 50:
        base_policies = ["Car", "Health", "Home"]
    else:
        base_policies = ["Health", "Life", "Home"]
    
    # Add policies based on financial situation
    if income > 30000 or assets > 500000:
        base_policies.extend(["Life", "Health"])
    if income > 50000:
        base_policies.append("Travel")
    
    # Remove duplicates and ensure at least one policy
    base_policies = list(set(base_policies))
    if not base_policies:
        base_policies = ["Health"]  # Default to Health if no policies selected
    
    # Get related policies
    related_policies = get_related_policies(base_policies, customer)
    all_possible = list(set(base_policies + related_policies))
    
    # Select policies (ensure at least one)
    num_possible = min(num_policies, len(all_possible))
    selected_policies = random.sample(all_possible, k=num_possible) if num_possible <= len(all_possible) else all_possible

    for idx, policy_type in enumerate(selected_policies):
        policy_config = POLICY_TYPES[policy_type]
        coverage_factor = policy_config["coverage_factor"]
        base_premium = policy_config["base_premium"]
        
        # Choose subtype
        subtype_info = POLICY_SUBTYPES[policy_type]
        subtype = np.random.choice(subtype_info["options"], p=subtype_info["weights"])

        # Coverage calculation
        min_coverage = COVERAGE_LIMITS[policy_type]["min"]
        max_coverage = min(COVERAGE_LIMITS[policy_type]["max"], coverage_factor * income * 12)
        mean_log_coverage = np.log(max_coverage * 0.7)
        coverage = int(lognorm.rvs(s=0.5, scale=np.exp(mean_log_coverage), size=1)[0])
        coverage = max(min_coverage, min(coverage, max_coverage))

        # Risk-based premium calculation
        risk_factor = calculate_risk_factor(customer, policy_type)
        age_factor = 1 + max(0, (age - 40) / 50) if policy_type in ["Life", "Health"] else 1
        premium_variation = norm.rvs(loc=1.0, scale=0.1, size=1)[0]
        premium = int(base_premium * age_factor * risk_factor * premium_variation)
        if not isinstance(premium, (int, float)) or premium <= 0:
            print(f"Warning: Invalid premium {premium} for Customer_ID {customer['Customer_ID']}, Policy_Type {policy_type}")
            premium = base_premium  # Fallback to base_premium

        # Deductible selection
        deductible_info = DEDUCTIBLE_OPTIONS[policy_type]
        deductible = np.random.choice(deductible_info["options"], p=deductible_info["weights"])

        # Reinsurance details
        has_xol, is_reinsured, reinsurance_company, reinsurance_share, xol_retention_amount, reinsurance_type = determine_xol_and_reinsurance(
            policy_type, subtype, coverage, policy_config
        )

        # Policy dates
        start_date = date(2015, 1, 1)
        end_date = date(2020, 12, 31)
        days_range = (end_date - start_date).days
        days_offset = int(beta.rvs(a=2, b=3, size=1)[0] * days_range)
        effective_date = start_date + timedelta(days=days_offset)

        # Policy duration
        if policy_type in ["Life", "Health"]:
            duration_years = gamma.rvs(a=2, scale=2, size=1)[0] + 1
            duration_days = int(duration_years * 365)
        elif policy_type == "Travel":
            duration_days = expon.rvs(scale=60, size=1)[0] + 30
        else:
            duration_years = gamma.rvs(a=1.5, scale=1, size=1)[0] + 1
            duration_days = int(duration_years * 365)
        
        expiration_date = effective_date + timedelta(days=int(duration_days))
        expiration_date = min(expiration_date, date(2020, 12, 31))

        # Renewal notice
        renewal_days = int(norm.rvs(loc=60, scale=15, size=1)[0])
        renewal_notice = expiration_date - timedelta(days=renewal_days)
        renewal_notice = max(renewal_notice, effective_date)
        renewal_notice = min(renewal_notice, date(2020, 12, 31))

        # Payment frequency
        payment_freq = np.random.choice(PAYMENT_FREQ["options"], p=PAYMENT_FREQ["weights"])
        
        # Discount
        discount = round(beta.rvs(a=1.5, b=4, scale=15, size=1)[0], 2)

        # Channel and agent assignment
        if customer["Job_Title"] == "Student":
            channel_weights = [0.5, 0.3, 0.2, 0.0, 0.0]
        elif is_commercial_customer:
            channel_weights = [0.2, 0.1, 0.1, 0.3, 0.3]
        else:
            channel_weights = [0.4, 0.3, 0.2, 0.05, 0.05]
        
        channel = np.random.choice(CHANNELS, p=channel_weights)
        
        if channel == "Walk-in":
            agent_name = random.choice(agents_region)
        elif channel == "Broker":
            agent_name = "Broker Channel"
        elif channel == "Corporate":
            agent_name = "Corporate Account Manager"
        else:
            agent_name = random.choice([a for agents in AGENTS_BY_REGION.values() for a in agents] + ["Online Agent"])

        policies.append({
            "Policy_Number": generate_policy_number(policy_type, effective_date, sequence_start + idx + 1),
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
            "Effective_Date": effective_date,
            "Expiration_Date": expiration_date,
            "Renewal_Notice_Date": renewal_notice,
            "Payment_Frequency": payment_freq,
            "Discount_Percentage": discount,
            "Agent_Name": agent_name,
            "Channel": channel
        })
    
    if not policies:
        print(f"Warning: No policies generated for Customer_ID {customer['Customer_ID']}")
        # Generate a default Health policy
        policy_type = "Health"
        policy_config = POLICY_TYPES[policy_type]
        coverage_factor = policy_config["coverage_factor"]
        base_premium = policy_config["base_premium"]
        subtype = np.random.choice(POLICY_SUBTYPES[policy_type]["options"], p=POLICY_SUBTYPES[policy_type]["weights"])
        min_coverage = COVERAGE_LIMITS[policy_type]["min"]
        max_coverage = min(COVERAGE_LIMITS[policy_type]["max"], coverage_factor * income * 12)
        mean_log_coverage = np.log(max_coverage * 0.7)
        coverage = int(lognorm.rvs(s=0.5, scale=np.exp(mean_log_coverage), size=1)[0])
        coverage = max(min_coverage, min(coverage, max_coverage))
        risk_factor = calculate_risk_factor(customer, policy_type)
        age_factor = 1 + max(0, (age - 40) / 50)
        premium_variation = norm.rvs(loc=1.0, scale=0.1, size=1)[0]
        premium = int(base_premium * age_factor * risk_factor * premium_variation)
        deductible = np.random.choice(DEDUCTIBLE_OPTIONS[policy_type]["options"], p=DEDUCTIBLE_OPTIONS[policy_type]["weights"])
        has_xol, is_reinsured, reinsurance_company, reinsurance_share, xol_retention_amount, reinsurance_type = determine_xol_and_reinsurance(
            policy_type, subtype, coverage, policy_config
        )
        effective_date = start_date + timedelta(days=int(beta.rvs(a=2, b=3, size=1)[0] * days_range))
        duration_years = gamma.rvs(a=2, scale=2, size=1)[0] + 1
        expiration_date = min(effective_date + timedelta(days=int(duration_years * 365)), date(2020, 12, 31))
        renewal_days = int(norm.rvs(loc=60, scale=15, size=1)[0])
        renewal_notice = max(effective_date, min(expiration_date - timedelta(days=renewal_days), date(2020, 12, 31)))
        payment_freq = np.random.choice(PAYMENT_FREQ["options"], p=PAYMENT_FREQ["weights"])
        discount = round(beta.rvs(a=1.5, b=4, scale=15, size=1)[0], 2)
        channel = np.random.choice(CHANNELS, p=channel_weights)
        agent_name = random.choice(agents_region) if channel == "Walk-in" else \
                     "Broker Channel" if channel == "Broker" else \
                     "Corporate Account Manager" if channel == "Corporate" else \
                     random.choice([a for agents in AGENTS_BY_REGION.values() for a in agents] + ["Online Agent"])
        
        policies.append({
            "Policy_Number": generate_policy_number(policy_type, effective_date, sequence_start + 1),
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
            "Effective_Date": effective_date,
            "Expiration_Date": expiration_date,
            "Renewal_Notice_Date": renewal_notice,
            "Payment_Frequency": payment_freq,
            "Discount_Percentage": discount,
            "Agent_Name": agent_name,
            "Channel": channel
        })
    
    return policies

# Generate policies for all customers
all_policies = []
sequence_start = 1
for _, customer in tqdm(df_customers.iterrows(), total=len(df_customers), desc="Generating Policies"):
    customer_policies = generate_policies(customer, sequence_start)
    all_policies.extend(customer_policies)
    sequence_start += len(customer_policies)

df_policies = pd.DataFrame(all_policies)

# Validate that every customer has at least one policy
customer_ids = set(df_customers["Customer_ID"])
policy_customer_ids = set(df_policies["Applicant_ID"])
missing_customers = customer_ids - policy_customer_ids
if missing_customers:
    print(f"Warning: {len(missing_customers)} customers have no policies: {missing_customers}")

# Validate Premium_Amount
invalid_premiums = df_policies[~df_policies["Premium_Amount"].apply(lambda x: isinstance(x, (int, float)) and x > 0)]
if not invalid_premiums.empty:
    print(f"Error: Invalid Premium_Amount values in df_policies: {invalid_premiums[['Policy_Number', 'Premium_Amount']].to_dict()}")
    raise ValueError("Invalid Premium_Amount detected")

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
print("\nPolicy type distribution:")
print(df_policies["Policy_Type"].value_counts())
print("\nXOL distribution:")
print(df_policies["Has_XOL"].value_counts())
print("\nReinsurance distribution:")
print(df_policies["Is_Reinsured"].value_counts())
print("\nReinsurance type distribution:")
print(df_policies["Reinsurance_Type"].value_counts())
print("\nSample policies:")
print(df_policies[["Policy_Number", "Applicant_ID", "Policy_Type", "Policy_Subtype", "Premium_Amount", "Coverage_Amount", "Payment_Frequency"]].head())