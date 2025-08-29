import pandas as pd
import random
import numpy as np
from faker import Faker
from datetime import timedelta, datetime
from tqdm import tqdm
import os
from scipy import stats

# Seed for reproducibility
random.seed(42)
np.random.seed(42)
fake = Faker()

# Define cutoff for reasonable pending claims
REASONABLE_PENDING = pd.Timestamp('2020-08-30')
CUTOFF_DATE = pd.Timestamp('2020-12-31')

# Define policy types (for reinsurance_threshold)
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

# Load customer and policy data
try:
    df_customers = pd.read_parquet("insurance_data/insurance_applicants.parquet")
    df_policies = pd.read_parquet("insurance_data/insurance_policies.parquet")
except FileNotFoundError as e:
    print(f"Error: {e}")
    exit(1)

# Ensure datetime
df_policies["Effective_Date"] = pd.to_datetime(df_policies["Effective_Date"])
df_policies["Expiration_Date"] = pd.to_datetime(df_policies["Expiration_Date"])

# Validate required columns
required_policy_columns = [
    "Applicant_ID", "Policy_Number", "Policy_Type", "Coverage_Amount", 
    "Deductible_Amount", "Risk_Factor", "Is_Reinsured", 
    "Reinsurance_Company", "Reinsurance_Share", "XOL_Retention_Amount"
]
required_customer_columns = ["Customer_ID", "Is_Smoker", "Long_Term_Medication", "Job_Title"]

missing_policy_cols = [col for col in required_policy_columns if col not in df_policies.columns]
missing_customer_cols = [col for col in required_customer_columns if col not in df_customers.columns]

if missing_policy_cols or missing_customer_cols:
    print(f"Error: Missing columns in df_policies: {missing_policy_cols}")
    print(f"Error: Missing columns in df_customers: {missing_customer_cols}")
    exit(1)

# Define policy-specific claim settings
CLAIM_SETTINGS = {
    "Life": {
        "claim_prob": 0.08,
        "max_claims": 1,
        "amount_dist": lambda scale, claim_type: stats.lognorm.rvs(s=0.4, scale=scale * (0.9 if claim_type != "Vandalism" else 0.1), size=1)[0],
        "claim_types": ["Death", "Disability", "Critical Illness", "Terminal Illness"],
        "claim_type_weights": [0.6, 0.2, 0.15, 0.05],
        "processing_days_range": (30, 90),
        "fraud_prob": 0.02
    },
    "Health": {
        "claim_prob": 0.25,
        "max_claims": 4,
        "amount_dist": lambda scale, claim_type: stats.lognorm.rvs(s=0.6, scale=scale * (0.6 if claim_type != "Vandalism" else 0.1), size=1)[0],
        "claim_types": ["Hospitalization", "Surgery", "Outpatient", "Chronic Condition", "Emergency"],
        "claim_type_weights": [0.3, 0.25, 0.2, 0.15, 0.1],
        "processing_days_range": (14, 45),
        "fraud_prob": 0.08
    },
    "Car": {
        "claim_prob": 0.22,
        "max_claims": 3,
        "amount_dist": lambda scale, claim_type: stats.gamma.rvs(a=2, scale=scale / (3 if claim_type != "Vandalism" else 20), size=1)[0],
        "claim_types": ["Collision", "Theft", "Vandalism", "Accident", "Glass Damage"],
        "claim_type_weights": [0.35, 0.25, 0.15, 0.15, 0.1],
        "processing_days_range": (7, 30),
        "fraud_prob": 0.12
    },
    "Home": {
        "claim_prob": 0.18,
        "max_claims": 2,
        "amount_dist": lambda scale, claim_type: stats.gamma.rvs(a=1.5, scale=scale / (2 if claim_type != "Vandalism" else 10), size=1)[0],
        "claim_types": ["Fire", "Theft", "Water Damage", "Structural Damage", "Storm Damage"],
        "claim_type_weights": [0.25, 0.25, 0.2, 0.15, 0.15],
        "processing_days_range": (21, 60),
        "fraud_prob": 0.06
    },
    "Travel": {
        "claim_prob": 0.35,
        "max_claims": 3,
        "amount_dist": lambda scale, claim_type: stats.expon.rvs(scale=scale / (5 if claim_type != "Vandalism" else 20), size=1)[0],
        "claim_types": ["Trip Cancellation", "Medical Emergency", "Lost Luggage", "Delay", "Travel Accident"],
        "claim_type_weights": [0.3, 0.25, 0.2, 0.15, 0.1],
        "processing_days_range": (5, 21),
        "fraud_prob": 0.15
    },
    "Commercial": {
        "claim_prob": 0.12,
        "max_claims": 2,
        "amount_dist": lambda scale, claim_type: stats.lognorm.rvs(s=0.7, scale=scale * (0.8 if claim_type != "Vandalism" else 0.1), size=1)[0],
        "claim_types": ["Property Damage", "Liability", "Business Interruption", "Cyber Incident", "Equipment Breakdown"],
        "claim_type_weights": [0.3, 0.25, 0.2, 0.15, 0.1],
        "processing_days_range": (45, 120),
        "fraud_prob": 0.05
    }
}

# High-risk jobs for claim probability adjustment
HIGH_RISK_JOBS = ["Security Guard", "Construction Worker", "Miner", "Electrician", "Mechanic", "Pilot", "Driver", "Fisherman"]

# Claim handlers by specialty
CLAIM_HANDLERS = {
    "Life": ["John LifeAdjuster", "Sarah Benefits", "Michael DeathClaims", "Lisa Disability", "Robert Annuities"],
    "Health": ["Dr. Smith", "Nurse Johnson", "Medical Review Team", "Health Specialist", "Hospital Liaison"],
    "Car": ["Auto Specialist", "Accident Investigator", "Collision Expert", "Vehicle Assessor", "Repair Coordinator"],
    "Home": ["Property Adjuster", "Structural Engineer", "Home Claims Dept", "Building Inspector", "Damage Assessor"],
    "Travel": ["Travel Claims", "Emergency Services", "Trip Specialist", "Tour Coordinator", "Travel Agent Liaison"],
    "Commercial": ["Commercial Specialist", "Risk Manager", "Business Claims", "Corporate Adjuster", "Enterprise Risk"]
}

# Fraud indicators
FRAUD_INDICATORS = [
    "Late reporting", "Inconsistent story", "Previous claims history", "Suspicious documentation",
    "Cash payment request", "New policy claim", "High claim frequency", "Unusual circumstances",
    "Witness discrepancies", "Photo inconsistencies", "Provider history", "Geographic mismatch",
    "Missing police report"  # Added for vandalism
]

# External factors by year
EXTERNAL_FACTORS = {
    2015: {"claim_factor": 1.0, "fraud_factor": 1.0, "processing_factor": 1.0},
    2016: {"claim_factor": 1.05, "fraud_factor": 1.1, "processing_factor": 0.95},
    2017: {"claim_factor": 0.95, "fraud_factor": 0.9, "processing_factor": 1.05},
    2018: {"claim_factor": 1.1, "fraud_factor": 1.2, "processing_factor": 0.9},
    2019: {"claim_factor": 1.0, "fraud_factor": 1.0, "processing_factor": 1.0},
    2020: {"claim_factor": 1.3, "fraud_factor": 1.4, "processing_factor": 1.2}  # COVID impact
}

def determine_reinsurance_type(coverage, reinsurance_threshold, reinsurance_share, xol_retention):
    """Determine reinsurance type based on policy details"""
    if xol_retention and coverage > reinsurance_threshold:
        return "XOL"
    elif reinsurance_share and reinsurance_share > 0:
        return "Proportional"
    return "None"

def detect_fraud_indicators(customer_id, policy_type, claim_type, claim_amount, claim_date, all_claims):
    """Detect potential fraud indicators"""
    indicators = []
    
    # Previous claims
    customer_claims = [c for c in all_claims if c["Customer_ID"] == customer_id and c["Date_of_Claim"] < claim_date]
    if len(customer_claims) > 2:
        indicators.append("Multiple previous claims")
    
    # High frequency
    if customer_claims:
        last_claim_date = max(c["Date_of_Claim"] for c in customer_claims)
        if (claim_date - last_claim_date).days < 30:
            indicators.append("High claim frequency")
    
    # Large claim
    if claim_amount > 50000 and policy_type != "Car" and claim_type != "Vandalism":
        indicators.append("Unusually large claim")
    
    # New policy
    policy_data = df_policies[df_policies["Applicant_ID"] == customer_id]
    if not policy_data.empty:
        policy_start = policy_data["Effective_Date"].min()
        if (claim_date - policy_start).days < 30:
            indicators.append("New policy claim")
    
    # Vandalism-specific: missing police report
    if claim_type == "Vandalism" and random.random() < 0.3:
        indicators.append("Missing police report")
    
    # Policy-specific fraud probability
    fraud_prob = CLAIM_SETTINGS[policy_type]["fraud_prob"] * EXTERNAL_FACTORS.get(claim_date.year, {"fraud_factor": 1.0})["fraud_factor"]
    if random.random() < fraud_prob:
        indicators.append(random.choice(FRAUD_INDICATORS))
    
    return indicators

def calculate_processing_days(policy_type, claim_type, claim_amount, fraud_indicators, documentation_status):
    """Calculate realistic processing time"""
    base_min, base_max = CLAIM_SETTINGS[policy_type]["processing_days_range"]
    year_factor = EXTERNAL_FACTORS.get(claim_date.year, {"processing_factor": 1.0})["processing_factor"]
    
    processing_days = random.randint(int(base_min * year_factor), int(base_max * year_factor))
    
    # Adjustments
    if claim_amount > 50000 and claim_type != "Vandalism":
        processing_days *= 1.5
    if fraud_indicators:
        processing_days *= 1.3
    if documentation_status == "Partial":
        processing_days *= 1.2
    elif documentation_status == "Pending":
        processing_days *= 1.5
    if claim_type == "Vandalism":
        processing_days = min(processing_days, 20)  # Faster for minor vandalism
    
    return int(max(7, processing_days))

def determine_claim_status(claim_date, processing_days, fraud_indicators, policy_type, documentation_status):
    """Determine claim status with realistic pending logic"""
    settlement_date = claim_date + timedelta(days=processing_days)
    
    # Claims before REASONABLE_PENDING that settle by CUTOFF_DATE should be resolved
    if claim_date <= REASONABLE_PENDING and settlement_date <= CUTOFF_DATE:
        status_probs = [0.4, 0.4, 0.2] if fraud_indicators else [0.7, 0.2, 0.1]
        if policy_type in ["Life", "Commercial"]:
            status_probs = [status_probs[0] * 0.9, status_probs[1] * 1.1, status_probs[2]]
        if documentation_status == "Pending":
            status_probs = [status_probs[0] * 0.8, status_probs[1] * 1.2, status_probs[2]]
        status_probs = [p / sum(status_probs) for p in status_probs]
        status = np.random.choice(["Approved", "Rejected", "Pending"], p=status_probs)
    else:
        status = "Pending"  # Claims too close to cutoff or with long processing stay pending
    
    settlement_date = None if status == "Pending" else settlement_date
    return status, settlement_date

def calculate_settlement(claim_amount, deductible, coverage, status, fraud_indicators, policy_type, claim_type):
    """Calculate settlement amount with realistic adjustments"""
    if status != "Approved":
        return 0 if status == "Rejected" else None
    
    settlement = max(0, claim_amount - deductible)
    settlement = min(settlement, coverage)
    
    if fraud_indicators:
        settlement *= random.uniform(0.5, 0.8)
    if policy_type == "Health":
        settlement *= random.uniform(0.8, 0.95)
    elif policy_type == "Travel":
        settlement *= random.uniform(0.7, 0.9)
    elif claim_type == "Vandalism":
        settlement = min(settlement, 20000)  # Cap vandalism at R20,000
    
    return int(settlement)

def calculate_reinsurance_settlement(claim_amount, settlement, reinsurance_type, reinsurance_share, xol_retention):
    """Calculate reinsurer's portion of settlement"""
    if not settlement or settlement == 0:
        return None
    
    if reinsurance_type == "Proportional" and reinsurance_share:
        return int(settlement * reinsurance_share)
    elif reinsurance_type == "XOL" and xol_retention:
        if claim_amount > xol_retention:
            return int(min(settlement, claim_amount - xol_retention))
    return None

# Main claims generation
claims = []
claim_id_counter = 1

for idx, policy in tqdm(df_policies.iterrows(), total=len(df_policies), desc="Generating Claims"):
    customer_id = policy["Applicant_ID"]
    customer = df_customers[df_customers["Customer_ID"] == customer_id]
    if customer.empty:
        continue
    
    customer = customer.iloc[0]
    policy_number = policy["Policy_Number"]
    policy_type = policy["Policy_Type"]
    
    if policy_type not in POLICY_TYPES:
        continue
    
    coverage = policy["Coverage_Amount"]
    deductible = policy["Deductible_Amount"]
    effective_date = max(pd.Timestamp('2015-01-01'), policy["Effective_Date"])
    expiration_date = min(CUTOFF_DATE, policy["Expiration_Date"])
    risk_factor = policy["Risk_Factor"]
    is_reinsured = policy["Is_Reinsured"]
    reinsurance_company = policy["Reinsurance_Company"]
    reinsurance_share = policy["Reinsurance_Share"]
    xol_retention_amount = policy["XOL_Retention_Amount"]
    reinsurance_threshold = POLICY_TYPES[policy_type]["reinsurance_threshold"]
    
    if effective_date >= expiration_date:
        continue

    # Adjust claim probability
    claim_settings = CLAIM_SETTINGS[policy_type]
    base_claim_prob = claim_settings["claim_prob"]
    claim_prob = base_claim_prob
    
    if customer["Is_Smoker"] == "Yes" and policy_type in ["Life", "Health"]:
        claim_prob *= 1.5
    if customer["Long_Term_Medication"] == "Yes" and policy_type in ["Life", "Health"]:
        claim_prob *= 1.3
    if customer["Job_Title"] in HIGH_RISK_JOBS:
        claim_prob *= 1.2
    
    year_factors = [EXTERNAL_FACTORS.get(y, {"claim_factor": 1.0})["claim_factor"] 
                    for y in range(effective_date.year, expiration_date.year + 1)]
    claim_prob *= np.mean(year_factors) if year_factors else 1.0
    claim_prob = min(claim_prob * risk_factor, 0.9)

    has_claim = stats.bernoulli.rvs(p=claim_prob)
    if not has_claim:
        continue

    duration_years = (expiration_date - effective_date).days / 365
    max_claims = min(claim_settings["max_claims"], int(duration_years * 1.5))
    num_claims = np.random.binomial(n=max_claims, p=0.5)
    num_claims = max(1, num_claims)

    for _ in range(num_claims):
        delta_days = (expiration_date - effective_date).days
        claim_day = int(stats.beta.rvs(a=3 if policy_type == "Travel" else 2, b=2 if policy_type == "Travel" else 3, size=1)[0] * delta_days)
        claim_date = effective_date + timedelta(days=claim_day)
        
        if claim_date > CUTOFF_DATE:
            continue

        claim_type = np.random.choice(claim_settings["claim_types"], p=claim_settings["claim_type_weights"])
        scale = coverage - deductible
        claim_amount = int(np.clip(claim_settings["amount_dist"](scale, claim_type), 0, min(scale, 20000 if claim_type == "Vandalism" else scale)))

        fraud_indicators = detect_fraud_indicators(customer_id, policy_type, claim_type, claim_amount, claim_date, claims)
        documentation_status = random.choice(["Complete", "Partial", "Pending"])
        processing_days = calculate_processing_days(policy_type, claim_type, claim_amount, fraud_indicators, documentation_status)
        status, settlement_date = determine_claim_status(claim_date, processing_days, fraud_indicators, policy_type, documentation_status)
        settlement = calculate_settlement(claim_amount, deductible, coverage, status, fraud_indicators, policy_type, claim_type)
        reinsurance_type = determine_reinsurance_type(coverage, reinsurance_threshold, reinsurance_share, xol_retention_amount)
        reinsurer_settlement = calculate_reinsurance_settlement(claim_amount, settlement, reinsurance_type, reinsurance_share, xol_retention_amount)
        claim_handler = random.choice(CLAIM_HANDLERS[policy_type])

        claims.append({
            "Claim_ID": f"CLM{claim_id_counter:06d}",
            "Customer_ID": customer_id,
            "Policy_Number": policy_number,
            "Claim_Type": claim_type,
            "Claim_Amount": claim_amount,
            "Date_of_Claim": claim_date,
            "Status": status,
            "Settlement_Amount": settlement,
            "Date_of_Settlement": settlement_date,
            "Processing_Days": processing_days,
            "Reinsurance": "Yes" if reinsurance_type != "None" else "No",
            "Reinsurance_Type": reinsurance_type,
            "Reinsurer_Settlement": reinsurer_settlement,
            "Reinsurance_Company": reinsurance_company if reinsurance_type != "None" else None,
            "Claim_Handler": claim_handler,
            "Fraud_Indicators": "; ".join(fraud_indicators) if fraud_indicators else None,
            "Complexity_Level": random.choice(["Simple", "Moderate", "Complex"]),
            "Documentation_Status": documentation_status
        })
        
        claim_id_counter += 1

# Convert to DataFrame
df_claims = pd.DataFrame(claims)

# Save to Parquet
os.makedirs("insurance_data", exist_ok=True)
save_path = "insurance_data/claims_history.parquet"
df_claims.to_parquet(save_path, index=False)

print(f"Saved {len(df_claims)} claim records to {save_path}")
print("\nClaim status distribution:")
print(df_claims["Status"].value_counts())
print("\nPolicy type distribution:")
policy_type_map = df_policies.set_index("Policy_Number")["Policy_Type"]
df_claims["Policy_Type"] = df_claims["Policy_Number"].map(policy_type_map)
print(df_claims["Policy_Type"].value_counts())
print("\nPending claims by month (2019-2020):")
pending_claims = df_claims[(df_claims["Status"] == "Pending") & (df_claims["Date_of_Claim"].dt.year >= 2019)]
print(pending_claims["Date_of_Claim"].dt.to_period("M").value_counts().sort_index())
print("\nSample claims:")
print(df_claims.head(10))