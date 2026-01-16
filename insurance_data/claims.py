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

# Define cutoff dates
REASONABLE_PENDING = pd.Timestamp('2020-08-30')
CUTOFF_DATE = pd.Timestamp('2020-12-31')

# REALISTIC reinsurance thresholds
POLICY_TYPES = {
    "Life": {"reinsurance_threshold": 5000000, "medical_exam_threshold": 500000},
    "Health": {"reinsurance_threshold": 1000000, "network_differential": 0.3},
    "Car": {"reinsurance_threshold": 1000000, "depreciation_rate": 0.10},
    "Home": {"reinsurance_threshold": 5000000, "underinsurance_penalty": 0.80},
    "Travel": {"reinsurance_threshold": 300000, "pre_existing_exclusion": True},
    "Commercial": {"reinsurance_threshold": 10000000, "aggregate_limit": True}
}

# Load customer and policy data
try:
    df_customers = pd.read_parquet("insurance_data/insurance_applicants.parquet")
    df_policies = pd.read_parquet("insurance_data/insurance_policies.parquet")
    df_payments = pd.read_parquet("insurance_data/payment_history.parquet")
except FileNotFoundError as e:
    print(f"Error: {e}")
    exit(1)

# Ensure datetime
df_policies["Effective_Date"] = pd.to_datetime(df_policies["Effective_Date"])
df_policies["Expiration_Date"] = pd.to_datetime(df_policies["Expiration_Date"])
df_payments["Payment_Date"] = pd.to_datetime(df_payments["Payment_Date"])

# Geographic risk factors
GEOGRAPHIC_RISK = {
    "Gauteng": {"Car": 1.20, "Home": 1.10, "theft_factor": 1.25},
    "Western Cape": {"Car": 1.10, "Home": 1.25, "water_damage": 1.40},
    "KwaZulu-Natal": {"Car": 1.10, "Home": 1.15, "storm_damage": 1.30},
    "Eastern Cape": {"Car": 1.05, "Home": 1.05, "theft_factor": 1.15},
}

# Seasonal claim patterns (multipliers by month)
SEASONAL_PATTERNS = {
    "Car": [1.0, 0.95, 0.95, 1.0, 1.0, 1.0, 1.05, 1.05, 1.0, 1.05, 1.10, 1.35],  # Dec peak
    "Home": [1.0, 1.0, 1.0, 1.05, 1.15, 1.25, 1.20, 1.15, 1.05, 1.0, 1.0, 1.05],  # Winter burglaries
    "Health": [1.05, 1.10, 1.05, 1.0, 1.15, 1.25, 1.20, 1.10, 1.0, 0.95, 0.95, 1.0],  # Winter flu
    "Travel": [1.0, 0.9, 0.9, 1.1, 1.0, 1.0, 1.3, 1.2, 1.0, 1.0, 1.0, 1.4],  # Holidays
    "Life": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
    "Commercial": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
}

# Claim amount distributions by type
CLAIM_AMOUNT_DISTRIBUTIONS = {
    "Car": {
        "small": (0.60, 5000, 25000),    # 60% of claims
        "medium": (0.30, 25000, 100000),  # 30% of claims
        "large": (0.10, 100000, 500000)   # 10% of claims
    },
    "Home": {
        "small_theft": (0.40, 5000, 30000),
        "medium_damage": (0.40, 30000, 150000),
        "major_damage": (0.20, 150000, 1000000)
    },
    "Health": {
        "outpatient": (0.50, 1000, 10000),
        "hospitalization": (0.35, 10000, 100000),
        "major_surgery": (0.15, 100000, 500000)
    },
    "Life": {
        "full_payout": (1.0, 0.90, 1.0)  # 90-100% of coverage
    },
    "Travel": {
        "small": (0.65, 1000, 10000),
        "medium": (0.25, 10000, 50000),
        "large": (0.10, 50000, 200000)
    }
}

# Define policy-specific claim settings
CLAIM_SETTINGS = {
    "Life": {
        "claim_prob": 0.05,
        "max_claims": 1,
        "claim_types": ["Death", "Disability", "Critical Illness", "Terminal Illness"],
        "claim_type_weights": [0.6, 0.2, 0.15, 0.05],
        "processing_days_range": (45, 120),
        "fraud_prob": 0.02
    },
    "Health": {
        "claim_prob": 0.30,
        "max_claims": 5,
        "claim_types": ["Hospitalization", "Surgery", "Outpatient", "Chronic Condition", "Emergency"],
        "claim_type_weights": [0.3, 0.25, 0.2, 0.15, 0.1],
        "processing_days_range": (7, 30),
        "fraud_prob": 0.08
    },
    "Car": {
        "claim_prob": 0.18,
        "max_claims": 3,
        "claim_types": ["Collision", "Theft", "Vandalism", "Accident", "Glass Damage"],
        "claim_type_weights": [0.35, 0.25, 0.15, 0.15, 0.1],
        "processing_days_range": (7, 21),
        "fraud_prob": 0.12
    },
    "Home": {
        "claim_prob": 0.12,
        "max_claims": 2,
        "claim_types": ["Fire", "Theft", "Water Damage", "Structural Damage", "Storm Damage"],
        "claim_type_weights": [0.25, 0.25, 0.2, 0.15, 0.15],
        "processing_days_range": (14, 45),
        "fraud_prob": 0.06
    },
    "Travel": {
        "claim_prob": 0.25,
        "max_claims": 2,
        "claim_types": ["Trip Cancellation", "Medical Emergency", "Lost Luggage", "Delay", "Travel Accident"],
        "claim_type_weights": [0.3, 0.25, 0.2, 0.15, 0.1],
        "processing_days_range": (5, 14),
        "fraud_prob": 0.15
    },
    "Commercial": {
        "claim_prob": 0.08,
        "max_claims": 2,
        "claim_types": ["Property Damage", "Liability", "Business Interruption", "Cyber Incident", "Equipment Breakdown"],
        "claim_type_weights": [0.3, 0.25, 0.2, 0.15, 0.1],
        "processing_days_range": (30, 90),
        "fraud_prob": 0.05
    }
}

# High-risk jobs
HIGH_RISK_JOBS = ["Security Guard", "Construction Worker", "Miner", "Electrician", "Mechanic", "Pilot", "Driver", "Fisherman"]

# Claim handlers
CLAIM_HANDLERS = {
    "Life": ["John LifeAdjuster", "Sarah Benefits", "Michael DeathClaims", "Lisa Disability", "Robert Annuities"],
    "Health": ["Dr. Smith", "Nurse Johnson", "Medical Review Team", "Health Specialist", "Hospital Liaison"],
    "Car": ["Auto Specialist", "Accident Investigator", "Collision Expert", "Vehicle Assessor", "Repair Coordinator"],
    "Home": ["Property Adjuster", "Structural Engineer", "Home Claims Dept", "Building Inspector", "Damage Assessor"],
    "Travel": ["Travel Claims", "Emergency Services", "Trip Specialist", "Tour Coordinator", "Travel Agent Liaison"],
    "Commercial": ["Commercial Specialist", "Risk Manager", "Business Claims", "Corporate Adjuster", "Enterprise Risk"]
}

# Enhanced fraud indicators
FRAUD_INDICATORS = [
    "Late reporting", "Inconsistent story", "Previous claims history", "Suspicious documentation",
    "Cash payment request", "New policy claim", "High claim frequency", "Unusual circumstances",
    "Witness discrepancies", "Photo inconsistencies", "Provider history", "Geographic mismatch",
    "Missing police report", "Weekend claim", "Holiday claim", "Multiple recent claims",
    "Same provider pattern", "Documentation metadata issues", "Social media contradiction"
]

# External factors
EXTERNAL_FACTORS = {
    2015: {"claim_factor": 1.0, "fraud_factor": 1.0, "processing_factor": 1.0},
    2016: {"claim_factor": 1.05, "fraud_factor": 1.1, "processing_factor": 0.95},
    2017: {"claim_factor": 0.95, "fraud_factor": 0.9, "processing_factor": 1.05},
    2018: {"claim_factor": 1.1, "fraud_factor": 1.2, "processing_factor": 0.9},
    2019: {"claim_factor": 1.0, "fraud_factor": 1.0, "processing_factor": 1.0},
    2020: {"claim_factor": 1.3, "fraud_factor": 1.4, "processing_factor": 1.2}  # COVID + economic downturn
}

def get_customer_loyalty(customer_id, claim_date, all_policies):
    """Calculate customer loyalty (years with company)"""
    customer_policies = all_policies[all_policies["Applicant_ID"] == customer_id]
    if customer_policies.empty:
        return 0
    
    first_policy = customer_policies["Effective_Date"].min()
    years_with_company = (claim_date - first_policy).days / 365
    return max(0, years_with_company)

def get_payment_history_quality(policy_number, claim_date, all_payments):
    """Assess payment history - late payments affect claim processing"""
    policy_payments = all_payments[
        (all_payments["Policy_Number"] == policy_number) & 
        (all_payments["Payment_Date"] <= claim_date)
    ]
    
    if policy_payments.empty:
        return "Unknown"
    
    # Get expected premium
    policy = df_policies[df_policies["Policy_Number"] == policy_number].iloc[0]
    expected_premium = policy["Premium_Amount"]
    
    # Calculate late/under payments
    late_count = 0
    under_count = 0
    
    for _, payment in policy_payments.iterrows():
        # Check if underpaid
        if payment["Amount_Paid"] < expected_premium * 0.95:
            under_count += 1
    
    total_payments = len(policy_payments)
    if total_payments == 0:
        return "Unknown"
    
    under_rate = under_count / total_payments
    
    if under_rate > 0.3:
        return "Poor"
    elif under_rate > 0.15:
        return "Fair"
    else:
        return "Good"

def get_previous_claims_count(customer_id, claim_date, all_claims):
    """Count previous claims for this customer"""
    previous = [c for c in all_claims if c["Customer_ID"] == customer_id and c["Date_of_Claim"] < claim_date]
    return len(previous)

def determine_reinsurance_type(coverage, reinsurance_threshold, reinsurance_share, xol_retention):
    """Determine reinsurance type"""
    if xol_retention and coverage > reinsurance_threshold:
        return "XOL"
    elif reinsurance_share and reinsurance_share > 0:
        return "Proportional"
    return "None"

def calculate_realistic_claim_amount(coverage, deductible, claim_type, policy_type, claim_date, province):
    """Calculate realistic claim amounts with all factors"""
    
    # Get base distribution
    if claim_type == "Death" or claim_type == "Terminal Illness":
        # Life claims: 90-100% of coverage
        base_amount = coverage * random.uniform(0.90, 1.0)
        
    elif claim_type in ["Critical Illness", "Disability"]:
        # Partial life claims: 50-80%
        base_amount = coverage * random.uniform(0.50, 0.80)
        
    elif claim_type in ["Vandalism", "Glass Damage"]:
        # Minor car claims
        base_amount = random.triangular(2000, 8000, 25000)
        
    elif claim_type in ["Lost Luggage", "Delay"]:
        # Minor travel claims
        base_amount = random.triangular(1000, 5000, 15000)
        
    elif claim_type == "Outpatient":
        # Small health claims
        base_amount = random.lognormvariate(np.log(3000), 0.6)
        
    elif claim_type in ["Hospitalization", "Surgery", "Emergency"]:
        # Major health claims
        base_amount = random.lognormvariate(np.log(40000), 0.9)
        
    elif claim_type == "Theft":
        if policy_type == "Car":
            base_amount = coverage * random.uniform(0.70, 0.95)
        elif policy_type == "Home":
            base_amount = random.lognormvariate(np.log(35000), 0.8)
        else:
            base_amount = coverage * random.uniform(0.40, 0.70)
            
    elif claim_type in ["Collision", "Accident"]:
        # Car accidents - use distribution
        category = random.choices(["small", "medium", "large"], weights=[0.60, 0.30, 0.10])[0]
        prob, min_amt, max_amt = CLAIM_AMOUNT_DISTRIBUTIONS["Car"][category]
        base_amount = random.triangular(min_amt, (min_amt + max_amt) / 2, max_amt)
        
    elif claim_type in ["Fire", "Water Damage", "Storm Damage", "Structural Damage"]:
        # Home damage - bimodal distribution
        if random.random() < 0.4:  # Small damage
            base_amount = random.triangular(5000, 20000, 50000)
        else:  # Major damage
            base_amount = random.lognormvariate(np.log(100000), 0.9)
            
    else:
        # General claim: 15-40% of coverage
        claim_pct = random.triangular(0.15, 0.25, 0.40)
        base_amount = coverage * claim_pct
    
    # Apply geographic factors
    if province in GEOGRAPHIC_RISK:
        if claim_type == "Theft" and "theft_factor" in GEOGRAPHIC_RISK[province]:
            base_amount *= GEOGRAPHIC_RISK[province]["theft_factor"]
        elif claim_type == "Water Damage" and "water_damage" in GEOGRAPHIC_RISK[province]:
            base_amount *= GEOGRAPHIC_RISK[province]["water_damage"]
        elif claim_type == "Storm Damage" and "storm_damage" in GEOGRAPHIC_RISK[province]:
            base_amount *= GEOGRAPHIC_RISK[province]["storm_damage"]
    
    # Apply seasonal factors
    seasonal_factor = SEASONAL_PATTERNS.get(policy_type, [1.0]*12)[claim_date.month - 1]
    base_amount *= seasonal_factor
    
    # Ensure doesn't exceed coverage
    claim_amount = int(min(base_amount, coverage))
    claim_amount = max(1000, claim_amount)
    
    return claim_amount

def detect_enhanced_fraud_indicators(customer_id, policy_type, claim_type, claim_amount, claim_date, 
                                     all_claims, payment_history, loyalty_years, policy_inception):
    """Enhanced fraud detection with multiple factors"""
    indicators = []
    
    # Previous claims
    previous_claims_count = get_previous_claims_count(customer_id, claim_date, all_claims)
    if previous_claims_count > 3:
        indicators.append("High claims frequency")
    elif previous_claims_count > 0:
        # Check for recent previous claim
        customer_claims = [c for c in all_claims if c["Customer_ID"] == customer_id and c["Date_of_Claim"] < claim_date]
        last_claim_date = max(c["Date_of_Claim"] for c in customer_claims)
        if (claim_date - last_claim_date).days < 90:
            indicators.append("Multiple claims within 90 days")
    
    # New policy claims (within 30 days)
    days_since_inception = (claim_date - policy_inception).days
    if days_since_inception < 30:
        indicators.append("Claim within first 30 days")
    
    # Weekend/holiday claims (30% higher fraud risk)
    if claim_date.weekday() >= 5:  # Saturday or Sunday
        if random.random() < 0.30:
            indicators.append("Weekend claim")
    
    if claim_date.month == 12 and claim_date.day >= 20:
        if random.random() < 0.25:
            indicators.append("Holiday period claim")
    
    # Late reporting (>30 days typical)
    if random.random() < 0.05:  # 5% of claims reported late
        indicators.append("Late reporting (>30 days)")
    
    # Payment history impact
    if payment_history == "Poor":
        if random.random() < 0.40:
            indicators.append("Poor payment history")
    
    # Low loyalty customers
    if loyalty_years < 1 and random.random() < 0.20:
        indicators.append("New customer - higher risk")
    
    # Base fraud probability by policy type
    fraud_prob = CLAIM_SETTINGS[policy_type]["fraud_prob"]
    
    # Economic factor (2020 +40% fraud)
    fraud_prob *= EXTERNAL_FACTORS.get(claim_date.year, {"fraud_factor": 1.0})["fraud_factor"]
    
    # Previous claims increase fraud probability
    if previous_claims_count > 0:
        fraud_prob *= (1 + previous_claims_count * 0.15)  # 15-40% increase per previous claim
    
    if random.random() < fraud_prob:
        indicators.append(random.choice(FRAUD_INDICATORS))
    
    return indicators

def calculate_processing_days(policy_type, claim_type, claim_amount, fraud_indicators, 
                              documentation_status, claim_date, loyalty_years):
    """Calculate realistic processing with escalation thresholds"""
    base_min, base_max = CLAIM_SETTINGS[policy_type]["processing_days_range"]
    year_factor = EXTERNAL_FACTORS.get(claim_date.year, {"processing_factor": 1.0})["processing_factor"]
    
    processing_days = random.randint(int(base_min * year_factor), int(base_max * year_factor))
    
    # Complexity adjustments
    if claim_type in ["Death", "Disability", "Critical Illness", "Business Interruption"]:
        processing_days *= 1.4
    elif claim_type in ["Surgery", "Hospitalization", "Fire", "Structural Damage"]:
        processing_days *= 1.2
    
    # Amount-based escalation thresholds
    if claim_amount >= 5000000:  # Board committee
        processing_days *= 2.0
    elif claim_amount >= 1000000:  # Executive approval
        processing_days *= 1.7
    elif claim_amount >= 250000:  # Department head
        processing_days *= 1.4
    elif claim_amount >= 50000:  # Team leader
        processing_days *= 1.2
    elif claim_amount < 10000:  # Fast track small claims
        processing_days *= 0.6
    
    # Fraud indicators increase time
    if fraud_indicators:
        if len(fraud_indicators) >= 3:
            processing_days *= 2.0  # SIU investigation
        elif len(fraud_indicators) >= 2:
            processing_days *= 1.6  # Manual investigation
        else:
            processing_days *= 1.3  # Enhanced review
    
    # Documentation impact
    if documentation_status == "Pending":
        processing_days *= 1.5
    elif documentation_status == "Partial":
        processing_days *= 1.3
    
    # Loyalty customers get faster processing (30% faster for 3+ years)
    if loyalty_years >= 3:
        processing_days *= 0.70
    elif loyalty_years >= 1:
        processing_days *= 0.85
    
    return int(max(3, min(processing_days, 180)))

def determine_claim_status(claim_date, processing_days, fraud_indicators, policy_type, 
                           documentation_status, loyalty_years):
    """Determine claim status with realistic approval rates"""
    settlement_date = claim_date + timedelta(days=processing_days)
    
    # Claims before REASONABLE_PENDING that settle by CUTOFF_DATE should be resolved
    if claim_date <= REASONABLE_PENDING and settlement_date <= CUTOFF_DATE:
        # Base approval rates
        if len(fraud_indicators) >= 3:
            status_probs = [0.25, 0.60, 0.15]  # Approved, Rejected, Pending (high fraud)
        elif len(fraud_indicators) >= 1:
            status_probs = [0.50, 0.35, 0.15]  # Medium fraud risk
        else:
            status_probs = [0.82, 0.10, 0.08]  # Clean claims - 82% approval
        
        # Documentation impact
        if documentation_status == "Complete":
            status_probs[0] *= 1.1  # Boost approval
            status_probs[1] *= 0.8  # Reduce rejection
        elif documentation_status == "Pending":
            status_probs[0] *= 0.7
            status_probs[1] *= 1.2
            status_probs[2] *= 1.5
        
        # Loyalty boost
        if loyalty_years >= 3:
            status_probs[0] *= 1.05  # 5% boost to approval
            status_probs[1] *= 0.90
        
        # Complex claim types
        if policy_type in ["Life", "Commercial"]:
            status_probs[0] *= 0.90
            status_probs[1] *= 1.10
            status_probs[2] *= 1.20
        
        # Normalize
        total = sum(status_probs)
        status_probs = [p / total for p in status_probs]
        status = np.random.choice(["Approved", "Rejected", "Pending"], p=status_probs)
    else:
        status = "Pending"
    
    settlement_date = None if status == "Pending" else settlement_date
    return status, settlement_date

def calculate_settlement(claim_amount, deductible, coverage, status, fraud_indicators, policy_type, 
                        claim_type, claim_settings, loyalty_years, policy_age_years):
    """Calculate settlement with co-payments, depreciation, and other realistic factors"""
    if status != "Approved":
        return 0 if status == "Rejected" else None
    
    # Start with claim minus deductible
    settlement = max(0, claim_amount - deductible)
    
    # Apply policy-specific adjustments
    if policy_type == "Health":
        # Network differential (in-network 90%, out-of-network 60%)
        if random.random() < 0.75:  # 75% use in-network
            network_factor = 0.90
        else:
            network_factor = 0.60
        settlement = int(settlement * network_factor)
        
        # Co-payment (10-20% for certain treatments)
        if claim_type in ["Surgery", "Chronic Condition"]:
            copay_pct = random.uniform(0.10, 0.20)
            settlement = int(settlement * (1 - copay_pct))
    
    elif policy_type == "Car":
        # Depreciation (10% per year, max 50% after 5 years)
        depreciation = min(0.50, policy_age_years * 0.10)
        if claim_type in ["Collision", "Accident", "Theft"]:
            settlement = int(settlement * (1 - depreciation))
        
        # Betterment charges (new parts on old cars)
        if policy_age_years > 3 and claim_type in ["Collision", "Accident"]:
            betterment = random.uniform(0.10, 0.20)
            settlement = int(settlement * (1 - betterment))
    
    elif policy_type == "Home":
        # Underinsurance penalty
        actual_value = coverage / random.uniform(0.70, 1.0)  # Estimate actual value
        if coverage < actual_value * 0.80:  # Under-insured
            underinsurance_factor = coverage / actual_value
            settlement = int(settlement * underinsurance_factor)
    
    elif policy_type == "Life":
        # Waiting periods
        if policy_age_years < 1 and claim_type == "Critical Illness":
            settlement = int(settlement * 0.50)  # 50% during waiting period
        elif policy_age_years < 2 and claim_type == "Death":
            # Suicide exclusion period
            if random.random() < 0.02:  # 2% suicide rate
                return 0  # Excluded
    
    # Fraud penalty
    if fraud_indicators:
        reduction_factor = 1.0 - (len(fraud_indicators) * 0.10)
        settlement = int(settlement * max(0.40, reduction_factor))
    
    # Medical exam requirements (Life >R500K)
    if policy_type == "Life" and claim_amount > 500000:
        # May reduce payout pending additional documentation
        if random.random() < 0.15:
            settlement = int(settlement * 0.85)
    
    # Ensure within coverage
    settlement = min(settlement, coverage)
    
    # Small claims paid in full (goodwill)
    if settlement < 5000 and settlement > 0:
        settlement = min(claim_amount - deductible, coverage)
    
    # Loyalty bonus (3+ years get 5% boost)
    if loyalty_years >= 3 and random.random() < 0.30:
        settlement = int(min(settlement * 1.05, coverage))
    
    return settlement

def calculate_reinsurance_settlement(claim_amount, settlement, reinsurance_type, reinsurance_share, xol_retention):
    """Calculate reinsurer's portion with realistic recovery timing"""
    if not settlement or settlement == 0:
        return None
    
    if reinsurance_type == "Proportional" and reinsurance_share:
        # Quota share: reinsurer takes percentage
        return int(settlement * reinsurance_share)
        
    elif reinsurance_type == "XOL" and xol_retention:
        # Excess of loss: only above retention
        if claim_amount > xol_retention:
            excess = claim_amount - xol_retention
            reinsurer_amount = min(settlement, excess)
            return int(reinsurer_amount * random.uniform(0.75, 0.95))  # Reinsurer may dispute
    
    return None

# Main claims generation
claims = []
claim_id_counter = 1

print("Generating realistic claims with comprehensive enhancements...")

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
    
    # Extract province
    address_parts = customer["Address"].split(",")
    province = address_parts[-2].strip() if len(address_parts) >= 2 else "Gauteng"
    
    if effective_date >= expiration_date:
        continue

    # Calculate loyalty
    loyalty_years = get_customer_loyalty(customer_id, effective_date, df_policies)
    
    # Get payment history quality
    payment_quality = get_payment_history_quality(policy_number, expiration_date, df_payments)
    
    # Adjust claim probability
    claim_settings = CLAIM_SETTINGS[policy_type]
    base_claim_prob = claim_settings["claim_prob"]
    claim_prob = base_claim_prob * risk_factor
    
    # Health risk adjustments
    if customer["Is_Smoker"] == "Yes" and policy_type in ["Life", "Health"]:
        claim_prob *= 1.3
    if customer["Long_Term_Medication"] == "Yes" and policy_type in ["Life", "Health"]:
        claim_prob *= 1.2
    if customer["Job_Title"] in HIGH_RISK_JOBS:
        claim_prob *= 1.15
    
    # Geographic adjustments
    if province in GEOGRAPHIC_RISK and policy_type in GEOGRAPHIC_RISK[province]:
        claim_prob *= GEOGRAPHIC_RISK[province][policy_type]
    
    # Year factors
    year_factors = [EXTERNAL_FACTORS.get(y, {"claim_factor": 1.0})["claim_factor"] 
                    for y in range(effective_date.year, expiration_date.year + 1)]
    claim_prob *= np.mean(year_factors) if year_factors else 1.0
    claim_prob = min(claim_prob, 0.85)

    has_claim = stats.bernoulli.rvs(p=claim_prob)
    if not has_claim:
        continue

    # Policy age in years
    policy_age_years = (expiration_date - effective_date).days / 365
    
    duration_years = (expiration_date - effective_date).days / 365
    max_claims = min(claim_settings["max_claims"], max(1, int(duration_years * 2)))
    num_claims = random.choices(range(1, max_claims + 1), weights=[0.70, 0.20, 0.07, 0.02, 0.01][:max_claims])[0]

    for _ in range(num_claims):
        delta_days = (expiration_date - effective_date).days
        
        # Claim reporting patterns: 60% within 48 hours, 20% within 7 days, 15% within 30 days, 5% late
        reporting_pattern = random.choices(
            [2, 7, 30, 60], 
            weights=[0.60, 0.20, 0.15, 0.05]
        )[0]
        
        # Claim occurs somewhere in policy period
        claim_day = int(stats.beta.rvs(a=2, b=3, size=1)[0] * max(1, delta_days - reporting_pattern))
        claim_date = effective_date + timedelta(days=claim_day)
        
        if claim_date > CUTOFF_DATE:
            continue

        # Add reporting delay
        claim_report_date = claim_date + timedelta(days=random.randint(0, reporting_pattern))
        
        claim_type = np.random.choice(claim_settings["claim_types"], p=claim_settings["claim_type_weights"])
        
        # Calculate realistic claim amount
        claim_amount = calculate_realistic_claim_amount(
            coverage, deductible, claim_type, policy_type, claim_date, province
        )

        # Enhanced fraud detection
        fraud_indicators = detect_enhanced_fraud_indicators(
            customer_id, policy_type, claim_type, claim_amount, claim_report_date,
            claims, payment_quality, loyalty_years, effective_date
        )
        
        # Documentation status
        if len(fraud_indicators) >= 2:
            doc_weights = [0.40, 0.30, 0.30]  # More likely to be incomplete with fraud
        else:
            doc_weights = [0.70, 0.20, 0.10]
        documentation_status = random.choices(["Complete", "Partial", "Pending"], weights=doc_weights)[0]
        
        # Processing days with all factors
        processing_days = calculate_processing_days(
            policy_type, claim_type, claim_amount, fraud_indicators,
            documentation_status, claim_report_date, loyalty_years
        )
        
        # Status determination
        status, settlement_date = determine_claim_status(
            claim_report_date, processing_days, fraud_indicators, policy_type,
            documentation_status, loyalty_years
        )
        
        # Settlement calculation
        settlement = calculate_settlement(
            claim_amount, deductible, coverage, status, fraud_indicators,
            policy_type, claim_type, claim_settings, loyalty_years, policy_age_years
        )
        
        # Reinsurance settlement
        reinsurance_type = determine_reinsurance_type(
            coverage, reinsurance_threshold, reinsurance_share, xol_retention_amount
        )
        reinsurer_settlement = calculate_reinsurance_settlement(
            claim_amount, settlement, reinsurance_type, reinsurance_share, xol_retention_amount
        )
        
        # Claim handler
        claim_handler = random.choice(CLAIM_HANDLERS[policy_type])
        
        # Complexity level based on amount and fraud
        if claim_amount >= 1000000 or len(fraud_indicators) >= 3:
            complexity = "Complex"
        elif claim_amount >= 100000 or len(fraud_indicators) >= 1:
            complexity = "Moderate"
        else:
            complexity = "Simple"

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
            "Complexity_Level": complexity,
            "Documentation_Status": documentation_status
        })
        
        claim_id_counter += 1

# Convert to DataFrame
df_claims = pd.DataFrame(claims)

# Save to Parquet
os.makedirs("insurance_data", exist_ok=True)
save_path = "insurance_data/claims_history.parquet"
df_claims.to_parquet(save_path, index=False)

print(f"\nSaved {len(df_claims)} claim records to {save_path}")
print(f"\n=== CLAIM STATISTICS ===")
print(f"Total claims: {len(df_claims)}")
print(f"\nClaim status distribution:")
print(df_claims["Status"].value_counts())
print(f"\nApproval rate: {(df_claims['Status'] == 'Approved').sum() / len(df_claims) * 100:.1f}%")
print(f"Rejection rate: {(df_claims['Status'] == 'Rejected').sum() / len(df_claims) * 100:.1f}%")
print(f"Pending rate: {(df_claims['Status'] == 'Pending').sum() / len(df_claims) * 100:.1f}%")

print(f"\n=== FRAUD ANALYSIS ===")
fraud_claims = df_claims[df_claims["Fraud_Indicators"].notna()]
print(f"Claims with fraud indicators: {len(fraud_claims)} ({len(fraud_claims)/len(df_claims)*100:.1f}%)")
print(f"Fraud indicator distribution:")
print(df_claims["Fraud_Indicators"].value_counts().head(10))

print(f"\n=== POLICY TYPE DISTRIBUTION ===")
policy_type_map = df_policies.set_index("Policy_Number")["Policy_Type"]
df_claims["Policy_Type"] = df_claims["Policy_Number"].map(policy_type_map)
print(df_claims["Policy_Type"].value_counts())

print(f"\n=== CLAIM AMOUNTS BY TYPE ===")
print(df_claims.groupby("Policy_Type")["Claim_Amount"].describe())

print(f"\n=== SETTLEMENT STATISTICS ===")
approved_claims = df_claims[df_claims["Status"] == "Approved"]
if len(approved_claims) > 0:
    print(f"Total approved claims: {len(approved_claims)}")
    print(f"Average settlement: R{approved_claims['Settlement_Amount'].mean():,.2f}")
    print(f"Total settlements: R{approved_claims['Settlement_Amount'].sum():,.2f}")
    print(f"Average claim-to-settlement ratio: {(approved_claims['Settlement_Amount'] / approved_claims['Claim_Amount']).mean():.1%}")

print(f"\n=== PROCESSING TIME ANALYSIS ===")
print(f"Average processing days: {df_claims['Processing_Days'].mean():.1f}")
print(df_claims.groupby("Policy_Type")["Processing_Days"].describe())

print(f"\n=== COMPLEXITY DISTRIBUTION ===")
print(df_claims["Complexity_Level"].value_counts())

print("\nSample claims:")
print(df_claims[["Claim_ID", "Policy_Type", "Claim_Type", "Claim_Amount", "Status", "Settlement_Amount", "Processing_Days"]].head(15))