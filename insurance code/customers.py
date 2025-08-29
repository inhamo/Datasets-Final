import pandas as pd
import random
from faker import Faker
from datetime import date, timedelta
import re
from tqdm import tqdm
import os
import numpy as np
from scipy import stats

# Set seed for reproducibility
random.seed(42)
np.random.seed(42)
fake = Faker('zu_ZA')

# --- South African Name Data with Correct Demographic Mapping ---
SA_NAMES = {
    'Black': {
        'first': ['Sipho', 'Thandiwe', 'Andile', 'Nomsa', 'Sibusiso', 'Zanele', 'Mpho', 'Lerato', 
                 'Nomvula', 'Khanyisile', 'Nokuthula', 'Mandla', 'Bongani', 'Refiloe', 'Precious',
                 'Zinhle', 'Tebogo', 'Nompumelelo', 'Kagiso', 'Lungile', 'Thulile', 'Vusi', 'Palesa',
                 'Ayanda', 'Dineo', 'Rethabile', 'Zodwa', 'Ntombi', 'Sizwe', 'Thandeka', 'Lindiwe',
                 'Nomalanga', 'Sive', 'Zola', 'Thandiswa', 'Zuko', 'Nolwazi', 'Lulama', 'Vuyo',
                 'Siphokazi', 'Bulelani', 'Ntando', 'Zintle', 'Qhayiya', 'Amahle'],
        'last': ['Ndlovu', 'Zuma', 'Mkhize', 'Ngcobo', 'Khumalo', 'Mthembu', 'Dlamini', 'Mabaso',
                'Mhlongo', 'Nxumalo', 'Sibiya', 'Hadebe', 'Ngwenya', 'Shabangu', 'Maseko', 'Buthelezi',
                'Tshabalala', 'Mofokeng', 'Mthethwa', 'Mokoena', 'Gumede', 'Zondi', 'Mabuza', 'Nkosi',
                'Mbeki', 'Sisulu', 'Tutu', 'Komani', 'Gqoba', 'Mqhayi', 'Mhlaba', 'Mtimkulu', 'Xego',
                'Tshaka', 'Mpofu', 'Ntshinga', 'Gxowa', 'Mpinga', 'Qunta']
    },
    'White': {
        'first': ['Johan', 'Pieter', 'Hendrik', 'Jacobus', 'Gerhard', 'Willem', 'Marthinus', 'Ruan',
                 'Jaco', 'Hannes', 'Elize', 'Maria', 'Susanna', 'Riana', 'Anri', 'Estelle', 'Chantel',
                 'Amelia', 'Annelie', 'Marthina', 'Willemien', 'Stefan', 'Christiaan', 'Frederick',
                 'John', 'David', 'Michael', 'Robert', 'James', 'William', 'Richard', 'Thomas',
                 'Christopher', 'Daniel', 'Sarah', 'Elizabeth', 'Jennifer', 'Susan', 'Margaret',
                 'Jessica', 'Emma', 'Olivia', 'Sophia', 'Amelia', 'Charlotte', 'Emily', 'Grace'],
        'last': ['van der Merwe', 'Botha', 'Pretorius', 'Smit', 'Coetzee', 'Viljoen', 'van Wyk', 'Steyn',
                'Fourie', 'Joubert', 'Venter', 'Kruger', 'Nel', 'Olivier', 'Engelbrecht', 'Swanepoel',
                'Marais', 'Barnard', 'Louw', 'du Plessis', 'de Klerk', 'du Toit', 'le Roux', 'van Zyl',
                'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Wilson',
                'Anderson', 'Taylor', 'Thomas', 'Moore', 'Martin', 'Jackson', 'White', 'Harris']
    },
    'Indian': {
        'first': ['Raj', 'Priya', 'Sanjay', 'Anitha', 'Vijay', 'Latha', 'Arjun', 'Meena', 'Kumar',
                 'Divya', 'Suresh', 'Lakshmi', 'Ravi', 'Geeta', 'Naresh', 'Shanti', 'Deepak', 'Sunita',
                 'Dinesh', 'Ashok', 'Niren', 'Pravin', 'Vishal', 'Anand', 'Ramesh', 'Krishna', 'Gopal'],
        'last': ['Naidoo', 'Pillay', 'Reddy', 'Govender', 'Singh', 'Khan', 'Patel', 'Abraham',
                'Petersen', 'Francis', 'Benjamin', 'David', 'Thomas', 'Isaacs', 'Meyer', 'van der Ross',
                'Maharaj', 'Pillai', 'Desai', 'Ramphal', 'Moodley', 'Padayachee', 'Narainsamy']
    },
    'Coloured': {
        'first': ['Ashley', 'Chad', 'Dwayne', 'Garth', 'Jody', 'Kyle', 'Lance', 'Marvin', 'Neville',
                 'Ronald', 'Shane', 'Troy', 'Wesley', 'Alicia', 'Bianca', 'Candice', 'Chantelle',
                 'Danielle', 'Jasmine', 'Juanita', 'Leticia', 'Monique', 'Nadine', 'Rochelle', 'Sharon',
                 'Tanya', 'Tracy', 'Yolanda'],
        'last': ['Adams', 'Arendse', 'Baartman', 'Basson', 'Booysen', 'Bruintjies', 'Cupido', 'Damon',
                'Davids', 'Fortuin', 'Goliath', 'Jansen', 'Jonkers', 'Klaasen', 'Meyer', 'Petersen',
                'Pieterse', 'Samuels', 'September', 'Swartz', 'Thomas', 'Williams', 'Witbooi', 'Zimmerman']
    },
    'Asian': {
        'first': ['Wei', 'Jing', 'Li', 'Min', 'Chen', 'Xiao', 'Yu', 'Hao', 'Yang', 'Feng', 'Lin',
                 'Mei', 'Qiang', 'Rui', 'Tao', 'Xin', 'Ying', 'Zhang', 'Zhi', 'An', 'Bao', 'Chun',
                 'Dong', 'Fang', 'Gang', 'Hong', 'Jian', 'Jun', 'Kai', 'Lan', 'Ling', 'Ning', 'Ping'],
        'last': ['Chen', 'Wang', 'Li', 'Zhang', 'Liu', 'Yang', 'Zhao', 'Huang', 'Zhou', 'Wu', 'Xu',
                'Sun', 'Ma', 'Zhu', 'Hu', 'Guo', 'Lin', 'Gao', 'Zheng', 'Xie', 'He', 'Tan', 'Song',
                'Liang', 'Fan', 'Ye', 'Wong', 'Chan', 'Lee', 'Kim', 'Park', 'Nguyen', 'Tran', 'Pham']
    }
}

# South African street names
SA_STREETS = ['Acacia Street', 'Oak Avenue', 'Pine Road', 'Cedar Lane', 'Elm Street', 'Maple Drive', 
              'Birch Road', 'Spruce Street', 'Willow Avenue', 'Palm Road', 'Protea Street', 'Marula Lane', 
              'Baobab Avenue', 'Jacaranda Road', 'Fevertree Street', 'Main Road', 'Church Street', 
              'Market Street', 'Victoria Road', 'Durban Road', 'Johannesburg Road', 'Cape Road']

# South African cities and provinces
SA_CITY_PROVINCE = {
    'Pretoria': 'Gauteng', 'Johannesburg': 'Gauteng', 'Cape Town': 'Western Cape', 
    'Durban': 'KwaZulu-Natal', 'Port Elizabeth': 'Eastern Cape', 'Bloemfontein': 'Free State', 
    'Polokwane': 'Limpopo', 'Rustenburg': 'North West', 'Nelspruit': 'Mpumalanga',
    'Kimberley': 'Northern Cape', 'East London': 'Eastern Cape', 'Pietermaritzburg': 'KwaZulu-Natal'
}

# Updated job dictionary with more tech jobs and realistic monthly salaries
JOB_SALARY = {
    "Intern": (5000, 10000), "Electrician": (15000, 30000), "Teacher": (20000, 40000),
    "Manager": (40000, 80000), "Software Developer": (30000, 70000), "Nurse": (25000, 45000),
    "Doctor": (60000, 120000), "Security Guard": (8000, 15000), "Retail Worker": (6000, 12000),
    "Accountant": (30000, 60000), "Engineer": (40000, 80000), "Plumber": (15000, 30000),
    "Mechanic": (15000, 35000), "Chef": (15000, 35000), "Waiter": (5000, 10000),
    "Lawyer": (50000, 100000), "Architect": (40000, 70000), "Pharmacist": (40000, 70000),
    "Dentist": (50000, 100000), "Sales Representative": (20000, 50000), "Marketing Specialist": (25000, 50000),
    "Student": (0, 5000), "Data Scientist": (45000, 90000), "DevOps Engineer": (40000, 85000),
    "Cybersecurity Analyst": (35000, 80000), "Cloud Architect": (50000, 100000), "AI Specialist": (50000, 110000),
    "Senior Data Scientist": (60000, 120000), "IT Manager": (50000, 100000), "Network Engineer": (35000, 75000),
    "UX Designer": (30000, 65000), "Project Manager": (45000, 90000), "Business Analyst": (35000, 75000)
}

# Age restrictions for certain jobs
JOB_AGE_RESTRICTIONS = {
    "Student": (18, 25),
    "Intern": (20, 28),
    "Cybersecurity Analyst": (25, 60),
    "Senior Data Scientist": (30, 65),
    "DevOps Engineer": (25, 60),
    "Cloud Architect": (28, 65),
    "AI Specialist": (25, 65),
    "Doctor": (28, 70),
    "Dentist": (26, 70),
    "Lawyer": (25, 70),
    "Manager": (28, 65),
    "IT Manager": (30, 65)
}

SA_EMAIL_DOMAINS = ['mweb.co.za', 'telkomsa.net', 'vodamail.co.za', 'gmail.com', 'yahoo.com', 'hotmail.com', 'webmail.co.za']
SA_MOBILE_PREFIXES = ['60', '71', '72', '73', '74', '76', '78', '79', '81', '82', '83', '84']

def generate_valid_sa_id(dob, gender):
    """Generate a valid South African ID number with correct checksum"""
    # Format YYMMDD
    yymmdd = dob.strftime('%y%m%d')
    
    # Gender digit (0000-4999 for female, 5000-9999 for male)
    gender_digit = random.randint(0, 4999) if gender == 'F' else random.randint(5000, 9999)
    
    # Citizenship (0 for SA citizen, 1 for permanent resident)
    citizenship = '0'  # Assuming all are citizens for simplicity
    
    # Random digit
    random_digit = str(random.randint(0, 9))
    
    # First 12 digits
    first_12 = f"{yymmdd}{gender_digit:04d}{citizenship}{random_digit}"
    
    # Calculate checksum using Luhn algorithm
    total = 0
    for i, digit in enumerate(first_12):
        d = int(digit)
        if i % 2 == 0:  # Even position (0-indexed)
            d *= 2
            if d > 9:
                d -= 9
        total += d
    
    checksum = (10 - (total % 10)) % 10
    
    return f"{first_12}{checksum}"

def format_sa_address():
    """Generate a realistic South African address"""
    street = random.choice(SA_STREETS)
    street_number = random.randint(1, 999)
    city = random.choice(list(SA_CITY_PROVINCE.keys()))
    province = SA_CITY_PROVINCE[city]
    postal_code = str(random.randint(1000, 9999))
    
    return f"{street_number} {street}, {city}, {province}, {postal_code}"

def generate_name_and_demographic():
    """Generate a name with appropriate demographic group"""
    demographic = random.choices(
        list(SA_NAMES.keys()), 
        weights=[0.80, 0.08, 0.03, 0.08, 0.01]  # Rough estimates of South African population distribution
    )[0]
    
    first_name = random.choice(SA_NAMES[demographic]['first'])
    last_name = random.choice(SA_NAMES[demographic]['last'])
    
    full_name = f"{first_name} {last_name}"
    
    # Determine gender based on name (simplified approach)
    female_names = ['Thandiwe', 'Nomsa', 'Zanele', 'Lerato', 'Nomvula', 'Khanyisile', 'Nokuthula', 
                   'Precious', 'Zinhle', 'Nompumelelo', 'Lungile', 'Thulile', 'Palesa', 'Ayanda', 
                   'Dineo', 'Zodwa', 'Ntombi', 'Thandeka', 'Lindiwe', 'Nomalanga', 'Thandiswa',
                   'Nolwazi', 'Lulama', 'Siphokazi', 'Zintle', 'Amahle', 'Elize', 'Maria', 'Susanna', 
                   'Riana', 'Anri', 'Estelle', 'Chantel', 'Amelia', 'Annelie', 'Marthina', 'Willemien', 
                   'Sarah', 'Elizabeth', 'Jennifer', 'Susan', 'Margaret', 'Jessica', 'Emma', 'Olivia',
                   'Sophia', 'Amelia', 'Charlotte', 'Emily', 'Grace', 'Priya', 'Anitha', 'Latha', 
                   'Meena', 'Divya', 'Lakshmi', 'Geeta', 'Shanti', 'Sunita', 'Alicia', 'Bianca',
                   'Candice', 'Chantelle', 'Danielle', 'Jasmine', 'Juanita', 'Leticia', 'Monique',
                   'Nadine', 'Rochelle', 'Sharon', 'Tanya', 'Tracy', 'Yolanda', 'Mei', 'Ying', 'Lan',
                   'Ling', 'Ning', 'Ping']
    
    gender = 'F' if first_name in female_names else 'M'
    
    return full_name, demographic, gender

def format_email(full_name):
    """Generate a realistic email address"""
    parts = full_name.lower().split()
    if len(parts) >= 2:
        # Various common email formats
        formats = [
            f"{parts[0][0]}{parts[1]}",
            f"{parts[0]}.{parts[1]}",
            f"{parts[0]}{parts[1][0]}",
            f"{parts[0]}{random.randint(1,99)}"
        ]
        username = random.choice(formats)
    else:
        username = f"{parts[0]}{random.randint(1,999)}"
    
    return f"{username}@{random.choice(SA_EMAIL_DOMAINS)}"

def generate_sa_phone():
    """Generate a South African phone number"""
    prefix = random.choice(SA_MOBILE_PREFIXES)
    subscriber = random.randint(1000000, 9999999)
    return f"+27{prefix}{subscriber}"

def smoking_data(age):
    """Generate smoking data based on age"""
    # Fixed the probability calculation to avoid negative values
    smoking_prob = min(0.35, max(0.05, 0.1 + (age-18)*0.005))  # Ensure probability is between 0.05 and 0.35
    is_smoker = "Yes" if random.random() < smoking_prob else "No"
    
    if is_smoker == "Yes":
        start_age = random.randint(16, min(age-1, 30))
        return is_smoker, age - start_age, None, None
    
    # Probability of being a previous smoker
    prev_smoker_prob = min(0.3, max(0.02, 0.05 + (age-30)*0.01))  # Ensure probability is reasonable
    prev_smoker = "Yes" if random.random() < prev_smoker_prob else "No"
    
    if prev_smoker == "Yes":
        start_age = random.randint(16, min(age-1, 40))
        quit_age = random.randint(start_age + 1, age)
        return "No", 0, prev_smoker, age - quit_age
    
    return "No", 0, prev_smoker, None

def alcohol_data(age):
    """Generate alcohol consumption data based on age"""
    # Younger people tend to drink more, older people less
    if age < 25:
        base_units = random.expovariate(1/10)
    elif age < 40:
        base_units = random.expovariate(1/8)
    else:
        base_units = random.expovariate(1/5)
    
    units = int(max(0, base_units))
    
    if units == 0: 
        category = "None"
    elif units <= 7: 
        category = "Low"
    elif units <= 14: 
        category = "Moderate"
    else: 
        category = "High"
    
    return category, units

def get_job_based_on_age(age):
    """Get a job that's appropriate for the age"""
    possible_jobs = []
    
    for job, salary_range in JOB_SALARY.items():
        if job in JOB_AGE_RESTRICTIONS:
            min_age, max_age = JOB_AGE_RESTRICTIONS[job]
            if min_age <= age <= max_age:
                possible_jobs.append(job)
        else:
            # Default age range for jobs without specific restrictions
            if age >= 22:  # Most professional jobs require at least 22
                possible_jobs.append(job)
            elif job in ["Student", "Intern", "Retail Worker", "Waiter"]:
                possible_jobs.append(job)
    
    if not possible_jobs:
        # Fallback for edge cases
        if age < 22:
            return "Student"
        else:
            return random.choice(["Retail Worker", "Security Guard", "Waiter"])
    
    return random.choice(possible_jobs)

def generate_insurance_data(n=5000):
    """Generate insurance applicant data"""
    data = []
    
    for i in tqdm(range(n), desc="Generating Insurance Applicants"):
        customer_id = f"APPL{i+1:05d}"
        name, demographic, gender = generate_name_and_demographic()
        
        # Generate age with more young people
        age = int(np.clip(np.random.gamma(2.5, scale=10), 18, 80))
        
        # Get appropriate job for age
        job = get_job_based_on_age(age)
        salary_min, salary_max = JOB_SALARY[job]
        
        # Generate salary based on job and age (experience)
        if salary_min > 0:
            # Salary increases with age (experience)
            experience_factor = min(1.0, max(0.1, (age - 22) / 40))
            base_salary = salary_min + (salary_max - salary_min) * random.betavariate(2, 2)
            salary = int(base_salary * (0.8 + 0.4 * experience_factor))
        else:
            salary = int(random.expovariate(1/2000))
        
        # Assets and debts logic (more realistic)
        has_assets = random.random() < min(0.8, 0.1 + (age-18)*0.02)
        
        if has_assets:
            # Assets grow with age
            assets = int(random.lognormvariate(0.5, 0.5) * (50000 + max(0, age-25)*5000))
        else:
            assets = 0
        
        has_debts = random.random() < max(0.2, 0.8 - (age-18)*0.01)
        
        if has_debts:
            # Younger people have more debt relative to income
            if age < 30:
                debt_factor = 0.8
            elif age < 40:
                debt_factor = 0.5
            else:
                debt_factor = 0.3
            
            debts = int(random.lognormvariate(0.5, 0.5) * (salary * 12 * debt_factor))
        else:
            debts = 0
        
        # Credit score based on financial behavior
        credit_score = int(np.clip(np.random.normal(650, 100), 300, 850))
        
        # Health data
        is_smoker, smoker_years, prev_smoker, years_quit = smoking_data(age)
        alcohol_use, alcohol_units = alcohol_data(age)
        
        # Medical conditions (more likely with age)
        med_prob = min(0.7, max(0.05, 0.1 + (age-30)*0.02))
        long_term_med = "Yes" if random.random() < med_prob else "No"
        
        if long_term_med == "Yes" and age > 25:
            med_since = int(random.uniform(1, max(1, age-25)))
        else:
            med_since = None
        
        # Generate date of birth
        dob = date(2021, 12, 31) - timedelta(days=age*365 + random.randint(0, 364))
        
        # Generate valid SA ID number
        id_number = generate_valid_sa_id(dob, gender)
        
        # Nationality (mostly South African)
        nationality = random.choices(
            ['South Africa', 'Zimbabwe', 'Namibia', 'Botswana', 'Mozambique', 'Lesotho'], 
            weights=[0.8, 0.05, 0.04, 0.04, 0.04, 0.03]
        )[0]
        
        # Banking details
        bank_name = random.choice(['Standard Bank', 'Absa', 'Nedbank', 'FNB', 'Capitec'])
        
        # Generate proper bank account number (without ZA prefix)
        bank_account = str(random.randint(10**9, (10**10)-1))  # 10-digit account number
        
        proof_of_address = random.choice(['Utility Bill', 'Bank Statement', 'Lease Agreement', 'ID Document'])
        
        data.append({
            "Customer_ID": customer_id,
            "Name": name,
            "Demographic": demographic,
            "Gender": gender,
            "Age": age,
            "Date_of_Birth": dob,
            "ID_Number": id_number,
            "Nationality": nationality,
            "Address": format_sa_address(),
            "Phone": generate_sa_phone(),
            "Email": format_email(name),
            "Job_Title": job,
            "Monthly_Income_ZAR": salary,
            "Assets_Value_ZAR": assets,
            "Debts_Value_ZAR": debts,
            "Credit_Score": credit_score,
            "Is_Smoker": is_smoker,
            "Smoker_Years": smoker_years,
            "Previous_Smoker": prev_smoker,
            "Years_Since_Quit": years_quit,
            "Alcohol_Use": alcohol_use,
            "Alcohol_Units_Per_Week": alcohol_units,
            "Long_Term_Medication": long_term_med,
            "Medication_Since_Years": med_since,
            "Bank_Name": bank_name,
            "Bank_Account": bank_account,
            "Proof_of_Address": proof_of_address
        })
    
    return pd.DataFrame(data)

# Generate and save
df_insurance = generate_insurance_data(random.randint(5000, 10000))
os.makedirs("insurance_data", exist_ok=True)
df_insurance.to_parquet("insurance_data/insurance_applicants.parquet", index=False)
print(f"Saved {len(df_insurance)} records to insurance_data/insurance_applicants.parquet")
print("\nSample data:")
print(df_insurance.head(10))

# Show some statistics
print("\nDataset Statistics:")
print(f"Total records: {len(df_insurance)}")
print(f"Age range: {df_insurance.Age.min()} - {df_insurance.Age.max()}")
print(f"Average income: ZAR {df_insurance.Monthly_Income_ZAR.mean():.2f}")
print("\nJob distribution:")
print(df_insurance.Job_Title.value_counts().head(10))
print("\nDemographic distribution:")
print(df_insurance.Demographic.value_counts())