import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import date, datetime, timedelta
import os
from tqdm import tqdm
import calendar

def generate_customer_data(year):
    fake = Faker()
    
    # Dynamic seeding for more realistic variation
    seed_bytes = os.urandom(4)
    seed_int = int.from_bytes(seed_bytes, byteorder='big')
    random.seed(seed_int)
    np.random.seed(seed_int)
    
    print(f"Using dynamic seed: {seed_int} for year {year}")

    # More realistic customer volume calculation with nested random ranges
    def get_realistic_customer_volumes(year):
        """
        Generate realistic customer volumes for South African financial institution
        Based on actual market conditions, economic cycles, and external shocks
        """
        # More realistic base ranges for a typical SA bank/financial institution
        base_individual_ranges = {
            2018: (2500, 4200),   # Good economy, steady growth
            2019: (2100, 3800),   # Economic slowdown, load shedding impact  
            2020: (150, 800),     # Hard lockdown March-June, severe restrictions
            2021: (600, 1500),    # Gradual recovery, stop-start restrictions
            2022: (1200, 2400),   # Recovery gaining momentum
            2023: (1800, 3200),   # Near normal, but economic pressures
            2024: (2200, 3800),   # Steady state operations
            2025: (2300, 4000),   # Projected normal operations
        }
        
        # Much more realistic company registration rates
        base_company_ranges = {
            2018: (120, 280),     # Healthy business formation
            2019: (85, 200),      # Business confidence declining
            2020: (15, 65),       # Severe impact - most businesses survival mode
            2021: (35, 120),      # Cautious business recovery
            2022: (60, 150),      # Steady business recovery
            2023: (80, 180),      # Business confidence returning
            2024: (100, 220),     # Normal business formation
            2025: (110, 240),     # Projected growth
        }
        
        # Get base ranges or use conservative defaults for unlisted years
        ind_range = base_individual_ranges.get(year, (1800, 3200))
        com_range = base_company_ranges.get(year, (80, 180))
        
        # Apply nested randomization for more realistic clustering
        # This simulates real-world variation within ranges
        ind_min, ind_max = ind_range
        com_min, com_max = com_range
        
        # Create weighted sub-ranges (favors middle values over extremes)
        ind_sub_ranges = [
            (ind_min, ind_min + (ind_max - ind_min) * 0.35),      # Lower range
            (ind_min + (ind_max - ind_min) * 0.25, ind_min + (ind_max - ind_min) * 0.75),  # Middle range
            (ind_min + (ind_max - ind_min) * 0.65, ind_max)       # Upper range
        ]
        
        com_sub_ranges = [
            (com_min, com_min + (com_max - com_min) * 0.35),
            (com_min + (com_max - com_min) * 0.25, com_min + (com_max - com_min) * 0.75),
            (com_min + (com_max - com_min) * 0.65, com_max)
        ]
        
        # Weight distribution: favor middle range for more realistic clustering
        range_weights = [0.25, 0.50, 0.25]
        
        # Select sub-ranges based on weights
        import random
        selected_ind_range = random.choices(ind_sub_ranges, weights=range_weights)[0]
        selected_com_range = random.choices(com_sub_ranges, weights=range_weights)[0]
        
        # Apply double randomization within selected sub-ranges
        num_individuals = random.randint(
            random.randint(int(selected_ind_range[0]), int(selected_ind_range[0] + (selected_ind_range[1] - selected_ind_range[0]) * 0.6)),
            random.randint(int(selected_ind_range[0] + (selected_ind_range[1] - selected_ind_range[0]) * 0.4), int(selected_ind_range[1]))
        )
        
        num_companies = random.randint(
            random.randint(int(selected_com_range[0]), int(selected_com_range[0] + (selected_com_range[1] - selected_com_range[0]) * 0.6)),
            random.randint(int(selected_com_range[0] + (selected_com_range[1] - selected_com_range[0]) * 0.4), int(selected_com_range[1]))
        )
        
        # Add contextual notes for extreme years
        context_notes = {
            2020: "COVID-19 Hard Lockdown: March 27 - May 1, 2020. Severe registration restrictions.",
            2021: "COVID-19 Recovery: Multiple waves, stop-start economy, cautious customer behavior.",
            2019: "Pre-COVID Economic Stress: Load shedding, political uncertainty, rand weakness."
        }
        
        if year in context_notes:
            print(f"📝 Context for {year}: {context_notes[year]}")
        
        return num_individuals, num_companies

        NUM_INDIVIDUALS, NUM_COMPANIES = get_realistic_customer_volumes(year)
        
        # Add seasonal registration patterns
        def get_seasonal_multiplier(date_obj):
            month = date_obj.month
            # Q1: January boost (New Year resolutions), February dip, March recovery
            # Q2: April steady, May steady, June mid-year planning boost  
            # Q3: July steady, August steady, September planning boost
            # Q4: October steady, November holiday prep, December holiday dip
            seasonal_multipliers = {
                1: 1.3,   # January - New Year registrations
                2: 0.7,   # February - post-holiday slump
                3: 1.1,   # March - recovery
                4: 1.0,   # April - steady
                5: 1.0,   # May - steady
                6: 1.2,   # June - mid-year planning
                7: 0.9,   # July - winter slow
                8: 0.9,   # August - winter slow
                9: 1.1,   # September - spring pickup
                10: 1.0,  # October - steady
                11: 0.8,  # November - holiday prep
                12: 0.6   # December - holidays
            }
            return seasonal_multipliers.get(month, 1.0)

        print(f"Year {year} - Target volumes: Individuals: {NUM_INDIVIDUALS:,}, Companies: {NUM_COMPANIES:,}")
        
        # COVID-specific adjustments for registration patterns
        def get_covid_impact_multiplier(date_obj, year):
            if year == 2020:
                # South Africa lockdown timeline: March 27 - May 1 (Level 5), gradual reopening
                if date_obj.month in [3, 4, 5]:
                    return 0.1  # Severe restriction
                elif date_obj.month in [6, 7]:
                    return 0.4  # Gradual reopening
                elif date_obj.month in [8, 9, 10]:
                    return 0.6  # Steady recovery
                else:
                    return 0.8  # Improved but not normal
            elif year == 2021:
                # Second wave impact (December 2020 - February 2021)
                if date_obj.month in [1, 2, 3]:
                    return 0.5  # Second wave impact
                elif date_obj.month in [6, 7]:
                    return 0.4  # Third wave (Delta variant)
                else:
                    return 0.7  # Recovery periods
            return 1.0

        # Ethnic groups and name pools for South Africa (keeping your existing comprehensive lists)
        black_sa_first_names = ['Sipho', 'Thabo', 'Nokuthula', 'Lerato', 'Tshepo', 'Palesa', 'Sibusiso', 'Thandiwe', 'Mpho', 'Zanele', 'Bongani', 'Nomvula', 'Thulani', 'Khanyisile', 'Siyabonga', 'Nompumelelo', 'Lungile', 'Zodwa', 'Mandla', 'Busisiwe', 'Vusi', 'Nontsikelelo', 'Themba', 'Phumzile']
        black_sa_last_names = ['Mokoena', 'Nkosi', 'Dlamini', 'Mthembu', 'Zulu', 'Mabena', 'Ndlovu', 'Khumbuza', 'Mkhize', 'Ngobeni', 'Sithole', 'Mahlangu', 'Tshabalala', 'Mnguni', 'Khoza', 'Buthelezi', 'Mofokeng', 'Hlatshwayo', 'Zungu', 'Mtshali', 'Ngcobo', 'Nxumalo', 'Gumede', 'Shabangu']
        afr_first_names = ['Jan', 'Pieter', 'Elsa', 'Marelize', 'Johan', 'Annelise', 'Hendrik', 'Marike', 'Willem', 'Lizette', 'Christo', 'Elmarie', 'Gerhard', 'Anri', 'Jacques', 'Susanna', 'Ruan', 'Carina', 'Theunis', 'Marissa', 'Frik', 'Elna', 'Deon', 'Riëtte']
        afr_last_names = ['van der Merwe', 'Botha', 'Smit', 'Kruger', 'Janse van Rensburg', 'Pretorius', 'Venter', 'de Klerk', 'Coetzee', 'van Wyk', 'Nel', 'du Plessis', 'Steyn', 'Vermaak', 'Joubert', 'Fourie', 'le Roux', 'van Niekerk', 'Pienaar', 'Swart', 'Bester', 'de Wet', 'Lombard', 'Malan']
        eng_first_names = ['John', 'Mary', 'David', 'Grace', 'Michael', 'Emma', 'James', 'Sarah', 'William', 'Elizabeth', 'Thomas', 'Rebecca', 'Charles', 'Emily', 'George', 'Charlotte', 'Henry', 'Victoria', 'Edward', 'Sophie', 'Richard', 'Laura', 'Daniel', 'Hannah']
        eng_last_names = ['Smith', 'Brown', 'Johnson', 'Williams', 'Taylor', 'Wilson', 'Davis', 'Clark', 'Harris', 'Lewis', 'Walker', 'Hall', 'Allen', 'Young', 'King', 'Wright', 'Scott', 'Green', 'Baker', 'Adams', 'Nelson', 'Carter', 'Mitchell', 'Turner']
        ind_first_names = ['Rajesh', 'Aisha', 'Sunil', 'Nisha', 'Amit', 'Priya', 'Vikram', 'Anjali', 'Ravi', 'Shalini', 'Arun', 'Divya', 'Sanjay', 'Pooja', 'Kiran', 'Meera', 'Rahul', 'Sneha', 'Deepak', 'Kavita', 'Vijay', 'Neha', 'Rakesh', 'Suman']
        ind_last_names = ['Naidoo', 'Pillay', 'Singh', 'Dadoo', 'Moodley', 'Govender', 'Chetty', 'Patel', 'Reddy', 'Naicker', 'Rama', 'Gopal', 'Khan', 'Mistry', 'Parbhoo', 'Sookraj', 'Maharaj', 'Nair', 'Desai', 'Chunder', 'Ramdass', 'Sewpersad', 'Brijlal', 'Lalla']
        col_first_names = ['Rene', 'Liezl', 'Hendrik', 'Mariette', 'Abdul', 'Fazila', 'Waseem', 'Natasha', 'Ibrahim', 'Shereen', 'Yusuf', 'Ameena', 'Ebrahim', 'Zainab', 'Mogamat', 'Fatima', 'Ismail', 'Raeeqa', 'Hassan', 'Nadia', 'Rashied', 'Soraya', 'Achmat', 'Zubeida']
        col_last_names = ['Adams', 'Davids', 'Williams', 'Johnson', 'Abrahams', 'Jacobs', 'Petersen', 'Fortuin', 'Arendse', 'Isaacs', 'Daniels', 'Hendricks', 'Manuel', 'Brown', 'Cupido', 'Saunders', 'Le Roux', 'Smith', 'Beukes', 'Jardine', 'Goliath', 'Salie', 'Kannemeyer', 'Parker']
        zw_first_names = ['Tariro', 'Farai', 'Kudzai', 'Ruvimbo', 'Tatenda', 'Chipo', 'Tinashe', 'Nyasha', 'Tapiwa', 'Rumbidzai', 'Tawanda', 'Shamiso', 'Tafadzwa', 'Mufaro', 'Tendai', 'Chiedza', 'Simbarashe', 'Nokutenda', 'Munyaradzi', 'Gamuchirai', 'Tinotenda', 'Rutendo', 'Kundai', 'Tsitsi']
        zw_last_names = ['Dube', 'Moyo', 'Chirwa', 'Ndlovu', 'Mutsvangwa', 'Sibanda', 'Mhlope', 'Gumbo', 'Mapfumo', 'Chigumbu', 'Mushonga', 'Makoni', 'Chinamasa', 'Mpofu', 'Shumba', 'Tshuma', 'Mabika', 'Chiweshe', 'Marufu', 'Zivai', 'Nyathi', 'Banda', 'Mudzuri', 'Gwande']

        # Combine name pools
        all_sa_first_names = black_sa_first_names + afr_first_names + eng_first_names + ind_first_names + col_first_names
        all_sa_last_names = black_sa_last_names + afr_last_names + eng_last_names + ind_last_names + col_last_names
        all_zw_first_names = zw_first_names
        all_zw_last_names = zw_last_names

        # Demographic weights for SA ethnicities
        sa_ethnicity_weights = {'Black': 0.8, 'Afrikaans': 0.06, 'English': 0.09, 'Indian': 0.03, 'Coloured': 0.02}
        ethnicity_keys, ethnicity_weights = list(sa_ethnicity_weights.keys()), list(sa_ethnicity_weights.values())

        # South African provinces and cities with more realistic distribution
        sa_cities_weights = {
            'Johannesburg': 0.22, 'Cape Town': 0.18, 'Durban': 0.15, 'Pretoria': 0.12,
            'Port Elizabeth': 0.06, 'Bloemfontein': 0.04, 'East London': 0.03, 
            'Pietermaritzburg': 0.03, 'Nelspruit': 0.02, 'Polokwane': 0.02,
            'Kimberley': 0.02, 'Rustenburg': 0.02, 'Other': 0.09
        }
        cities, city_weights = list(sa_cities_weights.keys()), list(sa_cities_weights.values())

        # Visa types
        visa_types = ['Work', 'Student', 'Tourist', 'Business', 'Diplomatic', 'Transit', 'Medical', 'Exchange', 'Permanent Resident', 'Refugee']

        # Industry and occupation data (keeping your comprehensive mapping)
        industry_occupation_map = {
            'Finance': ['Accountant', 'Banker', 'Financial Advisor', 'Auditor', 'Risk Manager', 'Loan Officer'],
            'Retail': ['Salesperson', 'Cashier', 'Store Manager', 'Inventory Specialist', 'Customer Service'],
            'Manufacturing': ['Engineer', 'Mechanic', 'Quality Control Inspector', 'Production Manager', 'Assembler'],
            'Healthcare': ['Nurse', 'Doctor', 'Pharmacist', 'Dentist', 'Therapist', 'Medical Technician'],
            'IT': ['Software Developer', 'Data Analyst', 'System Administrator', 'Network Engineer', 'IT Support'],
            'Transport': ['Driver', 'Logistician', 'Fleet Manager', 'Dispatcher', 'Warehouse Worker'],
            'Construction': ['Carpenter', 'Electrician', 'Plumber', 'Construction Worker', 'Site Manager'],
            'Consulting': ['Consultant', 'Business Analyst', 'Project Manager', 'Management Consultant'],
            'Agriculture': ['Farmer', 'Agricultural Technician', 'Animal Handler', 'Farm Manager'],
            'Education': ['Teacher', 'Professor', 'Librarian', 'Counselor', 'Administrator'],
            'Energy': ['Engineer', 'Technician', 'Safety Officer', 'Energy Analyst'],
            'Mining': ['Miner', 'Geologist', 'Safety Officer', 'Engineer'],
            'Hospitality': ['Chef', 'Waiter', 'Hotel Manager', 'Receptionist', 'Housekeeper'],
            'Tourism': ['Tour Guide', 'Travel Agent', 'Hotel Staff', 'Event Coordinator'],
            'Real Estate': ['Real Estate Agent', 'Property Manager', 'Appraiser', 'Broker'],
            'Telecommunications': ['Network Engineer', 'Technician', 'Salesperson', 'Customer Service'],
            'Media': ['Journalist', 'Editor', 'Photographer', 'Producer'],
            'Entertainment': ['Musician', 'Actor', 'Producer', 'Director'],
            'Logistics': ['Logistician', 'Warehouse Worker', 'Dispatcher', 'Driver'],
            'Automotive': ['Mechanic', 'Salesperson', 'Technician', 'Customer Service']
        }
        
        all_occupations = list({occ for occs in industry_occupation_map.values() for occ in occs}) + ['Unemployed', 'Student', 'Politician', 'Manager', 'Entrepreneur', 'Self-Employed']
        industry_sectors = list(industry_occupation_map.keys())
        company_sizes = ['Micro (<10)', 'Small (10-50)', 'Medium (51-250)', 'Large (>250)']

        # Big companies in South Africa
        big_companies = [
            'Sasol', 'MTN South Africa', 'Shoprite', 'Standard Bank', 'Nedbank', 'Vodacom', 
            'Bidvest Group', 'Anglo American', 'FirstRand', 'Absa Group', 'Telkom', 
            'Eskom', 'Pick n Pay', 'Barloworld', 'Impala Platinum', 'Gold Fields', 
            'Discovery Limited', 'Sanlam', 'Old Mutual', 'MultiChoice Group', 'Aspen Pharmacare', 
            'Exxaro Resources', 'Kumba Iron Ore', 'Woolworths Holdings', 'AngloGold Ashanti', 
            'South African Breweries', 'Naspers', 'Mediclinic International', 'RCL Foods', 
            'Tiger Brands', 'Distell Group', 'Clicks Group', 'Spar Group', 'Investec', 
            'Liberty Holdings', 'Remgro', 'Pepkor Holdings', 'Capitec Bank', 'Anglo Platinum', 'Sappi'
        ]
        faker_companies = [fake.company() for _ in range(500)]

        def weighted_choice(choices, weights):
            return random.choices(choices, weights=weights, k=1)[0]

        def generate_realistic_age():
            # More realistic age distribution for financial services customers
            age_ranges = [(18, 25), (26, 35), (36, 45), (46, 55), (56, 65), (66, 80)]
            weights = [0.20, 0.35, 0.25, 0.12, 0.06, 0.02]  # Skewed toward working age
            age_range = random.choices(age_ranges, weights=weights)[0]
            return random.randint(*age_range)

        def generate_income_and_risk(age, occupation, employer):
            # More realistic income ranges for South African context (in ZAR)
            income_ranges = {
                'Doctor': (800000, 2500000), 'Lawyer': (600000, 2000000), 'Engineer': (450000, 900000),
                'Teacher': (280000, 450000), 'Nurse': (350000, 550000), 'Unemployed': (0, 0),
                'Student': (0, 80000), 'Manager': (550000, 1500000), 'Banker': (400000, 1200000),
                'Accountant': (350000, 800000), 'IT Support': (300000, 600000), 'Software Developer': (400000, 1000000)
            }
            base_range = income_ranges.get(occupation, (250000, 600000))
            
            # Age-based income adjustment (experience premium)
            age_multiplier = min(1.0 + (age - 25) * 0.025, 1.8) if age > 25 else 0.7
            
            # Employer premium
            employer_multiplier = 1.4 if employer in big_companies else 1.0
            if employer == 'Self-Employed':
                employer_multiplier = random.uniform(0.6, 1.8)  # High variance for self-employed
            
            annual_income = int(random.uniform(*base_range) * age_multiplier * employer_multiplier)
            
            # Risk scoring with more nuanced factors
            base_risk = 0.05
            if annual_income > 0:
                income_risk = max(0.01, 1 / (annual_income / 100000 + 10)) * 0.3
            else:
                income_risk = 0.4
                
            age_risk = 0.1 if age < 25 or age > 65 else 0.05
            occupation_risk = {'Unemployed': 0.3, 'Student': 0.15, 'Self-Employed': 0.2}.get(occupation, 0.05)
            
            total_risk = min(base_risk + income_risk + age_risk + occupation_risk, 0.95)
            return annual_income, round(total_risk, 3)

        def generate_realistic_address(is_sa):
            if is_sa:
                city = weighted_choice(cities, city_weights)
                if city != 'Other':
                    # Add more realistic South African address formats
                    street_types = ['Street', 'Avenue', 'Road', 'Drive', 'Lane', 'Close']
                    street_name = f"{fake.street_name()} {random.choice(street_types)}"
                    house_number = random.randint(1, 999)
                    
                    # Add suburbs for major cities
                    suburbs = {
                        'Johannesburg': ['Sandton', 'Rosebank', 'Fourways', 'Randburg', 'Midrand', 'Alexandra', 'Soweto'],
                        'Cape Town': ['Sea Point', 'Green Point', 'Observatory', 'Wynberg', 'Bellville', 'Athlone'],
                        'Durban': ['Umhlanga', 'Morningside', 'Glenwood', 'Chatsworth', 'Pinetown'],
                        'Pretoria': ['Hatfield', 'Brooklyn', 'Centurion', 'Arcadia', 'Sunnyside']
                    }
                    
                    if city in suburbs:
                        suburb = random.choice(suburbs[city])
                        return f"{house_number} {street_name}, {suburb}, {city}, South Africa"
                    else:
                        return f"{house_number} {street_name}, {city}, South Africa"
                else:
                    province = random.choice(['Limpopo', 'Mpumalanga', 'Free State', 'Northern Cape', 'North West'])
                    return f"{fake.street_address()}, {province}, South Africa"
            else:
                return fake.address().replace('\n', ', ')

        def enhanced_company_data():
            return {
                'CompanyAge': random.randint(1, min(50, year - 1970)),  # Companies can't be older than realistic
                'NumberOfEmployees': weighted_choice([8, 25, 100, 500], [0.55, 0.30, 0.12, 0.03]),
                'AnnualTurnover': random.randint(1000000, 150000000),
                'DirectorsCount': random.randint(1, 12),
                'ShareholdersCount': random.randint(1, 25),
                'BEELevel': random.randint(1, 8),
                'VATRegistered': random.random() < 0.75,  # Higher VAT registration rate
                'IndustryRiskRating': weighted_choice(['Low', 'Medium', 'High'], [0.4, 0.45, 0.15])
            }

        def get_acquisition_date_realistic(year):
            # Generate dates with seasonal and COVID patterns
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)
            
            # Create weighted months based on seasonal patterns
            month_weights = []
            for month in range(1, 13):
                sample_date = date(year, month, 15)  # Mid-month sample
                seasonal_mult = get_seasonal_multiplier(sample_date)
                covid_mult = get_covid_impact_multiplier(sample_date, year)
                combined_weight = seasonal_mult * covid_mult
                month_weights.append(combined_weight)
            
            # Normalize weights
            total_weight = sum(month_weights)
            month_weights = [w / total_weight for w in month_weights]
            
            # Select month based on weights
            selected_month = random.choices(range(1, 13), weights=month_weights)[0]
            
            # Generate random day within selected month
            days_in_month = calendar.monthrange(year, selected_month)[1]
            selected_day = random.randint(1, days_in_month)
            
            return date(year, selected_month, selected_day)

        def generate_sa_name(is_sa):
            if is_sa:
                group = weighted_choice(ethnicity_keys, ethnicity_weights)
                first_names = {'Black': black_sa_first_names, 'Afrikaans': afr_first_names, 'English': eng_first_names, 
                            'Indian': ind_first_names, 'Coloured': col_first_names}
                last_names = {'Black': black_sa_last_names, 'Afrikaans': afr_last_names, 'English': eng_last_names, 
                            'Indian': ind_last_names, 'Coloured': col_last_names}
                return random.choice(first_names[group]), random.choice(last_names[group])
            else:
                # More diverse foreign nationalities for realism
                nationality = random.choices(
                    ['Zimbabwe', 'Botswana', 'Namibia', 'Mozambique', 'Nigeria', 'Ghana', 'Kenya'],
                    weights=[0.4, 0.15, 0.1, 0.15, 0.08, 0.07, 0.05]
                )[0]
                if nationality in ['Zimbabwe', 'Botswana', 'Namibia', 'Mozambique']:
                    return random.choice(all_zw_first_names), random.choice(all_zw_last_names), nationality
                else:
                    # Use faker for other nationalities
                    return fake.first_name(), fake.last_name(), nationality

        def generate_individual_customer(idx):
            is_sa = random.random() < 0.82  # Slightly higher SA percentage
            if is_sa:
                first, last = generate_sa_name(True)
                nationality, citizenship = 'South Africa', 'ZA'
            else:
                first, last, nationality = generate_sa_name(False)
                citizenship_map = {'Zimbabwe': 'ZW', 'Botswana': 'BW', 'Namibia': 'NA', 
                                'Mozambique': 'MZ', 'Nigeria': 'NG', 'Ghana': 'GH', 'Kenya': 'KE'}
                citizenship = citizenship_map.get(nationality, 'ZW')

            full_name = f"{first} {last}"
            age = generate_realistic_age()
            birth_date = date(year - age, random.randint(1, 12), random.randint(1, 28))
            
            gender = random.choices(['M', 'F', 'Other', 'Prefer not to say'], weights=[0.49, 0.49, 0.01, 0.01])[0]
            id_type = 'National ID' if is_sa else 'Passport'
            
            # More realistic ID numbers
            if id_type == 'National ID':
                # SA ID format: YYMMDD-SSSS-C-A-Z
                birth_year_2d = str(birth_date.year)[-2:]
                birth_month = f"{birth_date.month:02d}"
                birth_day = f"{birth_date.day:02d}"
                sequence = f"{random.randint(0, 9999):04d}"
                id_number = f"{birth_year_2d}{birth_month}{birth_day}{sequence}"
            else:
                id_number = f"{random.choice(['A', 'B', 'C', 'P'])}{random.randint(1000000, 9999999)}"
            
            expiry_date = None if id_type == 'National ID' else fake.date_between(start_date=date(year, 1, 1), end_date=date(year + 10, 12, 31))
            visa_type = random.choice(visa_types) if not is_sa else None
            visa_expiry = fake.date_between(start_date=date(year, 1, 1), end_date=date(year + 5, 12, 31)) if not is_sa else None
            
            # More realistic occupation assignment
            if age < 22:
                occupation = 'Student' if random.random() < 0.7 else random.choice(['Unemployed'] + all_occupations[:10])
            elif age < 30:
                occupation = 'Unemployed' if random.random() < 0.15 else random.choice(all_occupations)
            else:
                occupation = random.choice([occ for occ in all_occupations if occ not in ['Student']])
            
            # Employer assignment with more realistic distribution
            if occupation in ['Unemployed', 'Student']:
                employer = None
            elif occupation == 'Self-Employed' or random.random() < 0.18:
                employer = 'Self-Employed'
            elif random.random() < 0.25:  # Informal sector
                employer = 'Informal Business'
            elif random.random() < 0.35:  # Big companies
                employer = random.choice(big_companies)
            else:  # SMEs and other companies
                employer = random.choice(faker_companies)
            
            annual_income, risk_score = generate_income_and_risk(age, occupation, employer)
            
            # Source of funds with more realistic mapping
            if occupation == 'Student':
                source_of_funds = random.choices(['Allowance', 'Part-time Job', 'Bursary', 'Family Support'], 
                                            weights=[0.4, 0.3, 0.2, 0.1])[0]
            elif occupation == 'Unemployed':
                source_of_funds = random.choices(['Grants', 'Savings', 'Family Support', 'Pension'], 
                                            weights=[0.5, 0.2, 0.2, 0.1])[0]
            elif age > 60:
                source_of_funds = random.choices(['Pension', 'Savings', 'Investments', 'Salary'], 
                                            weights=[0.5, 0.2, 0.2, 0.1])[0]
            else:
                source_of_funds = random.choices(['Salary', 'Business Income', 'Investments', 'Inheritance', 'Savings'], 
                                            weights=[0.6, 0.15, 0.1, 0.05, 0.1])[0]
            
            # Add inheritance boost for older individuals
            if age > 50 and random.random() < 0.08:
                source_of_funds = 'Inheritance'
            
            # More realistic marital status distribution by age
            if age < 25:
                marital_status = random.choices(['Single', 'Married', 'In Relationship'], weights=[0.75, 0.15, 0.1])[0]
            elif age < 35:
                marital_status = random.choices(['Single', 'Married', 'Divorced', 'In Relationship'], weights=[0.4, 0.5, 0.05, 0.05])[0]
            elif age < 50:
                marital_status = random.choices(['Married', 'Single', 'Divorced', 'Widowed'], weights=[0.6, 0.2, 0.15, 0.05])[0]
            else:
                marital_status = random.choices(['Married', 'Divorced', 'Widowed', 'Single'], weights=[0.5, 0.2, 0.2, 0.1])[0]
            
            # PEP status - more realistic (very low percentage)
            is_pep = random.random() < 0.005  # 0.5% chance
            
            # Tax ID with more realistic format
            tax_id = ''.join([str(random.randint(0, 9)) for _ in range(10)])
            
            residential_address = generate_realistic_address(is_sa)
            preferred_contact = random.choices(['Email', 'Phone', 'SMS', 'WhatsApp'], weights=[0.4, 0.3, 0.15, 0.15])[0]
            date_of_entry = get_acquisition_date_realistic(year)

            return {
                'CustomerID': f'IND{year}{idx:06d}', 
                'CustomerType': 'Individual', 
                'FullName': full_name, 
                'BirthDate': birth_date,
                'Citizenship': citizenship, 
                'ResidentialAddress': residential_address, 
                'CommercialAddress': None,
                'Email': fake.email(), 
                'PhoneNumber': f"+27{random.randint(600000000, 899999999)}" if is_sa else fake.phone_number(), 
                'IDType': id_type, 
                'IDNumber': id_number,
                'ExpiryDate': expiry_date, 
                'VisaType': visa_type, 
                'VisaExpiryDate': visa_expiry, 
                'IsPEP': is_pep,
                'SanctionedCountry': False, 
                'RiskScore': risk_score, 
                'TaxIDNumber': tax_id, 
                'Occupation': occupation,
                'EmployerName': employer, 
                'SourceOfFunds': source_of_funds, 
                'MaritalStatus': marital_status,
                'Nationality': nationality, 
                'Gender': gender, 
                'PreferredContactMethod': preferred_contact, 
                'NextOfKin': None,
                'DateOfEntry': date_of_entry, 
                'AnnualIncome': annual_income, 
                'Age': age
            }

        def generate_company_customer(idx):
            # More realistic company names with South African context
            company_suffixes = ['(Pty) Ltd', 'CC', 'Inc', 'Trust', 'NPO', 'NPC']
            base_name = fake.company().replace('Ltd', '').replace('Inc', '').replace(',', '').strip()
            
            # Add some South African business naming patterns
            if random.random() < 0.3:
                sa_business_words = ['Trading', 'Investments', 'Holdings', 'Properties', 'Services', 'Solutions', 'Group']
                base_name = f"{base_name} {random.choice(sa_business_words)}"
            
            company_name = f"{base_name} {random.choice(company_suffixes)}"
            
            # More realistic registration numbers (SA format: YYYY/XXXXXX/XX)
            reg_year = random.randint(max(1980, year - 30), year)  # Companies registered in realistic timeframe
            reg_number = f"{reg_year}/{random.randint(100000, 999999)}/{random.randint(7, 23)}"
            
            industry = random.choice(industry_sectors)
            size = weighted_choice(company_sizes, [0.60, 0.25, 0.12, 0.03])  # Most companies are micro/small
            
            # Risk scoring for companies with industry-based adjustments
            industry_risk_multipliers = {
                'Finance': 0.8, 'Healthcare': 0.7, 'IT': 0.6, 'Education': 0.5,
                'Construction': 1.2, 'Mining': 1.3, 'Entertainment': 1.1, 'Tourism': 1.4
            }
            base_risk = np.random.beta(a=2, b=6)  # Skewed toward lower risk
            industry_multiplier = industry_risk_multipliers.get(industry, 1.0)
            risk_score = min(round(base_risk * industry_multiplier, 3), 0.95)
            
            tax_id = ''.join([str(random.randint(0, 9)) for _ in range(10)])
            commercial_address = generate_realistic_address(True)
            preferred_contact = random.choices(['Email', 'Phone', 'Fax'], weights=[0.6, 0.35, 0.05])[0]
            
            # Next of kin for companies (usually a director or contact person)
            next_of_kin = fake.name()
            
            date_of_entry = get_acquisition_date_realistic(year)
            company_details = enhanced_company_data()

            return {
                'CustomerID': f'COM{year}{idx:06d}', 
                'CustomerType': 'Company', 
                'FullName': company_name, 
                'BirthDate': None,
                'Citizenship': 'ZA', 
                'ResidentialAddress': None, 
                'CommercialAddress': commercial_address,
                'Email': f"info@{base_name.lower().replace(' ', '').replace('(', '').replace(')', '')}.co.za", 
                'PhoneNumber': f"+27{random.randint(100000000, 199999999)}", 
                'IDType': 'Registration Number',
                'IDNumber': reg_number, 
                'ExpiryDate': None, 
                'VisaType': None, 
                'VisaExpiryDate': None, 
                'IsPEP': False,
                'SanctionedCountry': False, 
                'RiskScore': risk_score, 
                'TaxIDNumber': tax_id, 
                'Occupation': industry,
                'EmployerName': None, 
                'SourceOfFunds': industry, 
                'MaritalStatus': None, 
                'Nationality': 'South Africa',
                'Gender': None, 
                'PreferredContactMethod': preferred_contact, 
                'NextOfKin': next_of_kin,
                'DateOfEntry': date_of_entry, 
                'Age': None,
                **company_details
            }

        print(f"Starting generation for year {year}...")
        print(f"Using dynamic seed: {seed_int}")
        print(f"Seasonal and COVID adjustments applied for realistic registration patterns")

        if NUM_INDIVIDUALS == 0 and NUM_COMPANIES == 0:
            print("No customers generated for this year due to extreme market conditions.")
            df = pd.DataFrame()
        else:
            # Generate individuals and companies with progress tracking
            print(f"Generating {NUM_INDIVIDUALS:,} individuals...")
            individuals = [generate_individual_customer(i + 1) for i in tqdm(range(NUM_INDIVIDUALS), desc="Individuals")]
            
            print(f"Generating {NUM_COMPANIES:,} companies...")
            companies = [generate_company_customer(i + 1) for i in tqdm(range(NUM_COMPANIES), desc="Companies")]

            all_customers = individuals + companies
            df = pd.DataFrame(all_customers)

            # Enhanced Next of Kin assignment for individuals
            if not df.empty and 'CustomerType' in df.columns:
                ind_rows = df[df['CustomerType'] == 'Individual'].copy()
                if len(ind_rows) > 0:
                    # Assign next of kin based on marital status and age
                    for idx in ind_rows.index:
                        row = df.loc[idx]
                        
                        # Higher probability for married individuals and those over 30
                        assign_probability = 0.1  # Base probability
                        if row['MaritalStatus'] == 'Married':
                            assign_probability = 0.6
                        elif row['MaritalStatus'] in ['Divorced', 'Widowed']:
                            assign_probability = 0.4
                        elif row['Age'] > 30:
                            assign_probability = 0.3
                        
                        if random.random() < assign_probability:
                            if random.random() < 0.3:  # 30% chance of existing customer
                                possible_kin = ind_rows[ind_rows['CustomerID'] != row['CustomerID']]['CustomerID'].values
                                if len(possible_kin) > 0:
                                    df.loc[idx, 'NextOfKin'] = np.random.choice(possible_kin)
                            else:  # 70% chance of family member (same surname pattern)
                                if row['Citizenship'] == 'ZA':
                                    kin_first = random.choice(all_sa_first_names)
                                    # 70% chance same surname (family member)
                                    kin_last = row['FullName'].split()[-1] if random.random() < 0.7 else random.choice(all_sa_last_names)
                                else:
                                    kin_first = random.choice(all_zw_first_names)
                                    kin_last = row['FullName'].split()[-1] if random.random() < 0.7 else random.choice(all_zw_last_names)
                                
                                df.loc[idx, 'NextOfKin'] = f"{kin_first} {kin_last}"

            # Sort by registration date to simulate realistic chronological order
            if not df.empty:
                df = df.sort_values('DateOfEntry').reset_index(drop=True)

        # Save to file with enhanced path handling
        output_dir = '/content/Datasets-Final'
        os.makedirs(output_dir, exist_ok=True)
        output_file = f'{output_dir}/customers_{year}_realistic.parquet'
        
        if not df.empty:
            df.to_parquet(output_file, index=False)
            
            # Generate summary statistics
            print(f"\n=== GENERATION SUMMARY FOR {year} ===")
            print(f"Total Customers Generated: {len(df):,}")
            print(f"  - Individuals: {len(df[df['CustomerType'] == 'Individual']):,}")
            print(f"  - Companies: {len(df[df['CustomerType'] == 'Company']):,}")
            
            if 'DateOfEntry' in df.columns:
                monthly_counts = df['DateOfEntry'].dt.month.value_counts().sort_index()
                print(f"\nMonthly Registration Distribution:")
                months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                for month_num, count in monthly_counts.items():
                    print(f"  {months[month_num-1]}: {count:,} customers")
            
            if 'Citizenship' in df.columns:
                citizenship_dist = df['Citizenship'].value_counts()
                print(f"\nCitizenship Distribution:")
                for citizenship, count in citizenship_dist.head().items():
                    print(f"  {citizenship}: {count:,} ({count/len(df)*100:.1f}%)")
            
            print(f"\nFile saved to: {output_file}")
        else:
            print(f"Empty dataset generated - no file saved")

        return df

# Usage example with dynamic seeding
if __name__ == "__main__":
    TARGET_YEAR = 2018
    
    print("=" * 60)
    print("REALISTIC CUSTOMER DATA GENERATOR")
    print("=" * 60)
    print("Features:")
    print("✓ Dynamic random seeding using os.urandom()")
    print("✓ Nested random ranges for customer volumes")
    print("✓ Seasonal registration patterns")
    print("✓ COVID-19 impact modeling (2020-2021)")
    print("✓ Realistic South African demographics")
    print("✓ Industry-based risk scoring")
    print("✓ Enhanced address and contact generation")
    print("=" * 60)
    
    df = generate_customer_data(TARGET_YEAR)
    
    if not df.empty:
        print(f"\nSuccessfully generated realistic customer data for {TARGET_YEAR}")
        print(f"Dataset shape: {df.shape}")
        print(f"Ready for financial services analysis")
    else:
        print(f"\nNo data generated for {TARGET_YEAR} - check market conditions")