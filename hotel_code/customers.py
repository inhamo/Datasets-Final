import pandas as pd
import numpy as np
import uuid
from datetime import datetime, timedelta
from faker import Faker
import random
from typing import List, Dict, Tuple
import hashlib
import os

# Initialize Faker with multiple locales for realism
locales = ['en_US', 'en_GB', 'de_DE', 'fr_FR', 'es_ES', 'it_IT', 'pt_BR', 'ru_RU', 'ja_JP', 'zh_CN', 'ar_SA', 'ko_KR']
fakers = {locale.split('_')[0]: Faker(locale) for locale in locales}
fake_en = fakers['en']

class HotelCustomerGenerator:
    def __init__(self, start_date='2019-01-01', end_date='2024-12-31'):
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Counter for sequential customer IDs
        self.customer_counter = 1000000
        
        # Global distribution weights by region
        self.region_distribution = {
            'North America': {'weight': 0.35, 'countries': ['US', 'CA', 'MX']},
            'Europe': {'weight': 0.30, 'countries': ['GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'SE', 'CH']},
            'Asia Pacific': {'weight': 0.25, 'countries': ['CN', 'JP', 'IN', 'AU', 'SG', 'KR', 'TH']},
            'Middle East': {'weight': 0.05, 'countries': ['AE', 'SA', 'QA', 'KW']},
            'South America': {'weight': 0.03, 'countries': ['BR', 'AR', 'CL', 'CO']},
            'Africa': {'weight': 0.02, 'countries': ['ZA', 'EG', 'KE', 'NG']}
        }
        
        # Country specific details
        self.country_details = {
            'US': {'phone_prefix': '+1', 'timezone': ['EST', 'CST', 'PST', 'MST']},
            'GB': {'phone_prefix': '+44', 'timezone': ['GMT']},
            'DE': {'phone_prefix': '+49', 'timezone': ['CET']},
            'FR': {'phone_prefix': '+33', 'timezone': ['CET']},
            'CN': {'phone_prefix': '+86', 'timezone': ['CST']},
            'JP': {'phone_prefix': '+81', 'timezone': ['JST']},
            'IN': {'phone_prefix': '+91', 'timezone': ['IST']},
            'AU': {'phone_prefix': '+61', 'timezone': ['AEST', 'ACST', 'AWST']},
            'AE': {'phone_prefix': '+971', 'timezone': ['GST']},
            'BR': {'phone_prefix': '+55', 'timezone': ['BRT']},
        }
        
        # Age distribution by customer type
        self.age_distribution = {
            'Business': {'min': 25, 'max': 65, 'peak': [30, 45]},
            'Leisure': {'min': 18, 'max': 80, 'peak': [25, 60]},
            'Family': {'min': 25, 'max': 55, 'peak': [35, 45]},
            'Group': {'min': 20, 'max': 70, 'peak': [30, 50]}
        }
        
        # Customer type distribution
        self.customer_type_dist = {
            'Business': 0.35,
            'Leisure': 0.45,
            'Family': 0.12,
            'Group': 0.08
        }
        
        # Email domain distribution
        self.email_domains = {
            'gmail.com': 0.45,
            'outlook.com': 0.15,
            'yahoo.com': 0.10,
            'hotmail.com': 0.08,
            'icloud.com': 0.05,
            'company': 0.17  # Will be replaced with actual company domains
        }
        
        # Common company domains for business travelers
        self.company_domains = [
            'acme.com', 'globex.com', 'initech.com', 'umbrellacorp.com',
            'waynecorp.com', 'starkindustries.com', 'oscorp.com',
            'cyberdyne.com', 'tyrellcorp.com', 'dummycorp.com'
        ]
        
        # Loyalty tiers
        self.loyalty_tiers = {
            'Non-Member': 0.55,
            'Basic': 0.25,
            'Silver': 0.12,
            'Gold': 0.06,
            'Platinum': 0.02
        }
    
    def _generate_customer_id(self) -> str:
        """Generate customer ID in format CXXXXXXXXXX (C followed by 10 digits)"""
        self.customer_counter += 1
        # Format as C + 10 digits with leading zeros
        return f"C{self.customer_counter:010d}"
    
    def _select_region_country(self) -> Tuple[str, str]:
        """Select region and country based on weights"""
        regions = list(self.region_distribution.keys())
        weights = [self.region_distribution[r]['weight'] for r in regions]
        
        region = random.choices(regions, weights=weights, k=1)[0]
        country = random.choice(self.region_distribution[region]['countries'])
        
        return region, country
    
    def _generate_signup_date(self, customer_type: str) -> datetime:
        """Generate realistic signup date considering seasonality and pandemic"""
        # Base random date
        days_diff = (self.end_date - self.start_date).days
        random_days = random.randint(0, days_diff)
        base_date = self.start_date + timedelta(days=random_days)
        
        # Apply seasonality weights
        month = base_date.month
        
        # Seasonal multipliers (higher in Jan-Feb and May-June for signups)
        seasonal_multiplier = {
            1: 1.3, 2: 1.2,  # Post-holiday planning
            5: 1.4, 6: 1.3,  # Summer planning
            12: 0.7, 7: 0.9, # Holiday season lull
        }.get(month, 1.0)
        
        # Pandemic impact (2020-2021 had fewer signups)
        if 2020 <= base_date.year <= 2021:
            if base_date.year == 2020 and base_date.month in [3, 4, 5]:  # Peak lockdown
                pandemic_multiplier = 0.3
            elif base_date.year == 2021 and base_date.month <= 6:  # Slow recovery
                pandemic_multiplier = 0.6
            else:
                pandemic_multiplier = 0.8
        else:
            pandemic_multiplier = 1.0
        
        # Adjust probability
        if random.random() > seasonal_multiplier * pandemic_multiplier * 0.5:
            # Try again for more realistic distribution
            return self._generate_signup_date(customer_type)
        
        # Business travelers more likely to sign up on weekdays
        if customer_type == 'Business' and base_date.weekday() >= 5:  # Weekend
            if random.random() > 0.3:
                return self._generate_signup_date(customer_type)
        
        return base_date
    
    def _generate_name(self, country: str) -> Tuple[str, str, str]:
        """Generate culturally appropriate names"""
        locale_map = {
            'US': 'en', 'CA': 'en', 'GB': 'en',
            'DE': 'de', 'FR': 'fr', 'ES': 'es', 'IT': 'it',
            'CN': 'zh', 'JP': 'ja', 'KR': 'ko',
            'IN': 'en',  # English names common in India for business
            'BR': 'pt', 'AR': 'es',
            'AE': 'ar', 'SA': 'ar',
            'ZA': 'en', 'AU': 'en', 'SG': 'en'
        }
        
        locale = locale_map.get(country, 'en')
        faker = fakers.get(locale, fake_en)
        
        # Gender distribution
        gender = 'M' if random.random() < 0.55 else 'F'
        
        if gender == 'M':
            first_name = faker.first_name_male()
        else:
            first_name = faker.first_name_female()
        
        last_name = faker.last_name()
        
        return first_name, last_name, gender
    
    def _generate_email(self, first_name: str, last_name: str, customer_type: str, country: str) -> str:
        """Generate realistic email address"""
        # Choose email pattern
        patterns = [
            f"{first_name.lower()}.{last_name.lower()}",
            f"{first_name.lower()}{last_name.lower()[0]}",
            f"{first_name[0].lower()}{last_name.lower()}",
            f"{first_name.lower()}{random.randint(1, 99)}",
            f"{first_name.lower()}_{last_name.lower()}",
        ]
        
        email_local = random.choice(patterns)
        
        # Choose domain
        if customer_type == 'Business' and random.random() < 0.7:
            # Business email
            if country == 'US' and random.random() < 0.8:
                company = random.choice(self.company_domains)
            else:
                # International company - sometimes local domain
                company = random.choice(self.company_domains + [f"example.{country.lower()}"])
            
            domain = company
        else:
            # Personal email
            domains = list(self.email_domains.keys())
            weights = list(self.email_domains.values())
            
            domain = random.choices(domains, weights=weights, k=1)[0]
            
            if domain == 'company':
                # Fallback for personal with 'company' selected
                domain = random.choice(['gmail.com', 'outlook.com'])
        
        # Add 5% chance of invalid/placeholder emails
        if random.random() < 0.05:
            return random.choice(['test@test.com', 'guest@email.com', 'noemail@provided.com'])
        
        return f"{email_local}@{domain}"
    
    def _generate_phone(self, country: str) -> str:
        """Generate country-specific phone number"""
        country_info = self.country_details.get(country, {'phone_prefix': '+1', 'timezone': ['EST']})
        
        # Different formats by country - FIXED: proper number of placeholders
        formats = {
            'US': ['({}{}{}) {}{}{}-{}{}{}{}',  # (XXX) XXX-XXXX - 10 digits
                   '{}{}{}-{}{}{}-{}{}{}{}'],   # XXX-XXX-XXXX - 10 digits
            'GB': ['0{}{}{}{} {}{}{} {}{}{}',   # 0XXXX XXX XXX - 10 digits
                   '+44 {}{}{}{} {}{}{}{}{}'],  # +44 XXXX XXXXXX - 10 digits after prefix
            'DE': ['+49 {}{}{} {}{}{}{}{}',     # +49 XXX XXXXXXX - 10 digits after prefix
                   '+49 {}{}{}{} {}{}{}{}'],    # +49 XXXX XXXXXX - 10 digits after prefix
            'CN': ['{}{}{} {}{}{}{} {}{}{}',    # XXX XXXX XXXX - 10 digits
                   '+86 {}{}{} {}{}{}{} {}{}{}'], # +86 XXX XXXX XXXX - 10 digits
            'FR': ['+33 {}{}{} {}{}{} {}{}{}',  # +33 XXX XXX XXX - 9 digits after prefix
                   '0{}{}{}{}{}{}{}{}{}'],      # 0XXXXXXXXX - 10 digits
            'JP': ['{}{}{}-{}{}{}-{}{}{}{}',    # XXX-XXX-XXXX - 10 digits
                   '+81 {}{}{}-{}{}{}-{}{}{}'],  # +81 XX-XXXX-XXXX - 10 digits after prefix
            'AU': ['0{}{}{} {}{}{} {}{}{}',     # 0XXX XXX XXX - 10 digits
                   '+61 {}{}{} {}{}{} {}{}{}'],  # +61 XXX XXX XXX - 9 digits after prefix
            'BR': ['({}{}{}) {}{}{}{}-{}{}{}',  # (XX) XXXX-XXXX - 10 digits
                   '+55 {}{}{} {}{}{}{}-{}{}{}'], # +55 XX XXXXX-XXXX - 10/11 digits
        }
        
        # Get format for country or use generic
        if country in formats:
            format_pattern = random.choice(formats[country])
            # Count the number of {} placeholders
            placeholder_count = format_pattern.count('{}')
            # Generate exactly that many digits
            digits = [str(random.randint(0, 9)) for _ in range(placeholder_count)]
            phone = format_pattern.format(*digits)
        else:
            # Generic international format
            phone = f"{country_info['phone_prefix']} {random.randint(100, 999)} {random.randint(100, 999)} {random.randint(1000, 9999)}"
        
        # 3% chance of invalid/missing phone
        if random.random() < 0.03:
            return '' if random.random() < 0.5 else '000-000-0000'
        
        return phone
    
    def _generate_age(self, customer_type: str) -> int:
        """Generate age based on customer type"""
        dist = self.age_distribution[customer_type]
        
        # Normal distribution around peak ages
        if 'peak' in dist:
            peak_min, peak_max = dist['peak']
            peak_center = (peak_min + peak_max) / 2
            # Generate with normal distribution
            age = int(np.random.normal(peak_center, 8))
            age = max(dist['min'], min(dist['max'], age))
        else:
            age = random.randint(dist['min'], dist['max'])
        
        return age
    
    def _determine_loyalty_tier(self, signup_date: datetime) -> str:
        """Determine loyalty tier with time-based logic"""
        # Older accounts more likely to be higher tiers
        account_age_years = (datetime.now() - signup_date).days / 365
        
        # Adjust probabilities based on account age
        adjusted_probs = self.loyalty_tiers.copy()
        
        if account_age_years > 3:
            # Very old accounts more likely to be Gold/Platinum
            adjusted_probs['Non-Member'] *= 0.3
            adjusted_probs['Basic'] *= 0.7
            adjusted_probs['Silver'] *= 1.5
            adjusted_probs['Gold'] *= 2.0
            adjusted_probs['Platinum'] *= 3.0
        elif account_age_years > 1:
            # Older accounts less likely to be non-members
            adjusted_probs['Non-Member'] *= 0.6
            adjusted_probs['Basic'] *= 1.2
            adjusted_probs['Silver'] *= 1.3
        elif account_age_years < 0.25:
            # New accounts more likely to be non-members
            adjusted_probs['Non-Member'] *= 1.3
            adjusted_probs['Basic'] *= 0.8
            adjusted_probs['Silver'] *= 0.5
            adjusted_probs['Gold'] *= 0.3
            adjusted_probs['Platinum'] *= 0.1
        
        # Normalize probabilities
        total = sum(adjusted_probs.values())
        normalized = {k: v/total for k, v in adjusted_probs.items()}
        
        tiers = list(normalized.keys())
        weights = list(normalized.values())
        
        return random.choices(tiers, weights=weights, k=1)[0]
    
    def _generate_preferences(self, customer_type: str, country: str, signup_date: datetime) -> Dict:
        """Generate customer preferences"""
        preferences = {}
        
        # Communication preferences
        comm_options = ['Email', 'SMS', 'Both', 'None']
        comm_weights = [0.7, 0.2, 0.08, 0.02]
        preferences['communication_pref'] = random.choices(comm_options, weights=comm_weights, k=1)[0]
        
        # Marketing opt-in (higher for recent years due to GDPR awareness)
        preferences['marketing_opt_in'] = random.random() < 0.65
        
        # Special requirements
        special_reqs = []
        if random.random() < 0.12:
            reqs = ['Wheelchair Accessible', 'Allergies', 'Early Check-in', 'Late Check-out', 'Non-smoking', 'High Floor']
            num_reqs = random.randint(1, 2)
            special_reqs = random.sample(reqs, num_reqs)
        preferences['special_requirements'] = ', '.join(special_reqs) if special_reqs else ''
        
        # Language preference based on country
        language_map = {
            'US': 'English', 'GB': 'English', 'CA': 'English',
            'DE': 'German', 'FR': 'French', 'ES': 'Spanish',
            'IT': 'Italian', 'CN': 'Chinese', 'JP': 'Japanese',
            'IN': 'English', 'AU': 'English', 'BR': 'Portuguese',
            'AE': 'Arabic', 'SA': 'Arabic', 'KR': 'Korean'
        }
        preferences['preferred_language'] = language_map.get(country, 'English')
        
        # Payment preference
        payment_methods = ['Visa', 'Mastercard', 'American Express', 'PayPal', 'Apple Pay', 'Google Pay']
        payment_weights = [0.45, 0.30, 0.15, 0.05, 0.03, 0.02]
        
        # Adjust for year (digital wallets more recent)
        if signup_date.year >= 2021:
            payment_weights = [0.40, 0.28, 0.13, 0.07, 0.08, 0.04]
        
        preferences['preferred_payment'] = random.choices(payment_methods, weights=payment_weights, k=1)[0]
        
        # Room preferences
        room_prefs = ['Standard', 'Deluxe', 'Suite', 'Executive', 'Family', 'Ocean View']
        room_weights = [0.40, 0.25, 0.15, 0.10, 0.07, 0.03]
        
        # Business travelers prefer Executive/Suite
        if customer_type == 'Business':
            room_weights = [0.20, 0.20, 0.25, 0.25, 0.05, 0.05]
        elif customer_type == 'Family':
            room_weights = [0.30, 0.30, 0.15, 0.05, 0.15, 0.05]
            
        preferences['preferred_room'] = random.choices(room_prefs, weights=room_weights, k=1)[0]
        
        return preferences
    
    def generate_customers(self, num_customers: int = None) -> pd.DataFrame:
        """Generate synthetic customer data with random count if not specified"""
        if num_customers is None:
            # Generate a random number between 3000 and 15000
            num_customers = random.randint(3000, 15000)
        
        customers = []
        
        print(f"Generating {num_customers} customers...")
        
        for i in range(num_customers):
            if i % 100 == 0:
                print(f"Progress: {i}/{num_customers}")
            
            # Generate customer type
            customer_types = list(self.customer_type_dist.keys())
            weights = list(self.customer_type_dist.values())
            customer_type = random.choices(customer_types, weights=weights, k=1)[0]
            
            # Generate region and country
            region, country = self._select_region_country()
            
            # Generate signup date
            signup_date = self._generate_signup_date(customer_type)
            
            # Generate name and gender
            first_name, last_name, gender = self._generate_name(country)
            
            # Generate age
            age = self._generate_age(customer_type)
            
            # Generate contact info
            email = self._generate_email(first_name, last_name, customer_type, country)
            phone = self._generate_phone(country)
            
            # Generate loyalty tier
            loyalty_tier = self._determine_loyalty_tier(signup_date)
            
            # Generate preferences - FIXED: passing signup_date parameter
            preferences = self._generate_preferences(customer_type, country, signup_date)
            
            # Generate city
            try:
                city_faker = fakers.get(country[:2].lower(), fake_en)
                city = city_faker.city()
            except:
                city = fake_en.city()
            
            # Create customer record with proper CXXXXXXXXXX format
            customer = {
                'customer_id': self._generate_customer_id(),
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'phone': phone,
                'country': country,
                'region': region,
                'city': city,
                'age': age,
                'gender': gender,
                'customer_type': customer_type,
                'signup_date': signup_date.strftime('%Y-%m-%d'),
                'signup_year': signup_date.year,
                'signup_month': signup_date.month,
                'loyalty_tier': loyalty_tier,
                'communication_preference': preferences['communication_pref'],
                'marketing_opt_in': preferences['marketing_opt_in'],
                'preferred_language': preferences['preferred_language'],
                'preferred_payment': preferences['preferred_payment'],
                'preferred_room': preferences['preferred_room'],
                'special_requirements': preferences['special_requirements'],
                'account_status': 'Active' if random.random() < 0.85 else 'Inactive',
                'last_active_date': (signup_date + timedelta(days=random.randint(0, 365*3))).strftime('%Y-%m-%d') if random.random() < 0.7 else '',
                'data_source': random.choice(['Website Direct', 'OTA Referral', 'Corporate Contract', 'Walk-in', 'Phone Booking']),
                'total_stays': random.randint(0, 50) if loyalty_tier != 'Non-Member' else random.randint(0, 5),
                'total_spend': round(random.uniform(0, 25000), 2)
            }
            
            customers.append(customer)
        
        df = pd.DataFrame(customers)
        
        # Add some duplicates (1-2%)
        if num_customers > 100:
            num_duplicates = max(1, int(num_customers * 0.015))
            duplicates = df.sample(n=num_duplicates).copy()
            duplicates['customer_id'] = [self._generate_customer_id() for _ in range(num_duplicates)]
            duplicates['email'] = duplicates['email'].apply(lambda e: e.replace('@', '2@') if '@' in e else e + '2')
            df = pd.concat([df, duplicates], ignore_index=True)
        
        return df
    
    def analyze_distribution(self, df: pd.DataFrame):
        """Analyze the distribution of generated data"""
        print("\n=== Distribution Analysis ===")
        print(f"Total customers: {len(df)}")
        print(f"Date range: {df['signup_date'].min()} to {df['signup_date'].max()}")
        
        print("\nBy Region:")
        print(df['region'].value_counts(normalize=True).round(3))
        
        print("\nBy Customer Type:")
        print(df['customer_type'].value_counts(normalize=True).round(3))
        
        print("\nBy Loyalty Tier:")
        print(df['loyalty_tier'].value_counts(normalize=True).round(3))
        
        print("\nBy Signup Year:")
        yearly_counts = df.groupby('signup_year').size()
        print(yearly_counts)
        
        print("\nEmail Domain Distribution (sample):")
        # Extract domains
        domains = df['email'].str.split('@').str[1]
        top_domains = domains.value_counts().head(10)
        print(top_domains)
        
        print("\nCustomer ID Sample (first 10):")
        print(df['customer_id'].head(10).tolist())

# Usage example
if __name__ == "__main__":
    # Initialize generator
    generator = HotelCustomerGenerator()
    
    # Generate random number of customers (between 3000 and 15000)
    customers_df = generator.generate_customers()  # No parameter = random count
    
    # Create directory if it doesn't exist
    os.makedirs('hotel_data', exist_ok=True)
    
    # Save to CSV with timestamp in filename
    filename = f'hotel_data/hotel_customers.csv'
    customers_df.to_csv(filename, index=False)
    print(f"\nData saved to: {filename}")
        
    # Analyze distribution
    generator.analyze_distribution(customers_df)
    
    # Display sample
    print("\n=== Sample Data (First 10 Rows) ===")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(customers_df.head(10))
    
    # Additional analysis
    print("\n=== Additional Insights ===")
    print(f"Average age: {customers_df['age'].mean():.1f}")
    print(f"Gender distribution:\n{customers_df['gender'].value_counts(normalize=True).round(3)}")
    print(f"Marketing opt-in rate: {customers_df['marketing_opt_in'].mean():.2%}")
    print(f"Active accounts: {customers_df['account_status'].value_counts(normalize=True).round(3)}")
    print(f"Average total stays: {customers_df['total_stays'].mean():.1f}")
    print(f"Average total spend: ${customers_df['total_spend'].mean():,.2f}")