import pandas as pd
import numpy as np
import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from faker import Faker
import itertools
from tqdm import tqdm

class HotelStaffGenerator:
    def __init__(self):
        self.faker = Faker()
        
        # Departments with positions and hierarchy
        self.departments = {
            'Front Desk': {
                'positions': ['Receptionist', 'Front Desk Agent', 'Concierge', 'Bell Captain', 'Bellhop', 'Guest Relations Manager'],
                'manager_position': 'Front Office Manager',
                'size_factor': 0.25,  # Proportion of total staff
                'shifts': ['Morning', 'Afternoon', 'Night', 'Rotating']
            },
            'Housekeeping': {
                'positions': ['Housekeeper', 'Room Attendant', 'Housekeeping Supervisor', 'Laundry Attendant', 'Public Area Cleaner'],
                'manager_position': 'Executive Housekeeper',
                'size_factor': 0.35,
                'shifts': ['Morning', 'Afternoon']
            },
            'Food & Beverage': {
                'positions': ['Waiter/Waitress', 'Bartender', 'Barista', 'Restaurant Manager', 'Sous Chef', 'Line Cook', 'Pastry Chef'],
                'manager_position': 'F&B Director',
                'size_factor': 0.20,
                'shifts': ['Morning', 'Afternoon', 'Night', 'Split']
            },
            'Management': {
                'positions': ['General Manager', 'Assistant Manager', 'Operations Manager', 'Revenue Manager'],
                'manager_position': 'General Manager',
                'size_factor': 0.05,
                'shifts': ['Day']
            },
            'Maintenance': {
                'positions': ['Maintenance Technician', 'Electrician', 'Plumber', 'HVAC Technician', 'Groundskeeper'],
                'manager_position': 'Chief Engineer',
                'size_factor': 0.08,
                'shifts': ['Day', 'On-Call']
            },
            'Sales & Marketing': {
                'positions': ['Sales Executive', 'Marketing Coordinator', 'Event Planner', 'Revenue Analyst'],
                'manager_position': 'Sales & Marketing Director',
                'size_factor': 0.04,
                'shifts': ['Day']
            },
            'Security': {
                'positions': ['Security Guard', 'Security Supervisor', 'CCTV Operator'],
                'manager_position': 'Security Director',
                'size_factor': 0.03,
                'shifts': ['Night', 'Rotating', '24-hour']
            }
        }
        
        # Position hierarchy and career progression
        self.position_hierarchy = {
            'Entry Level': ['Housekeeper', 'Room Attendant', 'Bellhop', 'Laundry Attendant'],
            'Junior': ['Receptionist', 'Front Desk Agent', 'Waiter/Waitress', 'Maintenance Technician'],
            'Intermediate': ['Concierge', 'Housekeeping Supervisor', 'Bartender', 'Line Cook', 'Security Guard'],
            'Senior': ['Guest Relations Manager', 'Restaurant Manager', 'Sous Chef', 'Security Supervisor'],
            'Managerial': ['Front Office Manager', 'Executive Housekeeper', 'F&B Director', 'Assistant Manager'],
            'Executive': ['General Manager', 'Chief Engineer', 'Sales & Marketing Director', 'Security Director']
        }
        
        # Salary ranges by position category (annual USD)
        self.salary_ranges = {
            'Entry Level': {'min': 22000, 'max': 35000},
            'Junior': {'min': 28000, 'max': 45000},
            'Intermediate': {'min': 35000, 'max': 60000},
            'Senior': {'min': 45000, 'max': 80000},
            'Managerial': {'min': 60000, 'max': 120000},
            'Executive': {'min': 80000, 'max': 250000}
        }
        
        # Regional salary adjustments
        self.regional_adjustments = {
            'North America': 1.0,
            'Europe': 1.15,
            'Asia Pacific': 0.85,
            'Middle East': 0.95,
            'South America': 0.60,
            'Africa': 0.55
        }
        
        # Hotel star rating salary multipliers
        self.star_multipliers = {
            1: 0.8,
            2: 0.9,
            3: 1.0,
            4: 1.2,
            5: 1.5
        }
        
        # Name generation by region
        self.regional_names = {
            'North America': ['en_US', 'es_MX', 'fr_CA'],
            'Europe': ['en_GB', 'de_DE', 'fr_FR', 'es_ES', 'it_IT'],
            'Asia Pacific': ['ja_JP', 'zh_CN', 'ko_KR', 'hi_IN', 'th_TH'],
            'Middle East': ['ar_SA', 'ar_AE'],
            'South America': ['pt_BR', 'es_AR', 'es_CO'],
            'Africa': ['zu_ZA', 'ar_EG']
        }
        
        # Performance rating distribution
        self.performance_ratings = {
            'Exceeds Expectations': 0.15,
            'Meets Expectations': 0.70,
            'Needs Improvement': 0.10,
            'Unsatisfactory': 0.05
        }
        
        # Turnover rates by position (annual)
        self.turnover_rates = {
            'Entry Level': 0.40,
            'Junior': 0.30,
            'Intermediate': 0.20,
            'Senior': 0.15,
            'Managerial': 0.10,
            'Executive': 0.05
        }
        
        # Employment types
        self.employment_types = {
            'Full-time': 0.75,
            'Part-time': 0.20,
            'Seasonal': 0.04,
            'Contract': 0.01
        }
        
        # Education levels
        self.education_levels = {
            'High School': 0.45,
            'Vocational Training': 0.20,
            "Bachelor's Degree": 0.25,
            "Master's Degree": 0.08,
            'PhD': 0.02
        }
        
        # Language proficiencies
        self.languages = ['English', 'Spanish', 'French', 'German', 'Chinese', 'Arabic', 'Japanese', 'Russian']
        
        # Skills by department
        self.department_skills = {
            'Front Desk': ['Customer Service', 'Reservation Systems', 'Multi-tasking', 'Communication', 'Problem Solving'],
            'Housekeeping': ['Attention to Detail', 'Time Management', 'Cleaning Techniques', 'Teamwork'],
            'Food & Beverage': ['Food Safety', 'Wine Knowledge', 'Menu Planning', 'Customer Service'],
            'Management': ['Leadership', 'Budgeting', 'Strategic Planning', 'Team Management'],
            'Maintenance': ['Technical Skills', 'Safety Procedures', 'Equipment Repair'],
            'Sales & Marketing': ['Negotiation', 'Digital Marketing', 'CRM Systems', 'Presentation Skills'],
            'Security': ['Surveillance', 'First Aid', 'Conflict Resolution', 'Emergency Procedures']
        }
    
    def _generate_staff_id(self) -> str:
        """Generate UUID for staff"""
        return str(uuid.uuid4())
    
    def _generate_name(self, region: str, gender: str = None) -> Tuple[str, str, str]:
        """Generate culturally appropriate name based on region"""
        if gender is None:
            gender = 'M' if random.random() < 0.55 else 'F'
        
        # Select appropriate locale for region
        locales = self.regional_names.get(region, ['en_US'])
        locale = random.choice(locales)
        
        # Create faker instance for this locale
        locale_faker = Faker(locale)
        
        if gender == 'M':
            first_name = locale_faker.first_name_male()
        else:
            first_name = locale_faker.first_name_female()
        
        last_name = locale_faker.last_name()
        
        return first_name, last_name, gender
    
    def _generate_start_date(self, hotel_open_year: int, position_level: str) -> datetime:
        """Generate realistic start date for work"""
        current_year = datetime.now().year
        
        # Hotels hire staff after opening
        start_year = max(hotel_open_year, 2010)  # Don't go too far back
        end_year = current_year
        
        # Different turnover patterns based on position level
        if position_level in ['Entry Level', 'Junior']:
            # High turnover - more recent hires
            min_years_ago = 0
            max_years_ago = min(5, current_year - start_year)
        elif position_level in ['Intermediate', 'Senior']:
            # Medium tenure
            min_years_ago = 1
            max_years_ago = min(10, current_year - start_year)
        else:
            # Long tenure for managers/executives
            min_years_ago = 3
            max_years_ago = min(20, current_year - start_year)
        
        # Ensure max_years_ago >= min_years_ago
        max_years_ago = max(min_years_ago, max_years_ago)
        
        # Generate random date within range
        years_ago = random.randint(min_years_ago, max_years_ago)
        hire_year = current_year - years_ago
        
        # Seasonal hiring patterns
        months_weights = {
            1: 0.7,   # January - post-holiday hiring
            2: 0.8,   # February
            3: 1.0,   # March - spring hiring
            4: 1.1,   # April
            5: 1.3,   # May - pre-summer hiring
            6: 1.2,   # June
            7: 0.9,   # July
            8: 0.8,   # August
            9: 1.0,   # September - post-summer
            10: 0.9,  # October
            11: 0.7,  # November - pre-holiday slowdown
            12: 0.6   # December - holiday slowdown
        }
        
        # Adjust for COVID impact (2020 had fewer hires)
        if hire_year == 2020:
            # Reduce hiring probability
            if random.random() < 0.7:
                # Try for 2021 instead
                hire_year = 2021
        
        month = random.choices(list(months_weights.keys()), 
                              weights=list(months_weights.values()), k=1)[0]
        
        # Avoid weekends for hire dates (most hiring happens weekdays)
        for attempt in range(10):
            day = random.randint(1, 28)
            try:
                start_date = datetime(hire_year, month, day)
                if start_date.weekday() < 5:  # Weekday
                    return start_date
            except ValueError:
                continue
        
        # Fallback
        return datetime(hire_year, month, 15)
    
    def _determine_position_level(self, position: str) -> str:
        """Determine the hierarchy level of a position"""
        for level, positions in self.position_hierarchy.items():
            if position in positions:
                return level
        
        # Default based on position name
        if 'Manager' in position or 'Director' in position or 'Chief' in position:
            return 'Executive'
        elif 'Supervisor' in position or 'Lead' in position:
            return 'Senior'
        elif position in ['Receptionist', 'Waiter/Waitress', 'Bartender']:
            return 'Junior'
        else:
            return 'Intermediate'
    
    def _calculate_salary(self, position_level: str, region: str, 
                         hotel_stars: int, start_date: datetime) -> float:
        """Calculate realistic salary based on position and tenure"""
        # Base salary from range
        base_range = self.salary_ranges[position_level]
        base_salary = random.uniform(base_range['min'], base_range['max'])
        
        # Regional adjustment
        regional_mult = self.regional_adjustments.get(region, 1.0)
        
        # Hotel star rating multiplier
        star_mult = self.star_multipliers.get(hotel_stars, 1.0)
        
        # Tenure bonus (2-5% per year, capped at 10 years)
        years_with_company = max(0, (datetime.now() - start_date).days / 365.25)
        exp_bonus = min(years_with_company, 10) * random.uniform(0.02, 0.05)
        
        # Performance multiplier
        perf_mult = random.uniform(0.95, 1.10)
        
        # Calculate final salary
        salary = base_salary * regional_mult * star_mult * (1 + exp_bonus) * perf_mult
        
        # Round to nearest 100
        salary = round(salary / 100) * 100
        
        # Add benefits value (20-40% of salary)
        benefits_mult = random.uniform(1.20, 1.40)
        total_comp = salary * benefits_mult
        
        return total_comp
    
    def _generate_skills_and_languages(self, department: str) -> Dict:
        """Generate skills and language proficiencies"""
        # Get skills for the department
        available_skills = self.department_skills.get(department, ['Customer Service'])
        num_skills_to_select = min(random.randint(2, 5), len(available_skills))
        skills = random.sample(available_skills, k=num_skills_to_select)
        
        # Language proficiencies
        languages = []
        native_lang = random.choice(self.languages)
        languages.append(f"{native_lang} (Native)")
        
        if random.random() < 0.6:  # 60% speak additional languages
            available_langs = [l for l in self.languages if l != native_lang]
            num_langs = min(random.randint(1, 3), len(available_langs))
            additional_langs = random.sample(available_langs, k=num_langs)
            for lang in additional_langs:
                proficiency = random.choice(['Basic', 'Intermediate', 'Fluent'])
                languages.append(f"{lang} ({proficiency})")
        
        return {
            'skills': ', '.join(skills),
            'languages': ', '.join(languages)
        }
    
    def _generate_certifications(self, position_level: str) -> str:
        """Generate certifications based on position level"""
        certifications = []
        if position_level in ['Executive', 'Managerial']:
            if random.random() < 0.7:
                certs = ['Hotel Management Certificate', 'CHIA', 'CRDE', 'Food Safety Manager']
                num_certs = random.randint(1, min(2, len(certs)))
                certifications.extend(random.sample(certs, k=num_certs))
        
        return ', '.join(certifications) if certifications else ''
    
    def _generate_contact_info(self, first_name: str, last_name: str, 
                              department: str) -> Dict:
        """Generate contact information"""
        # Email based on department and hotel convention
        email_pattern = random.choice([
            f"{first_name[0].lower()}{last_name.lower()}",
            f"{first_name.lower()}.{last_name.lower()}",
            f"{first_name.lower()}{last_name[0].lower()}",
        ])
        
        # Company email domain
        domain = random.choice(['hotelgroup.com', 'luxuryhotels.com', 'hospitalitygroup.com'])
        email = f"{email_pattern}@{domain}"
        
        # Phone number
        phone = f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
        
        # Emergency contact
        emergency_name = self.faker.name()
        emergency_relation = random.choice(['Spouse', 'Parent', 'Sibling', 'Friend'])
        emergency_phone = f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
        
        return {
            'email': email,
            'phone': phone,
            'emergency_contact': emergency_name,
            'emergency_relation': emergency_relation,
            'emergency_phone': emergency_phone
        }
    
    def _generate_employment_details(self, position_level: str, start_date: datetime) -> Dict:
        """Generate employment details"""
        employment_type = random.choices(
            list(self.employment_types.keys()),
            weights=list(self.employment_types.values()),
            k=1
        )[0]
        
        education_level = random.choices(
            list(self.education_levels.keys()),
            weights=list(self.education_levels.values()),
            k=1
        )[0]
        
        # Work schedule
        schedules = {
            'Full-time': ['9-5', '8-4', '10-6', 'Shift Work'],
            'Part-time': ['20 hours/week', '30 hours/week', 'Weekends Only'],
            'Seasonal': ['Summer Only', 'Winter Only', 'Peak Season'],
            'Contract': ['Project Basis', 'Temporary']
        }
        schedule = random.choice(schedules.get(employment_type, ['9-5']))
        
        # Performance rating
        performance = random.choices(
            list(self.performance_ratings.keys()),
            weights=list(self.performance_ratings.values()),
            k=1
        )[0]
        
        # Annual vacation days based on position level and region
        if position_level in ['Entry Level', 'Junior']:
            vacation_days_per_year = random.randint(10, 15)
        elif position_level in ['Intermediate', 'Senior']:
            vacation_days_per_year = random.randint(15, 20)
        else:  # Managerial/Executive
            vacation_days_per_year = random.randint(20, 30)
        
        # Annual sick days
        sick_days_per_year = random.randint(5, 12)
        
        # Is currently active (some turnover)
        is_active = random.random() > self.turnover_rates.get(position_level, 0.2) / 2
        
        # End date if not active
        end_date = None
        if not is_active:
            years_with_company = max(0, (datetime.now() - start_date).days / 365.25)
            if years_with_company > 0:
                # Ensure termination is after start date
                min_days = 90
                max_days = max(min_days, int(years_with_company * 365))
                termination_days = random.randint(min_days, max_days)
                end_date = start_date + timedelta(days=termination_days)
                end_date = min(end_date, datetime.now())
        
        return {
            'employment_type': employment_type,
            'education_level': education_level,
            'work_schedule': schedule,
            'performance_rating': performance,
            'vacation_days_per_year': vacation_days_per_year,
            'sick_days_per_year': sick_days_per_year,
            'is_active': is_active,
            'end_date': end_date.strftime('%Y-%m-%d') if end_date else '',
            'termination_reason': random.choice(['Resigned', 'Terminated', 'End of Contract', 'Retired']) 
                                  if end_date else ''
        }
    
    def generate_staff_for_hotel(self, hotel_id: str, hotel_region: str, 
                                hotel_stars: int, hotel_size: int, 
                                hotel_open_year: int) -> pd.DataFrame:
        """Generate staff for a specific hotel"""
        staff_data = []
        
        # Determine total staff based on hotel size
        # Rule of thumb: 0.8-1.2 staff per room
        staff_count = max(10, int(hotel_size * random.uniform(0.8, 1.2)))
        
        # Allocate staff to departments
        department_counts = {}
        remaining_staff = staff_count
        
        for dept, info in self.departments.items():
            dept_count = max(1, int(staff_count * info['size_factor']))
            department_counts[dept] = dept_count
            remaining_staff -= dept_count
        
        # Distribute remaining staff
        if remaining_staff > 0:
            departments = list(self.departments.keys())
            for i in range(remaining_staff):
                dept = random.choice(departments)
                department_counts[dept] += 1
        
        # Generate staff for each department
        for dept, count in department_counts.items():
            dept_info = self.departments[dept]
            
            for i in range(count):
                # Determine position
                if i == 0 and count > 3:
                    position = dept_info['manager_position']
                else:
                    position = random.choice(dept_info['positions'])
                
                # Determine position level
                position_level = self._determine_position_level(position)
                
                # Generate name and gender
                first_name, last_name, gender = self._generate_name(hotel_region)
                
                # Generate start date
                start_date = self._generate_start_date(hotel_open_year, position_level)
                
                # Generate skills and languages
                skills_langs = self._generate_skills_and_languages(dept)
                
                # Generate certifications
                certifications = self._generate_certifications(position_level)
                
                # Calculate salary
                salary = self._calculate_salary(
                    position_level, hotel_region, hotel_stars, start_date
                )
                
                # Generate contact info
                contact_info = self._generate_contact_info(first_name, last_name, dept)
                
                # Generate employment details
                emp_details = self._generate_employment_details(position_level, start_date)
                
                # Generate shift
                shift = random.choice(dept_info['shifts'])
                
                # Create staff record
                staff = {
                    'staff_id': self._generate_staff_id(),
                    'hotel_id': hotel_id,
                    'first_name': first_name,
                    'last_name': last_name,
                    'gender': gender,
                    'position': position,
                    'department': dept,
                    'position_level': position_level,
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'salary': round(salary, 2),
                    'annual_bonus': round(salary * random.uniform(0.05, 0.20), 2),
                    'shift': shift,
                    'email': contact_info['email'],
                    'phone': contact_info['phone'],
                    'emergency_contact': contact_info['emergency_contact'],
                    'emergency_relation': contact_info['emergency_relation'],
                    'emergency_phone': contact_info['emergency_phone'],
                    'skills': skills_langs['skills'],
                    'languages': skills_langs['languages'],
                    'certifications': certifications,
                    'employment_type': emp_details['employment_type'],
                    'education_level': emp_details['education_level'],
                    'work_schedule': emp_details['work_schedule'],
                    'performance_rating': emp_details['performance_rating'],
                    'vacation_days_per_year': emp_details['vacation_days_per_year'],
                    'sick_days_per_year': emp_details['sick_days_per_year'],
                    'is_active': emp_details['is_active'],
                    'end_date': emp_details['end_date'],
                    'termination_reason': emp_details['termination_reason'],
                    'date_of_birth': (start_date - timedelta(days=random.randint(6570, 14600))).strftime('%Y-%m-%d'),
                    'nationality': hotel_region
                }
                
                staff_data.append(staff)
        
        return pd.DataFrame(staff_data)
    
    def generate_staff_for_multiple_hotels(self, hotels_df: pd.DataFrame) -> pd.DataFrame:
        """Generate staff for multiple hotels"""
        all_staff = []
        
        print(f"Generating staff for {len(hotels_df)} hotels...")
        
        for idx, hotel in tqdm(hotels_df.iterrows(), total=len(hotels_df), desc="Processing hotels"):
            hotel_staff = self.generate_staff_for_hotel(
                hotel_id=hotel['hotel_id'],
                hotel_region=hotel['region'],
                hotel_stars=hotel['star_rating'],
                hotel_size=hotel['total_rooms'],
                hotel_open_year=hotel['year_opened']
            )
            all_staff.append(hotel_staff)
        
        combined_df = pd.concat(all_staff, ignore_index=True)
        return combined_df
    
    def analyze_staff_distribution(self, staff_df: pd.DataFrame, hotels_df: pd.DataFrame = None):
        """Analyze staff distribution"""
        print("\n=== Staff Distribution Analysis ===")
        print(f"Total staff generated: {len(staff_df)}")
        
        print("\nDepartment Distribution:")
        dept_dist = staff_df['department'].value_counts(normalize=True).round(3)
        print(dept_dist)
        
        print("\nPosition Level Distribution:")
        level_dist = staff_df['position_level'].value_counts(normalize=True).round(3)
        print(level_dist)
        
        print("\nEmployment Type Distribution:")
        emp_dist = staff_df['employment_type'].value_counts(normalize=True).round(3)
        print(emp_dist)
        
        print("\nSalary Statistics by Department:")
        salary_stats = staff_df.groupby('department')['salary'].agg(['mean', 'min', 'max', 'count']).round(2)
        print(salary_stats)
        
        print("\nActive vs Inactive Staff:")
        active_dist = staff_df['is_active'].value_counts(normalize=True).round(3)
        print(active_dist)
        
        # Merge with hotels if available
        if hotels_df is not None:
            merged_df = staff_df.merge(hotels_df[['hotel_id', 'region', 'star_rating']], on='hotel_id')
            
            print("\nAverage Salary by Region:")
            region_salaries = merged_df.groupby('region')['salary'].mean().round(2)
            print(region_salaries)
            
            print("\nAverage Salary by Hotel Star Rating:")
            star_salaries = merged_df.groupby('star_rating')['salary'].mean().round(2)
            print(star_salaries)

# Usage example
if __name__ == "__main__":
    # Initialize generator
    generator = HotelStaffGenerator()
    
    # Load hotel data from your CSV
    sample_hotels = pd.read_csv("hotel data/hotel_chain_hotels.csv")
    
    print(f"Loaded {len(sample_hotels)} hotels from CSV")
    print("\nSample hotel data:")
    print(sample_hotels[['hotel_id', 'hotel_name', 'region', 'star_rating', 'total_rooms']].head())
    
    # Generate staff for hotels
    staff_df = generator.generate_staff_for_multiple_hotels(sample_hotels)
    
    # Save to CSV
    staff_df.to_csv('hotel data/hotel_chain_staff.csv', index=False)
    print(f"\nSaved staff data to hotel_chain_staff.csv")
    
    # Analyze distribution
    generator.analyze_staff_distribution(staff_df, sample_hotels)
    
    # Display samples
    print("\n=== Sample Staff (First 10 Rows) ===")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', 30)
    
    display_cols = ['staff_id', 'hotel_id', 'first_name', 'last_name', 'position', 
                   'department', 'start_date', 'salary', 'is_active', 'performance_rating']
    print(staff_df[display_cols].head(10))
    
    # Additional analysis
    print("\n=== Key Metrics ===")
    print(f"Total annual payroll: ${staff_df['salary'].sum():,.2f}")
    print(f"Average staff per hotel: {len(staff_df) / len(sample_hotels):.1f}")
    
    if 'total_rooms' in sample_hotels.columns:
        total_rooms = sample_hotels['total_rooms'].sum()
        print(f"Staff to room ratio: {len(staff_df) / total_rooms:.2f}")
    
    # Department headcount analysis
    print("\n=== Department Headcount ===")
    dept_headcount = staff_df.groupby('department').size()
    for dept, count in dept_headcount.items():
        print(f"{dept}: {count} staff ({count/len(staff_df):.1%})")
    
    # Salary distribution analysis
    print("\n=== Salary Distribution ===")
    salary_bins = [0, 30000, 50000, 75000, 100000, 150000, float('inf')]
    salary_labels = ['<30k', '30-50k', '50-75k', '75-100k', '100-150k', '>150k']
    staff_df['salary_bracket'] = pd.cut(staff_df['salary'], bins=salary_bins, labels=salary_labels)
    print(staff_df['salary_bracket'].value_counts().sort_index())
    
    # Generate summary report
    print("\n=== Staff Summary Report ===")
    active_staff = staff_df[staff_df['is_active'] == True]
    print(f"Active staff: {len(active_staff)}")
    print(f"Inactive staff: {len(staff_df) - len(active_staff)}")
    
    # Calculate average tenure
    active_staff['tenure_days'] = (pd.to_datetime('today') - pd.to_datetime(active_staff['start_date'])).dt.days
    print(f"Average tenure: {active_staff['tenure_days'].mean()/365:.1f} years")