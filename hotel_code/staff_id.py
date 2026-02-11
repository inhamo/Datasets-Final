import pandas as pd
import numpy as np
import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from faker import Faker
import itertools

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
            'Intermediate': ['Concierge', 'Housekeeping Supervisor', 'Bartender', 'Line Cook', 'Security Guard', 'Barista'],
            'Senior': ['Guest Relations Manager', 'Restaurant Manager', 'Sous Chef', 'Security Supervisor'],
            'Managerial': ['Front Office Manager', 'Executive Housekeeper', 'F&B Director', 'Assistant Manager', 'Sales Executive'],
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
            'Africa': ['en_ZA', 'sw_KE', 'ar_EG']
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
        
        # Skills by department - EXPANDED to have more options
        self.department_skills = {
            'Front Desk': ['Customer Service', 'Reservation Systems', 'Multi-tasking', 'Communication', 'Problem Solving', 
                          'Conflict Resolution', 'Computer Skills', 'Attention to Detail', 'Time Management'],
            'Housekeeping': ['Attention to Detail', 'Time Management', 'Cleaning Techniques', 'Teamwork', 'Inventory Management',
                           'Organization', 'Chemical Safety', 'Quality Control', 'Efficiency'],
            'Food & Beverage': ['Food Safety', 'Wine Knowledge', 'Menu Planning', 'Customer Service', 'Culinary Skills',
                              'Sanitation', 'Inventory Control', 'Mixology', 'Table Service'],
            'Management': ['Leadership', 'Budgeting', 'Strategic Planning', 'Team Management', 'Financial Analysis',
                          'Human Resources', 'Operations Management', 'Project Management'],
            'Maintenance': ['Technical Skills', 'Safety Procedures', 'Equipment Repair', 'Preventive Maintenance',
                          'Electrical Systems', 'Plumbing', 'HVAC Systems', 'Painting', 'Carpentry'],
            'Sales & Marketing': ['Negotiation', 'Digital Marketing', 'CRM Systems', 'Presentation Skills', 'Sales Strategy',
                                'Social Media', 'Event Planning', 'Market Analysis', 'Brand Management'],
            'Security': ['Surveillance', 'First Aid', 'Conflict Resolution', 'Emergency Procedures', 'Patrol',
                        'Access Control', 'CPR Certified', 'Risk Assessment', 'Fire Safety']
        }
        
        # Staff ID counter
        self.staff_counter = 1000000
    
    def _generate_staff_id(self) -> str:
        """Generate staff ID in format SXXXXXXXXXX (S followed by 10 digits)"""
        self.staff_counter += 1
        return f"S{self.staff_counter:010d}"
    
    def _generate_name(self, region: str, gender: str = None) -> Tuple[str, str, str]:
        """Generate culturally appropriate name based on region"""
        if gender is None:
            gender = 'M' if random.random() < 0.55 else 'F'
        
        # Select appropriate locale for region
        locales = self.regional_names.get(region, ['en_US'])
        locale = random.choice(locales)
        
        try:
            # Create faker instance for this locale
            locale_faker = Faker(locale)
            
            if gender == 'M':
                first_name = locale_faker.first_name_male()
            else:
                first_name = locale_faker.first_name_female()
            
            last_name = locale_faker.last_name()
        except:
            # Fallback to English if locale fails
            locale_faker = Faker('en_US')
            if gender == 'M':
                first_name = locale_faker.first_name_male()
            else:
                first_name = locale_faker.first_name_female()
            last_name = locale_faker.last_name()
        
        return first_name, last_name, gender
    
    def _generate_hire_date(self, hotel_open_year: int, position_level: str) -> datetime:
        """Generate realistic hire date"""
        current_year = datetime.now().year
        
        # Hotels hire staff after opening
        start_year = max(hotel_open_year, 2000)  # Don't go too far back
        end_year = current_year
        
        # Different turnover patterns based on position level
        if position_level in ['Entry Level', 'Junior']:
            # High turnover - more recent hires
            min_years_ago = 0
            max_years_ago = min(3, current_year - start_year)
        elif position_level in ['Intermediate', 'Senior']:
            # Medium tenure
            min_years_ago = 1
            max_years_ago = min(8, current_year - start_year)
        else:
            # Long tenure for managers/executives
            min_years_ago = 2
            max_years_ago = min(15, current_year - start_year)
        
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
        
        month = random.choices(list(months_weights.keys()), 
                              weights=list(months_weights.values()), k=1)[0]
        
        # Generate random day
        day = random.randint(1, 28)
        
        try:
            hire_date = datetime(hire_year, month, day)
            return hire_date
        except ValueError:
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
        elif position in ['Receptionist', 'Waiter/Waitress', 'Bartender', 'Barista']:
            return 'Junior'
        elif position in ['Housekeeper', 'Room Attendant', 'Bellhop']:
            return 'Entry Level'
        else:
            return 'Intermediate'
    
    def _calculate_salary(self, position_level: str, region: str, 
                         hotel_stars: int, experience_years: float) -> float:
        """Calculate realistic salary"""
        # Base salary from range
        base_range = self.salary_ranges[position_level]
        base_salary = random.uniform(base_range['min'], base_range['max'])
        
        # Regional adjustment
        regional_mult = self.regional_adjustments.get(region, 1.0)
        
        # Hotel star rating multiplier
        star_mult = self.star_multipliers.get(hotel_stars, 1.0)
        
        # Experience bonus (2-5% per year, capped at 10 years)
        exp_bonus = min(experience_years, 10) * random.uniform(0.02, 0.05)
        
        # Performance multiplier
        perf_mult = random.uniform(0.95, 1.10)
        
        # Calculate final salary
        salary = base_salary * regional_mult * star_mult * (1 + exp_bonus) * perf_mult
        
        # Round to nearest 100
        salary = round(salary / 100) * 100
        
        return salary
    
    def _generate_experience(self, position_level: str, hire_date: datetime, department: str) -> Dict:
        """Generate work experience and skills"""
        current_date = datetime.now()
        years_experience = max(0.1, (current_date - hire_date).days / 365.25)  # Ensure non-negative, minimum 0.1
        
        # Previous experience based on position level
        if position_level in ['Entry Level']:
            prev_experience_years = random.uniform(0, 2)
        elif position_level in ['Junior']:
            prev_experience_years = random.uniform(1, 4)
        elif position_level == 'Intermediate':
            prev_experience_years = random.uniform(2, 7)
        else:
            prev_experience_years = random.uniform(5, 12)
        
        total_experience = years_experience + prev_experience_years
        
        # Skills - FIXED: ensure we don't sample more than available
        dept_skills = self.department_skills.get(department, ['Customer Service', 'Communication', 'Teamwork'])
        num_skills = random.randint(3, min(6, len(dept_skills)))
        skills = random.sample(dept_skills, num_skills)
        
        # Language proficiencies
        languages = []
        native_lang = random.choice(self.languages)
        languages.append(f"{native_lang} (Native)")
        
        if random.random() < 0.6:  # 60% speak additional languages
            available_langs = [l for l in self.languages if l != native_lang]
            num_additional = random.randint(1, min(3, len(available_langs)))
            additional_langs = random.sample(available_langs, num_additional)
            for lang in additional_langs:
                proficiency = random.choice(['Basic', 'Intermediate', 'Fluent'])
                languages.append(f"{lang} ({proficiency})")
        
        # Certifications
        certifications = []
        if position_level in ['Executive', 'Managerial']:
            if random.random() < 0.7:
                certs = ['Hotel Management Certificate', 'CHIA', 'CRDE', 'Food Safety Manager', 
                        'CPR Certified', 'ServSafe', 'Certified Hotel Administrator']
                num_certs = random.randint(1, min(3, len(certs)))
                certifications.extend(random.sample(certs, num_certs))
        elif position_level in ['Senior']:
            if random.random() < 0.5:
                certs = ['First Aid Certified', 'Food Handler', 'Wine & Spirits Certification']
                num_certs = random.randint(1, min(2, len(certs)))
                certifications.extend(random.sample(certs, num_certs))
        
        return {
            'total_experience_years': round(total_experience, 1),
            'company_tenure_years': round(years_experience, 1),
            'skills': ', '.join(skills),
            'languages': ', '.join(languages),
            'certifications': ', '.join(certifications) if certifications else '',
            'previous_hotels_worked': random.randint(0, 4)
        }
    
    def _generate_contact_info(self, first_name: str, last_name: str) -> Dict:
        """Generate contact information"""
        # Email based on department and hotel convention
        email_pattern = random.choice([
            f"{first_name[0].lower()}{last_name.lower()}",
            f"{first_name.lower()}.{last_name.lower()}",
            f"{first_name.lower()}{last_name[0].lower()}",
            f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}"
        ])
        
        # Company email domain
        domain = random.choice(['hotelgroup.com', 'luxuryhotels.com', 'hospitalitygroup.com', 'hotels.com'])
        email = f"{email_pattern}@{domain}"
        
        # Phone number
        phone = f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
        
        # Emergency contact
        emergency_name = self.faker.name()
        emergency_relation = random.choice(['Spouse', 'Parent', 'Sibling', 'Friend', 'Partner'])
        emergency_phone = f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
        
        return {
            'email': email,
            'phone': phone,
            'emergency_contact': emergency_name,
            'emergency_relation': emergency_relation,
            'emergency_phone': emergency_phone
        }
    
    def _generate_employment_details(self, position_level: str, hire_date: datetime) -> Dict:
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
            'Full-time': ['9am-5pm', '8am-4pm', '10am-6pm', '7am-3pm', '3pm-11pm', '11pm-7am'],
            'Part-time': ['20 hours/week', '30 hours/week', 'Weekends Only', 'Evenings'],
            'Seasonal': ['Summer Only', 'Winter Only', 'Peak Season'],
            'Contract': ['Project Basis', 'Temporary', '6-month Contract']
        }
        schedule = random.choice(schedules.get(employment_type, ['9am-5pm']))
        
        # Performance rating
        performance = random.choices(
            list(self.performance_ratings.keys()),
            weights=list(self.performance_ratings.values()),
            k=1
        )[0]
        
        # Vacation days based on tenure
        tenure_years = max(0, (datetime.now() - hire_date).days / 365.25)
        if tenure_years < 1:
            vacation_days = random.randint(5, 10)
        elif tenure_years < 5:
            vacation_days = random.randint(10, 15)
        else:
            vacation_days = random.randint(15, 25)
        
        # Sick days
        sick_days = random.randint(5, 12)
        
        # Is currently active (some turnover)
        turnover_rate = self.turnover_rates.get(position_level, 0.2)
        is_active = random.random() > (turnover_rate / 2)
        
        # Termination date if not active
        termination_date = None
        termination_reason = ''
        
        if not is_active:
            termination_reason = random.choice(['Resigned', 'Terminated', 'End of Contract', 'Retired', 'Relocated'])
            # Generate termination date after hire date
            min_days = 90
            max_days = min(1825, max(min_days, int((datetime.now() - hire_date).days - 1)))
            if max_days > min_days:
                termination_days = random.randint(min_days, max_days)
                termination_date = hire_date + timedelta(days=termination_days)
        
        return {
            'employment_type': employment_type,
            'education_level': education_level,
            'work_schedule': schedule,
            'performance_rating': performance,
            'vacation_days_remaining': vacation_days,
            'sick_days_remaining': sick_days,
            'is_active': is_active,
            'termination_date': termination_date.strftime('%Y-%m-%d') if termination_date else '',
            'termination_reason': termination_reason
        }
    
    def generate_staff_for_hotel(self, hotel_id: str, hotel_region: str, 
                                hotel_stars: int, hotel_size: int, 
                                hotel_open_year: int) -> pd.DataFrame:
        """Generate staff for a specific hotel"""
        staff_data = []
        
        # Determine total staff based on hotel size
        # Rule of thumb: 0.6-1.0 staff per room for most hotels
        staff_count = max(15, int(hotel_size * random.uniform(0.6, 1.0)))
        
        print(f"  Generating {staff_count} staff for hotel {hotel_id[:8]}...")
        
        # Allocate staff to departments
        department_counts = {}
        
        # First, ensure each department gets at least 1 staff
        for dept in self.departments.keys():
            department_counts[dept] = 1
        
        remaining_staff = staff_count - len(self.departments)
        
        # Distribute remaining staff based on size factors
        for dept, info in self.departments.items():
            if remaining_staff > 0:
                dept_extra = max(0, int(remaining_staff * info['size_factor']))
                department_counts[dept] += dept_extra
                remaining_staff -= dept_extra
        
        # Distribute any remaining staff
        if remaining_staff > 0:
            departments = list(self.departments.keys())
            for i in range(remaining_staff):
                dept = random.choice(departments)
                department_counts[dept] += 1
        
        # Generate staff for each department
        for dept, count in department_counts.items():
            dept_info = self.departments[dept]
            
            # Ensure at least one manager for larger departments
            has_manager = False
            
            for i in range(count):
                # Determine position
                if not has_manager and count > 3:
                    position = dept_info['manager_position']
                    has_manager = True
                else:
                    position = random.choice(dept_info['positions'])
                    # Avoid duplicate managers
                    if position == dept_info['manager_position']:
                        if has_manager:
                            position = random.choice([p for p in dept_info['positions'] 
                                                    if p != dept_info['manager_position']])
                        else:
                            has_manager = True
                
                # Determine position level
                position_level = self._determine_position_level(position)
                
                # Generate name and gender
                first_name, last_name, gender = self._generate_name(hotel_region)
                
                # Generate hire date
                hire_date = self._generate_hire_date(hotel_open_year, position_level)
                
                # Calculate experience - FIXED: pass department
                experience_info = self._generate_experience(position_level, hire_date, dept)
                
                # Calculate salary
                salary = self._calculate_salary(
                    position_level, hotel_region, hotel_stars,
                    experience_info['total_experience_years']
                )
                
                # Generate contact info
                contact_info = self._generate_contact_info(first_name, last_name)
                
                # Generate employment details
                emp_details = self._generate_employment_details(position_level, hire_date)
                
                # Generate shift
                shift = random.choice(dept_info['shifts'])
                
                # Date of birth (18-65 years old)
                today = datetime.now()
                hire_date_obj = hire_date if isinstance(hire_date, datetime) else datetime.now()
                min_birth_year = hire_date_obj.year - 65
                max_birth_year = hire_date_obj.year - 18
                birth_year = random.randint(min_birth_year, max_birth_year)
                birth_month = random.randint(1, 12)
                birth_day = random.randint(1, 28)
                
                try:
                    date_of_birth = datetime(birth_year, birth_month, birth_day)
                except ValueError:
                    date_of_birth = datetime(birth_year, birth_month, 15)
                
                # Create staff record with proper SXXXXXXXXXX format
                staff = {
                    'staff_id': self._generate_staff_id(),
                    'hotel_id': hotel_id,
                    'first_name': first_name,
                    'last_name': last_name,
                    'gender': gender,
                    'position': position,
                    'department': dept,
                    'position_level': position_level,
                    'hire_date': hire_date.strftime('%Y-%m-%d'),
                    'salary': round(salary, 2),
                    'annual_bonus': round(salary * random.uniform(0.03, 0.15), 2),
                    'shift': shift,
                    'email': contact_info['email'],
                    'phone': contact_info['phone'],
                    'emergency_contact': contact_info['emergency_contact'],
                    'emergency_relation': contact_info['emergency_relation'],
                    'emergency_phone': contact_info['emergency_phone'],
                    'total_experience_years': experience_info['total_experience_years'],
                    'company_tenure_years': experience_info['company_tenure_years'],
                    'skills': experience_info['skills'],
                    'languages': experience_info['languages'],
                    'certifications': experience_info['certifications'],
                    'previous_hotels_worked': experience_info['previous_hotels_worked'],
                    'employment_type': emp_details['employment_type'],
                    'education_level': emp_details['education_level'],
                    'work_schedule': emp_details['work_schedule'],
                    'performance_rating': emp_details['performance_rating'],
                    'vacation_days_remaining': emp_details['vacation_days_remaining'],
                    'sick_days_remaining': emp_details['sick_days_remaining'],
                    'is_active': emp_details['is_active'],
                    'termination_date': emp_details['termination_date'],
                    'termination_reason': emp_details['termination_reason'],
                    'date_of_birth': date_of_birth.strftime('%Y-%m-%d'),
                    'nationality': hotel_region,
                    'emergency_contact_name': contact_info['emergency_contact'],  # Added for clarity
                    'emergency_contact_phone': contact_info['emergency_phone']   # Added for clarity
                }
                
                staff_data.append(staff)
        
        return pd.DataFrame(staff_data)
    
    def generate_staff_for_multiple_hotels(self, hotels_df: pd.DataFrame) -> pd.DataFrame:
        """Generate staff for multiple hotels"""
        all_staff = []
        
        print(f"Generating staff for {len(hotels_df)} hotels...")
        
        for idx, hotel in hotels_df.iterrows():
            print(f"  Processing hotel {idx+1}/{len(hotels_df)}: {hotel.get('hotel_name', 'Unknown Hotel')}")
            
            # Ensure hotel has required fields
            hotel_id = hotel['hotel_id'] if 'hotel_id' in hotel else str(uuid.uuid4())
            hotel_region = hotel['region'] if 'region' in hotel else 'North America'
            hotel_stars = hotel['star_rating'] if 'star_rating' in hotel else 3
            hotel_size = hotel['total_rooms'] if 'total_rooms' in hotel else 100
            hotel_open_year = hotel['year_opened'] if 'year_opened' in hotel else 2010
            
            hotel_staff = self.generate_staff_for_hotel(
                hotel_id=hotel_id,
                hotel_region=hotel_region,
                hotel_stars=hotel_stars,
                hotel_size=hotel_size,
                hotel_open_year=hotel_open_year
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
        
        print("\nAverage Tenure by Position Level:")
        tenure_stats = staff_df.groupby('position_level')['company_tenure_years'].mean().round(2)
        print(tenure_stats)
        
        # Staff ID sample
        print("\nStaff ID Sample (first 10):")
        print(staff_df['staff_id'].head(10).tolist())

# Usage example with your actual data
if __name__ == "__main__":
    # Initialize generator
    generator = HotelStaffGenerator()
    
    # Load hotel data from your CSV
    try:
        sample_hotels = pd.read_csv("hotel_data/hotel_chain_hotels.csv")
        print(f"Loaded {len(sample_hotels)} hotels from CSV")
        print("\nSample hotel data:")
        print(sample_hotels[['hotel_id', 'hotel_name', 'region', 'star_rating', 'total_rooms']].head())
    except FileNotFoundError:
        print("Hotel CSV not found. Creating sample hotel data for testing...")
        # Create sample hotel data if file doesn't exist
        sample_hotels = pd.DataFrame({
            'hotel_id': [str(uuid.uuid4()) for _ in range(5)],
            'hotel_name': ['Luxury Grand Hotel', 'Business Inn', 'Beach Resort', 'City Center Hotel', 'Airport Hotel'],
            'region': ['North America', 'Europe', 'Asia Pacific', 'North America', 'Europe'],
            'star_rating': [5, 3, 4, 4, 3],
            'total_rooms': [450, 200, 350, 300, 250],
            'year_opened': [2005, 2010, 2008, 2012, 2015]
        })
        print("Created 5 sample hotels for testing")
    
    # Generate staff for hotels
    staff_df = generator.generate_staff_for_multiple_hotels(sample_hotels)
    
    # Save to CSV
    staff_filename = f'hotel_data/hotel_chain_staff.csv'
    staff_df.to_csv(staff_filename, index=False)
    print(f"\nSaved staff data to {staff_filename}")
        
    # Analyze distribution
    generator.analyze_staff_distribution(staff_df, sample_hotels)
    
    # Display samples
    print("\n=== Sample Staff (First 10 Rows) ===")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', 30)
    
    display_cols = ['staff_id', 'hotel_id', 'first_name', 'last_name', 'position', 
                   'department', 'hire_date', 'salary', 'is_active', 'performance_rating']
    
    if all(col in staff_df.columns for col in display_cols):
        print(staff_df[display_cols].head(10))
    else:
        print(staff_df.head(10))
    
    # Additional analysis
    print("\n=== Key Metrics ===")
    print(f"Total annual payroll: ${staff_df['salary'].sum():,.2f}")
    print(f"Average staff per hotel: {len(staff_df) / len(sample_hotels):.1f}")
    
    if 'total_rooms' in sample_hotels.columns:
        total_rooms = sample_hotels['total_rooms'].sum()
        print(f"Staff to room ratio: {len(staff_df) / total_rooms:.2f}")
    
    # Department headcount analysis
    print("\n=== Department Headcount ===")
    dept_headcount = staff_df.groupby('department').size().sort_values(ascending=False)
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
    print(f"Average tenure: {active_staff['company_tenure_years'].mean():.1f} years")
    
    # Calculate average age at hire
    if 'date_of_birth' in active_staff.columns:
        active_staff['hire_date_dt'] = pd.to_datetime(active_staff['hire_date'])
        active_staff['dob_dt'] = pd.to_datetime(active_staff['date_of_birth'])
        avg_age_days = (active_staff['hire_date_dt'] - active_staff['dob_dt']).dt.days.mean()
        print(f"Average age at hire: {avg_age_days/365:.1f} years")