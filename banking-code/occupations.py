import numpy as np

def get_occupations_data():
    # Pre-compute occupations
    occupations = [
        # Education
        'Junior Teacher', 'Intermediate Teacher', 'Senior Teacher', 'School Principal', 'University Lecturer', 'Professor',
        'Kindergarten Teacher', 'Special Education Teacher', 'Math Teacher', 'English Teacher',
        # Healthcare
        'Junior Nurse', 'Intermediate Nurse', 'Senior Nurse', 'Doctor General Practitioner', 'Junior Doctor', 'Specialist Doctor', 'Surgeon',
        'Pharmacist', 'Dentist', 'Physiotherapist', 'Radiographer', 'Medical Technician', 'Healthcare Manager',
        # Engineering
        'Junior Engineer Civil', 'Intermediate Engineer Civil', 'Senior Engineer Civil', 'Junior Engineer Electrical', 'Intermediate Engineer Electrical', 'Senior Engineer Electrical',
        'Junior Engineer Mechanical', 'Intermediate Engineer Mechanical', 'Senior Engineer Mechanical', 'Engineer Project Manager',
        # Transportation
        'Junior Driver', 'Senior Driver', 'Truck Driver', 'Taxi Driver', 'Bus Driver', 'Logistics Coordinator',
        # Sales and Marketing
        'Junior Salesperson', 'Intermediate Salesperson', 'Senior Salesperson', 'Sales Manager', 'Marketing Coordinator', 'Junior Marketer', 'Senior Marketer',
        'Account Executive', 'Business Development Manager',
        # Finance and Accounting
        'Junior Accountant', 'Intermediate Accountant', 'Senior Accountant', 'Chartered Accountant', 'Financial Analyst Junior', 'Financial Analyst Senior',
        'Auditor', 'Tax Consultant', 'Bank Teller', 'Investment Banker',
        # Management
        'Junior Manager Operations', 'Intermediate Manager Operations', 'Senior Manager Operations', 'HR Manager', 'IT Manager Junior', 'IT Manager Senior',
        'Project Manager', 'CEO', 'Director',
        # Retail
        'Junior Cashier', 'Senior Cashier', 'Retail Supervisor', 'Store Manager', 'Merchandiser',
        # Students and Unemployed
        'Student Undergraduate', 'Student Postgraduate', 'Unemployed Skilled', 'Unemployed Unskilled',
        # IT and Data
        'Junior Data Analyst', 'Intermediate Data Analyst', 'Senior Data Analyst', 'Data Scientist I', 'Data Scientist II', 'Senior Data Scientist',
        'Junior Software Developer', 'Intermediate Software Developer', 'Senior Software Developer', 'DevOps Engineer', 'Cybersecurity Analyst',
        'Junior IT Support', 'Senior IT Support', 'Systems Administrator',
        # Legal
        'Junior Lawyer', 'Senior Lawyer', 'Judge', 'Legal Secretary', 'Paralegal',
        # Construction
        'Junior Architect', 'Senior Architect', 'Construction Worker', 'Site Supervisor', 'Quantity Surveyor',
        # Hospitality
        'Waiter/Waitress', 'Chef Junior', 'Chef Senior', 'Hotel Manager', 'Tour Guide',
        # Agriculture
        'Farm Worker', 'Agricultural Technician', 'Farm Manager',
        # Media
        'Journalist Junior', 'Journalist Senior', 'Graphic Designer', 'Copywriter',
        # Other
        'Electrician', 'Plumber', 'Mechanic', 'Security Guard', 'Cleaner', 'Call Center Agent'
    ]

    # Realistic annual income ranges in ZAR and required education levels for South Africa (2025 estimates)
    income_ranges = {
        'Junior Teacher': {'range': (200000, 350000), 'required_education': 'Bachelor Degree'},
        'Intermediate Teacher': {'range': (300000, 450000), 'required_education': 'Bachelor Degree'},
        'Senior Teacher': {'range': (400000, 600000), 'required_education': 'Bachelor Degree'},
        'School Principal': {'range': (600000, 1000000), 'required_education': 'Masters Degree'},
        'University Lecturer': {'range': (450000, 800000), 'required_education': 'Masters Degree'},
        'Professor': {'range': (800000, 1500000), 'required_education': 'Doctorate/PhD'},
        'Kindergarten Teacher': {'range': (180000, 300000), 'required_education': 'Diploma'},
        'Special Education Teacher': {'range': (250000, 400000), 'required_education': 'Bachelor Degree'},
        'Math Teacher': {'range': (280000, 450000), 'required_education': 'Bachelor Degree'},
        'English Teacher': {'range': (250000, 420000), 'required_education': 'Bachelor Degree'},
        'Junior Nurse': {'range': (220000, 320000), 'required_education': 'Diploma'},
        'Intermediate Nurse': {'range': (300000, 450000), 'required_education': 'Diploma'},
        'Senior Nurse': {'range': (400000, 650000), 'required_education': 'Bachelor Degree'},
        'Doctor General Practitioner': {'range': (800000, 1500000), 'required_education': 'Bachelor Degree'},
        'Junior Doctor': {'range': (500000, 800000), 'required_education': 'Bachelor Degree'},
        'Specialist Doctor': {'range': (1200000, 2500000), 'required_education': 'Masters Degree'},
        'Surgeon': {'range': (1500000, 3000000), 'required_education': 'Masters Degree'},
        'Pharmacist': {'range': (450000, 750000), 'required_education': 'Bachelor Degree'},
        'Dentist': {'range': (600000, 1200000), 'required_education': 'Bachelor Degree'},
        'Physiotherapist': {'range': (300000, 500000), 'required_education': 'Bachelor Degree'},
        'Radiographer': {'range': (350000, 550000), 'required_education': 'Bachelor Degree'},
        'Medical Technician': {'range': (250000, 400000), 'required_education': 'Diploma'},
        'Healthcare Manager': {'range': (700000, 1200000), 'required_education': 'Bachelor Degree'},
        'Junior Engineer Civil': {'range': (300000, 450000), 'required_education': 'Bachelor Degree'},
        'Intermediate Engineer Civil': {'range': (450000, 700000), 'required_education': 'Bachelor Degree'},
        'Senior Engineer Civil': {'range': (700000, 1100000), 'required_education': 'Bachelor Degree'},
        'Junior Engineer Electrical': {'range': (320000, 480000), 'required_education': 'Bachelor Degree'},
        'Intermediate Engineer Electrical': {'range': (480000, 750000), 'required_education': 'Bachelor Degree'},
        'Senior Engineer Electrical': {'range': (750000, 1200000), 'required_education': 'Bachelor Degree'},
        'Junior Engineer Mechanical': {'range': (310000, 460000), 'required_education': 'Bachelor Degree'},
        'Intermediate Engineer Mechanical': {'range': (460000, 720000), 'required_education': 'Bachelor Degree'},
        'Senior Engineer Mechanical': {'range': (720000, 1150000), 'required_education': 'Bachelor Degree'},
        'Engineer Project Manager': {'range': (800000, 1400000), 'required_education': 'Bachelor Degree'},
        'Junior Driver': {'range': (150000, 250000), 'required_education': 'High School Completed'},
        'Senior Driver': {'range': (250000, 400000), 'required_education': 'High School Completed'},
        'Truck Driver': {'range': (200000, 350000), 'required_education': 'High School Completed'},
        'Taxi Driver': {'range': (120000, 240000), 'required_education': 'High School Completed'},
        'Bus Driver': {'range': (180000, 300000), 'required_education': 'High School Completed'},
        'Logistics Coordinator': {'range': (300000, 500000), 'required_education': 'Diploma'},
        'Junior Salesperson': {'range': (150000, 280000), 'required_education': 'High School Completed'},
        'Intermediate Salesperson': {'range': (250000, 400000), 'required_education': 'High School Completed'},
        'Senior Salesperson': {'range': (350000, 600000), 'required_education': 'Diploma'},
        'Sales Manager': {'range': (500000, 900000), 'required_education': 'Bachelor Degree'},
        'Marketing Coordinator': {'range': (250000, 400000), 'required_education': 'Diploma'},
        'Junior Marketer': {'range': (200000, 350000), 'required_education': 'Diploma'},
        'Senior Marketer': {'range': (450000, 800000), 'required_education': 'Bachelor Degree'},
        'Account Executive': {'range': (300000, 500000), 'required_education': 'Diploma'},
        'Business Development Manager': {'range': (600000, 1000000), 'required_education': 'Bachelor Degree'},
        'Junior Accountant': {'range': (250000, 400000), 'required_education': 'Diploma'},
        'Intermediate Accountant': {'range': (350000, 550000), 'required_education': 'Bachelor Degree'},
        'Senior Accountant': {'range': (500000, 800000), 'required_education': 'Bachelor Degree'},
        'Chartered Accountant': {'range': (700000, 1200000), 'required_education': 'Bachelor Degree'},
        'Financial Analyst Junior': {'range': (300000, 450000), 'required_education': 'Bachelor Degree'},
        'Financial Analyst Senior': {'range': (550000, 900000), 'required_education': 'Bachelor Degree'},
        'Auditor': {'range': (400000, 650000), 'required_education': 'Bachelor Degree'},
        'Tax Consultant': {'range': (450000, 700000), 'required_education': 'Bachelor Degree'},
        'Bank Teller': {'range': (150000, 250000), 'required_education': 'High School Completed'},
        'Investment Banker': {'range': (800000, 1500000), 'required_education': 'Bachelor Degree'},
        'Junior Manager Operations': {'range': (400000, 600000), 'required_education': 'Bachelor Degree'},
        'Intermediate Manager Operations': {'range': (550000, 850000), 'required_education': 'Bachelor Degree'},
        'Senior Manager Operations': {'range': (800000, 1300000), 'required_education': 'Bachelor Degree'},
        'HR Manager': {'range': (500000, 900000), 'required_education': 'Bachelor Degree'},
        'IT Manager Junior': {'range': (450000, 700000), 'required_education': 'Bachelor Degree'},
        'IT Manager Senior': {'range': (800000, 1400000), 'required_education': 'Bachelor Degree'},
        'Project Manager': {'range': (600000, 1100000), 'required_education': 'Bachelor Degree'},
        'CEO': {'range': (2000000, 5000000), 'required_education': 'Bachelor Degree'},
        'Director': {'range': (1200000, 3000000), 'required_education': 'Bachelor Degree'},
        'Junior Cashier': {'range': (100000, 180000), 'required_education': 'High School Completed'},
        'Senior Cashier': {'range': (180000, 280000), 'required_education': 'High School Completed'},
        'Retail Supervisor': {'range': (220000, 350000), 'required_education': 'High School Completed'},
        'Store Manager': {'range': (350000, 600000), 'required_education': 'Diploma'},
        'Merchandiser': {'range': (200000, 350000), 'required_education': 'High School Completed'},
        'Student Undergraduate': {'range': (0, 60000), 'required_education': 'High School Completed'},
        'Student Postgraduate': {'range': (0, 100000), 'required_education': 'Bachelor Degree'},
        'Unemployed Skilled': {'range': (0, 50000), 'required_education': 'High School Completed'},
        'Unemployed Unskilled': {'range': (0, 30000), 'required_education': 'No Formal Education'},
        'Junior Data Analyst': {'range': (250000, 400000), 'required_education': 'Bachelor Degree'},
        'Intermediate Data Analyst': {'range': (350000, 550000), 'required_education': 'Bachelor Degree'},
        'Senior Data Analyst': {'range': (500000, 800000), 'required_education': 'Bachelor Degree'},
        'Data Scientist I': {'range': (450000, 700000), 'required_education': 'Bachelor Degree'},
        'Data Scientist II': {'range': (650000, 1000000), 'required_education': 'Masters Degree'},
        'Senior Data Scientist': {'range': (900000, 1500000), 'required_education': 'Masters Degree'},
        'Junior Software Developer': {'range': (280000, 450000), 'required_education': 'Bachelor Degree'},
        'Intermediate Software Developer': {'range': (400000, 650000), 'required_education': 'Bachelor Degree'},
        'Senior Software Developer': {'range': (600000, 1000000), 'required_education': 'Bachelor Degree'},
        'DevOps Engineer': {'range': (550000, 900000), 'required_education': 'Bachelor Degree'},
        'Cybersecurity Analyst': {'range': (500000, 850000), 'required_education': 'Bachelor Degree'},
        'Junior IT Support': {'range': (150000, 280000), 'required_education': 'Diploma'},
        'Senior IT Support': {'range': (300000, 500000), 'required_education': 'Diploma'},
        'Systems Administrator': {'range': (400000, 700000), 'required_education': 'Bachelor Degree'},
        'Junior Lawyer': {'range': (300000, 500000), 'required_education': 'Bachelor Degree'},
        'Senior Lawyer': {'range': (700000, 1500000), 'required_education': 'Bachelor Degree'},
        'Judge': {'range': (1200000, 2000000), 'required_education': 'Bachelor Degree'},
        'Legal Secretary': {'range': (180000, 300000), 'required_education': 'High School Completed'},
        'Paralegal': {'range': (200000, 350000), 'required_education': 'Diploma'},
        'Junior Architect': {'range': (300000, 500000), 'required_education': 'Bachelor Degree'},
        'Senior Architect': {'range': (600000, 1100000), 'required_education': 'Bachelor Degree'},
        'Construction Worker': {'range': (120000, 250000), 'required_education': 'No Formal Education'},
        'Site Supervisor': {'range': (250000, 450000), 'required_education': 'Diploma'},
        'Quantity Surveyor': {'range': (400000, 700000), 'required_education': 'Bachelor Degree'},
        'Waiter/Waitress': {'range': (100000, 200000), 'required_education': 'No Formal Education'},
        'Chef Junior': {'range': (150000, 300000), 'required_education': 'High School Completed'},
        'Chef Senior': {'range': (350000, 600000), 'required_education': 'Diploma'},
        'Hotel Manager': {'range': (400000, 800000), 'required_education': 'Bachelor Degree'},
        'Tour Guide': {'range': (150000, 300000), 'required_education': 'High School Completed'},
        'Farm Worker': {'range': (80000, 180000), 'required_education': 'No Formal Education'},
        'Agricultural Technician': {'range': (200000, 400000), 'required_education': 'Diploma'},
        'Farm Manager': {'range': (350000, 700000), 'required_education': 'Bachelor Degree'},
        'Journalist Junior': {'range': (200000, 350000), 'required_education': 'Bachelor Degree'},
        'Journalist Senior': {'range': (400000, 700000), 'required_education': 'Bachelor Degree'},
        'Graphic Designer': {'range': (250000, 500000), 'required_education': 'Diploma'},
        'Copywriter': {'range': (280000, 450000), 'required_education': 'Bachelor Degree'},
        'Electrician': {'range': (250000, 450000), 'required_education': 'Diploma'},
        'Plumber': {'range': (220000, 400000), 'required_education': 'Diploma'},
        'Mechanic': {'range': (200000, 380000), 'required_education': 'Diploma'},
        'Security Guard': {'range': (120000, 220000), 'required_education': 'High School Completed'},
        'Cleaner': {'range': (80000, 150000), 'required_education': 'No Formal Education'},
        'Call Center Agent': {'range': (150000, 280000), 'required_education': 'High School Completed'}
    }

    # Normalize probabilities (uniform for simplicity; can be adjusted with real labor market data)
    num_occupations = len(occupations)
    occupation_probs = np.ones(num_occupations) / num_occupations

    return occupations, income_ranges, occupation_probs