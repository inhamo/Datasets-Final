import pandas as pd
import os
from faker import Faker
import random
from datetime import date

fake = Faker()
random.seed(42)  # Ensure reproducibility

def generate_flights():
    flights = [
        {
            'Flight_ID': f'FLT{str(i+1).zfill(4)}',
            'Flight_Name': fake.bothify(text='??###').upper(),
            'Date_Added': fake.date_between(start_date=date(2009, 1, 1), end_date=date(2018, 12, 31)),
            'Capacity': random.choice([150, 180, 200, 220, 250, 280, 300]),
            'Flight_Type': random.choice(['Domestic', 'International']),
            'Aircraft_Model': random.choice([
                'Boeing 737-800', 'Airbus A330-300', 'Boeing 737-900',
                'Boeing 777-300ER', 'Embraer E190', 'Airbus A321neo', 'Bombardier Q400'
            ]),
            'Aircraft_Registration': fake.bothify(text='N###??').upper()
        }
        for i in range(7)
    ]
    return pd.DataFrame(flights)

# Generate flights data
df_flights = generate_flights()

# Save to Parquet
output_folder = 'airline_data'
os.makedirs(output_folder, exist_ok=True)
df_flights.to_parquet(f'{output_folder}/flights.parquet', index=False)

print(f"Flights dataset saved to '{output_folder}/flights.parquet'")
print(f"Total records: {len(df_flights)}")