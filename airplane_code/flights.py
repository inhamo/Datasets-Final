import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from tqdm import tqdm
import random
import os

# Set random seeds for reproducibility
seed_bytes = os.urandom(4)
seed_int = int.from_bytes(seed_bytes, byteorder='big')
random.seed(seed_int)
np.random.seed(seed_int)

# Constants
TARGET_YEAR = 2024
NUM_PLANES = random.randint(1, 5)

# Aircraft models with their specifications
AIRCRAFT_MODELS = {
    'Boeing 737-800': {
        'capacity': 162,
        'fuel_capacity_gallons': 6875,
        'fuel_efficiency_mpg': 0.55,
        'range_miles': 2935,
        'cruise_speed_mph': 583,
        'length_ft': 129.6,
        'wingspan_ft': 117.5,
        'height_ft': 41.2,
        'max_takeoff_weight_lbs': 174200,
        'engine_type': 'CFM56-7B',
        'engine_count': 2,
        'manufacturing_cost_millions': 89.1
    },
    'Airbus A320-200': {
        'capacity': 150,
        'fuel_capacity_gallons': 6590,
        'fuel_efficiency_mpg': 0.58,
        'range_miles': 3300,
        'cruise_speed_mph': 576,
        'length_ft': 123.3,
        'wingspan_ft': 111.9,
        'height_ft': 38.7,
        'max_takeoff_weight_lbs': 169750,
        'engine_type': 'CFM56-5B',
        'engine_count': 2,
        'manufacturing_cost_millions': 91.5
    },
    'Boeing 787-8 Dreamliner': {
        'capacity': 242,
        'fuel_capacity_gallons': 33640,
        'fuel_efficiency_mpg': 0.95,
        'range_miles': 7635,
        'cruise_speed_mph': 593,
        'length_ft': 186,
        'wingspan_ft': 197,
        'height_ft': 55.6,
        'max_takeoff_weight_lbs': 502500,
        'engine_type': 'GEnx-1B',
        'engine_count': 2,
        'manufacturing_cost_millions': 224.6
    },
    'Airbus A330-200': {
        'capacity': 246,
        'fuel_capacity_gallons': 36135,
        'fuel_efficiency_mpg': 0.82,
        'range_miles': 7250,
        'cruise_speed_mph': 586,
        'length_ft': 193.6,
        'wingspan_ft': 197.8,
        'height_ft': 58.8,
        'max_takeoff_weight_lbs': 507060,
        'engine_type': 'Trent 700',
        'engine_count': 2,
        'manufacturing_cost_millions': 216.1
    },
    'Embraer E190': {
        'capacity': 106,
        'fuel_capacity_gallons': 3450,
        'fuel_efficiency_mpg': 0.48,
        'range_miles': 2400,
        'cruise_speed_mph': 541,
        'length_ft': 118.9,
        'wingspan_ft': 94.3,
        'height_ft': 34.7,
        'max_takeoff_weight_lbs': 114200,
        'engine_type': 'CF34-10E',
        'engine_count': 2,
        'manufacturing_cost_millions': 41.5
    },
    'Bombardier CRJ900': {
        'capacity': 90,
        'fuel_capacity_gallons': 2880,
        'fuel_efficiency_mpg': 0.42,
        'range_miles': 1720,
        'cruise_speed_mph': 528,
        'length_ft': 119.2,
        'wingspan_ft': 81.5,
        'height_ft': 24.7,
        'max_takeoff_weight_lbs': 84500,
        'engine_type': 'CF34-8C5',
        'engine_count': 2,
        'manufacturing_cost_millions': 36.5
    }
}

# Airline names for registration prefixes
AIRLINE_PREFIXES = {
    'South Africa': 'ZS-',
    'United States': 'N',
    'United Kingdom': 'G-',
    'Canada': 'C-',
    'Germany': 'D-',
    'France': 'F-',
    'Australia': 'VH-',
    'Brazil': 'PP-',
    'Japan': 'JA'
}

def generate_tail_number(aircraft_model):
    """Generate a realistic tail number based on aircraft model and airline."""
    country = random.choice(list(AIRLINE_PREFIXES.keys()))
    prefix = AIRLINE_PREFIXES[country]
    
    if 'Boeing' in aircraft_model:
        suffix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2)) + str(random.randint(100, 999))
    elif 'Airbus' in aircraft_model:
        suffix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=1)) + str(random.randint(1000, 9999))
    elif 'Embraer' in aircraft_model:
        suffix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2)) + str(random.randint(10, 99))
    else:  # Bombardier
        suffix = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=1)) + str(random.randint(100, 999))
    
    return f"{prefix}{suffix}"

def generate_plane_name(tail_number, aircraft_model):
    """Generate a creative name for the plane."""
    names = [
        "Sky Eagle", "Ocean Voyager", "Mountain Explorer", "Desert Wind", 
        "City Hopper", "Star Gazer", "Sun Seeker", "Moon Runner",
        "Thunder Bird", "Silver Arrow", "Golden Wing", "Blue Horizon",
        "Red Phoenix", "Green Dragon", "White Cloud", "Black Panther"
    ]
    return f"{random.choice(names)} ({tail_number})"

def generate_maintenance_schedule(date_added):
    """Generate maintenance schedule for the aircraft."""
    next_a_check = date_added + timedelta(days=random.randint(30, 60))
    next_b_check = next_a_check + timedelta(days=random.randint(180, 240))
    next_c_check = next_b_check + timedelta(days=random.randint(540, 720))
    next_d_check = next_c_check + timedelta(days=random.randint(1800, 2400))
    
    return {
        'next_a_check': next_a_check,
        'next_b_check': next_b_check,
        'next_c_check': next_c_check,
        'next_d_check': next_d_check
    }

def generate_aircraft_data():
    """Generate aircraft fleet data for the target year."""
    data = []
    
    # Select aircraft models for the fleet (mix of different types)
    selected_models = random.choices(
        list(AIRCRAFT_MODELS.keys()),
        weights=[0.3, 0.3, 0.1, 0.1, 0.1, 0.1],  # Higher probability for 737 and A320
        k=NUM_PLANES
    )
    
    for plane_id in tqdm(range(1, NUM_PLANES + 1), desc="Generating aircraft data"):
        aircraft_model = selected_models[plane_id - 1]
        specs = AIRCRAFT_MODELS[aircraft_model]
        
        # Generate registration details
        tail_number = generate_tail_number(aircraft_model)
        plane_name = generate_plane_name(tail_number, aircraft_model)
        
        # Date added (purchased in the target year)
        date_added = date(TARGET_YEAR, random.randint(1, 12), random.randint(1, 28))
        
        # Maintenance schedule
        maintenance = generate_maintenance_schedule(date_added)
        
        # Additional metrics
        current_flight_hours = random.randint(0, 500)
        total_cycles = random.randint(0, 300)
        operational_status = random.choices(
            ['Active', 'Maintenance', 'Standby'],
            weights=[0.85, 0.1, 0.05]
        )[0]
        
        # Fuel consumption metrics
        avg_fuel_consumption_ph = specs['fuel_capacity_gallons'] / (specs['range_miles'] / specs['cruise_speed_mph'])
        
        plane_data = {
            'plane_id': f'FL{TARGET_YEAR}{plane_id:04d}',
            'tail_number': tail_number,
            'plane_name': plane_name,
            'aircraft_model': aircraft_model,
            'date_added': date_added,
            'capacity': specs['capacity'],
            'fuel_capacity_gallons': specs['fuel_capacity_gallons'],
            'fuel_efficiency_mpg': specs['fuel_efficiency_mpg'],
            'range_miles': specs['range_miles'],
            'cruise_speed_mph': specs['cruise_speed_mph'],
            'length_ft': specs['length_ft'],
            'wingspan_ft': specs['wingspan_ft'],
            'height_ft': specs['height_ft'],
            'max_takeoff_weight_lbs': specs['max_takeoff_weight_lbs'],
            'engine_type': specs['engine_type'],
            'engine_count': specs['engine_count'],
            'manufacturing_cost_millions': specs['manufacturing_cost_millions'],
            'current_flight_hours': current_flight_hours,
            'total_cycles': total_cycles,
            'operational_status': operational_status,
            'avg_fuel_consumption_ph': round(avg_fuel_consumption_ph, 2),
            'next_a_check': maintenance['next_a_check'],
            'next_b_check': maintenance['next_b_check'],
            'next_c_check': maintenance['next_c_check'],
            'next_d_check': maintenance['next_d_check']
        }
        
        data.append(plane_data)
    
    return pd.DataFrame(data)

def generate_planes_dataset():
    """Main function to generate and save the planes dataset."""
    print(f"Generating aircraft fleet data for {TARGET_YEAR}...")
    print(f"Number of planes: {NUM_PLANES}")
    
    # Generate the data
    planes_df = generate_aircraft_data()
    
    # Save to parquet
    os.makedirs('airplane_data', exist_ok=True)
    output_file = f'airplane_data/planes_{TARGET_YEAR}.parquet'
    planes_df.to_parquet(output_file, index=False)
    
    print(f"Saved {len(planes_df)} aircraft records to {output_file}")
    
    # Display summary
    print("\nFleet Summary:")
    print("=" * 50)
    model_counts = planes_df['aircraft_model'].value_counts()
    for model, count in model_counts.items():
        print(f"{model}: {count} aircraft")
    
    total_capacity = planes_df['capacity'].sum()
    print(f"\nTotal passenger capacity: {total_capacity} seats")
    print(f"Total fleet value: ${planes_df['manufacturing_cost_millions'].sum():.1f} million")
    
    return planes_df

# Generate the dataset
if __name__ == "__main__":
    planes_data = generate_planes_dataset()
    
    # Display sample data
    print("\nSample aircraft data:")
    print("=" * 50)
    print(planes_data[['plane_id', 'tail_number', 'plane_name', 'aircraft_model', 'capacity', 'date_added']].head().to_string(index=False))
