import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from tqdm import tqdm
import random
import os
import re

# Set random seeds for reproducibility
seed_bytes = os.urandom(4)
seed_int = int.from_bytes(seed_bytes, byteorder='big')
random.seed(seed_int)
np.random.seed(seed_int)

# Constants
TARGET_YEAR = 2021
BASE_YEAR = 2020
SA_AIRPORTS = ['JNB', 'CPT', 'DUR', 'PLZ', 'GRJ']

# Updated holidays data for 2021-2024
HOLIDAYS_DATA = {
    "events_affecting_flying": [
        {
            "name": "Easter 2021",
            "dates": {"Good Friday": "2021-04-02", "Family Day": "2021-04-05"},
            "description": "Easter holidays lead to increased domestic and international travel, with higher flight demand and potential delays around Johannesburg, Cape Town, and Durban airports."
        },
        {"name": "Durban July 2021", "date": "2021-07-03", "description": "Major horse racing event in Durban attracts thousands, causing flight surges to King Shaka International Airport and traffic disruptions."},
        {
            "name": "Easter 2022",
            "dates": {"Good Friday": "2022-04-15", "Family Day": "2022-04-18"},
            "description": "Peak travel period for family reunions and vacations, impacting major routes with elevated air traffic."
        },
        {"name": "Durban July 2022", "date": "2022-07-02", "description": "High-profile social and racing event boosting travel to Durban, with increased flight bookings and potential congestion."},
        {
            "name": "Easter 2023",
            "dates": {"Good Friday": "2023-04-07", "Family Day": "2023-04-10"},
            "description": "Holiday weekend drives up passenger volumes on flights, especially to coastal and safari destinations."
        },
        {"name": "Durban July 2023", "date": "2023-07-01", "description": "Africa's premier horse race event, leading to flight demand spikes for Durban and surrounding areas."},
        {
            "name": "Easter 2024",
            "dates": {"Good Friday": "2024-03-29", "Family Day": "2024-04-01"},
            "description": "Early Easter causes early-year travel rush, affecting schedules at key airports like O.R. Tambo."
        },
        {"name": "Durban July 2024", "date": "2024-07-06", "description": "Event draws national and international visitors, increasing air travel to KwaZulu-Natal."}
    ],
    "note": "These events, including Easter and the Durban July, increase air travel demand in South Africa, leading to busier airports, higher fares, and potential disruptions."
}

# Airport cost tiers (base prices in ZAR)
AIRPORT_TIERS = {
    'JNB': {'tier': 1, 'base_price': 650, 'min_price': 500, 'max_price': 1200},
    'CPT': {'tier': 1, 'base_price': 700, 'min_price': 550, 'max_price': 1300},
    'DUR': {'tier': 1, 'base_price': 600, 'min_price': 450, 'max_price': 1100},
    'PLZ': {'tier': 2, 'base_price': 450, 'min_price': 350, 'max_price': 900},
    'GRJ': {'tier': 2, 'base_price': 400, 'min_price': 300, 'max_price': 800}
}

# Peak hours
PEAK_HOURS = [(6, 9), (16, 19)]

# Seasonal rescheduling reasons
SEASONAL_RESCHEDULING_REASONS = {
    'summer': {'Thunderstorms': 0.50, 'Mechanical Issues': 0.20, 'Crew Availability': 0.15, 'Air Traffic Control': 0.10, 'Operational Issues': 0.05},
    'winter': {'Fog/Low Visibility': 0.50, 'Mechanical Issues': 0.20, 'Crew Availability': 0.15, 'Air Traffic Control': 0.10, 'Operational Issues': 0.05},
    'shoulder': {'Mechanical Issues': 0.30, 'Crew Availability': 0.25, 'Air Traffic Control': 0.20, 'Operational Issues': 0.15, 'Thunderstorms': 0.05, 'Fog/Low Visibility': 0.05}
}

# Delay distributions
DELAY_DISTRIBUTIONS = {
    'on_time': (0.70, 0, 5),
    'minor': (0.20, 5, 30),
    'moderate': (0.08, 30, 120),
    'major': (0.02, 120, 360)
}

# Fixed flight times for popular routes
POPULAR_FLIGHT_TIMES = {
    ('JNB', 'CPT'): [6, 7, 8, 9, 12, 15, 16, 17, 18, 19, 20],
    ('CPT', 'JNB'): [6, 7, 8, 9, 12, 15, 16, 17, 18, 19, 20],
    ('JNB', 'DUR'): [6, 7, 8, 9, 12, 14, 16, 18, 19],
    ('DUR', 'JNB'): [6, 7, 8, 9, 12, 14, 16, 18, 19]
}

# Fixed flight times for other routes
OTHER_FLIGHT_TIMES = [6, 8, 12, 16, 20]

def introduce_typo(text, error_rate=0.02):
    """Introduce random typos in text (e.g., swap letters)."""
    if not text or random.random() > error_rate:
        return text
    chars = list(text)
    if len(chars) < 2:
        return text
    idx = random.randint(0, len(chars) - 2)
    chars[idx], chars[idx + 1] = chars[idx + 1], chars[idx]
    return ''.join(chars)

def get_season(flight_date):
    """Determine season based on flight date."""
    month = flight_date.month
    if month in [12, 1, 2]:
        return 'summer'
    elif month in [6, 7, 8]:
        return 'winter'
    else:
        return 'shoulder'

def load_datasets():
    """Load and concatenate planes and routes datasets for all years from BASE_YEAR to TARGET_YEAR."""
    planes_dfs = []
    routes_dfs = []
    
    for yr in range(BASE_YEAR, TARGET_YEAR + 1):
        print(f"Loading data for year {yr}...")
        planes_file = f'airplane_data/planes_{yr}.parquet'
        if os.path.exists(planes_file):
            planes_df = pd.read_parquet(planes_file)
            planes_dfs.append(planes_df)
            print(f"Loaded {len(planes_df)} plane records for {yr}")
        else:
            print(f"Planes file for {yr} not found: {planes_file}")
        
        routes_file = f'airplane_data/routes_{yr}.parquet'
        if os.path.exists(routes_file):
            routes_df = pd.read_parquet(routes_file)
            if 'date_effective' in routes_df.columns and not pd.api.types.is_datetime64_any_dtype(routes_df['date_effective']):
                routes_df['date_effective'] = pd.to_datetime(routes_df['date_effective'])
            routes_dfs.append(routes_df)
            print(f"Loaded {len(routes_df)} route records for {yr}")
        else:
            print(f"Routes file for {yr} not found: {routes_file}")
    
    if planes_dfs:
        combined_planes = pd.concat(planes_dfs, ignore_index=True)
        print(f"Combined {len(combined_planes)} plane records")
    else:
        print("No planes data found to concatenate")
        combined_planes = None
    
    if routes_dfs:
        combined_routes = pd.concat(routes_dfs, ignore_index=True)
        combined_routes = combined_routes.sort_values('date_effective').drop_duplicates(
            subset=['route_pair_id', 'origin_airport', 'destination_airport'], keep='last'
        )
        # Error: 2% chance of typo in airport codes
        for col in ['origin_airport', 'destination_airport']:
            combined_routes[col] = combined_routes[col].apply(lambda x: introduce_typo(x) if random.random() < 0.02 else x)
        print(f"Combined {len(combined_routes)} route records")
    else:
        print("No routes data found to concatenate")
        combined_routes = None
    
    return combined_planes, combined_routes

def is_peak_time(departure_time):
    """Check if departure time is during peak hours."""
    hour = departure_time.hour if isinstance(departure_time, datetime) else departure_time
    return any(start <= hour < end for start, end in PEAK_HOURS)

def is_holiday_or_event(flight_date):
    """Check if flight date is during a holiday or special event."""
    date_str = flight_date.strftime('%Y-%m-%d')
    for event in HOLIDAYS_DATA['events_affecting_flying']:
        if 'dates' in event:
            if date_str in event['dates'].values():
                return True
        elif event.get('date') == date_str:
            return True
    return False

def calculate_dynamic_price(base_price, flight_date, departure_time, demand_factor=1.0):
    """Calculate dynamic pricing."""
    price = base_price
    if is_peak_time(departure_time):
        price *= random.uniform(1.15, 1.30)
    if is_holiday_or_event(flight_date):
        price *= random.uniform(1.20, 1.50)
    if flight_date.month in [11, 12, 1, 2]:
        price *= random.uniform(1.10, 1.25)
    price *= demand_factor
    price *= random.uniform(0.95, 1.05)
    # Error: 2% chance of missing price
    if random.random() < 0.02:
        return None
    return round(price)

def generate_delay():
    """Generate delay based on statistical distribution."""
    delay_type = random.choices(
        list(DELAY_DISTRIBUTIONS.keys()),
        weights=[prob for prob, _, _ in DELAY_DISTRIBUTIONS.values()]
    )[0]
    prob, min_delay, max_delay = DELAY_DISTRIBUTIONS[delay_type]
    delay = random.randint(min_delay, max_delay)
    return delay

def generate_rescheduling_reason(flight_date):
    """Generate seasonal rescheduling reason."""
    season = get_season(flight_date)
    reasons = SEASONAL_RESCHEDULING_REASONS[season]
    # Error: 2% chance of non-seasonal reason
    if random.random() < 0.02:
        reasons = SEASONAL_RESCHEDULING_REASONS[random.choice(['summer', 'winter', 'shoulder'])]
    return random.choices(list(reasons.keys()), weights=list(reasons.values()))[0]

def generate_flight_schedule(planes_df, routes_df):
    """Generate a complete flight schedule for the year."""
    flights = []
    flight_id = 1
    
    available_routes = routes_df[routes_df['date_effective'].dt.year == TARGET_YEAR] if 'date_effective' in routes_df.columns else routes_df
    popular_routes = available_routes[
        ((available_routes['origin_airport'] == 'JNB') & (available_routes['destination_airport'].isin(['CPT', 'DUR']))) |
        ((available_routes['origin_airport'] == 'CPT') & (available_routes['destination_airport'] == 'JNB')) |
        ((available_routes['origin_airport'] == 'DUR') & (available_routes['destination_airport'] == 'JNB'))
    ]
    other_routes = available_routes[~available_routes.index.isin(popular_routes.index)]
    
    print(f"Generating flight schedule for {TARGET_YEAR}...")
    print(f"Popular routes: {len(popular_routes)}, Other routes: {len(other_routes)}")
    
    aircraft_status = {
        plane_id: {'location': 'JNB', 'last_arrival': None}
        for plane_id in planes_df['plane_id']
    }
    
    date_range = pd.date_range(date(TARGET_YEAR, 1, 1), date(TARGET_YEAR, 12, 31))
    
    for current_date in tqdm(date_range, desc="Generating daily schedules"):
        # Process popular routes
        for _, route in popular_routes.iterrows():
            origin = route['origin_airport']
            destination = route['destination_airport']
            flight_times = POPULAR_FLIGHT_TIMES.get((origin, destination), [8, 12, 16])
            
            for hour in flight_times:
                scheduled_departure = datetime.combine(current_date, datetime.min.time()).replace(hour=hour)
                # Error: 1% chance of string datetime
                if random.random() < 0.01:
                    scheduled_departure = pd.to_datetime(scheduled_departure.strftime('%Y-%m-%d %H:%M:%S'))
                
                available_planes = [
                    plane_id for plane_id, status in aircraft_status.items()
                    if status['location'] == origin and 
                    (status['last_arrival'] is None or status['last_arrival'] + timedelta(minutes=45) <= scheduled_departure)
                ]
                
                if not available_planes:
                    continue
                
                plane_id = random.choice(available_planes)
                
                scheduled_arrival = scheduled_departure + timedelta(minutes=route['estimated_duration_min']) if isinstance(scheduled_departure, datetime) else None
                delay_minutes = generate_delay()
                actual_departure = (scheduled_departure + timedelta(minutes=delay_minutes)) if isinstance(scheduled_departure, datetime) else None
                actual_arrival = (actual_departure + timedelta(minutes=route['estimated_duration_min'])) if isinstance(actual_departure, datetime) else None
                
                # Rescheduling logic
                is_rescheduled = delay_minutes > 120 and random.random() < 0.02
                rescheduling_reason = generate_rescheduling_reason(current_date) if is_rescheduled else None
                
                # Handle Mechanical Issues
                if is_rescheduled and rescheduling_reason == "Mechanical Issues":
                    # Try to find another plane
                    available_planes = [
                        p_id for p_id, status in aircraft_status.items()
                        if p_id != plane_id and  # Exclude current plane
                        status['location'] == origin and
                        (status['last_arrival'] is None or status['last_arrival'] + timedelta(minutes=45) <= scheduled_departure)
                    ]
                    if available_planes and random.random() < 0.5:  # 50% chance to use another plane
                        plane_id = random.choice(available_planes)  # Assign new plane
                        rescheduled_departure = scheduled_departure + timedelta(hours=random.randint(1, 4))  # Shorter delay
                    else:
                        # Keep same plane with longer delay
                        rescheduled_departure = actual_departure + timedelta(hours=random.randint(4, 12)) if isinstance(actual_departure, datetime) else None
                else:
                    rescheduled_departure = actual_departure + timedelta(hours=random.randint(2, 12)) if is_rescheduled and isinstance(actual_departure, datetime) else None
                
                rescheduled_arrival = rescheduled_departure + timedelta(minutes=route['estimated_duration_min']) if rescheduled_departure else None
                
                # Error: 1% chance of future actual times
                if random.random() < 0.01 and isinstance(actual_departure, datetime):
                    actual_departure = actual_departure + timedelta(days=365)
                    actual_arrival = actual_arrival + timedelta(days=365)
                
                base_price = AIRPORT_TIERS.get(origin, AIRPORT_TIERS['JNB'])['base_price']
                final_price = calculate_dynamic_price(base_price, current_date, scheduled_departure, random.uniform(0.9, 1.3)) if isinstance(scheduled_departure, datetime) else None
                
                # Error: 1% chance of negative duration (not needed since duration_actual_min is dropped)
                
                # Error: 2% chance of invalid planning_id format
                planning_id = f'PLN{TARGET_YEAR}{flight_id:04d}'
                if random.random() < 0.02:
                    planning_id = f'PLN{flight_id:04d}' if random.choice([True, False]) else f'PLAN{TARGET_YEAR}{flight_id:03d}'
                
                flight_data = {
                    'planning_id': planning_id,
                    'route_id': route['route_id'],
                    'plane_id': plane_id,
                    'scheduled_departure': scheduled_departure,
                    'scheduled_arrival': scheduled_arrival,
                    'actual_departure': actual_departure,
                    'actual_arrival': actual_arrival,
                    'is_rescheduled': is_rescheduled,
                    'rescheduling_reason': rescheduling_reason,
                    'rescheduled_departure': rescheduled_departure,
                    'rescheduled_arrival': rescheduled_arrival,
                    'final_price_zar': final_price,
                }
                
                flights.append(flight_data)
                
                # Error: 1% chance of duplicate flight
                if random.random() < 0.01:
                    duplicate = flight_data.copy()
                    duplicate['planning_id'] = f'PLN{TARGET_YEAR}{flight_id:04d}_DUP'
                    duplicate['final_price_zar'] = final_price * random.uniform(0.9, 1.1) if final_price else None
                    flights.append(duplicate)
                
                # Update aircraft status
                if not is_rescheduled or (is_rescheduled and rescheduling_reason != "Mechanical Issues"):
                    aircraft_status[plane_id].update({
                        'location': destination,
                        'last_arrival': scheduled_arrival if isinstance(scheduled_arrival, datetime) else None
                    })
                elif is_rescheduled and rescheduling_reason == "Mechanical Issues" and available_planes:
                    aircraft_status[plane_id].update({
                        'location': destination,
                        'last_arrival': rescheduled_arrival if isinstance(rescheduled_arrival, datetime) else None
                    })
                
                flight_id += 1
        
        # Process other routes
        for _, route in other_routes.iterrows():
            origin = route['origin_airport']
            destination = route['destination_airport']
            
            for hour in OTHER_FLIGHT_TIMES:
                scheduled_departure = datetime.combine(current_date, datetime.min.time()).replace(hour=hour)
                # Error: 1% chance of string datetime
                if random.random() < 0.01:
                    scheduled_departure = pd.to_datetime(scheduled_departure.strftime('%Y-%m-%d %H:%M:%S'))
                
                available_planes = [
                    plane_id for plane_id, status in aircraft_status.items()
                    if status['location'] == origin and 
                    (status['last_arrival'] is None or status['last_arrival'] + timedelta(minutes=60) <= scheduled_departure)
                ]
                
                if not available_planes:
                    continue
                
                plane_id = random.choice(available_planes)
                
                scheduled_arrival = scheduled_departure + timedelta(minutes=route['estimated_duration_min']) if isinstance(scheduled_departure, datetime) else None
                delay_minutes = generate_delay()
                actual_departure = (scheduled_departure + timedelta(minutes=delay_minutes)) if isinstance(scheduled_departure, datetime) else None
                actual_arrival = (actual_departure + timedelta(minutes=route['estimated_duration_min'])) if isinstance(actual_departure, datetime) else None
                
                # Rescheduling logic
                is_rescheduled = delay_minutes > 120 and random.random() < 0.03
                rescheduling_reason = generate_rescheduling_reason(current_date) if is_rescheduled else None
                
                # Handle Mechanical Issues
                if is_rescheduled and rescheduling_reason == "Mechanical Issues":
                    available_planes = [
                        p_id for p_id, status in aircraft_status.items()
                        if p_id != plane_id and
                        status['location'] == origin and
                        (status['last_arrival'] is None or status['last_arrival'] + timedelta(minutes=60) <= scheduled_departure)
                    ]
                    if available_planes and random.random() < 0.5:
                        plane_id = random.choice(available_planes)
                        rescheduled_departure = scheduled_departure + timedelta(hours=random.randint(1, 4))
                    else:
                        rescheduled_departure = actual_departure + timedelta(hours=random.randint(4, 12)) if isinstance(actual_departure, datetime) else None
                else:
                    rescheduled_departure = actual_departure + timedelta(hours=random.randint(2, 12)) if is_rescheduled and isinstance(actual_departure, datetime) else None
                
                rescheduled_arrival = rescheduled_departure + timedelta(minutes=route['estimated_duration_min']) if rescheduled_departure else None
                
                # Error: 1% chance of future actual times
                if random.random() < 0.01 and isinstance(actual_departure, datetime):
                    actual_departure = actual_departure + timedelta(days=365)
                    actual_arrival = actual_arrival + timedelta(days=365)
                
                base_price = AIRPORT_TIERS.get(origin, AIRPORT_TIERS['JNB'])['base_price']
                final_price = calculate_dynamic_price(base_price, current_date, scheduled_departure, random.uniform(0.8, 1.2)) if isinstance(scheduled_departure, datetime) else None
                
                # Error: 2% chance of invalid planning_id format
                planning_id = f'PLN{TARGET_YEAR}{flight_id:04d}'
                if random.random() < 0.02:
                    planning_id = f'PLN{flight_id:04d}' if random.choice([True, False]) else f'PLAN{TARGET_YEAR}{flight_id:03d}'
                
                flight_data = {
                    'planning_id': planning_id,
                    'route_id': route['route_id'],
                    'plane_id': plane_id,
                    'scheduled_departure': scheduled_departure,
                    'scheduled_arrival': scheduled_arrival,
                    'actual_departure': actual_departure,
                    'actual_arrival': actual_arrival,
                    'is_rescheduled': is_rescheduled,
                    'rescheduling_reason': rescheduling_reason,
                    'rescheduled_departure': rescheduled_departure,
                    'rescheduled_arrival': rescheduled_arrival,
                    'final_price_zar': final_price,
                }
                
                flights.append(flight_data)
                
                # Error: 1% chance of duplicate flight
                if random.random() < 0.01:
                    duplicate = flight_data.copy()
                    duplicate['planning_id'] = f'PLN{TARGET_YEAR}{flight_id:04d}_DUP'
                    duplicate['final_price_zar'] = final_price * random.uniform(0.9, 1.1) if final_price else None
                    flights.append(duplicate)
                
                # Update aircraft status
                if not is_rescheduled or (is_rescheduled and rescheduling_reason != "Mechanical Issues"):
                    aircraft_status[plane_id].update({
                        'location': destination,
                        'last_arrival': scheduled_arrival if isinstance(scheduled_arrival, datetime) else None
                    })
                elif is_rescheduled and rescheduling_reason == "Mechanical Issues" and available_planes:
                    aircraft_status[plane_id].update({
                        'location': destination,
                        'last_arrival': rescheduled_arrival if isinstance(rescheduled_arrival, datetime) else None
                    })
                
                flight_id += 1
    
    return pd.DataFrame(flights)

def generate_flight_schedule_dataset():
    """Main function to generate and save the flight schedule."""
    print("Loading datasets...")
    planes_df, routes_df = load_datasets()
    
    if planes_df is None or routes_df is None:
        print("Failed to load datasets.")
        return None
    
    print("Generating flight schedule...")
    schedule_df = generate_flight_schedule(planes_df, routes_df)
    
    if schedule_df.empty:
        print("No flights were generated. Check your routes data.")
        return None
    
    # Drop specified columns
    schedule_df = schedule_df.drop(columns=['rescheduled_departure', 'rescheduled_arrival'])
    
    # Error: 3% chance of missing rescheduling reason
    schedule_df.loc[schedule_df['is_rescheduled'] & (np.random.random(len(schedule_df)) < 0.03), 'rescheduling_reason'] = None
    
    # Save to parquet
    os.makedirs('airplane_data', exist_ok=True)
    output_file = f'airplane_data/flight_schedule_{TARGET_YEAR}.parquet'
    schedule_df.to_parquet(output_file, index=False)
    
    print(f"Saved {len(schedule_df)} flight records to {output_file}")
    
    # Display summary
    print("\nFlight Schedule Summary:")
    print("=" * 50)
    print(f"Total flights: {len(schedule_df):,}")
    print(f"Rescheduled flights: {schedule_df['is_rescheduled'].sum():,} ({schedule_df['is_rescheduled'].mean()*100:.1f}%)")
    print(f"Average final price: R{schedule_df['final_price_zar'].mean():.0f}")
    
    if not schedule_df[schedule_df['is_rescheduled']].empty:
        print("\nRescheduling Reasons:")
        for reason, count in schedule_df[schedule_df['is_rescheduled']]['rescheduling_reason'].value_counts(dropna=False).items():
            print(f"  {reason if reason else 'None'}: {count}")
    
    # Display sample data
    print("\nSample flight data:")
    print("=" * 60)
    print(schedule_df.head(10).to_string(index=False))
    
    return schedule_df

if __name__ == "__main__":
    schedule_data = generate_flight_schedule_dataset()