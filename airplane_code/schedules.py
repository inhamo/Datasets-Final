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
HUB_AIRPORTS = ['JNB', 'CPT', 'NBO', 'LHR', 'DXB', 'JFK', 'SYD', 'FRA', 'CDG', 'HKG']

# Define regions based on countries
COUNTRY_TO_REGION = {
    'South Africa': 'Africa',
    'Zimbabwe': 'Africa',
    'Kenya': 'Africa',
    'Nigeria': 'Africa',
    'United Kingdom': 'Europe',
    'Germany': 'Europe',
    'France': 'Europe',
    'UAE': 'Middle East',
    'USA': 'North America',
    'China': 'Asia',
    'Australia': 'Australia'
}

# Updated holidays data for 2021-2024
HOLIDAYS_DATA = {
    "events_affecting_flying": [
        {"name": "Easter 2021", "dates": {"Good Friday": "2021-04-02", "Family Day": "2021-04-05"}, "description": "Easter holidays lead to increased domestic and international travel."},
        {"name": "Durban July 2021", "date": "2021-07-03", "description": "Major horse racing event in Durban attracts thousands."},
        {"name": "Easter 2022", "dates": {"Good Friday": "2022-04-15", "Family Day": "2022-04-18"}, "description": "Peak travel period for family reunions."},
        {"name": "Durban July 2022", "date": "2022-07-02", "description": "High-profile social and racing event."},
        {"name": "Easter 2023", "dates": {"Good Friday": "2023-04-07", "Family Day": "2023-04-10"}, "description": "Holiday weekend drives up passenger volumes."},
        {"name": "Durban July 2023", "date": "2023-07-01", "description": "Africa's premier horse race event."},
        {"name": "Easter 2024", "dates": {"Good Friday": "2024-03-29", "Family Day": "2024-04-01"}, "description": "Early Easter causes early-year travel rush."},
        {"name": "Durban July 2024", "date": "2024-07-06", "description": "Event draws national and international visitors."}
    ],
    "note": "These events increase air travel demand in South Africa, leading to busier airports."
}

# Airport cost tiers (base prices in ZAR, updated for realism)
AIRPORT_TIERS = {
    'JNB': {'tier': 1, 'base_price': 600, 'min_price': 450, 'max_price': 1000},
    'CPT': {'tier': 1, 'base_price': 650, 'min_price': 500, 'max_price': 1100},
    'DUR': {'tier': 1, 'base_price': 550, 'min_price': 400, 'max_price': 950},
    'PLZ': {'tier': 2, 'base_price': 400, 'min_price': 300, 'max_price': 800},
    'GRJ': {'tier': 2, 'base_price': 350, 'min_price': 250, 'max_price': 700},
    'HRE': {'tier': 2, 'base_price': 450, 'min_price': 350, 'max_price': 900},
    'NBO': {'tier': 1, 'base_price': 700, 'min_price': 550, 'max_price': 1300},
    'LOS': {'tier': 1, 'base_price': 650, 'min_price': 500, 'max_price': 1200},
    'LHR': {'tier': 1, 'base_price': 800, 'min_price': 650, 'max_price': 1600},
    'DXB': {'tier': 1, 'base_price': 750, 'min_price': 600, 'max_price': 1500},
    'JFK': {'tier': 1, 'base_price': 850, 'min_price': 700, 'max_price': 1700},
    'SYD': {'tier': 1, 'base_price': 900, 'min_price': 750, 'max_price': 1800},
    'FRA': {'tier': 1, 'base_price': 750, 'min_price': 600, 'max_price': 1500},
    'CDG': {'tier': 1, 'base_price': 750, 'min_price': 600, 'max_price': 1500},
    'HKG': {'tier': 1, 'base_price': 800, 'min_price': 650, 'max_price': 1600}
}

# Aircraft types with range and turn times (updated to include Airbus A320-200)
AIRCRAFT_TYPES = {
    'Boeing 737-800': {'max_range_km': 5400, 'turn_time_min': 45},
    'Airbus A320-200': {'max_range_km': 6100, 'turn_time_min': 45},
    'Boeing 787-9': {'max_range_km': 14140, 'turn_time_min': 60},
    'Airbus A350-900': {'max_range_km': 15000, 'turn_time_min': 60}
}

# Peak hours and wave scheduling for hubs
PEAK_HOURS = [(6, 9), (16, 19)]
WAVE_SCHEDULES = {
    'JNB': [(6, 7), (12, 13), (16, 17)],
    'CPT': [(7, 8), (13, 14), (17, 18)],
    'NBO': [(8, 9), (14, 15), (18, 19)],
    'LHR': [(6, 8), (12, 14), (18, 20)],
    'DXB': [(7, 9), (13, 15), (19, 21)],
    'JFK': [(6, 8), (12, 14), (18, 20)],
    'SYD': [(7, 9), (13, 15), (19, 21)],
    'FRA': [(6, 8), (12, 14), (18, 20)],
    'CDG': [(6, 8), (12, 14), (18, 20)],
    'HKG': [(7, 9), (13, 15), (19, 21)]
}

# Seasonal rescheduling reasons
SEASONAL_RESCHEDULING_REASONS = {
    'summer': {'Thunderstorms': 0.50, 'Mechanical Issues': 0.20, 'Crew Timeout': 0.15, 'Air Traffic Control': 0.10, 'Operational Issues': 0.05},
    'winter': {'Fog/Low Visibility': 0.50, 'Mechanical Issues': 0.20, 'Crew Timeout': 0.15, 'Air Traffic Control': 0.10, 'Operational Issues': 0.05},
    'shoulder': {'Mechanical Issues': 0.30, 'Crew Timeout': 0.25, 'Air Traffic Control': 0.20, 'Operational Issues': 0.15, 'Thunderstorms': 0.05}
}

# Delay distributions
DELAY_DISTRIBUTIONS = {
    'on_time': (0.70, 0, 5),
    'minor': (0.20, 5, 30),
    'moderate': (0.08, 30, 120),
    'major': (0.02, 120, 360)
}

# Weather event regions
WEATHER_REGIONS = {
    'Africa': ['JNB', 'CPT', 'DUR', 'PLZ', 'GRJ', 'HRE', 'NBO', 'LOS'],
    'Europe': ['LHR', 'FRA', 'CDG'],
    'North America': ['JFK'],
    'Asia': ['HKG'],
    'Australia': ['SYD'],
    'Middle East': ['DXB']
}

# Direct flight routes (example: HRE to CPT via JNB)
DIRECT_FLIGHT_ROUTES = {
    ('HRE', 'CPT'): {'intermediate': 'JNB', 'stop_duration_min': 45},
    ('CPT', 'HRE'): {'intermediate': 'JNB', 'stop_duration_min': 45},
    ('HRE', 'DUR'): {'intermediate': 'JNB', 'stop_duration_min': 45},
    ('DUR', 'HRE'): {'intermediate': 'JNB', 'stop_duration_min': 45},
    ('NBO', 'CPT'): {'intermediate': 'JNB', 'stop_duration_min': 45},
    ('CPT', 'NBO'): {'intermediate': 'JNB', 'stop_duration_min': 45}
}

def introduce_typo(text, error_rate=0.02):
    """Introduce random typos in text."""
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
    """Load and concatenate planes and routes datasets."""
    planes_dfs = []
    routes_dfs = []
    
    for yr in range(BASE_YEAR, TARGET_YEAR + 1):
        print(f"Loading data for year {yr}...")
        planes_file = f'airplane_data/planes_{yr}.parquet'
        if os.path.exists(planes_file):
            planes_df = pd.read_parquet(planes_file)
            planes_dfs.append(planes_df)
        else:
            print(f"Planes file for {yr} not found: {planes_file}. Creating a sample DataFrame.")
            planes_df = pd.DataFrame({
                'plane_id': [f'PLN{yr}{i:03d}' for i in range(1, 11)],
                'model': [random.choice(list(AIRCRAFT_TYPES.keys())) for _ in range(10)]
            })
            planes_df.to_parquet(planes_file)
            planes_dfs.append(planes_df)
        
        routes_file = f'airplane_data/routes_{yr}.parquet'
        if os.path.exists(routes_file):
            routes_df = pd.read_parquet(routes_file)
            if 'date_effective' in routes_df.columns and not pd.api.types.is_datetime64_any_dtype(routes_df['date_effective']):
                routes_df['date_effective'] = pd.to_datetime(routes_df['date_effective'])
            routes_dfs.append(routes_df)
        else:
            print(f"Routes file for {yr} not found: {routes_file}")
    
    if planes_dfs:
        combined_planes = pd.concat(planes_dfs, ignore_index=True)
        if 'model' not in combined_planes.columns:
            print("Warning: 'model' column not found in planes_df. Assigning random aircraft models.")
            combined_planes['model'] = [random.choice(list(AIRCRAFT_TYPES.keys())) for _ in range(len(combined_planes))]
        print(f"Combined {len(combined_planes)} plane records")
        print("Columns in planes_df:", combined_planes.columns.tolist())
    else:
        print("No planes data found to concatenate")
        combined_planes = None
    
    if routes_dfs:
        combined_routes = pd.concat(routes_dfs, ignore_index=True)
        combined_routes = combined_routes.sort_values('date_effective').drop_duplicates(
            subset=['route_pair_id', 'origin_airport', 'destination_airport'], keep='last'
        )
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

def calculate_dynamic_price(base_price, flight_date, departure_time, load_factor, distance_km, is_direct=False):
    """Calculate dynamic pricing based on demand, season, load factor, and flight type."""
    price = base_price
    if is_peak_time(departure_time):
        price *= random.uniform(1.15, 1.30)
    if is_holiday_or_event(flight_date):
        price *= random.uniform(1.20, 1.50)
    if flight_date.month in [11, 12, 1, 2]:
        price *= random.uniform(1.10, 1.25)
    price *= (0.8 + 0.4 * load_factor)
    price *= (1 + distance_km / 10000)
    if is_direct:
        price *= random.uniform(1.05, 1.15)
    price *= random.uniform(0.95, 1.05)
    if random.random() < 0.02:
        return None
    return max(round(price), AIRPORT_TIERS.get('JNB')['min_price'])

def generate_delay(flight_date, origin, region_weather_events, is_direct=False):
    """Generate delay considering regional weather events and direct flights."""
    if is_direct:
        delay = random.randint(15, 60)
    else:
        delay = 0
    if origin in region_weather_events:
        delay += random.randint(30, 240)
    else:
        delay_type = random.choices(
            list(DELAY_DISTRIBUTIONS.keys()),
            weights=[prob for prob, _, _ in DELAY_DISTRIBUTIONS.values()]
        )[0]
        _, min_delay, max_delay = DELAY_DISTRIBUTIONS[delay_type]
        delay += random.randint(min_delay, max_delay)
    return delay

def generate_rescheduling_reason(flight_date, delay_minutes):
    """Generate seasonal rescheduling reason or cancellation."""
    season = get_season(flight_date)
    if delay_minutes > 360:
        return 'Cancelled - Crew Timeout'
    reasons = SEASONAL_RESCHEDULING_REASONS[season]
    if random.random() < 0.02:
        reasons = SEASONAL_RESCHEDULING_REASONS[random.choice(['summer', 'winter', 'shoulder'])]
    return random.choices(list(reasons.keys()), weights=list(reasons.values()))[0]

def generate_positioning_flight(aircraft_status, plane_id, current_location, target_location, current_date, flight_id, routes_df):
    """Generate a positioning flight to move an aircraft."""
    route = routes_df[(routes_df['origin_airport'] == current_location) & 
                     (routes_df['destination_airport'] == target_location) & 
                     (routes_df['is_operational'] == True)]
    if route.empty:
        return None, flight_id
    route = route.iloc[0]
    
    # Use standard times (0, 10, 30, 45 minutes)
    minute_options = [0, 10, 30, 45]
    scheduled_departure = datetime.combine(current_date, datetime.min.time()).replace(hour=random.randint(0, 23), minute=random.choice(minute_options))
    scheduled_arrival = scheduled_departure + timedelta(minutes=int(route['estimated_duration_min']))
    planning_id = f'POS{TARGET_YEAR}{flight_id:04d}'
    
    flight_data = {
        'planning_id': planning_id,
        'route_id': route['route_id'],
        'plane_id': plane_id,
        'scheduled_departure': scheduled_departure,
        'scheduled_arrival': scheduled_arrival,
        'actual_departure': scheduled_departure,
        'actual_arrival': scheduled_arrival,
        'is_rescheduled': False,
        'rescheduling_reason': None,
        'final_price_zar': None,
        'is_positioning': True,
        'is_connecting': route['is_connecting'],
        'load_factor': 0.0,
        'flight_type': 'non-stop',
        'intermediate_stop': None,
        'stop_duration_min': None
    }
    
    aircraft_status[plane_id].update({
        'location': target_location,
        'last_arrival': scheduled_arrival
    })
    return flight_data, flight_id + 1

def generate_flight_schedule(planes_df, routes_df):
    """Generate a complete flight schedule for the year."""
    if planes_df is None or planes_df.empty:
        print("Error: planes_df is empty or None")
        return pd.DataFrame()
    if 'plane_id' not in planes_df.columns or 'model' not in planes_df.columns:
        print("Error: planes_df is missing required columns 'plane_id' or 'model'")
        return pd.DataFrame()
    
    flights = []
    flight_id = 1
    slot_usage = {airport: {hour: 0 for hour in range(24)} for airport in routes_df['origin_airport'].unique()}
    SLOT_LIMITS = {airport: 10 if airport in HUB_AIRPORTS else 5 for airport in slot_usage}
    
    aircraft_status = {
        plane_id: {'location': 'JNB', 'last_arrival': None, 'flight_hours': 0, 'crew_id': f'CREW{plane_id}', 'last_maintenance': None}
        for plane_id in planes_df['plane_id']
    }
    
    date_range = pd.date_range(date(TARGET_YEAR, 1, 1), date(TARGET_YEAR, 12, 31))
    region_weather_events = {}
    
    for current_date in tqdm(date_range, desc="Generating daily schedules"):
        # Generate regional weather events
        for region, airports in WEATHER_REGIONS.items():
            if random.random() < 0.02:
                region_weather_events[region] = airports
            else:
                region_weather_events[region] = []
        
        available_routes = routes_df[(routes_df['date_effective'].dt.year <= TARGET_YEAR) & 
                                    (routes_df['is_operational'] == True)]
        popular_routes = available_routes[
            ((available_routes['origin_airport'].isin(['JNB', 'CPT', 'DUR'])) & 
             (available_routes['destination_airport'].isin(['JNB', 'CPT', 'DUR']))) |
            (available_routes['is_connecting'] == True)
        ]
        other_routes = available_routes[~available_routes.index.isin(popular_routes.index)]
        
        # Process hub routes with wave scheduling
        for hub in HUB_AIRPORTS:
            hub_routes = available_routes[available_routes['origin_airport'] == hub]
            if hub_routes.empty:
                continue
            for wave_start, wave_end in WAVE_SCHEDULES.get(hub, PEAK_HOURS):
                for _, route in hub_routes.sample(frac=1).iterrows():
                    origin = route['origin_airport']
                    destination = route['destination_airport']
                    distance_km = route['distance_km']
                    is_connecting = route['is_connecting']
                    is_direct = (origin, destination) in DIRECT_FLIGHT_ROUTES and not is_connecting
                    flight_type = 'non-stop' if route['origin_country'] == route['destination_country'] else ('direct' if is_direct else 'connecting' if is_connecting else 'non-stop')
                    intermediate_stop = DIRECT_FLIGHT_ROUTES.get((origin, destination), {}).get('intermediate')
                    stop_duration_min = DIRECT_FLIGHT_ROUTES.get((origin, destination), {}).get('stop_duration_min', 0)
                    
                    # Ensure intermediate_stop is not origin or destination
                    if intermediate_stop == origin or intermediate_stop == destination:
                        intermediate_stop = None
                        stop_duration_min = 0
                        is_direct = False
                        flight_type = 'non-stop' if route['origin_country'] == route['destination_country'] else 'connecting' if is_connecting else 'non-stop'
                    
                    plane_id = random.choice(planes_df['plane_id'].tolist())
                    plane_model = planes_df[planes_df['plane_id'] == plane_id]['model'].iloc[0]
                    if distance_km > AIRCRAFT_TYPES[plane_model]['max_range_km']:
                        continue
                    
                    hour = random.randint(wave_start, wave_end - 1)
                    if slot_usage[origin][hour] >= SLOT_LIMITS[origin]:
                        continue
                    slot_usage[origin][hour] += 1
                    if intermediate_stop:
                        slot_usage[intermediate_stop][hour + int(distance_km / 800)] += 1
                    
                    # Use standard times (0, 10, 30, 45 minutes)
                    minute_options = [0, 10, 30, 45]
                    scheduled_departure = datetime.combine(current_date, datetime.min.time()).replace(hour=hour, minute=random.choice(minute_options))
                    if random.random() < 0.01:
                        scheduled_departure = pd.to_datetime(scheduled_departure.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    available_planes = [
                        plane_id for plane_id, status in aircraft_status.items()
                        if status['location'] == origin and 
                        (status['last_arrival'] is None or status['last_arrival'] + timedelta(minutes=AIRCRAFT_TYPES[plane_model]['turn_time_min']) <= scheduled_departure) and
                        (status['last_maintenance'] is None or status['last_maintenance'] + timedelta(days=7) <= current_date)
                    ]
                    
                    if not available_planes:
                        plane_id = random.choice(list(aircraft_status.keys()))
                        pos_flight, flight_id = generate_positioning_flight(aircraft_status, plane_id, aircraft_status[plane_id]['location'], origin, current_date, flight_id, routes_df)
                        if pos_flight:
                            flights.append(pos_flight)
                        continue
                    
                    plane_id = random.choice(available_planes)
                    plane_model = planes_df[planes_df['plane_id'] == plane_id]['model'].iloc[0]
                    
                    total_duration_min = int(route['estimated_duration_min']) + stop_duration_min if is_direct else int(route['estimated_duration_min'])
                    scheduled_arrival = scheduled_departure + timedelta(minutes=int(route['estimated_duration_min']))
                    load_factor = random.uniform(0.5, 1.0) if is_connecting else random.uniform(0.3, 0.9)
                    delay_minutes = generate_delay(current_date, origin, region_weather_events.get(COUNTRY_TO_REGION.get(route['origin_country'], 'Other'), []), is_direct)
                    
                    actual_departure = (scheduled_departure + timedelta(minutes=delay_minutes)) if isinstance(scheduled_departure, datetime) else None
                    actual_arrival = (actual_departure + timedelta(minutes=total_duration_min)) if isinstance(actual_departure, datetime) else None
                    
                    is_rescheduled = delay_minutes > 120 and random.random() < 0.03
                    rescheduling_reason = generate_rescheduling_reason(current_date, delay_minutes) if is_rescheduled or delay_minutes > 360 else None
                    
                    if rescheduling_reason == "Cancelled - Crew Timeout":
                        continue
                    
                    if is_rescheduled and rescheduling_reason == "Mechanical Issues":
                        available_planes = [
                            p_id for p_id, status in aircraft_status.items()
                            if p_id != plane_id and
                            status['location'] == origin and
                            (status['last_arrival'] is None or status['last_arrival'] + timedelta(minutes=AIRCRAFT_TYPES[plane_model]['turn_time_min']) <= scheduled_departure)
                        ]
                        if available_planes and random.random() < 0.5:
                            plane_id = random.choice(available_planes)
                            rescheduled_departure = scheduled_departure + timedelta(hours=random.randint(1, 4))
                        else:
                            rescheduled_departure = actual_departure + timedelta(hours=random.randint(4, 12)) if isinstance(actual_departure, datetime) else None
                            aircraft_status[plane_id]['last_maintenance'] = current_date
                    else:
                        rescheduled_departure = actual_departure + timedelta(hours=random.randint(2, 12)) if is_rescheduled and isinstance(actual_departure, datetime) else None
                    
                    rescheduled_arrival = rescheduled_departure + timedelta(minutes=total_duration_min) if rescheduled_departure else None
                    
                    if random.random() < 0.01 and isinstance(actual_departure, datetime):
                        actual_departure = actual_departure + timedelta(days=365)
                        actual_arrival = actual_arrival + timedelta(days=365)
                    
                    base_price = AIRPORT_TIERS.get(origin, AIRPORT_TIERS['JNB'])['base_price']
                    final_price = calculate_dynamic_price(base_price, current_date, scheduled_departure, load_factor, distance_km, is_direct) if isinstance(scheduled_departure, datetime) else None
                    
                    planning_id = f'PLN{TARGET_YEAR}{flight_id:07d}'
                    if random.random() < 0.02:
                        planning_id = f'PLN{flight_id:04d}' if random.choice([True, False]) else f'PLAN{TARGET_YEAR}{flight_id:03d}'
                    
                    flight_data = {
                        'planning_id': planning_id,
                        'route_id': route['route_id'],
                        'plane_id': plane_id,
                        'scheduled_departure': scheduled_departure,
                        'scheduled_arrival': scheduled_arrival,
                        'actual_departure': scheduled_departure,
                        'actual_arrival': actual_arrival,
                        'is_rescheduled': is_rescheduled,
                        'rescheduling_reason': rescheduling_reason,
                        'rescheduled_departure': rescheduled_departure,
                        'rescheduled_arrival': rescheduled_arrival,
                        'final_price_zar': final_price,
                        'is_positioning': False,
                        'is_connecting': is_connecting,
                        'load_factor': round(load_factor, 2),
                        'flight_type': flight_type,
                        'intermediate_stop': intermediate_stop,
                        'stop_duration_min': stop_duration_min
                    }
                    
                    flights.append(flight_data)
                    
                    if random.random() < 0.01:
                        duplicate = flight_data.copy()
                        duplicate['planning_id'] = f'PLN{TARGET_YEAR}{flight_id:04d}_DUP'
                        duplicate['final_price_zar'] = final_price * random.uniform(0.9, 1.1) if final_price else None
                        flights.append(duplicate)
                    
                    if not is_rescheduled or (is_rescheduled and rescheduling_reason != "Mechanical Issues"):
                        aircraft_status[plane_id].update({
                            'location': destination,
                            'last_arrival': scheduled_arrival if isinstance(scheduled_arrival, datetime) else None,
                            'flight_hours': aircraft_status[plane_id]['flight_hours'] + total_duration_min / 60
                        })
                    elif is_rescheduled and rescheduling_reason == "Mechanical Issues" and available_planes:
                        aircraft_status[plane_id].update({
                            'location': destination,
                            'last_arrival': rescheduled_arrival if isinstance(rescheduled_arrival, datetime) else None,
                            'flight_hours': aircraft_status[plane_id]['flight_hours'] + total_duration_min / 60
                        })
                    
                    flight_id += 1
        
        # Schedule maintenance and overnight parking
        for plane_id, status in aircraft_status.items():
            if status['flight_hours'] > 500 and (status['last_maintenance'] is None or status['last_maintenance'] + timedelta(days=7) <= current_date):
                status['last_maintenance'] = current_date
                status['flight_hours'] = 0
                if status['location'] not in HUB_AIRPORTS:
                    pos_flight, flight_id = generate_positioning_flight(aircraft_status, plane_id, status['location'], random.choice(HUB_AIRPORTS), current_date, flight_id, routes_df)
                    if pos_flight:
                        flights.append(pos_flight)
        
        # Reset slot usage for next day
        slot_usage = {airport: {hour: 0 for hour in range(24)} for airport in slot_usage}
    
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
    
    schedule_df = schedule_df.drop(columns=['rescheduled_departure', 'rescheduled_arrival', 'is_positioning', 'load_factor'])
    schedule_df.loc[schedule_df['is_rescheduled'] & (np.random.random(len(schedule_df)) < 0.03), 'rescheduling_reason'] = None
    
    os.makedirs('airplane_data', exist_ok=True)
    output_file = f'airplane_data/flight_schedule_{TARGET_YEAR}.parquet'
    schedule_df.to_parquet(output_file, index=False)
    
    print(f"Saved {len(schedule_df)} flight records to {output_file}")
    
    print("\nFlight Schedule Summary:")
    print("=" * 50)
    print(f"Total flights: {len(schedule_df):,}")
    print(f"Connecting flights: {schedule_df['is_connecting'].sum():,}")
    print(f"Non-stop flights: {schedule_df[schedule_df['flight_type'] == 'non-stop'].shape[0]:,}")
    print(f"Direct flights: {schedule_df[schedule_df['flight_type'] == 'direct'].shape[0]:,}")
    print(f"Rescheduled flights: {schedule_df['is_rescheduled'].sum():,} ({schedule_df['is_rescheduled'].mean()*100:.1f}%)")
    print(f"Cancelled flights: {(schedule_df['rescheduling_reason'] == 'Cancelled - Crew Timeout').sum():,}")
    print(f"Average final price: R{schedule_df['final_price_zar'].mean():.0f}")
    
    print("\nRescheduling Reasons:")
    for reason, count in schedule_df[schedule_df['is_rescheduled'] | (schedule_df['rescheduling_reason'] == 'Cancelled - Crew Timeout')]['rescheduling_reason'].value_counts(dropna=False).items():
        print(f"  {reason if reason else 'None'}: {count}")
    
    print("\nFlight Type Distribution:")
    for flight_type, count in schedule_df['flight_type'].value_counts().items():
        print(f"  {flight_type}: {count}")
    
    print("\nSample flight data:")
    print("=" * 60)
    sample_cols = ['planning_id', 'route_id', 'plane_id', 'scheduled_departure', 'actual_departure', 'is_rescheduled', 'rescheduling_reason', 'final_price_zar', 'is_connecting', 'flight_type', 'intermediate_stop', 'stop_duration_min']
    print(schedule_df[sample_cols].head(10).to_string(index=False))
    
    return schedule_df

if __name__ == "__main__":
    schedule_data = generate_flight_schedule_dataset()
