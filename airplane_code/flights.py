import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from tqdm import tqdm
import random
import os
import glob

# Set random seeds for reproducibility
seed_bytes = os.urandom(4)
seed_int = int.from_bytes(seed_bytes, byteorder='big')
random.seed(seed_int)
np.random.seed(seed_int)

# Constants
BASE_YEAR = 2020
END_YEAR = 2024

# Airport database with detailed information
AIRPORTS = {
    # South Africa (Domestic)
    'JNB': {'name': 'O.R. Tambo International Airport', 'city': 'Johannesburg', 'country': 'South Africa', 'iata': 'JNB', 'latitude': -26.1392, 'longitude': 28.2460, 'is_hub': True},
    'CPT': {'name': 'Cape Town International Airport', 'city': 'Cape Town', 'country': 'South Africa', 'iata': 'CPT', 'latitude': -33.9648, 'longitude': 18.6017, 'is_hub': True},
    'DUR': {'name': 'King Shaka International Airport', 'city': 'Durban', 'country': 'South Africa', 'iata': 'DUR', 'latitude': -29.6145, 'longitude': 31.1198, 'is_hub': False},
    'PLZ': {'name': 'Port Elizabeth International Airport', 'city': 'Port Elizabeth', 'country': 'South Africa', 'iata': 'PLZ', 'latitude': -33.9849, 'longitude': 25.6173, 'is_hub': False},
    'GRJ': {'name': 'George Airport', 'city': 'George', 'country': 'South Africa', 'iata': 'GRJ', 'latitude': -34.0056, 'longitude': 22.3789, 'is_hub': False},
    
    # African destinations (for 2021+)
    'HRE': {'name': 'Robert Gabriel Mugabe International Airport', 'city': 'Harare', 'country': 'Zimbabwe', 'iata': 'HRE', 'latitude': -17.9318, 'longitude': 31.0928, 'is_hub': False},
    'NBO': {'name': 'Jomo Kenyatta International Airport', 'city': 'Nairobi', 'country': 'Kenya', 'iata': 'NBO', 'latitude': -1.3192, 'longitude': 36.9278, 'is_hub': True},
    'LOS': {'name': 'Murtala Muhammed International Airport', 'city': 'Lagos', 'country': 'Nigeria', 'iata': 'LOS', 'latitude': 6.5774, 'longitude': 3.3210, 'is_hub': True},
    
    # International destinations (for 2022+)
    'LHR': {'name': 'Heathrow Airport', 'city': 'London', 'country': 'United Kingdom', 'iata': 'LHR', 'latitude': 51.4700, 'longitude': -0.4543, 'is_hub': True},
    'DXB': {'name': 'Dubai International Airport', 'city': 'Dubai', 'country': 'UAE', 'iata': 'DXB', 'latitude': 25.2528, 'longitude': 55.3644, 'is_hub': True},
    'JFK': {'name': 'John F. Kennedy International Airport', 'city': 'New York', 'country': 'USA', 'iata': 'JFK', 'latitude': 40.6398, 'longitude': -73.7789, 'is_hub': True},
    'SYD': {'name': 'Sydney Kingsford Smith Airport', 'city': 'Sydney', 'country': 'Australia', 'iata': 'SYD', 'latitude': -33.9461, 'longitude': 151.1772, 'is_hub': True},
    'FRA': {'name': 'Frankfurt Airport', 'city': 'Frankfurt', 'country': 'Germany', 'iata': 'FRA', 'latitude': 50.0333, 'longitude': 8.5706, 'is_hub': True},
    'CDG': {'name': 'Charles de Gaulle Airport', 'city': 'Paris', 'country': 'France', 'iata': 'CDG', 'latitude': 49.0097, 'longitude': 2.5479, 'is_hub': True},
    'HKG': {'name': 'Hong Kong International Airport', 'city': 'Hong Kong', 'country': 'China', 'iata': 'HKG', 'latitude': 22.3080, 'longitude': 113.9185, 'is_hub': True}
}

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

# Known distances (km) and durations for key domestic routes
KNOWN_ROUTES = {
    ('JNB', 'CPT'): {'distance_km': 1264.4, 'duration_min': 105},
    ('CPT', 'JNB'): {'distance_km': 1264.4, 'duration_min': 105},
    ('JNB', 'DUR'): {'distance_km': 480.0, 'duration_min': 45},
    ('DUR', 'JNB'): {'distance_km': 480.0, 'duration_min': 45},
    ('CPT', 'DUR'): {'distance_km': 1200.0, 'duration_min': 100},
    ('DUR', 'CPT'): {'distance_km': 1200.0, 'duration_min': 100}
}

# Expanded popular routes based on market demand
POPULAR_ROUTES = [
    # South African domestic (2020 only)
    ('JNB', 'CPT'), ('CPT', 'JNB'), ('JNB', 'DUR'), ('DUR', 'JNB'), ('CPT', 'DUR'), ('DUR', 'CPT'),
    ('JNB', 'PLZ'), ('PLZ', 'JNB'), ('CPT', 'GRJ'), ('GRJ', 'CPT'),
    # Regional African routes (2021+)
    ('JNB', 'HRE'), ('HRE', 'JNB'), ('JNB', 'NBO'), ('NBO', 'JNB'), ('JNB', 'LOS'), ('LOS', 'JNB'),
    ('CPT', 'HRE'), ('HRE', 'CPT'), ('NBO', 'LOS'), ('LOS', 'NBO'),
    # International business/tourism routes (2022+)
    ('JNB', 'LHR'), ('LHR', 'JNB'), ('CPT', 'LHR'), ('LHR', 'CPT'), ('JNB', 'DXB'), ('DXB', 'JNB'),
    ('JNB', 'JFK'), ('JFK', 'JNB'), ('CPT', 'FRA'), ('FRA', 'CPT'), ('JNB', 'SYD'), ('SYD', 'JNB'),
    ('JNB', 'CDG'), ('CDG', 'JNB'), ('JNB', 'HKG'), ('HKG', 'JNB'),
    # International non-SA routes (2022+)
    ('LHR', 'JFK'), ('JFK', 'LHR'), ('FRA', 'CDG'), ('CDG', 'FRA'), ('SYD', 'HKG'), ('HKG', 'SYD'),
    ('LHR', 'DXB'), ('DXB', 'LHR'), ('FRA', 'JFK'), ('JFK', 'FRA'), ('CDG', 'HKG'), ('HKG', 'CDG'),
    # Additional routes for connectivity
    ('NBO', 'LHR'), ('LHR', 'NBO'), ('NBO', 'CDG'), ('CDG', 'NBO'), ('NBO', 'HKG'), ('HKG', 'NBO'),
    ('FRA', 'SYD'), ('SYD', 'FRA'), ('DXB', 'SYD'), ('SYD', 'DXB')
]

# Expanded seasonal routes (holiday destinations)
SEASONAL_ROUTES = {
    ('CPT', 'GRJ'): {'months': [1, 2, 12]},  # Summer holiday season
    ('GRJ', 'CPT'): {'months': [1, 2, 12]},
    ('DUR', 'PLZ'): {'months': [1, 2, 12]},
    ('PLZ', 'DUR'): {'months': [1, 2, 12]},
    ('JNB', 'GRJ'): {'months': [1, 2, 12]},
    ('GRJ', 'JNB'): {'months': [1, 2, 12]},
    ('CPT', 'PLZ'): {'months': [1, 2, 12]},
    ('PLZ', 'CPT'): {'months': [1, 2, 12]}
}

def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points using Haversine formula."""
    R = 6371  # Earth radius in kilometers
    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = np.sin(dlat/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    distance = R * c
    # Error: 2% chance of slight distance miscalculation
    if random.random() < 0.02:
        distance *= random.uniform(0.95, 1.05)  # +/- 5% error
    return distance

def calculate_duration(distance_km):
    """Calculate estimated flight duration based on distance."""
    base_time = 30  # minutes for takeoff/landing procedures
    cruise_time = (distance_km / 800) * 60  # minutes, assuming 800 km/h cruise speed
    duration = base_time + cruise_time
    # Error: 3% chance of duration variation due to headwinds/ATC
    if random.random() < 0.03:
        duration *= random.uniform(0.9, 1.1)  # +/- 10% variation
    return round(duration)

def get_available_airports(year):
    """Get available airports based on the target year."""
    if year == BASE_YEAR:
        return {code: info for code, info in AIRPORTS.items() if info['country'] == 'South Africa'}
    elif year == BASE_YEAR + 1:
        return {code: info for code, info in AIRPORTS.items() if info['country'] in ['South Africa', 'Zimbabwe', 'Kenya', 'Nigeria']}
    else:
        return AIRPORTS

def generate_route_pair_id(origin, destination):
    """Generate a consistent route pair ID regardless of direction."""
    sorted_codes = sorted([origin, destination])
    return f"RP_{sorted_codes[0]}_{sorted_codes[1]}"

def get_existing_routes(years):
    """Get all routes that exist in previous years."""
    existing_routes = set()
    for year in years:
        file_pattern = f"airplane_data/routes_{year}.parquet"
        matching_files = glob.glob(file_pattern)
        if matching_files:
            try:
                df = pd.read_parquet(matching_files[0])
                for _, row in df.iterrows():
                    route_key = (row['origin_airport'], row['destination_airport'])
                    existing_routes.add(route_key)
            except Exception as e:
                print(f"Warning: Could not read routes for year {year}: {e}")
    return existing_routes

def is_connecting_route(origin_info, dest_info):
    """Determine if a route is a connecting flight based on intercontinental logic."""
    origin_region = COUNTRY_TO_REGION.get(origin_info['country'], 'Other')
    dest_region = COUNTRY_TO_REGION.get(dest_info['country'], 'Other')
    
    if origin_region == dest_region:
        return False  # Domestic or same-region routes are not connecting
    
    # Connecting base logic
    if dest_region in ['Europe', 'Asia']:
        return origin_info['iata'] == 'NBO' or dest_info['iata'] == 'NBO'  # Kenya (NBO) for Europe/Asia
    elif dest_region == 'North America':
        return origin_info['iata'] == 'FRA' or dest_info['iata'] == 'FRA'  # Germany (FRA) for America
    elif dest_region == 'Australia':
        return origin_info['iata'] == 'HKG' or dest_info['iata'] == 'HKG'  # Hong Kong (HKG) for Australia
    return False

def is_route_valid_for_year(year, route, origin_info, dest_info):
    """Check if a route is valid for the given year, considering seasonal and hub constraints."""
    month = datetime(year, random.randint(1, 12), 1).month
    if route in SEASONAL_ROUTES:
        return month in SEASONAL_ROUTES[route]['months']
    # Prefer hub-based routes (80% chance if one airport is a hub)
    if origin_info['is_hub'] or dest_info['is_hub']:
        return random.random() < 0.8
    # Non-hub routes have a lower chance (20%)
    return random.random() < 0.2

def generate_routes(year, existing_routes=None):
    """Generate routes based on the target year, excluding duplicates from previous years."""
    routes = []
    route_id = 1
    available_airports = get_available_airports(year)
    airport_codes = list(available_airports.keys())

    print(f"Generating routes for {year} with {len(airport_codes)} airports...")

    # Prioritize popular routes
    for route in POPULAR_ROUTES:
        origin, destination = route
        if origin not in available_airports or destination not in available_airports:
            continue
        if origin == destination:
            continue
        route_key = (origin, destination)
        if existing_routes and route_key in existing_routes:
            continue
        origin_info = available_airports[origin]
        dest_info = available_airports[destination]

        # Skip South African domestic routes in 2021+ as they are in 2020
        if year > BASE_YEAR and origin_info['country'] == 'South Africa' and dest_info['country'] == 'South Africa':
            continue

        if not is_route_valid_for_year(year, route_key, origin_info, dest_info):
            continue

        # Calculate distance and duration
        if route_key in KNOWN_ROUTES:
            distance_km = KNOWN_ROUTES[route_key]['distance_km']
            duration_min = KNOWN_ROUTES[route_key]['duration_min']
        else:
            distance_km = calculate_distance(
                origin_info['latitude'], origin_info['longitude'],
                dest_info['latitude'], dest_info['longitude']
            )
            duration_min = calculate_duration(distance_km)

        # Determine flight category and region
        flight_category = 'Domestic' if origin_info['country'] == dest_info['country'] else 'International'
        region = COUNTRY_TO_REGION.get(origin_info['country'], 'Other')
        if flight_category == 'International':
            region = f"{COUNTRY_TO_REGION.get(origin_info['country'], 'Other')}-{COUNTRY_TO_REGION.get(dest_info['country'], 'Other')}"

        # Determine if route is connecting
        is_connecting = is_connecting_route(origin_info, dest_info)

        route_data = {
            'route_id': f'RTE{year}{route_id:04d}',
            'route_pair_id': generate_route_pair_id(origin, destination),
            'date_effective': date(year, random.randint(1, 12), 1),
            'origin_airport': origin,
            'origin_airport_name': origin_info['name'],
            'origin_city': origin_info['city'],
            'origin_country': origin_info['country'],
            'destination_airport': destination,
            'destination_airport_name': dest_info['name'],
            'destination_city': dest_info['city'],
            'destination_country': dest_info['country'],
            'distance_km': round(distance_km, 1),
            'estimated_duration_min': duration_min,
            'flight_category': flight_category,
            'region': region,
            'is_operational': random.choices([True, False], weights=[0.95, 0.05])[0],
            'is_connecting': is_connecting
        }
        routes.append(route_data)
        route_id += 1

    # Generate additional non-popular routes (increased limit for more routes)
    max_additional_routes = min(len(airport_codes) * 3, 100)  # Increased to allow more routes
    additional_routes = []
    for i, origin in enumerate(airport_codes):
        for j, destination in enumerate(airport_codes):
            if origin != destination and (origin, destination) not in POPULAR_ROUTES:
                route_key = (origin, destination)
                if existing_routes and route_key in existing_routes:
                    continue
                origin_info = available_airports[origin]
                dest_info = available_airports[destination]
                if year > BASE_YEAR and origin_info['country'] == 'South Africa' and dest_info['country'] == 'South Africa':
                    continue
                if not is_route_valid_for_year(year, route_key, origin_info, dest_info):
                    continue
                additional_routes.append(route_key)
    
    # Sample a subset of additional routes
    selected_routes = random.sample(additional_routes, min(len(additional_routes), max_additional_routes))
    for route_key in selected_routes:
        origin, destination = route_key
        origin_info = available_airports[origin]
        dest_info = available_airports[destination]

        if route_key in KNOWN_ROUTES:
            distance_km = KNOWN_ROUTES[route_key]['distance_km']
            duration_min = KNOWN_ROUTES[route_key]['duration_min']
        else:
            distance_km = calculate_distance(
                origin_info['latitude'], origin_info['longitude'],
                dest_info['latitude'], dest_info['longitude']
            )
            duration_min = calculate_duration(distance_km)

        flight_category = 'Domestic' if origin_info['country'] == dest_info['country'] else 'International'
        region = COUNTRY_TO_REGION.get(origin_info['country'], 'Other')
        if flight_category == 'International':
            region = f"{COUNTRY_TO_REGION.get(origin_info['country'], 'Other')}-{COUNTRY_TO_REGION.get(dest_info['country'], 'Other')}"

        is_connecting = is_connecting_route(origin_info, dest_info)

        route_data = {
            'route_id': f'RTE{year}{route_id:04d}',
            'route_pair_id': generate_route_pair_id(origin, destination),
            'date_effective': date(year, random.randint(1, 12), 1),
            'origin_airport': origin,
            'origin_airport_name': origin_info['name'],
            'origin_city': origin_info['city'],
            'origin_country': origin_info['country'],
            'destination_airport': destination,
            'destination_airport_name': dest_info['name'],
            'destination_city': dest_info['city'],
            'destination_country': dest_info['country'],
            'distance_km': round(distance_km, 1),
            'estimated_duration_min': duration_min,
            'flight_category': flight_category,
            'region': region,
            'is_operational': random.choices([True, False], weights=[0.95, 0.05])[0],
            'is_connecting': is_connecting
        }
        routes.append(route_data)
        route_id += 1

    return pd.DataFrame(routes)

def generate_routes_dataset():
    """Main function to generate and save the routes dataset for all years."""
    os.makedirs('airplane_data', exist_ok=True)
    
    total_routes = 0
    year_stats = {}
    
    for year in range(BASE_YEAR, END_YEAR + 1):
        print(f"\n{'='*60}")
        print(f"Processing year {year}")
        print(f"{'='*60}")
        
        previous_years = range(BASE_YEAR, year)
        existing_routes = get_existing_routes(previous_years) if year > BASE_YEAR else None
        
        routes_df = generate_routes(year, existing_routes)
        
        if len(routes_df) > 0:
            routes_df['date_effective'] = pd.to_datetime(routes_df['date_effective'])
            output_file = f'airplane_data/routes_{year}.parquet'
            routes_df.to_parquet(output_file, index=False)
            
            print(f"Saved {len(routes_df)} route records to {output_file}")
            total_routes += len(routes_df)
            year_stats[year] = len(routes_df)
            
            print("\nRoutes Summary:")
            print("-" * 40)
            
            category_counts = routes_df['flight_category'].value_counts()
            for category, count in category_counts.items():
                print(f"{category}: {count} routes")
            
            region_counts = routes_df['region'].value_counts()
            for region, count in region_counts.items():
                print(f"{region}: {count} routes")
            
            suspended_routes = len(routes_df[routes_df['is_operational'] == False])
            print(f"Suspended routes: {suspended_routes}")
            
            connecting_routes = len(routes_df[routes_df['is_connecting'] == True])
            print(f"Connecting routes: {connecting_routes}")
            
            longest_routes = routes_df.nlargest(5, 'distance_km')[['origin_airport', 'destination_airport', 'distance_km', 'estimated_duration_min', 'is_connecting']]
            print(f"\nTop 5 longest routes:")
            for _, route in longest_routes.iterrows():
                print(f"{route['origin_airport']} -> {route['destination_airport']}: {route['distance_km']}km ({route['estimated_duration_min']} min, Connecting: {route['is_connecting']})")
            
            print(f"\nSample data for {year}:")
            print("-" * 40)
            sample_cols = ['route_id', 'origin_airport', 'destination_airport', 'distance_km', 'estimated_duration_min', 'flight_category', 'region', 'is_operational', 'is_connecting']
            print(routes_df[sample_cols].head(5).to_string(index=False))
            
        else:
            print(f"No new routes generated for year {year}")
            year_stats[year] = 0
    
    print(f"\n{'='*60}")
    print("TOTAL STATISTICS ACROSS ALL YEARS:")
    print(f"{'='*60}")
    print(f"Total routes generated: {total_routes}")
    
    print("\nRoutes by year:")
    for year, count in year_stats.items():
        print(f"{year}: {count} routes")

if __name__ == "__main__":
    generate_routes_dataset()
