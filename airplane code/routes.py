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
    'JNB': {'name': 'O.R. Tambo International Airport', 'city': 'Johannesburg', 'country': 'South Africa', 'iata': 'JNB', 'latitude': -26.1392, 'longitude': 28.2460},
    'CPT': {'name': 'Cape Town International Airport', 'city': 'Cape Town', 'country': 'South Africa', 'iata': 'CPT', 'latitude': -33.9648, 'longitude': 18.6017},
    'DUR': {'name': 'King Shaka International Airport', 'city': 'Durban', 'country': 'South Africa', 'iata': 'DUR', 'latitude': -29.6145, 'longitude': 31.1198},
    'PLZ': {'name': 'Port Elizabeth International Airport', 'city': 'Port Elizabeth', 'country': 'South Africa', 'iata': 'PLZ', 'latitude': -33.9849, 'longitude': 25.6173},
    'GRJ': {'name': 'George Airport', 'city': 'George', 'country': 'South Africa', 'iata': 'GRJ', 'latitude': -34.0056, 'longitude': 22.3789},
    
    # African destinations (for 2021+)
    'HRE': {'name': 'Robert Gabriel Mugabe International Airport', 'city': 'Harare', 'country': 'Zimbabwe', 'iata': 'HRE', 'latitude': -17.9318, 'longitude': 31.0928},
    'NBO': {'name': 'Jomo Kenyatta International Airport', 'city': 'Nairobi', 'country': 'Kenya', 'iata': 'NBO', 'latitude': -1.3192, 'longitude': 36.9278},
    'LOS': {'name': 'Murtala Muhammed International Airport', 'city': 'Lagos', 'country': 'Nigeria', 'iata': 'LOS', 'latitude': 6.5774, 'longitude': 3.3210},
    
    # International destinations (for 2022+)
    'LHR': {'name': 'Heathrow Airport', 'city': 'London', 'country': 'United Kingdom', 'iata': 'LHR', 'latitude': 51.4700, 'longitude': -0.4543},
    'DXB': {'name': 'Dubai International Airport', 'city': 'Dubai', 'country': 'UAE', 'iata': 'DXB', 'latitude': 25.2528, 'longitude': 55.3644},
    'JFK': {'name': 'John F. Kennedy International Airport', 'city': 'New York', 'country': 'USA', 'iata': 'JFK', 'latitude': 40.6398, 'longitude': -73.7789},
    'SYD': {'name': 'Sydney Kingsford Smith Airport', 'city': 'Sydney', 'country': 'Australia', 'iata': 'SYD', 'latitude': -33.9461, 'longitude': 151.1772},
    'FRA': {'name': 'Frankfurt Airport', 'city': 'Frankfurt', 'country': 'Germany', 'iata': 'FRA', 'latitude': 50.0333, 'longitude': 8.5706},
    'CDG': {'name': 'Charles de Gaulle Airport', 'city': 'Paris', 'country': 'France', 'iata': 'CDG', 'latitude': 49.0097, 'longitude': 2.5479},
    'HKG': {'name': 'Hong Kong International Airport', 'city': 'Hong Kong', 'country': 'China', 'iata': 'HKG', 'latitude': 22.3080, 'longitude': 113.9185}
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
    
    return R * c

def calculate_duration(distance_km):
    """Calculate estimated flight duration based on distance."""
    base_time = 30  # minutes for takeoff/landing procedures
    cruise_time = (distance_km / 800) * 60  # minutes
    return round(base_time + cruise_time)

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
                # Create unique route identifiers (origin-destination pairs)
                for _, row in df.iterrows():
                    route_key = (row['origin_airport'], row['destination_airport'])
                    existing_routes.add(route_key)
            except Exception as e:
                print(f"Warning: Could not read routes for year {year}: {e}")
    
    return existing_routes

def generate_routes(year, existing_routes=None):
    """Generate routes based on the target year, excluding duplicates from previous years."""
    routes = []
    route_id = 1
    
    available_airports = get_available_airports(year)
    airport_codes = list(available_airports.keys())
    
    print(f"Generating routes for {year} with {len(airport_codes)} airports...")
    
    # Generate route combinations
    for i, origin in enumerate(airport_codes):
        for j, destination in enumerate(airport_codes):
            if origin != destination:  # No self-routes
                origin_info = available_airports[origin]
                dest_info = available_airports[destination]
                
                # For 2021, skip routes where both origin and destination are South African
                if year == (BASE_YEAR + 1) and origin_info['country'] == 'South Africa' and dest_info['country'] == 'South Africa':
                    continue
                
                # Check if this route already exists in previous years
                route_key = (origin, destination)
                if existing_routes and route_key in existing_routes:
                    continue  # Skip this route as it already exists
                
                # Check if this is a known route
                if route_key in KNOWN_ROUTES:
                    distance_km = KNOWN_ROUTES[route_key]['distance_km']
                    duration_min = KNOWN_ROUTES[route_key]['duration_min']
                else:
                    # Calculate distance using coordinates
                    distance_km = calculate_distance(
                        origin_info['latitude'], origin_info['longitude'],
                        dest_info['latitude'], dest_info['longitude']
                    )
                    duration_min = calculate_duration(distance_km)
                
                route_data = {
                    'route_id': f'RTE{year}{route_id:04d}',
                    'route_pair_id': generate_route_pair_id(origin, destination),
                    'date_effective': date(year, 1, 1),
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
                    'estimated_duration_hrs': f"{duration_min // 60}h {duration_min % 60}m",
                    'flight_category': 'Domestic' if origin_info['country'] == dest_info['country'] else 'International',
                    'region': 'Africa' if origin_info['country'] == 'South Africa' and dest_info['country'] == 'South Africa' else 'Regional'
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
        
        # Get existing routes from previous years
        previous_years = range(BASE_YEAR, year)
        existing_routes = get_existing_routes(previous_years) if year > BASE_YEAR else None
        
        # Generate the data for current year
        routes_df = generate_routes(year, existing_routes)
        
        if len(routes_df) > 0:
            # Convert date_effective to datetime to avoid the .dt accessor error
            routes_df['date_effective'] = pd.to_datetime(routes_df['date_effective'])
            
            # Save to parquet
            output_file = f'airplane_data/routes_{year}.parquet'
            routes_df.to_parquet(output_file, index=False)
            
            print(f"Saved {len(routes_df)} route records to {output_file}")
            
            # Store statistics
            total_routes += len(routes_df)
            year_stats[year] = len(routes_df)
            
            # Display summary
            print("\nRoutes Summary:")
            print("-" * 40)
            
            # Count by category
            category_counts = routes_df['flight_category'].value_counts()
            for category, count in category_counts.items():
                print(f"{category}: {count} routes")
            
            # Count by region
            region_counts = routes_df['region'].value_counts()
            for region, count in region_counts.items():
                print(f"{region}: {count} routes")
            
            # Top 5 longest routes
            longest_routes = routes_df.nlargest(5, 'distance_km')[['origin_airport', 'destination_airport', 'distance_km', 'estimated_duration_hrs']]
            print(f"\nTop 5 longest routes:")
            for _, route in longest_routes.iterrows():
                print(f"{route['origin_airport']} -> {route['destination_airport']}: {route['distance_km']}km ({route['estimated_duration_hrs']})")
            
            # Display sample data for this year
            print(f"\nSample data for {year}:")
            print("-" * 40)
            sample_cols = ['route_id', 'origin_airport', 'destination_airport', 'distance_km', 'estimated_duration_hrs', 'flight_category']
            print(routes_df[sample_cols].head(5).to_string(index=False))
            
        else:
            print(f"No new routes generated for year {year}")
            year_stats[year] = 0
    
    # Display total statistics
    print(f"\n{'='*60}")
    print("TOTAL STATISTICS ACROSS ALL YEARS:")
    print(f"{'='*60}")
    print(f"Total routes generated: {total_routes}")
    
    print("\nRoutes by year:")
    for year, count in year_stats.items():
        print(f"{year}: {count} routes")

# Generate the dataset
if __name__ == "__main__":
    generate_routes_dataset()