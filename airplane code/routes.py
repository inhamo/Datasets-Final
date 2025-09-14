import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from tqdm import tqdm
import random
import os

# Set random seeds for reproducibility
random.seed(12)
np.random.seed(12)

# Constants
TARGET_YEAR = 2024

# Airport database with detailed information
AIRPORTS = {
    # South Africa (Domestic)
    'JNB': {'name': 'O.R. Tambo International Airport', 'city': 'Johannesburg', 'country': 'South Africa', 'iata': 'JNB', 'latitude': -26.1392, 'longitude': 28.2460},
    'CPT': {'name': 'Cape Town International Airport', 'city': 'Cape Town', 'country': 'South Africa', 'iata': 'CPT', 'latitude': -33.9648, 'longitude': 18.6017},
    'DUR': {'name': 'King Shaka International Airport', 'city': 'Durban', 'country': 'South Africa', 'iata': 'DUR', 'latitude': -29.6145, 'longitude': 31.1198},
    'PLZ': {'name': 'Port Elizabeth International Airport', 'city': 'Port Elizabeth', 'country': 'South Africa', 'iata': 'PLZ', 'latitude': -33.9849, 'longitude': 25.6173},
    'GRJ': {'name': 'George Airport', 'city': 'George', 'country': 'South Africa', 'iata': 'GRJ', 'latitude': -34.0056, 'longitude': 22.3789},
    
    # African destinations (for 2014+)
    'HRE': {'name': 'Robert Gabriel Mugabe International Airport', 'city': 'Harare', 'country': 'Zimbabwe', 'iata': 'HRE', 'latitude': -17.9318, 'longitude': 31.0928},
    'NBO': {'name': 'Jomo Kenyatta International Airport', 'city': 'Nairobi', 'country': 'Kenya', 'iata': 'NBO', 'latitude': -1.3192, 'longitude': 36.9278},
    'LOS': {'name': 'Murtala Muhammed International Airport', 'city': 'Lagos', 'country': 'Nigeria', 'iata': 'LOS', 'latitude': 6.5774, 'longitude': 3.3210},
    
    # International destinations (for 2015+)
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
    if year == 2013:
        return {code: info for code, info in AIRPORTS.items() if info['country'] == 'South Africa'}
    elif year == 2014:
        return {code: info for code, info in AIRPORTS.items() if info['country'] in ['South Africa', 'Zimbabwe', 'Kenya', 'Nigeria']}
    else:
        return AIRPORTS

def generate_route_pair_id(origin, destination):
    """Generate a consistent route pair ID regardless of direction."""
    sorted_codes = sorted([origin, destination])
    return f"RP_{sorted_codes[0]}_{sorted_codes[1]}"

def generate_routes(year):
    """Generate routes based on the target year, excluding 2013 domestic routes for 2014."""
    routes = []
    route_id = 1
    
    available_airports = get_available_airports(year)
    airport_codes = list(available_airports.keys())
    
    print(f"Generating routes for {year} with {len(airport_codes)} airports...")
    
    # Generate route combinations, excluding domestic South African routes for 2014
    for i, origin in enumerate(airport_codes):
        for j, destination in enumerate(airport_codes):
            if origin != destination:  # No self-routes
                origin_info = available_airports[origin]
                dest_info = available_airports[destination]
                
                # For 2014, skip routes where both origin and destination are South African
                if year == 2014 and origin_info['country'] == 'South Africa' and dest_info['country'] == 'South Africa':
                    continue
                
                # Check if this is a known route
                route_key = (origin, destination)
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
                    'route_id': f'RTE{TARGET_YEAR}{route_id:04d}',
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
    """Main function to generate and save the routes dataset."""
    print(f"Generating routes for {TARGET_YEAR}...")
    
    # Generate the data
    routes_df = generate_routes(TARGET_YEAR)
    
    # Save to parquet
    os.makedirs('airplane_data', exist_ok=True)
    output_file = f'airplane_data/routes_{TARGET_YEAR}.parquet'
    routes_df.to_parquet(output_file, index=False)
    
    print(f"Saved {len(routes_df)} route records to {output_file}")
    
    # Display summary
    print("\nRoutes Summary:")
    print("=" * 60)
    
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
    
    return routes_df

# Generate the dataset
if __name__ == "__main__":
    routes_data = generate_routes_dataset()
    
    # Display sample data
    print("\nSample route data:")
    print("=" * 60)
    sample_cols = ['route_id', 'origin_airport', 'destination_airport', 'distance_km', 'estimated_duration_hrs', 'flight_category']
    print(routes_data[sample_cols].head(10).to_string(index=False))
