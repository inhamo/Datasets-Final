import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from tqdm import tqdm
import random
import os

# Set random seeds for reproducibility
NUM = 30
random.seed(NUM)
np.random.seed(NUM)

# Constants
TARGET_YEAR = 2024
BASE_YEAR = 2020

# Create holidays data
HOLIDAYS_DATA = {
  "events_affecting_flying": [
    {
      "name": "Easter 2013",
      "dates": {
        "Good Friday": "2013-03-29",
        "Family Day": "2013-04-01"
      },
      "description": "Easter holidays lead to increased domestic and international travel, with higher flight demand and potential delays around Johannesburg, Cape Town, and Durban airports."
    },
    {
      "name": "Durban July 2013",
      "date": "2013-07-06",
      "description": "Major horse racing event in Durban attracts thousands, causing flight surges to King Shaka International Airport and traffic disruptions."
    },
    {
      "name": "Easter 2014",
      "dates": {
        "Good Friday": "2014-04-18",
        "Family Day": "2014-04-21"
      },
      "description": "Peak travel period for family reunions and vacations, impacting major routes with elevated air traffic."
    },
    {
      "name": "Durban July 2014",
      "date": "2014-07-05",
      "description": "High-profile social and racing event boosting travel to Durban, with increased flight bookings and potential congestion."
    },
    {
      "name": "Easter 2015",
      "dates": {
        "Good Friday": "2015-04-03",
        "Family Day": "2015-04-06"
      },
      "description": "Holiday weekend drives up passenger volumes on flights, especially to coastal and safari destinations."
    },
    {
      "name": "Durban July 2015",
      "date": "2015-07-04",
      "description": "Africa's premier horse race event, leading to flight demand spikes for Durban and surrounding areas."
    },
    {
      "name": "Easter 2016",
      "dates": {
        "Good Friday": "2016-03-25",
        "Family Day": "2016-03-28"
      },
      "description": "Early Easter causes early-year travel rush, affecting schedules at key airports like O.R. Tambo."
    },
    {
      "name": "Durban July 2016",
      "date": "2016-07-02",
      "description": "Event draws national and international visitors, increasing air travel to KwaZulu-Natal."
    },
    {
      "name": "Easter 2017",
      "dates": {
        "Good Friday": "2017-04-14",
        "Family Day": "2017-04-17"
      },
      "description": "Long weekend promotes getaways, resulting in higher flight occupancy and possible delays."
    },
    {
      "name": "Durban July 2017",
      "date": "2017-07-01",
      "description": "Signature event with fashion and entertainment, heightening travel to Durban."
    },
    {
      "name": "Easter 2018",
      "dates": {
        "Good Friday": "2018-03-30",
        "Family Day": "2018-04-02"
      },
      "description": "Easter break sees surges in leisure travel, impacting domestic flights significantly."
    },
    {
      "name": "Durban July 2018",
      "date": "2018-07-07",
      "description": "Culmination of racing season attracts crowds, with elevated flight traffic to Durban."
    }
  ],
  "note": "These events, including Easter (with its variable dates for Good Friday and Family Day) and the Durban July horse race, are known to increase air travel demand in South Africa, leading to busier airports, higher fares, and potential disruptions. Other public holidays like Christmas and New Year may also affect flying but are not specified here. Dates sourced from official calendars and event records."
}

# Airport cost tiers (base prices in ZAR)
AIRPORT_TIERS = {
    'JNB': {'tier': 1, 'base_price': 650, 'min_price': 500, 'max_price': 1200},
    'CPT': {'tier': 1, 'base_price': 700, 'min_price': 550, 'max_price': 1300},
    'DUR': {'tier': 1, 'base_price': 600, 'min_price': 450, 'max_price': 1100},
    'PLZ': {'tier': 2, 'base_price': 450, 'min_price': 350, 'max_price': 900},
    'GRJ': {'tier': 2, 'base_price': 400, 'min_price': 300, 'max_price': 800},
}

# Peak hours
PEAK_HOURS = [(6, 9), (16, 19)]

# Cancellation reasons with probabilities
CANCELLATION_REASONS = {
    'Weather': 0.45,
    'Mechanical Issues': 0.28,
    'Crew Availability': 0.12,
    'Air Traffic Control': 0.08,
    'Operational Issues': 0.07
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
OTHER_FLIGHT_TIMES = [8, 12, 16]

def load_datasets():
    """Load and concatenate planes and routes datasets for all years from BASE_YEAR to TARGET_YEAR."""
    planes_dfs = []
    routes_dfs = []
    
    # Loop through years
    for yr in range(BASE_YEAR, TARGET_YEAR + 1):
        print(f"Loading data for year {yr}...")
        
        # Load planes dataset
        planes_file = f'airplane_data/planes_{yr}.parquet'
        if os.path.exists(planes_file):
            planes_df = pd.read_parquet(planes_file)
            planes_dfs.append(planes_df)
            print(f"Loaded {len(planes_df)} plane records for {yr}")
        else:
            print(f"Planes file for {yr} not found: {planes_file}")
        
        # Load routes dataset
        routes_file = f'airplane_data/routes_{yr}.parquet'
        if os.path.exists(routes_file):
            routes_df = pd.read_parquet(routes_file)
            # Convert date_effective to datetime if needed
            if 'date_effective' in routes_df.columns and not pd.api.types.is_datetime64_any_dtype(routes_df['date_effective']):
                routes_df['date_effective'] = pd.to_datetime(routes_df['date_effective'])
            routes_dfs.append(routes_df)
            print(f"Loaded {len(routes_df)} route records for {yr}")
        else:
            print(f"Routes file for {yr} not found: {routes_file}")
    
    # Concatenate planes DataFrames
    if planes_dfs:
        combined_planes = pd.concat(planes_dfs, ignore_index=True)
        # Optional: Deduplicate planes if there's a unique identifier (e.g., plane_id)
        # combined_planes = combined_planes.drop_duplicates(subset=['plane_id'], keep='last')
        print(f"Combined {len(combined_planes)} plane records")
    else:
        print("No planes data found to concatenate")
        combined_planes = None
    
    # Concatenate routes DataFrames
    if routes_dfs:
        combined_routes = pd.concat(routes_dfs, ignore_index=True)
        # Deduplicate routes based on route_pair_id, origin_airport, destination_airport
        combined_routes = combined_routes.sort_values('date_effective').drop_duplicates(
            subset=['route_pair_id', 'origin_airport', 'destination_airport'], keep='last'
        )
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
    
    return round(price)

def generate_delay():
    """Generate delay based on statistical distribution."""
    delay_type = random.choices(
        list(DELAY_DISTRIBUTIONS.keys()),
        weights=[prob for prob, _, _ in DELAY_DISTRIBUTIONS.values()]
    )[0]
    
    prob, min_delay, max_delay = DELAY_DISTRIBUTIONS[delay_type]
    delay = random.randint(min_delay, max_delay)
    
    return delay, 'Weather' if delay_type in ['moderate', 'major'] and random.random() < 0.6 else None

def generate_cancellation_reason():
    """Generate cancellation reason based on probabilities."""
    return random.choices(
        list(CANCELLATION_REASONS.keys()),
        weights=list(CANCELLATION_REASONS.values())
    )[0]

def generate_flight_schedule(planes_df, routes_df):
    """Generate a complete flight schedule for the year."""
    flights = []
    flight_id = 1
    
    # Filter routes for the target year
    available_routes = routes_df[routes_df['date_effective'].dt.year == TARGET_YEAR] if 'date_effective' in routes_df.columns else routes_df
    
    # Group routes by popularity
    popular_routes = available_routes[
        ((available_routes['origin_airport'] == 'JNB') & (available_routes['destination_airport'].isin(['CPT', 'DUR']))) |
        ((available_routes['origin_airport'] == 'CPT') & (available_routes['destination_airport'] == 'JNB')) |
        ((available_routes['origin_airport'] == 'DUR') & (available_routes['destination_airport'] == 'JNB'))
    ]
    other_routes = available_routes[~available_routes.index.isin(popular_routes.index)]
    
    print(f"Generating flight schedule for {TARGET_YEAR}...")
    print(f"Popular routes: {len(popular_routes)}, Other routes: {len(other_routes)}")
    
    # Track aircraft location and schedule
    aircraft_status = {
        plane_id: {'location': 'JNB', 'last_arrival': None}
        for plane_id in planes_df['plane_id']
    }
    
    # Generate date range
    date_range = pd.date_range(date(TARGET_YEAR, 1, 1), date(TARGET_YEAR, 12, 31))
    
    for current_date in tqdm(date_range, desc="Generating daily schedules"):
        # Process popular routes
        for _, route in popular_routes.iterrows():
            origin = route['origin_airport']
            destination = route['destination_airport']
            flight_times = POPULAR_FLIGHT_TIMES.get((origin, destination), [8, 12, 16])
            
            for hour in flight_times:
                scheduled_departure = datetime.combine(current_date, datetime.min.time()).replace(hour=hour)
                
                # Find available planes at the origin
                available_planes = [
                    plane_id for plane_id, status in aircraft_status.items()
                    if status['location'] == origin and 
                    (status['last_arrival'] is None or status['last_arrival'] + timedelta(minutes=45) <= scheduled_departure)
                ]
                
                if not available_planes:
                    continue
                
                plane_id = random.choice(available_planes)
                
                # Calculate timings and price
                scheduled_arrival = scheduled_departure + timedelta(minutes=route['estimated_duration_min'])
                delay_minutes, delay_reason = generate_delay()
                actual_departure = scheduled_departure + timedelta(minutes=delay_minutes) if delay_minutes else scheduled_departure
                actual_arrival = actual_departure + timedelta(minutes=route['estimated_duration_min']) if delay_minutes else scheduled_arrival
                
                is_cancelled = delay_minutes > 120 and random.random() < 0.02
                cancellation_reason = generate_cancellation_reason() if is_cancelled else None
                
                base_price = AIRPORT_TIERS.get(origin, AIRPORT_TIERS['JNB'])['base_price']
                final_price = calculate_dynamic_price(base_price, current_date, scheduled_departure, random.uniform(0.9, 1.3))
                
                # Create flight record
                flight_data = {
                    'planning_id': f'PLN{TARGET_YEAR}{flight_id:04d}',
                    'route_id': route['route_id'],
                    'plane_id': plane_id,
                    'scheduled_departure': scheduled_departure,
                    'scheduled_arrival': scheduled_arrival,
                    'actual_departure': actual_departure if not is_cancelled else None,
                    'actual_arrival': actual_arrival if not is_cancelled else None,
                    'duration_actual_min': (actual_arrival - actual_departure).total_seconds() / 60 if not is_cancelled else None,
                    'is_cancelled': is_cancelled,
                    'cancellation_reason': cancellation_reason,
                    'final_price_zar': final_price if not is_cancelled else None
                }
                
                flights.append(flight_data)
                
                # Update aircraft status
                if not is_cancelled:
                    aircraft_status[plane_id].update({
                        'location': destination,
                        'last_arrival': scheduled_arrival
                    })
                
                flight_id += 1
        
        # Process other routes
        for _, route in other_routes.iterrows():
            origin = route['origin_airport']
            destination = route['destination_airport']
            
            for hour in OTHER_FLIGHT_TIMES:
                scheduled_departure = datetime.combine(current_date, datetime.min.time()).replace(hour=hour)
                
                available_planes = [
                    plane_id for plane_id, status in aircraft_status.items()
                    if status['location'] == origin and 
                    (status['last_arrival'] is None or status['last_arrival'] + timedelta(minutes=60) <= scheduled_departure)
                ]
                
                if not available_planes:
                    continue
                
                plane_id = random.choice(available_planes)
                
                scheduled_arrival = scheduled_departure + timedelta(minutes=route['estimated_duration_min'])
                delay_minutes, delay_reason = generate_delay()
                actual_departure = scheduled_departure + timedelta(minutes=delay_minutes) if delay_minutes else scheduled_departure
                actual_arrival = actual_departure + timedelta(minutes=route['estimated_duration_min']) if delay_minutes else scheduled_arrival
                
                is_cancelled = delay_minutes > 120 and random.random() < 0.03
                cancellation_reason = generate_cancellation_reason() if is_cancelled else None
                
                base_price = AIRPORT_TIERS.get(origin, AIRPORT_TIERS['JNB'])['base_price']
                final_price = calculate_dynamic_price(base_price, current_date, scheduled_departure, random.uniform(0.8, 1.2))
                
                flight_data = {
                    'planning_id': f'PLN{TARGET_YEAR}{flight_id:04d}',
                    'route_id': route['route_id'],
                    'plane_id': plane_id,
                    'scheduled_departure': scheduled_departure,
                    'scheduled_arrival': scheduled_arrival,
                    'actual_departure': actual_departure if not is_cancelled else None,
                    'actual_arrival': actual_arrival if not is_cancelled else None,
                    'duration_actual_min': (actual_arrival - actual_departure).total_seconds() / 60 if not is_cancelled else None,
                    'is_cancelled': is_cancelled,
                    'cancellation_reason': cancellation_reason,
                    'final_price_zar': final_price if not is_cancelled else None
                }
                
                flights.append(flight_data)
                
                if not is_cancelled:
                    aircraft_status[plane_id].update({
                        'location': destination,
                        'last_arrival': scheduled_arrival
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
    
    # Save to parquet
    os.makedirs('airplane_data', exist_ok=True)
    output_file = f'airplane_data/flight_schedule_{TARGET_YEAR}.parquet'
    schedule_df.to_parquet(output_file, index=False)
    
    print(f"Saved {len(schedule_df)} flight records to {output_file}")
    
    # Display summary
    print("\nFlight Schedule Summary:")
    print("=" * 50)
    print(f"Total flights: {len(schedule_df):,}")
    print(f"Cancelled flights: {schedule_df['is_cancelled'].sum():,} ({schedule_df['is_cancelled'].mean()*100:.1f}%)")
    print(f"Average delay: {schedule_df['duration_actual_min'].mean():.1f} minutes")
    print(f"Average final price: R{schedule_df['final_price_zar'].mean():.0f}")
    
    if not schedule_df[schedule_df['is_cancelled']].empty:
        print("\nCancellation Reasons:")
        for reason, count in schedule_df[schedule_df['is_cancelled']]['cancellation_reason'].value_counts().items():
            print(f"  {reason}: {count}")
    
    # Display sample data
    print("\nSample flight data:")
    print("=" * 60)
    print(schedule_df.head(10).to_string(index=False))
    
    return schedule_df

if __name__ == "__main__":
    schedule_data = generate_flight_schedule_dataset()
