import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import random
import os
from tqdm import tqdm

np.random.seed(42)
random.seed(42)

# Load existing tables
output_folder = 'airline_data'
routes_df = pd.read_parquet(f'{output_folder}/routes.parquet')
flights_df = pd.read_parquet(f'{output_folder}/flights.parquet')

# Define captains
captains = [
    {'name': 'John Smith', 'location': 'JNB'},
    {'name': 'Sarah Johnson', 'location': 'CPT'},
    {'name': 'Michael Chen', 'location': 'JNB'},
    {'name': 'Emily Davis', 'location': 'DUR'},
    {'name': 'David Wilson', 'location': 'JNB'},
    {'name': 'Aisha Patel', 'location': 'CPT'},
    {'name': 'Thomas Brown', 'location': 'HRE'},
    {'name': 'Linda Mwangi', 'location': 'NBO'},
    {'name': 'James Taylor', 'location': 'JNB'},
    {'name': 'Sophie Müller', 'location': 'FRA'}
]

# Fixed departure times
DOMESTIC_TIMES = ['06:00', '10:00', '13:00', '18:00', '20:00']
REGIONAL_TIMES = ['07:00', '12:00', '17:00']
INTERNATIONAL_TIMES = ['08:00', '14:00', '22:00']

# Departure bays by route type
BAYS = {
    'Domestic': [f'A{i}' for i in range(1, 11)],
    'Regional': [f'B{i}' for i in range(1, 11)],
    'International': [f'C{i}' for i in range(1, 11)]
}

# Cancellation reasons
CANCELLATION_REASONS = [
    'Weather', 'Technical Issue', 'Air Traffic Control', 'Crew Delay',
    'Security Issue', 'Operational Delay', 'Aircraft Maintenance',
    'Fueling Delay', 'Passenger Issue', 'Ground Handling Delay'
]

# Aircraft suitability
DOMESTIC_MODELS = ['Embraer E190', 'Bombardier Q400', 'Boeing 737-800', 'Boeing 737-900', 'Airbus A321neo']
INTERNATIONAL_MODELS = ['Boeing 777-300ER', 'Airbus A330-300']

def get_available_flights(date, origin, flight_availability, flight_locations):
    available_routes = routes_df[routes_df['date_effective'] <= date]
    origin_routes = available_routes[available_routes['origin_IATA'] == origin]
    valid_flights = []
    for _, route in origin_routes.iterrows():
        route_type = route['route_type']
        suitable_models = DOMESTIC_MODELS if route_type == 'Domestic' else INTERNATIONAL_MODELS
        flights = flights_df[
            (flights_df['Aircraft_Model'].isin(suitable_models)) &
            (flights_df['Date_Added'] <= date)
        ]
        for _, flight in flights.iterrows():
            flight_id = flight['Flight_ID']
            if flight_id in flight_availability and flight_locations[flight_id] == origin:
                last_arrival = flight_availability[flight_id]
                if last_arrival and last_arrival > datetime.combine(date, datetime.min.time()):
                    continue  # Flight not available yet
                valid_flights.append((route['route_id'], flight_id, route['estimated_duration_min'], route['base_price_ZAR'], route_type, route['destination_IATA'], route['route_pair_id']))
    return valid_flights

def get_return_route(route_id, route_pair_id):
    return routes_df[(routes_df['route_pair_id'] == route_pair_id) & (routes_df['route_id'] != route_id)][['route_id', 'origin_IATA', 'destination_IATA', 'estimated_duration_min', 'base_price_ZAR', 'route_type', 'date_effective']].iloc[0]

def assign_captain(current_date, origin, used_captains):
    available = [c for c in captains if c['location'] == origin and c['name'] not in used_captains]
    if not available:
        return None, None
    captain = random.choice(available)
    return captain['name'], captain['location']

def generate_flight_schedules():
    data = []
    planning_id_counter = 1
    start_date = date(2013, 1, 1)
    end_date = date(2018, 12, 31)
    flight_locations = {row['Flight_ID']: 'JNB' for _, row in flights_df.iterrows()}
    flight_availability = {flight_id: None for flight_id in flight_locations}
    daily_captains = {}

    total_days = (end_date - start_date).days + 1

    for current_date in tqdm(range(total_days), desc="Generating Flight Schedules"):
        current_date = start_date + timedelta(days=current_date)
        daily_captains[current_date] = set()
        available_routes = routes_df[routes_df['date_effective'] <= current_date]
        origins = available_routes['origin_IATA'].unique()

        for origin in origins:
            times = DOMESTIC_TIMES if origin in ['JNB', 'CPT', 'DUR'] else (REGIONAL_TIMES if origin in ['HRE', 'NBO'] else INTERNATIONAL_TIMES)
            for time in times:
                valid_flights = get_available_flights(current_date, origin, flight_availability, flight_locations)
                if not valid_flights:
                    continue
                route_id, flight_id, duration, base_price, route_type, destination, route_pair_id = random.choice(valid_flights)

                planning_id = f'PLN{planning_id_counter:06d}'
                planning_id_counter += 1
                scheduled_departure = datetime.strptime(f'{current_date} {time}', '%Y-%m-%d %H:%M')
                scheduled_arrival = scheduled_departure + timedelta(minutes=int(duration))

                is_canceled = np.random.binomial(1, 0.03)
                cancellation_note = random.choice(CANCELLATION_REASONS) if is_canceled else None

                if not is_canceled:
                    is_on_time = np.random.binomial(1, 0.75)
                    delay = 0 if is_on_time else max(5, int(np.random.exponential(30)))
                    if np.random.random() < 0.05:
                        delay = max(delay, int(np.random.lognormal(mean=5.5, sigma=0.5)))
                    actual_departure = scheduled_departure + timedelta(minutes=delay)
                    arrival_status = np.random.choice(['on_time', 'early', 'delayed'], p=[0.90, 0.05, 0.05])
                    if arrival_status == 'on_time':
                        actual_arrival = actual_departure + timedelta(minutes=int(duration))
                    elif arrival_status == 'early':
                        early_offset = int(np.random.normal(-10, 5))
                        actual_arrival = actual_departure + timedelta(minutes=int(duration) + early_offset)
                    else:
                        delay = int(np.random.exponential(20))
                        actual_arrival = actual_departure + timedelta(minutes=int(duration) + delay)
                else:
                    actual_departure = None
                    actual_arrival = None

                captain, _ = assign_captain(current_date, origin, daily_captains.get(current_date, set()))
                if captain:
                    daily_captains[current_date].add(captain)
                    for c in captains:
                        if c['name'] == captain:
                            c['location'] = destination
                else:
                    is_canceled = True
                    cancellation_note = 'Crew Delay'
                    actual_departure = None
                    actual_arrival = None

                departure_bay = random.choice(BAYS[route_type])

                data.append({
                    'planning_id': planning_id,
                    'flight_id': flight_id,
                    'route_id': route_id,
                    'scheduled_departure': scheduled_departure,
                    'actual_departure': actual_departure,
                    'scheduled_arrival': scheduled_arrival,
                    'actual_arrival': actual_arrival,
                    'captain': captain,
                    'departure_bay': departure_bay,
                    'cancellation_note': cancellation_note,
                    'base_price_ZAR': base_price,
                    'is_return': False
                })

                if not is_canceled:
                    layover_minutes = random.randint(120, 240)
                    flight_availability[flight_id] = actual_arrival + timedelta(minutes=layover_minutes)
                    flight_locations[flight_id] = destination

                    return_route = get_return_route(route_id, route_pair_id)
                    return_time = (actual_arrival + timedelta(minutes=layover_minutes)).time()
                    available_times = times if destination in ['JNB', 'CPT', 'DUR'] else (REGIONAL_TIMES if destination in ['HRE', 'NBO'] else INTERNATIONAL_TIMES)
                    return_time_str = min([t for t in available_times if datetime.strptime(t, '%H:%M').time() >= return_time], default=None)
                    if return_time_str and return_route['date_effective'] <= current_date:
                        planning_id = f'PLN{planning_id_counter:06d}'
                        planning_id_counter += 1
                        scheduled_departure = datetime.strptime(f'{current_date} {return_time_str}', '%Y-%m-%d %H:%M')
                        scheduled_arrival = scheduled_departure + timedelta(minutes=int(return_route['estimated_duration_min']))

                        is_canceled = np.random.binomial(1, 0.03)
                        cancellation_note = random.choice(CANCELLATION_REASONS) if is_canceled else None

                        if not is_canceled:
                            is_on_time = np.random.binomial(1, 0.75)
                            delay = 0 if is_on_time else max(5, int(np.random.exponential(30)))
                            if np.random.random() < 0.05:
                                delay = max(delay, int(np.random.lognormal(mean=5.5, sigma=0.5)))
                            actual_departure = scheduled_departure + timedelta(minutes=delay)
                            arrival_status = np.random.choice(['on_time', 'early', 'delayed'], p=[0.90, 0.05, 0.05])
                            if arrival_status == 'on_time':
                                actual_arrival = actual_departure + timedelta(minutes=int(return_route['estimated_duration_min']))
                            elif arrival_status == 'early':
                                early_offset = int(np.random.normal(-10, 5))
                                actual_arrival = actual_departure + timedelta(minutes=int(return_route['estimated_duration_min']) + early_offset)
                            else:
                                delay = int(np.random.exponential(20))
                                actual_arrival = actual_departure + timedelta(minutes=int(return_route['estimated_duration_min']) + delay)
                        else:
                            actual_departure = None
                            actual_arrival = None

                        captain, _ = assign_captain(current_date, destination, daily_captains.get(current_date, set()))
                        if captain:
                            daily_captains[current_date].add(captain)
                            for c in captains:
                                if c['name'] == captain:
                                    c['location'] = return_route['destination_IATA']
                        else:
                            is_canceled = True
                            cancellation_note = 'Crew Delay'
                            actual_departure = None
                            actual_arrival = None

                        departure_bay = random.choice(BAYS[return_route['route_type']])

                        data.append({
                            'planning_id': planning_id,
                            'flight_id': flight_id,
                            'route_id': return_route['route_id'],
                            'scheduled_departure': scheduled_departure,
                            'actual_departure': actual_departure,
                            'scheduled_arrival': scheduled_arrival,
                            'actual_arrival': actual_arrival,
                            'captain': captain,
                            'departure_bay': departure_bay,
                            'cancellation_note': cancellation_note,
                            'base_price_ZAR': return_route['base_price_ZAR'],
                            'is_return': True
                        })

                        if not is_canceled:
                            layover_minutes = random.randint(120, 240)
                            flight_availability[flight_id] = actual_arrival + timedelta(minutes=layover_minutes)
                            flight_locations[flight_id] = return_route['destination_IATA']

    return pd.DataFrame(data)

# Generate and save
df_schedules = generate_flight_schedules()
os.makedirs(output_folder, exist_ok=True)
df_schedules.to_parquet(f'{output_folder}/flight_schedules.parquet', index=False)

print(f"Flight schedules dataset saved to '{output_folder}/flight_schedules.parquet'")
print(f"Total records: {len(df_schedules)}")