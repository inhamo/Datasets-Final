import pandas as pd
import random
from datetime import date
import os
from math import radians, sin, cos, sqrt, atan2

random.seed(42)

# SADC countries (simplified for provided airports)
SADC_COUNTRIES = {'South Africa', 'Zimbabwe'}
AFRICAN_COUNTRIES = {'South Africa', 'Zimbabwe', 'Kenya'}

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def random_date(year):
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return date(year, month, day)

airports = {
    'JNB': {'name': 'O.R. Tambo International Airport', 'lat': -26.134789, 'lon': 28.240528, 'country': 'South Africa', 'continent': 'Africa'},
    'CPT': {'name': 'Cape Town International Airport', 'lat': -33.971463, 'lon': 18.602085, 'country': 'South Africa', 'continent': 'Africa'},
    'DUR': {'name': 'King Shaka International Airport', 'lat': -29.614444, 'lon': 31.119722, 'country': 'South Africa', 'continent': 'Africa'},
    'HRE': {'name': 'Robert Gabriel Mugabe International Airport', 'lat': -17.931801, 'lon': 31.092800, 'country': 'Zimbabwe', 'continent': 'Africa'},
    'NBO': {'name': 'Jomo Kenyatta International Airport', 'lat': -1.319240, 'lon': 36.927799, 'country': 'Kenya', 'continent': 'Africa'},
    'LHR': {'name': 'London Heathrow Airport', 'lat': 51.470020, 'lon': -0.454295, 'country': 'United Kingdom', 'continent': 'Europe'},
    'FRA': {'name': 'Frankfurt am Main Airport', 'lat': 50.037933, 'lon': 8.562152, 'country': 'Germany', 'continent': 'Europe'},
    'JFK': {'name': 'John F. Kennedy International Airport', 'lat': 40.641766, 'lon': -73.780968, 'country': 'United States', 'continent': 'North America'},
    'DXB': {'name': 'Dubai International Airport', 'lat': 25.252777, 'lon': 55.364445, 'country': 'United Arab Emirates', 'continent': 'Asia'},
    'SYD': {'name': 'Sydney Kingsford Smith Airport', 'lat': -33.947346, 'lon': 151.179428, 'country': 'Australia', 'continent': 'Oceania'},
}

pairs_by_year = {
    2013: [('JNB', 'CPT'), ('JNB', 'DUR'), ('CPT', 'DUR')],
    2014: [('JNB', 'HRE'), ('CPT', 'HRE'), ('DUR', 'HRE')],
    2015: [('JNB', 'NBO')],
    2016: [('JNB', 'LHR'), ('JNB', 'FRA')],
    2017: [('JNB', 'JFK')],
    2018: [('JNB', 'DXB'), ('JNB', 'SYD')]
}

route_id_counter = 1
pair_id_counter = 1
data = []

for year in range(2013, 2019):
    if year not in pairs_by_year:
        continue
    for pair in pairs_by_year[year]:
        origin, dest = pair
        route_id = f'RTE{route_id_counter:04d}'
        route_id_counter += 1
        route_pair_id = f'PRP{pair_id_counter:04d}'
        date_effective = random_date(year)
        origin_iata = origin
        origin_airport = airports[origin]['name']
        destination_iata = dest
        destination_airport = airports[dest]['name']
        o_country = airports[origin]['country']
        d_country = airports[dest]['country']
        o_cont = airports[origin]['continent']
        d_cont = airports[dest]['continent']

        # Route type
        if o_country == d_country:
            route_type = 'Domestic'
        elif o_country in AFRICAN_COUNTRIES and d_country in AFRICAN_COUNTRIES:
            route_type = 'Regional'
        else:
            route_type = 'International'

        # Direct or connecting
        is_direct = o_cont == d_cont
        is_connecting = not is_direct
        if route_type == 'International':
            if o_cont == 'Africa' and d_cont in ['Europe', 'Asia']:
                is_connecting = True
                is_direct = False
            elif o_cont == 'Africa' and d_cont == 'North America':
                is_connecting = True
                is_direct = False
            elif o_cont == 'Africa' and d_cont == 'Oceania':
                is_connecting = True
                is_direct = False

        # Distance
        lat1, lon1 = airports[origin]['lat'], airports[origin]['lon']
        lat2, lon2 = airports[dest]['lat'], airports[dest]['lon']
        distance = haversine(lat1, lon1, lat2, lon2)

        # Duration
        if (origin, dest) in [('JNB', 'DUR'), ('DUR', 'JNB')]:
            duration = 45
        elif (origin, dest) in [('JNB', 'CPT'), ('CPT', 'JNB'), ('JNB', 'HRE'), ('HRE', 'JNB')]:
            duration = 105
        else:
            speed = 700  # km/h
            duration = round((distance / speed) * 60 + 30)

        # Base price
        if (origin, dest) in [('JNB', 'DUR'), ('DUR', 'JNB')]:
            base_price = random.randint(500, 800)
        else:
            price_per_km = 1.5 if route_type == 'Domestic' else (1.2 if route_type == 'Regional' else 1.0)
            base_price = round(distance * price_per_km)

        data.append({
            'route_id': route_id,
            'route_pair_id': route_pair_id,
            'date_effective': date_effective,
            'origin_IATA': origin_iata,
            'origin_airport': origin_airport,
            'destination_IATA': destination_iata,
            'destination_airport': destination_airport,
            'is_direct': is_direct,
            'is_connecting': is_connecting,
            'route_type': route_type,
            'distance_km': round(distance),
            'estimated_duration_min': duration,
            'base_price_ZAR': base_price,
        })

        # Reverse route
        route_id = f'RTE{route_id_counter:04d}'
        route_id_counter += 1
        data.append({
            'route_id': route_id,
            'route_pair_id': route_pair_id,
            'date_effective': date_effective,
            'origin_IATA': destination_iata,
            'origin_airport': destination_airport,
            'destination_IATA': origin_iata,
            'destination_airport': origin_airport,
            'is_direct': is_direct,
            'is_connecting': is_connecting,
            'route_type': route_type,
            'distance_km': round(distance),
            'estimated_duration_min': duration,
            'base_price_ZAR': base_price,
        })
        pair_id_counter += 1

df = pd.DataFrame(data)
output_folder = 'airline_data'
os.makedirs(output_folder, exist_ok=True)
df.to_parquet(f'{output_folder}/routes.parquet', index=False)

print(f"Routes dataset saved to '{output_folder}/routes.parquet'")
print(f"Total records: {len(df)}")