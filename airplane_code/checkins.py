import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from typing import Dict, List, Optional, Set, Tuple
import uuid
from tqdm import tqdm
from faker import Faker
import warnings
import os
warnings.filterwarnings('ignore')

class RealisticCheckInsGenerator:
    def __init__(self, target_year: int = 2021):
        """
        Initialize with target year and realistic check-in parameters.
        
        Args:
            target_year (int): Year to generate check-ins for
        """
        self.TARGET_YEAR = target_year
        
        # Load data
        try:
            self.bookings_df = pd.read_parquet(f'airplane_data/bookings_{target_year}.parquet')
            self.clients_df = pd.read_parquet(f'airplane_data/clients_{target_year}.parquet')
            self.flight_schedule_df = pd.read_parquet(f'airplane_data/flight_schedule_{target_year}.parquet')
            self.routes_df = pd.read_parquet(f'airplane_data/routes_{target_year}.parquet')
            self.planes_df = pd.read_parquet(f'airplane_data/planes_{target_year}.parquet')
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Missing data file for {target_year}: {str(e)}")
        
        # REALISTIC check-in rates - industry standard
        self.realistic_checkin_rates = {
            'domestic_short': 0.92,    # Domestic flights under 2 hours
            'domestic_medium': 0.90,   # Domestic flights 2-4 hours  
            'domestic_long': 0.88,     # Domestic flights over 4 hours
            'international': 0.85,     # International flights
            'business_class': 0.95,    # Business class has higher show rates
            'group_bookings': 0.98     # Group bookings have very high show rates
        }
        
        # Realistic seat configurations
        self.seat_configs = {
            'Boeing 737-800': {'rows': 32, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 'business_rows': 4},
            'Boeing 737-400': {'rows': 25, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 'business_rows': 3},
            'Airbus A320': {'rows': 30, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 'business_rows': 4},
            'Airbus A319': {'rows': 26, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 'business_rows': 3},
            'Embraer E190': {'rows': 25, 'layout': ['A', 'C', 'D', 'F'], 'business_rows': 5},
            'Embraer E170': {'rows': 19, 'layout': ['A', 'C', 'D', 'F'], 'business_rows': 4},
            'ATR 72': {'rows': 18, 'layout': ['A', 'C', 'D', 'F'], 'business_rows': 0},
            'Bombardier Q400': {'rows': 20, 'layout': ['A', 'C', 'D', 'F'], 'business_rows': 0},
            'default': {'rows': 25, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 'business_rows': 3}
        }
        
        # Initialize Faker for realistic names
        self.faker = Faker(['en_US', 'en_GB', 'zu_ZA'])
        
        # Prepare data
        self._prepare_data()

    def _prepare_data(self):
        """Prepare and merge datasets."""
        # Convert date columns
        self.bookings_df['booking_date'] = pd.to_datetime(self.bookings_df['booking_date'])
        self.flight_schedule_df['scheduled_departure'] = pd.to_datetime(self.flight_schedule_df['scheduled_departure'])
        self.clients_df['date_of_registration'] = pd.to_datetime(self.clients_df['date_of_registration'])
        self.clients_df['dob'] = pd.to_datetime(self.clients_df['dob'])
        
        # Filter valid bookings (only confirmed bookings should generate check-ins)
        self.valid_bookings = self.bookings_df[
            self.bookings_df['booking_status'] == 'confirmed'
        ].copy()
        
        # Extract actual passenger count from bookings
        self.valid_bookings['actual_passenger_count'] = (
            self.valid_bookings['num_adults'] + self.valid_bookings['num_children']
        )
        
        # Merge with flight data
        self.flight_data = self.flight_schedule_df.merge(
            self.routes_df[['route_id', 'origin_airport', 'destination_airport', 'origin_city', 'destination_city']],
            on='route_id', how='left'
        ).merge(
            self.planes_df[['plane_id', 'aircraft_model', 'capacity']],
            on='plane_id', how='left'
        )
        
        # Merge bookings with flight data
        self.checkin_data = self.valid_bookings.merge(
            self.flight_data[['planning_id', 'route_id', 'plane_id', 'scheduled_departure', 
                             'origin_airport', 'destination_airport', 'origin_city', 
                             'destination_city', 'aircraft_model', 'capacity']],
            on='planning_id', how='left'
        )
        
        # Clean data
        self.checkin_data['aircraft_type'] = self.checkin_data['aircraft_model'].fillna('default')
        self.checkin_data['aircraft_capacity'] = self.checkin_data['capacity'].fillna(150)
        
        # Remove invalid records
        self.checkin_data = self.checkin_data.dropna(subset=['planning_id', 'scheduled_departure'])
        
        # Create customer lookup for names
        self.customer_names = dict(zip(self.clients_df['client_id'], self.clients_df['name']))
        
        # Calculate flight statistics
        passengers_per_flight = self.checkin_data.groupby('planning_id')['actual_passenger_count'].sum()
        
        print(f"Data prepared for {self.TARGET_YEAR}:")
        print(f"- {len(self.valid_bookings):,} confirmed bookings")
        print(f"- {len(self.checkin_data):,} check-in eligible records")
        print(f"- {self.checkin_data['planning_id'].nunique()} unique flights")
        print(f"- Average passengers per flight: {passengers_per_flight.mean():.1f}")
        print(f"- Min passengers per flight: {passengers_per_flight.min()}")
        print(f"- Max passengers per flight: {passengers_per_flight.max()}")

    def _determine_flight_type(self, origin: str, destination: str, scheduled_departure: datetime) -> str:
        """Determine flight type for realistic check-in rates."""
        domestic_airports = {'JNB', 'CPT', 'DBN', 'PLZ', 'BFN', 'ELS', 'GRJ', 'HDS', 'KIM', 'MQP', 'NTY', 'PZB', 'SBU', 'UTN', 'WEL'}
        
        origin_domestic = any(airport in str(origin) for airport in domestic_airports)
        dest_domestic = any(airport in str(destination) for airport in domestic_airports)
        
        if origin_domestic and dest_domestic:
            if random.random() < 0.6:  # 60% short domestic
                return 'domestic_short'
            elif random.random() < 0.8:  # 20% medium domestic
                return 'domestic_medium'
            else:  # 20% long domestic
                return 'domestic_long'
        else:
            return 'international'

    def _calculate_realistic_checkin_probability(self, booking: pd.Series) -> float:
        """Calculate realistic check-in probability based on multiple factors."""
        flight_type = self._determine_flight_type(
            booking['origin_airport'], 
            booking['destination_airport'], 
            booking['scheduled_departure']
        )
        
        base_prob = self.realistic_checkin_rates[flight_type]
        
        if booking['booking_class'] == 'business':
            base_prob = max(base_prob, self.realistic_checkin_rates['business_class'])
        
        if booking['group_booking_type'] != 'individual' or booking['num_adults'] >= 4:
            base_prob = max(base_prob, self.realistic_checkin_rates['group_bookings'])
        
        days_in_advance = (booking['scheduled_departure'] - booking['booking_date']).days
        if days_in_advance > 30:
            base_prob += 0.02
        elif days_in_advance < 2:
            base_prob -= 0.05
        
        if booking.get('is_special_needs', False) or booking.get('is_assisted', False):
            base_prob += 0.03
        
        return max(0.75, min(0.98, base_prob))

    def _generate_realistic_checkin_time(self, scheduled_departure: datetime, booking_class: str) -> datetime:
        """Generate realistic check-in time."""
        if booking_class == 'business':
            if random.random() < 0.7:  # 70% online check-in
                hours_before = random.uniform(2, 18)
            else:  # Airport check-in
                hours_before = random.uniform(1, 2.5)
        else:
            if random.random() < 0.8:  # 80% online check-in
                hours_before = random.uniform(4, 24)
            else:  # Airport check-in
                hours_before = random.uniform(1.5, 3)
        
        return scheduled_departure - timedelta(hours=hours_before)

    def _create_seat_map(self, aircraft_type: str, capacity: int) -> Dict[str, bool]:
        """Create seat map for aircraft."""
        config = self.seat_configs.get(aircraft_type, self.seat_configs['default'])
        seat_map = {}
        
        for row in range(1, config['rows'] + 1):
            for letter in config['layout']:
                seat = f"{row}{letter}"
                seat_map[seat] = True
        
        total_seats = len(seat_map)
        blocked_count = max(1, int(total_seats * 0.02))
        available_seats = list(seat_map.keys())
        blocked = random.sample(available_seats, min(blocked_count, len(available_seats)))
        
        for seat in blocked:
            seat_map[seat] = False
        
        return seat_map

    def _assign_seat(self, seat_map: Dict[str, bool], aircraft_type: str, booking_class: str, passenger_type: str) -> Optional[str]:
        """Assign seat with realistic logic."""
        config = self.seat_configs.get(aircraft_type, self.seat_configs['default'])
        available_seats = [seat for seat, available in seat_map.items() if available]
        
        if not available_seats:
            return None
        
        if booking_class == 'business' and config['business_rows'] > 0:
            business_seats = [s for s in available_seats if int(s[:-1]) <= config['business_rows']]
            if business_seats:
                available_seats = business_seats
        else:
            economy_seats = [s for s in available_seats if int(s[:-1]) > config['business_rows']]
            if economy_seats:
                available_seats = economy_seats
        
        if not available_seats:
            return None
        
        available_seats.sort(key=lambda x: (int(x[:-1]), x[-1]))
        return available_seats[0]

    def _assign_luggage(self, passenger_type: str, booking_class: str) -> Tuple[float, float]:
        """Assign realistic luggage weights."""
        if passenger_type == 'infant':
            weight = random.uniform(0, 8)
            max_weight = 10
        elif passenger_type == 'child':
            weight = random.uniform(8, 20)
            max_weight = 32 if booking_class == 'business' else 23
        else:  # adult
            weight = random.uniform(12, 28)
            max_weight = 32 if booking_class == 'business' else 23
        
        if random.random() < 0.15:
            weight *= 0.4
        elif random.random() < 0.20:
            weight = max_weight * random.uniform(0.85, 0.98)
        
        return round(weight, 2), max_weight

    def _generate_gate(self, origin_airport: str, aircraft_type: str) -> str:
        """Generate realistic gate assignment."""
        large_airports = ['JNB', 'CPT', 'DBN', 'PLZ']
        
        if origin_airport in large_airports:
            gates = [f"{letter}{num}" for letter in ['A', 'B', 'C'] for num in range(1, 25)]
        else:
            gates = [f"A{num}" for num in range(1, 12)]
        
        return random.choice(gates)

    def _generate_passenger_name(self, customer_id: str, passenger_idx: int, passenger_type: str, group_type: str) -> str:
        """Generate realistic passenger name."""
        main_name = self.customer_names.get(customer_id, f"Customer_{customer_id}")
        
        if passenger_idx == 0 and passenger_type != 'infant':
            return main_name
        
        if passenger_type == 'infant':
            surname = main_name.split()[-1] if ' ' in main_name else main_name
            return f"Infant {surname}"
        
        if group_type == 'family':
            if ' ' in main_name:
                surname = main_name.split()[-1]
                return f"{self.faker.first_name()} {surname}"
            else:
                return self.faker.name()
        else:
            return self.faker.name()

    def generate_checkins(self):
        """Generate realistic check-ins with 100% check-in rate for flights with ≤75 passengers."""
        print(f"Generating REALISTIC check-ins for {self.TARGET_YEAR}")
        print("Realistic features:")
        print("- Industry-standard check-in rates (85-98%) for flights with >75 passengers")
        print("- 100% check-in rate for flights with ≤75 passengers (excluding infants)")
        print("- Business class higher show rates")
        print("- Group booking higher show rates")
        print("- Proper seat allocation with blocking")
        print("- Realistic luggage distributions")
        
        checkins = []
        checkin_counter = 1
        flight_seat_maps = {}
        flight_gates = {}
        
        flight_groups = self.checkin_data.groupby('planning_id')
        
        for planning_id, flight_bookings in tqdm(flight_groups, desc="Processing flights"):
            if len(flight_bookings) == 0:
                continue
            
            first_booking = flight_bookings.iloc[0]
            aircraft_type = first_booking['aircraft_type']
            aircraft_capacity = int(first_booking['aircraft_capacity'])
            scheduled_departure = first_booking['scheduled_departure']
            origin_airport = first_booking['origin_airport']
            
            flight_seat_maps[planning_id] = self._create_seat_map(aircraft_type, aircraft_capacity)
            flight_gates[planning_id] = self._generate_gate(origin_airport, aircraft_type)
            
            total_passengers = flight_bookings['actual_passenger_count'].sum()
            total_with_infants = total_passengers + flight_bookings['num_infants'].sum()
            
            is_small_flight = total_passengers <= 75
            
            checked_in_passengers = 0
            
            available_seats = sum(1 for seat, available in flight_seat_maps[planning_id].items() if available)
            if is_small_flight and available_seats < total_passengers:
                print(f"WARNING: Flight {planning_id} has {total_passengers} passengers but only {available_seats} seats available! Adjusting capacity.")
                aircraft_capacity = max(aircraft_capacity, total_passengers)
                flight_seat_maps[planning_id] = self._create_seat_map(aircraft_type, aircraft_capacity)
            
            for _, booking in flight_bookings.iterrows():
                checkin_probability = self._calculate_realistic_checkin_probability(booking)
                
                passengers = []
                for i in range(booking['num_adults']):
                    passengers.append(('adult', i))
                for i in range(booking['num_children']):
                    passengers.append(('child', i + booking['num_adults']))
                for i in range(booking['num_infants']):
                    passengers.append(('infant', i + booking['num_adults'] + booking['num_children']))
                
                for passenger_type, passenger_idx in passengers:
                    if is_small_flight and passenger_type != 'infant':
                        checkin_status = 'checked_in'
                    else:
                        will_checkin = random.random() < checkin_probability
                        checkin_status = 'checked_in' if will_checkin else ('no_show' if random.random() < 0.8 else 'cancelled')
                    
                    passenger_name = self._generate_passenger_name(
                        booking['customer_id'], passenger_idx, passenger_type, 
                        booking.get('group_booking_type', 'individual')
                    )
                    
                    checkin_time = self._generate_realistic_checkin_time(
                        scheduled_departure, booking['booking_class']
                    )
                    
                    seat_allocation = None
                    if checkin_status == 'checked_in':
                        if passenger_type == 'infant':
                            seat_allocation = 'Lap'
                        else:
                            seat_allocation = self._assign_seat(
                                flight_seat_maps[planning_id], aircraft_type,
                                booking['booking_class'], passenger_type
                            )
                            if seat_allocation:
                                flight_seat_maps[planning_id][seat_allocation] = False
                            elif is_small_flight:
                                print(f"ERROR: Flight {planning_id} (small flight) has no seat for passenger {passenger_name}! Forcing seat assignment.")
                                seat_allocation = self._assign_seat(
                                    flight_seat_maps[planning_id], aircraft_type,
                                    booking['booking_class'], passenger_type
                                ) or f"Emergency_{checkin_counter}"
                                flight_seat_maps[planning_id][seat_allocation] = False
                            else:
                                checkin_status = 'denied_boarding'
                    
                    if checkin_status == 'checked_in' and passenger_type != 'infant':
                        checked_in_passengers += 1
                    
                    luggage, max_luggage = self._assign_luggage(passenger_type, booking['booking_class'])
                    
                    checkin = {
                        'checkin_id': f"CI{self.TARGET_YEAR}{checkin_counter:06d}",
                        'booking_id': booking['booking_id'],
                        'planning_id': planning_id,
                        'customer_id': booking['customer_id'],
                        'passenger_name': passenger_name,
                        'passenger_type': passenger_type,
                        'checkin_status': checkin_status,
                        'gate_number': flight_gates[planning_id],
                        'seat_allocation': seat_allocation,
                        'max_luggage': max_luggage,
                        'checkin_luggage': luggage,
                        'checkin_time': checkin_time,
                        'booking_class': booking['booking_class'],
                        'group_booking_type': booking.get('group_booking_type', 'individual'),
                        'total_flight_passengers': total_passengers
                    }
                    
                    checkins.append(checkin)
                    checkin_counter += 1
            
            if is_small_flight and checked_in_passengers != total_passengers:
                print(f"ERROR: Flight {planning_id} has {total_passengers} passengers but only {checked_in_passengers} checked in!")
        
        checkins_df = pd.DataFrame(checkins)
        
        checkins_df['checkin_status'] = checkins_df['checkin_status'].astype('category')
        checkins_df['passenger_type'] = checkins_df['passenger_type'].astype('category')
        checkins_df['booking_class'] = checkins_df['booking_class'].astype('category')
        checkins_df['group_booking_type'] = checkins_df['group_booking_type'].astype('category')
        checkins_df['gate_number'] = checkins_df['gate_number'].astype('category')
        checkins_df['checkin_time'] = pd.to_datetime(checkins_df['checkin_time'])
        
        checked_in_by_flight = checkins_df[
            (checkins_df['checkin_status'] == 'checked_in') & 
            (checkins_df['passenger_type'] != 'infant')
        ].groupby('planning_id').size()
        
        total_passengers_by_flight = checkins_df[
            checkins_df['passenger_type'] != 'infant'
        ].groupby('planning_id').size()
        
        checkin_rates = checked_in_by_flight / total_passengers_by_flight
        
        small_flights = checkins_df[
            (checkins_df['total_flight_passengers'] <= 75) &
            (checkins_df['passenger_type'] != 'infant')
        ].groupby('planning_id').agg({
            'checkin_status': lambda x: (x == 'checked_in').all(),
            'total_flight_passengers': 'first',
            'checkin_status': lambda x: x.value_counts().to_dict()
        })
        
        non_compliant_small_flights = small_flights[small_flights['checkin_status'] != {'checked_in': small_flights['total_flight_passengers']}]
        
        print(f"\n=== REALISTIC CHECK-IN GENERATION COMPLETE ===")
        print(f"Total check-ins generated: {len(checkins_df):,}")
        print(f"Unique bookings: {checkins_df['booking_id'].nunique():,}")
        print(f"Unique customers: {checkins_df['customer_id'].nunique():,}")
        print(f"Unique flights: {checkins_df['planning_id'].nunique():,}")
        
        print(f"\nCheck-in status distribution:")
        for status, count in checkins_df['checkin_status'].value_counts().items():
            pct = count / len(checkins_df)
            print(f"  {status}: {count:,} ({pct:.1%})")
        
        print(f"\nCheck-in rate statistics (excluding infants):")
        print(f"  Average check-in rate: {checkin_rates.mean():.1%}")
        print(f"  Minimum check-in rate: {checkin_rates.min():.1%}")
        print(f"  Maximum check-in rate: {checkin_rates.max():.1%}")
        
        if not non_compliant_small_flights.empty:
            print(f"\nERROR: {len(non_compliant_small_flights)} small flights (≤75 passengers) do not have 100% check-in rates:")
            for planning_id, row in non_compliant_small_flights.iterrows():
                print(f"  Flight {planning_id}: {row['total_flight_passengers']} passengers, Status counts: {row['checkin_status']}")
        else:
            print(f"\nSUCCESS: All flights with ≤75 passengers have 100% check-in rates")
        
        low_checkin_flights = checkin_rates[(checkin_rates < 0.75) & (checkins_df.groupby('planning_id')['total_flight_passengers'].first() > 75)]
        if not low_checkin_flights.empty:
            print(f"\nWARNING: {len(low_checkin_flights)} flights with >75 passengers have <75% check-in rate")
        else:
            print(f"\nSUCCESS: All flights with >75 passengers have realistic check-in rates (≥75%)")
        
        min_passengers_per_flight = checked_in_by_flight.min() if not checked_in_by_flight.empty else 0
        print(f"\nMinimum checked-in passengers on any flight: {min_passengers_per_flight}")
        
        checked_in_with_seats = checkins_df[
            (checkins_df['checkin_status'] == 'checked_in') & 
            (checkins_df['seat_allocation'].notna()) &
            (checkins_df['seat_allocation'] != 'Lap')
        ]
        
        seat_conflicts = checked_in_with_seats.groupby(['planning_id', 'seat_allocation']).size()
        conflicts = seat_conflicts[seat_conflicts > 1]
        
        if len(conflicts) > 0:
            print(f"\nWARNING: {len(conflicts)} seat conflicts detected!")
        else:
            print(f"\nSUCCESS: No seat conflicts detected!")
        
        output_path = f'airplane_data/checkins_{self.TARGET_YEAR}.parquet'
        checkins_df.to_parquet(output_path, index=False)
        print(f"\nCheck-ins data saved to {output_path}")
        
        return checkins_df

def generate_realistic_checkins(target_year: int = 2021):
    """Generate realistic check-ins with proper industry-standard rates."""
    print(f"Starting realistic check-ins generation for {target_year}")
    print("Key improvements:")
    print("- Industry-standard check-in rates (85-98%)")
    print("- 100% check-in rate for flights with ≤75 passengers")
    print("- No more unrealistic low check-in rates")
    print("-" * 50)
    
    try:
        generator = RealisticCheckInsGenerator(target_year=target_year)
        checkins_df = generator.generate_checkins()
        
        print(f"\nSuccessfully generated realistic check-ins for {target_year}!")
        return checkins_df
        
    except Exception as e:
        print(f"Error generating check-ins: {str(e)}")
        raise

if __name__ == "__main__":
    seed_bytes = os.urandom(4)
    seed_int = int.from_bytes(seed_bytes, byteorder='big')
    random.seed(seed_int)
    np.random.seed(seed_int)
    
    TARGET_YEAR = 2021
    checkins = generate_realistic_checkins(target_year=TARGET_YEAR)
    
    if not checkins.empty:
        print(f"\nSample check-ins:")
        print(checkins.head())
        
        checked_in_by_flight = checkins[
            (checkins['checkin_status'] == 'checked_in') & 
            (checkins['passenger_type'] != 'infant')
        ].groupby('planning_id').size()
        
        total_by_flight = checkins[
            checkins['passenger_type'] != 'infant'
        ].groupby('planning_id').size()
        
        checkin_rates = checked_in_by_flight / total_by_flight
        
        print(f"\nRealistic check-in rates achieved:")
        print(f"Flights with 85%+ check-in rate: {(checkin_rates >= 0.85).sum()} of {len(checkin_rates)}")
        print(f"Flights with 90%+ check-in rate: {(checkin_rates >= 0.90).sum()} of {len(checkin_rates)}")
        print(f"Flights with 95%+ check-in rate: {(checkin_rates >= 0.95).sum()} of {len(checkin_rates)}")