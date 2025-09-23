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

class Minimum75CheckInsGenerator:
    def __init__(self, target_year: int = 2021):
        """
        Initialize generator that ensures MINIMUM 75 check-ins per flight.
        
        Args:
            target_year (int): Year to generate check-ins for
        """
        self.TARGET_YEAR = target_year
        self.MIN_CHECKINS_PER_FLIGHT = 75
        
        # Load data
        try:
            self.bookings_df = pd.read_parquet(f'airplane_data/bookings_{target_year}.parquet')
            self.clients_df = pd.read_parquet(f'airplane_data/clients_{target_year}.parquet')
            self.flight_schedule_df = pd.read_parquet(f'airplane_data/flight_schedule_{target_year}.parquet')
            self.routes_df = pd.read_parquet(f'airplane_data/routes_{target_year}.parquet')
            self.planes_df = pd.read_parquet(f'airplane_data/planes_{target_year}.parquet')
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Missing data file for {target_year}: {str(e)}")
        
        # Seat configurations for different aircraft
        self.seat_configs = {
            'Boeing 737-800': {'rows': 32, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 'business_rows': 4},
            'Boeing 737-400': {'rows': 25, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 'business_rows': 3},
            'Airbus A320': {'rows': 30, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 'business_rows': 4},
            'Airbus A319': {'rows': 26, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 'business_rows': 3},
            'Embraer E190': {'rows': 25, 'layout': ['A', 'C', 'D', 'F'], 'business_rows': 5},
            'Embraer E170': {'rows': 19, 'layout': ['A', 'C', 'D', 'F'], 'business_rows': 4},
            'ATR 72': {'rows': 18, 'layout': ['A', 'C', 'D', 'F'], 'business_rows': 0},
            'Bombardier Q400': {'rows': 20, 'layout': ['A', 'C', 'D', 'F'], 'business_rows': 0},
            'default': {'rows': 35, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 'business_rows': 5}  # Larger default
        }
        
        # Initialize Faker for generating names
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
        
        # Get all bookings (including cancelled ones for potential check-ins)
        self.valid_bookings = self.bookings_df.copy()
        
        # Calculate passenger count
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
        self.checkin_data['aircraft_capacity'] = self.checkin_data['capacity'].fillna(200)  # Larger default
        
        # Remove invalid records
        self.checkin_data = self.checkin_data.dropna(subset=['planning_id', 'scheduled_departure'])
        
        # Create customer lookup for names
        self.customer_names = dict(zip(self.clients_df['client_id'], self.clients_df['name']))
        
        # Get unique flights
        self.unique_flights = self.checkin_data['planning_id'].unique()
        
        print(f"Data prepared for {self.TARGET_YEAR}:")
        print(f"- {len(self.valid_bookings):,} total bookings")
        print(f"- {len(self.checkin_data):,} booking records with flight data")
        print(f"- {len(self.unique_flights)} unique flights")
        print(f"- MINIMUM {self.MIN_CHECKINS_PER_FLIGHT} check-ins will be generated per flight")

    def _create_expanded_seat_map(self, aircraft_type: str, min_capacity: int = 75) -> Dict[str, bool]:
        """Create seat map with enough seats for minimum passengers."""
        config = self.seat_configs.get(aircraft_type, self.seat_configs['default'])
        
        # Calculate how many rows we need for minimum capacity
        seats_per_row = len(config['layout'])
        min_rows = max(config['rows'], (min_capacity + seats_per_row - 1) // seats_per_row)
        
        seat_map = {}
        for row in range(1, min_rows + 1):
            for letter in config['layout']:
                seat = f"{row}{letter}"
                seat_map[seat] = True
        
        # Block 2% of seats randomly (but ensure we still have enough)
        total_seats = len(seat_map)
        max_blocked = max(0, total_seats - min_capacity - 5)  # Leave buffer
        blocked_count = min(max_blocked, int(total_seats * 0.02))
        
        if blocked_count > 0:
            available_seats = list(seat_map.keys())
            blocked = random.sample(available_seats, blocked_count)
            for seat in blocked:
                seat_map[seat] = False
        
        return seat_map

    def _assign_seat(self, seat_map: Dict[str, bool], aircraft_type: str, booking_class: str) -> str:
        """Assign seat, creating new ones if needed."""
        config = self.seat_configs.get(aircraft_type, self.seat_configs['default'])
        available_seats = [seat for seat, available in seat_map.items() if available]
        
        # If no seats available, create emergency seats
        if not available_seats:
            emergency_seat = f"E{len(seat_map) + 1}A"
            seat_map[emergency_seat] = False
            return emergency_seat
        
        # Try to assign appropriate class seats
        if booking_class == 'business' and config['business_rows'] > 0:
            business_seats = [s for s in available_seats if int(s[:-1]) <= config['business_rows']]
            if business_seats:
                seat = business_seats[0]
                seat_map[seat] = False
                return seat
        
        # Assign any available seat
        seat = available_seats[0]
        seat_map[seat] = False
        return seat

    def _generate_realistic_checkin_time(self, scheduled_departure: datetime) -> datetime:
        """Generate realistic check-in time."""
        if random.random() < 0.75:  # 75% online check-in
            hours_before = random.uniform(2, 24)
        else:  # Airport check-in
            hours_before = random.uniform(1, 3)
        
        return scheduled_departure - timedelta(hours=hours_before)

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
        
        # Some passengers have lighter or heavier luggage
        if random.random() < 0.15:
            weight *= 0.4
        elif random.random() < 0.10:
            weight = max_weight * random.uniform(0.85, 0.98)
        
        return round(weight, 2), max_weight

    def _generate_gate(self, origin_airport: str) -> str:
        """Generate gate assignment."""
        large_airports = ['JNB', 'CPT', 'DBN', 'PLZ']
        
        if origin_airport in large_airports:
            gates = [f"{letter}{num}" for letter in ['A', 'B', 'C'] for num in range(1, 30)]
        else:
            gates = [f"A{num}" for num in range(1, 15)]
        
        return random.choice(gates)

    def _create_synthetic_passenger(self, flight_info: Dict, passenger_idx: int) -> Dict:
        """Create a synthetic passenger for flights needing more check-ins."""
        passenger_types = ['adult'] * 85 + ['child'] * 14 + ['infant'] * 1
        passenger_type = random.choice(passenger_types)
        
        booking_classes = ['economy'] * 85 + ['business'] * 15
        booking_class = random.choice(booking_classes)
        
        # Generate synthetic booking ID
        synthetic_booking_id = f"SYN{self.TARGET_YEAR}{passenger_idx:06d}"
        synthetic_customer_id = f"CUST{self.TARGET_YEAR}{passenger_idx:05d}"
        
        return {
            'booking_id': synthetic_booking_id,
            'customer_id': synthetic_customer_id,
            'passenger_name': self.faker.name(),
            'passenger_type': passenger_type,
            'booking_class': booking_class,
            'group_booking_type': 'individual',
            'scheduled_departure': flight_info['scheduled_departure'],
            'origin_airport': flight_info['origin_airport'],
            'destination_airport': flight_info['destination_airport'],
            'aircraft_type': flight_info['aircraft_type'],
            'aircraft_capacity': flight_info['aircraft_capacity']
        }

    def generate_checkins(self):
        """Generate check-ins ensuring minimum 75 per flight."""
        print(f"Generating check-ins with MINIMUM {self.MIN_CHECKINS_PER_FLIGHT} per flight for {self.TARGET_YEAR}")
        print("Strategy:")
        print("- All existing passengers check in")
        print("- Generate synthetic passengers to reach minimum 75 per flight")
        print("- Expand aircraft capacity as needed")
        print("- Assign seats to all checked-in passengers")
        
        checkins = []
        checkin_counter = 1
        flight_seat_maps = {}
        flight_gates = {}
        
        for planning_id in tqdm(self.unique_flights, desc="Processing flights"):
            flight_bookings = self.checkin_data[self.checkin_data['planning_id'] == planning_id]
            
            if len(flight_bookings) == 0:
                continue
            
            first_booking = flight_bookings.iloc[0]
            flight_info = {
                'planning_id': planning_id,
                'aircraft_type': first_booking['aircraft_type'],
                'aircraft_capacity': max(int(first_booking['aircraft_capacity']), self.MIN_CHECKINS_PER_FLIGHT + 10),
                'scheduled_departure': first_booking['scheduled_departure'],
                'origin_airport': first_booking['origin_airport'],
                'destination_airport': first_booking['destination_airport']
            }
            
            # Create seat map and gate
            flight_seat_maps[planning_id] = self._create_expanded_seat_map(
                flight_info['aircraft_type'], 
                self.MIN_CHECKINS_PER_FLIGHT + 20  # Buffer for infants and extras
            )
            flight_gates[planning_id] = self._generate_gate(flight_info['origin_airport'])
            
            flight_passengers = []
            
            # Add all existing passengers (they all check in)
            for _, booking in flight_bookings.iterrows():
                passengers_in_booking = []
                
                # Adults
                for i in range(booking['num_adults']):
                    passengers_in_booking.append({
                        'booking_id': booking['booking_id'],
                        'customer_id': booking['customer_id'],
                        'passenger_name': self.customer_names.get(booking['customer_id'], f"Customer_{booking['customer_id']}") if i == 0 else self.faker.name(),
                        'passenger_type': 'adult',
                        'booking_class': booking['booking_class'],
                        'group_booking_type': booking.get('group_booking_type', 'individual'),
                        'scheduled_departure': booking['scheduled_departure'],
                        'origin_airport': booking['origin_airport'],
                        'destination_airport': booking['destination_airport'],
                        'aircraft_type': booking['aircraft_type'],
                        'aircraft_capacity': flight_info['aircraft_capacity']
                    })
                
                # Children
                for i in range(booking['num_children']):
                    passengers_in_booking.append({
                        'booking_id': booking['booking_id'],
                        'customer_id': booking['customer_id'],
                        'passenger_name': self.faker.name(),
                        'passenger_type': 'child',
                        'booking_class': booking['booking_class'],
                        'group_booking_type': booking.get('group_booking_type', 'individual'),
                        'scheduled_departure': booking['scheduled_departure'],
                        'origin_airport': booking['origin_airport'],
                        'destination_airport': booking['destination_airport'],
                        'aircraft_type': booking['aircraft_type'],
                        'aircraft_capacity': flight_info['aircraft_capacity']
                    })
                
                # Infants
                for i in range(booking['num_infants']):
                    passengers_in_booking.append({
                        'booking_id': booking['booking_id'],
                        'customer_id': booking['customer_id'],
                        'passenger_name': f"Infant {self.faker.last_name()}",
                        'passenger_type': 'infant',
                        'booking_class': booking['booking_class'],
                        'group_booking_type': booking.get('group_booking_type', 'individual'),
                        'scheduled_departure': booking['scheduled_departure'],
                        'origin_airport': booking['origin_airport'],
                        'destination_airport': booking['destination_airport'],
                        'aircraft_type': booking['aircraft_type'],
                        'aircraft_capacity': flight_info['aircraft_capacity']
                    })
                
                flight_passengers.extend(passengers_in_booking)
            
            # Count non-infant passengers
            non_infant_passengers = [p for p in flight_passengers if p['passenger_type'] != 'infant']
            current_count = len(non_infant_passengers)
            
            # Generate synthetic passengers if needed
            if current_count < self.MIN_CHECKINS_PER_FLIGHT:
                needed = self.MIN_CHECKINS_PER_FLIGHT - current_count
                print(f"Flight {planning_id}: Adding {needed} synthetic passengers ({current_count} -> {self.MIN_CHECKINS_PER_FLIGHT})")
                
                for i in range(needed):
                    synthetic_passenger = self._create_synthetic_passenger(flight_info, checkin_counter + i)
                    flight_passengers.append(synthetic_passenger)
            
            # Generate check-ins for all passengers
            for passenger in flight_passengers:
                checkin_time = self._generate_realistic_checkin_time(passenger['scheduled_departure'])
                
                # All passengers check in (infants go on laps)
                if passenger['passenger_type'] == 'infant':
                    seat_allocation = 'Lap'
                else:
                    seat_allocation = self._assign_seat(
                        flight_seat_maps[planning_id],
                        passenger['aircraft_type'],
                        passenger['booking_class']
                    )
                
                luggage, max_luggage = self._assign_luggage(
                    passenger['passenger_type'],
                    passenger['booking_class']
                )
                
                checkin = {
                    'checkin_id': f"CI{self.TARGET_YEAR}{checkin_counter:06d}",
                    'booking_id': passenger['booking_id'],
                    'planning_id': planning_id,
                    'customer_id': passenger['customer_id'],
                    'passenger_name': passenger['passenger_name'],
                    'passenger_type': passenger['passenger_type'],
                    'checkin_status': 'checked_in',
                    'gate_number': flight_gates[planning_id],
                    'seat_allocation': seat_allocation,
                    'max_luggage': max_luggage,
                    'checkin_luggage': luggage,
                    'checkin_time': checkin_time,
                    'booking_class': passenger['booking_class'],
                    'group_booking_type': passenger['group_booking_type'],
                    'total_flight_passengers': len([p for p in flight_passengers if p['passenger_type'] != 'infant']),
                    'aircraft_type': passenger['aircraft_type']
                }
                
                checkins.append(checkin)
                checkin_counter += 1
        
        # Convert to DataFrame
        checkins_df = pd.DataFrame(checkins)
        
        # Optimize data types
        checkins_df['checkin_status'] = checkins_df['checkin_status'].astype('category')
        checkins_df['passenger_type'] = checkins_df['passenger_type'].astype('category')
        checkins_df['booking_class'] = checkins_df['booking_class'].astype('category')
        checkins_df['group_booking_type'] = checkins_df['group_booking_type'].astype('category')
        checkins_df['gate_number'] = checkins_df['gate_number'].astype('category')
        checkins_df['checkin_time'] = pd.to_datetime(checkins_df['checkin_time'])
        
        self._validate_and_report_results(checkins_df)
        
        # Save to file
        output_path = f'airplane_data/checkins_{self.TARGET_YEAR}.parquet'
        checkins_df.to_parquet(output_path, index=False)
        print(f"\nCheck-ins data saved to {output_path}")
        
        return checkins_df

    def _validate_and_report_results(self, checkins_df: pd.DataFrame):
        """Validate and report results."""
        # Count non-infant checked-in passengers per flight
        non_infant_checkins = checkins_df[
            (checkins_df['checkin_status'] == 'checked_in') & 
            (checkins_df['passenger_type'] != 'infant')
        ]
        
        checkins_per_flight = non_infant_checkins.groupby('planning_id').size()
        
        print(f"\n=== MINIMUM 75 CHECK-INS GENERATION COMPLETE ===")
        print(f"Total check-ins generated: {len(checkins_df):,}")
        print(f"Total flights: {checkins_df['planning_id'].nunique():,}")
        print(f"Unique bookings: {checkins_df['booking_id'].nunique():,}")
        print(f"Unique customers: {checkins_df['customer_id'].nunique():,}")
        
        print(f"\nCheck-in status distribution:")
        for status, count in checkins_df['checkin_status'].value_counts().items():
            pct = count / len(checkins_df)
            print(f"  {status}: {count:,} ({pct:.1%})")
        
        print(f"\nPassenger type distribution:")
        for ptype, count in checkins_df['passenger_type'].value_counts().items():
            pct = count / len(checkins_df)
            print(f"  {ptype}: {count:,} ({pct:.1%})")
        
        print(f"\nFlight size validation (non-infant passengers):")
        print(f"  Minimum passengers per flight: {checkins_per_flight.min()}")
        print(f"  Maximum passengers per flight: {checkins_per_flight.max()}")
        print(f"  Average passengers per flight: {checkins_per_flight.mean():.1f}")
        print(f"  Flights with < {self.MIN_CHECKINS_PER_FLIGHT} passengers: {(checkins_per_flight < self.MIN_CHECKINS_PER_FLIGHT).sum()}")
        
        # Validate minimum requirement
        compliant_flights = (checkins_per_flight >= self.MIN_CHECKINS_PER_FLIGHT).sum()
        total_flights = len(checkins_per_flight)
        compliance_rate = compliant_flights / total_flights if total_flights > 0 else 0
        
        print(f"\nCOMPLIANCE CHECK:")
        print(f"  Flights meeting minimum {self.MIN_CHECKINS_PER_FLIGHT}: {compliant_flights:,} of {total_flights:,}")
        print(f"  Compliance rate: {compliance_rate:.1%}")
        
        if compliance_rate < 1.0:
            non_compliant = checkins_per_flight[checkins_per_flight < self.MIN_CHECKINS_PER_FLIGHT]
            print(f"  Non-compliant flights: {list(non_compliant.index)}")
        else:
            print(f"  ✅ SUCCESS: All flights meet minimum requirement!")
        
        # Check for seat conflicts
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
            print(f"\n✅ SUCCESS: No seat conflicts detected!")

def generate_minimum_75_checkins(target_year: int = 2021):
    """Generate check-ins with minimum 75 passengers per flight."""
    print(f"Starting MINIMUM 75 CHECK-INS generation for {target_year}")
    print("=" * 60)
    print("GUARANTEE: Every flight will have at least 75 checked-in passengers")
    print("METHOD: Existing passengers + synthetic passengers as needed")
    print("=" * 60)
    
    try:
        generator = Minimum75CheckInsGenerator(target_year=target_year)
        checkins_df = generator.generate_checkins()
        
        print(f"\n🎉 Successfully generated minimum 75 check-ins per flight for {target_year}!")
        return checkins_df
        
    except Exception as e:
        print(f"❌ Error generating check-ins: {str(e)}")
        raise

if __name__ == "__main__":
    # Set random seed
    seed_bytes = os.urandom(4)
    seed_int = int.from_bytes(seed_bytes, byteorder='big')
    random.seed(seed_int)
    np.random.seed(seed_int)
    
    TARGET_YEAR = 2021
    checkins = generate_minimum_75_checkins(target_year=TARGET_YEAR)
    
    if not checkins.empty:
        print(f"\nSample check-ins:")
        print(checkins.head(10))
        
        # Verify minimum requirement
        non_infant_per_flight = checkins[
            checkins['passenger_type'] != 'infant'
        ].groupby('planning_id').size()
        
        min_passengers = non_infant_per_flight.min()
        max_passengers = non_infant_per_flight.max()
        avg_passengers = non_infant_per_flight.mean()
        
        print(f"\n📊 FINAL VALIDATION:")
        print(f"   Minimum passengers per flight: {min_passengers}")
        print(f"   Maximum passengers per flight: {max_passengers}")
        print(f"   Average passengers per flight: {avg_passengers:.1f}")
        print(f"   Total flights: {len(non_infant_per_flight):,}")
        
        if min_passengers >= 75:
            print(f"\n✅ SUCCESS: All flights have at least 75 passengers!")
        else:
            print(f"\n❌ FAILURE: Some flights have fewer than 75 passengers!")
            problem_flights = non_infant_per_flight[non_infant_per_flight < 75]
            print(f"   Problem flights: {len(problem_flights)}")
            for flight_id, count in problem_flights.head().items():
                print(f"     {flight_id}: {count} passengers")