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

class Minimum50CheckInsGenerator:
    def __init__(self, target_year: int = 2021):
        """
        Initialize generator that ensures MINIMUM 50 check-ins per flight.
        
        Args:
            target_year (int): Year to generate check-ins for
        """
        self.TARGET_YEAR = target_year
        self.MIN_CHECKINS_PER_FLIGHT = 50
        self.DOMESTIC_AIRPORTS = {'JNB', 'CPT', 'DBN', 'PLZ', 'ELS', 'GRJ'}
        
        # Load data
        try:
            self.bookings_df = pd.read_parquet(f'airplane_data/bookings_{target_year}.parquet')
            self.clients_df = pd.read_parquet(f'airplane_data/clients_{target_year}.parquet')
            self.flight_schedule_df = pd.read_parquet(f'airplane_data/flight_schedule_{target_year}.parquet')
            self.routes_df = pd.read_parquet(f'airplane_data/routes_{target_year}.parquet')
            self.planes_df = pd.read_parquet(f'airplane_data/planes_{target_year}.parquet')
            self.cancellations_df = pd.read_parquet(f'airplane_data/cancellations_{target_year}.parquet')
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
            'default': {'rows': 35, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 'business_rows': 5}
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
        self.cancellations_df['cancellation_date'] = pd.to_datetime(self.cancellations_df['cancellation_date'])
        self.cancellations_df['rescheduled_date'] = pd.to_datetime(self.cancellations_df['rescheduled_date'])
        self.cancellations_df['on_hold_date'] = pd.to_datetime(self.cancellations_df['on_hold_date'])
        
        # Filter valid bookings (exclude cancelled, handle rescheduled/on-hold)
        valid_bookings = self.bookings_df.merge(
            self.cancellations_df[['ref_booking_id', 'is_cancelled', 'is_rescheduled', 'is_onhold', 'rescheduled_planning_id', 'rescheduled_date']],
            left_on='booking_id', right_on='ref_booking_id', how='left'
        )
        valid_bookings['is_cancelled'] = valid_bookings['is_cancelled'].fillna(False)
        valid_bookings['is_rescheduled'] = valid_bookings['is_rescheduled'].fillna(False)
        valid_bookings['is_onhold'] = valid_bookings['is_onhold'].fillna(False)
        
        # Exclude cancelled bookings
        self.valid_bookings = valid_bookings[~valid_bookings['is_cancelled']].copy()
        
        # Calculate passenger count
        self.valid_bookings['actual_passenger_count'] = (
            self.valid_bookings['num_adults'] + self.valid_bookings['num_children']
        )
        
        # Merge with flight data
        self.checkin_data = self.valid_bookings.merge(
            self.flight_schedule_df[['planning_id', 'route_id', 'plane_id', 'scheduled_departure']],
            on='planning_id', how='left'
        ).merge(
            self.routes_df[['route_id', 'origin_airport', 'destination_airport', 'origin_city', 'destination_city']],
            on='route_id', how='left'
        ).merge(
            self.planes_df[['plane_id', 'aircraft_model', 'capacity']],
            on='plane_id', how='left'
        )
        
        # Clean data
        self.checkin_data['aircraft_type'] = self.checkin_data['aircraft_model'].fillna('default')
        self.checkin_data['aircraft_capacity'] = self.checkin_data['capacity'].fillna(200)
        
        # Remove invalid records
        self.checkin_data = self.checkin_data.dropna(subset=['planning_id', 'scheduled_departure'])
        
        # Create customer lookup for names and surnames
        self.customer_info = self.clients_df.set_index('client_id')[['name', 'partner_company']].to_dict()
        self.customer_surnames = {cid: name.split()[-1] for cid, name in self.customer_info['name'].items()}
        
        # Get unique flights
        self.unique_flights = self.checkin_data['planning_id'].unique()
        
        # Create seat request lookup from bookings
        self.seat_lookup = self.bookings_df[['booking_id', 'seat_request']].dropna().set_index('booking_id')['seat_request'].to_dict()
        
        print(f"Data prepared for {self.TARGET_YEAR}:")
        print(f"- {len(self.valid_bookings):,} valid bookings (after excluding cancellations)")
        print(f"- {len(self.checkin_data):,} booking records with flight data")
        print(f"- {len(self.unique_flights)} unique flights")
        print(f"- MINIMUM {self.MIN_CHECKINS_PER_FLIGHT} check-ins will be generated per flight")

    def _is_domestic_flight(self, origin_airport: str, destination_airport: str) -> bool:
        """Determine if a flight is domestic."""
        return origin_airport in self.DOMESTIC_AIRPORTS and destination_airport in self.DOMESTIC_AIRPORTS

    def _create_expanded_seat_map(self, aircraft_type: str, min_capacity: int = 50) -> Dict[str, bool]:
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
        max_blocked = max(0, total_seats - min_capacity - 5)
        blocked_count = min(max_blocked, int(total_seats * 0.02))
        
        if blocked_count > 0:
            available_seats = list(seat_map.keys())
            blocked = random.sample(available_seats, blocked_count)
            for seat in blocked:
                seat_map[seat] = False
        
        # Inject operational seat blocks
        if random.random() < 0.05:  # 5% chance of additional blocked seats for maintenance
            maintenance_blocked = random.randint(1, 5)
            available_seats = [seat for seat, avail in seat_map.items() if avail]
            if available_seats:
                blocked = random.sample(available_seats, min(maintenance_blocked, len(available_seats)))
                for seat in blocked:
                    seat_map[seat] = False
        
        return seat_map

    def _assign_seat(self, seat_map: Dict[str, bool], aircraft_type: str, booking_id: str, passenger_idx: int, total_passengers: int) -> str:
        """Assign seat, respecting preselected seats from seat_request or creating new ones."""
        # Check if booking has a seat_request
        seat_request = self.seat_lookup.get(booking_id)
        if seat_request:
            # Split seat_request into list of seats (e.g., "A1, A2, A4" -> ["A1", "A2", "A4"])
            requested_seats = [s.strip() for s in seat_request.split(',')]
            # Ensure enough seats for all passengers in the booking
            if passenger_idx < len(requested_seats):
                requested_seat = requested_seats[passenger_idx]
                # Allow 10% chance of seat change even if preselected
                if random.random() < 0.1:
                    available_seats = [seat for seat, available in seat_map.items() if available]
                    if available_seats:
                        seat = available_seats[0]
                        seat_map[seat] = False
                        return seat
                if requested_seat in seat_map and seat_map[requested_seat]:
                    seat_map[requested_seat] = False
                    return requested_seat
        
        config = self.seat_configs.get(aircraft_type, self.seat_configs['default'])
        available_seats = [seat for seat, available in seat_map.items() if available]
        
        if not available_seats:
            emergency_seat = f"E{len(seat_map) + 1}A"
            seat_map[emergency_seat] = False
            return emergency_seat
        
        seat = available_seats[0]
        seat_map[seat] = False
        
        # Inject seat assignment conflicts
        if random.random() < 0.02:  # 2% chance
            # Assign a seat that's already taken
            taken_seats = [s for s, avail in seat_map.items() if not avail]
            if taken_seats:
                seat = random.choice(taken_seats)
        
        return seat

    def _generate_realistic_checkin_time(self, scheduled_departure: datetime, is_domestic: bool, is_synthetic: bool = False) -> datetime:
        """Generate realistic check-in time within allowed window."""
        checkin_deadline = timedelta(minutes=40 if is_domestic else 60)
        gate_deadline = timedelta(minutes=15 if is_domestic else 20)
        min_hours_before = (checkin_deadline.total_seconds() / 3600) + 0.1  # Small buffer
        
        # For synthetic passengers, ensure valid check-in time
        if is_synthetic:
            hours_before = random.uniform(min_hours_before, 24)
        else:
            if random.random() < 0.75:  # 75% online check-in
                hours_before = random.uniform(2, 24)
            else:  # Airport check-in
                hours_before = random.uniform(1, 3)
        
        checkin_time = scheduled_departure - timedelta(hours=hours_before)
        
        # Validate against check-in deadline
        if (scheduled_departure - checkin_time) < checkin_deadline:
            return None  # Too late for check-in
        
        # Ensure check-in allows boarding before gate closure
        if (scheduled_departure - checkin_time) < gate_deadline:
            return None  # Cannot board in time
        
        # Inject weather-related delays
        if scheduled_departure.month in [6, 7, 8] and random.random() < 0.1:  # Winter weather
            checkin_time = checkin_time - timedelta(hours=random.randint(2, 8))
        
        # Holiday chaos
        if scheduled_departure.month == 12 and scheduled_departure.day > 20:
            if random.random() < 0.2:  # 20% chance during holidays
                checkin_time = checkin_time + timedelta(minutes=random.randint(15, 60))
        
        # Strike delays
        if random.random() < 0.005:  # 0.5% chance
            checkin_time = checkin_time + timedelta(hours=random.randint(2, 12))
        
        return checkin_time

    def _assign_luggage(self, passenger_type: str) -> Tuple[float, float]:
        """Assign realistic luggage weights."""
        if passenger_type == 'infant':
            weight = random.uniform(0, 8)
            max_weight = 10
        elif passenger_type == 'child':
            weight = random.uniform(8, 20)
            max_weight = 23
        else:
            weight = random.uniform(12, 28)
            max_weight = 23
        
        if random.random() < 0.15:
            weight *= 0.4
        elif random.random() < 0.10:
            weight = max_weight * random.uniform(0.85, 0.98)
        
        # Baggage weight discrepancies
        if random.random() < 0.06:  # 6% chance
            weight = weight * random.uniform(0.8, 1.3)
        
        return round(weight, 2), max_weight

    def _generate_gate(self, origin_airport: str) -> str:
        """Generate gate assignment."""
        large_airports = ['JNB', 'CPT', 'JKF', 'CGD']
        gates = [f"{letter}{num}" for letter in ['A', 'B', 'C'] for num in range(1, 30)] if origin_airport in large_airports else [f"A{num}" for num in range(1, 15)]
        return random.choice(gates)

    def _create_synthetic_passenger(self, flight_info: Dict, passenger_idx: int) -> Dict:
        """Create a synthetic passenger for flights needing more check-ins."""
        passenger_types = ['adult'] * 85 + ['child'] * 14 + ['infant'] * 1
        passenger_type = random.choice(passenger_types)
        
        synthetic_booking_id = f"SYN{self.TARGET_YEAR}{passenger_idx:06d}"
        synthetic_client_id = f"CUST{self.TARGET_YEAR}{passenger_idx:05d}"
        
        return {
            'booking_id': synthetic_booking_id,
            'client_id': synthetic_client_id,
            'passenger_name': self.faker.name(),
            'passenger_type': passenger_type,
            'scheduled_departure': flight_info['scheduled_departure'],
            'origin_airport': flight_info['origin_airport'],
            'destination_airport': flight_info['destination_airport'],
            'aircraft_type': flight_info['aircraft_type'],
            'aircraft_capacity': flight_info['aircraft_capacity'],
            'num_children': 1 if passenger_type == 'child' else 0,
            'num_infants': 1 if passenger_type == 'infant' else 0,
            'num_adults': 1 if passenger_type == 'adult' else 0
        }

    def _needs_affidavit(self, client_id: str, passenger_name: str, passenger_type: str, parent_surname: str) -> bool:
        """Determine if affidavit/parental consent is needed for children/infants."""
        if passenger_type not in ['child', 'infant']:
            return False
        passenger_surname = passenger_name.split()[-1] if passenger_name else ''
        return passenger_surname != parent_surname

    def _introduce_typo(self, name: str) -> str:
        """Introduce a realistic typo in name."""
        if random.random() < 0.5:
            # Swap two letters
            name_list = list(name)
            i = random.randint(0, len(name_list)-2)
            name_list[i], name_list[i+1] = name_list[i+1], name_list[i]
            return ''.join(name_list)
        else:
            # Replace a letter
            name_list = list(name)
            i = random.randint(0, len(name_list)-1)
            if name_list[i].isalpha():
                name_list[i] = random.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
            return ''.join(name_list)

    def generate_checkins(self):
        """Generate check-ins ensuring minimum 50 per flight."""
        print(f"Generating check-ins with MINIMUM {self.MIN_CHECKINS_PER_FLIGHT} per flight for {self.TARGET_YEAR}")
        print("Strategy:")
        print("- Exclude cancelled bookings, handle rescheduled/on-hold")
        print("- Enforce check-in deadlines: 40 min (domestic), 60 min (international) before departure")
        print("- Generate synthetic passengers upfront to ensure minimum 50")
        print("- Expand aircraft capacity as needed")
        print("- Children take parent's surname, affidavit if surnames differ")
        print("- Same booking passengers check-in together with 1-3 min variation")
        print("- Infants/children match adult check-in channel")
        print("- Reduced declared items, adults only")
        print("- Respect preselected seats from seat_request, with 10% chance of change")
        
        checkins = []
        checkin_counter = 1
        flight_seat_maps = {}
        flight_gates = {}
        existing_checkins = {}  # To track existing check-ins for duplicates
        
        for planning_id in tqdm(self.unique_flights, desc="Processing flights"):
            flight_bookings = self.checkin_data[self.checkin_data['planning_id'] == planning_id]
            
            if len(flight_bookings) == 0:
                continue
            
            first_booking = flight_bookings.iloc[0]
            is_domestic = self._is_domestic_flight(first_booking['origin_airport'], first_booking['destination_airport'])
            
            flight_info = {
                'planning_id': planning_id,
                'aircraft_type': first_booking['aircraft_type'],
                'aircraft_capacity': max(int(first_booking['aircraft_capacity']), self.MIN_CHECKINS_PER_FLIGHT + 10),
                'scheduled_departure': first_booking['scheduled_departure'],
                'origin_airport': first_booking['origin_airport'],
                'destination_airport': first_booking['destination_airport']
            }
            
            flight_seat_maps[planning_id] = self._create_expanded_seat_map(
                flight_info['aircraft_type'], self.MIN_CHECKINS_PER_FLIGHT + 20
            )
            flight_gates[planning_id] = self._generate_gate(flight_info['origin_airport'])
            
            flight_passengers = []
            
            # Process existing passengers
            for _, booking in flight_bookings.iterrows():
                if booking['is_rescheduled'] and booking['rescheduled_planning_id'] != planning_id:
                    continue
                if booking['is_onhold'] and pd.isna(booking['rescheduled_date']):
                    continue
                if booking['is_onhold'] and booking['rescheduled_date'] > flight_info['scheduled_departure']:
                    continue
                
                passengers_in_booking = []
                base_checkin_time = self._generate_realistic_checkin_time(
                    flight_info['scheduled_departure'], is_domestic, is_synthetic=False
                )
                
                # Assign check-in channel for the group
                channel_probs = [0.20, 0.20, 0.50, 0.10]
                channel_probs = np.array(channel_probs) / sum(channel_probs)
                group_checkin_channel = np.random.choice(['mobile', 'website', 'counter', 'self-service'], p=channel_probs)
                
                # Get parent surname from first adult
                parent_name = self.customer_info['name'].get(booking['client_id'], f"Customer_{booking['client_id']}")
                parent_surname = parent_name.split()[-1]
                
                # Calculate total passengers in this booking (excluding infants for seat allocation)
                total_passengers = booking['num_adults'] + booking['num_children']
                
                for i in range(booking['num_adults']):
                    name = self.customer_info['name'].get(booking['client_id'], f"Customer_{booking['client_id']}") if i == 0 else self.faker.name()
                    checkin_time = base_checkin_time
                    if checkin_time and i > 0 and random.random() < 0.1:  # 10% chance an adult is late
                        checkin_time += timedelta(minutes=random.uniform(1, 3))
                        if (flight_info['scheduled_departure'] - checkin_time) < timedelta(minutes=40 if is_domestic else 60):
                            checkin_time = None
                    passenger = {
                        'booking_id': booking['booking_id'],
                        'client_id': booking['client_id'],
                        'passenger_name': name,
                        'passenger_type': 'adult',
                        'checkin_time': checkin_time,
                        'checkin_channel': group_checkin_channel,
                        'scheduled_departure': booking['scheduled_departure'],
                        'origin_airport': booking['origin_airport'],
                        'destination_airport': booking['destination_airport'],
                        'aircraft_type': booking['aircraft_type'],
                        'aircraft_capacity': flight_info['aircraft_capacity'],
                        'num_children': booking['num_children'],
                        'num_infants': booking['num_infants'],
                        'passenger_idx': i,
                        'total_passengers': total_passengers
                    }
                    passengers_in_booking.append(passenger)
                
                for i in range(booking['num_children']):
                    # Child takes parent's surname
                    child_first_name = self.faker.first_name()
                    child_name = f"{child_first_name} {parent_surname}"
                    checkin_time = base_checkin_time
                    if checkin_time and random.random() < 0.1:  # 10% chance a child is late
                        checkin_time += timedelta(minutes=random.uniform(1, 3))
                        if (flight_info['scheduled_departure'] - checkin_time) < timedelta(minutes=40 if is_domestic else 60):
                            checkin_time = None
                    passenger = {
                        'booking_id': booking['booking_id'],
                        'client_id': booking['client_id'],
                        'passenger_name': child_name,
                        'passenger_type': 'child',
                        'checkin_time': checkin_time,
                        'checkin_channel': group_checkin_channel,
                        'scheduled_departure': booking['scheduled_departure'],
                        'origin_airport': booking['origin_airport'],
                        'destination_airport': booking['destination_airport'],
                        'aircraft_type': booking['aircraft_type'],
                        'aircraft_capacity': flight_info['aircraft_capacity'],
                        'num_children': booking['num_children'],
                        'num_infants': booking['num_infants'],
                        'passenger_idx': booking['num_adults'] + i,
                        'total_passengers': total_passengers
                    }
                    passengers_in_booking.append(passenger)
                
                for i in range(booking['num_infants']):
                    # Infant takes parent's surname
                    infant_name = f"Infant {parent_surname}"
                    passenger = {
                        'booking_id': booking['booking_id'],
                        'client_id': booking['client_id'],
                        'passenger_name': infant_name,
                        'passenger_type': 'infant',
                        'checkin_time': base_checkin_time,  # Infants never late
                        'checkin_channel': group_checkin_channel,
                        'scheduled_departure': booking['scheduled_departure'],
                        'origin_airport': booking['origin_airport'],
                        'destination_airport': booking['destination_airport'],
                        'aircraft_type': booking['aircraft_type'],
                        'aircraft_capacity': flight_info['aircraft_capacity'],
                        'num_children': booking['num_children'],
                        'num_infants': booking['num_infants'],
                        'passenger_idx': -1,  # Infants don't need seats
                        'total_passengers': total_passengers
                    }
                    passengers_in_booking.append(passenger)
                
                flight_passengers.extend(passengers_in_booking)
            
            # Count non-infant passengers before filtering
            non_infant_passengers = [p for p in flight_passengers if p['passenger_type'] != 'infant']
            current_count = len(non_infant_passengers)
            
            # Generate synthetic passengers upfront if needed
            if current_count < self.MIN_CHECKINS_PER_FLIGHT:
                needed = self.MIN_CHECKINS_PER_FLIGHT - current_count + 10  # Buffer for filtering
                print(f"Flight {planning_id}: Adding {needed} synthetic passengers ({current_count} -> {self.MIN_CHECKINS_PER_FLIGHT})")
                for i in range(needed):
                    synthetic_passenger = self._create_synthetic_passenger(flight_info, checkin_counter + i)
                    checkin_time = self._generate_realistic_checkin_time(
                        flight_info['scheduled_departure'], is_domestic, is_synthetic=True
                    )
                    synthetic_passenger['checkin_time'] = checkin_time
                    synthetic_passenger['checkin_channel'] = np.random.choice(['mobile', 'website', 'counter', 'self-service'], p=[0.20, 0.20, 0.50, 0.10])
                    synthetic_passenger['passenger_idx'] = 0
                    synthetic_passenger['total_passengers'] = 1
                    flight_passengers.append(synthetic_passenger)
            
            # Filter passengers by check-in time
            valid_passengers = [p for p in flight_passengers if p['checkin_time'] is not None]
            
            # Re-check non-infant count and add more synthetic passengers if needed
            non_infant_valid = [p for p in valid_passengers if p['passenger_type'] != 'infant']
            current_count = len(non_infant_valid)
            if current_count < self.MIN_CHECKINS_PER_FLIGHT:
                needed = self.MIN_CHECKINS_PER_FLIGHT - current_count
                print(f"Flight {planning_id}: Adding {needed} more synthetic passengers after filtering ({current_count} -> {self.MIN_CHECKINS_PER_FLIGHT})")
                for i in range(needed):
                    synthetic_passenger = self._create_synthetic_passenger(flight_info, checkin_counter + i)
                    checkin_time = self._generate_realistic_checkin_time(
                        flight_info['scheduled_departure'], is_domestic, is_synthetic=True
                    )
                    if checkin_time:
                        synthetic_passenger['checkin_time'] = checkin_time
                        synthetic_passenger['checkin_channel'] = np.random.choice(['mobile', 'website', 'counter', 'self-service'], p=[0.20, 0.20, 0.50, 0.10])
                        synthetic_passenger['passenger_idx'] = 0
                        synthetic_passenger['total_passengers'] = 1
                        valid_passengers.append(synthetic_passenger)
            
            # Generate check-ins for valid passengers
            for passenger in valid_passengers:
                seat_allocation = 'Lap' if passenger['passenger_type'] == 'infant' else self._assign_seat(
                    flight_seat_maps[planning_id], 
                    passenger['aircraft_type'], 
                    passenger['booking_id'],
                    passenger['passenger_idx'],
                    passenger['total_passengers']
                )
                
                luggage, max_luggage = self._assign_luggage(passenger['passenger_type'])
                
                is_affidavit = self._needs_affidavit(passenger['client_id'], passenger['passenger_name'], passenger['passenger_type'], parent_surname)
                
                declared_item = 'firearm' if passenger['passenger_type'] == 'adult' and random.random() < 0.01 else None
                
                checkin = {
                    'checkin_id': f"CI{self.TARGET_YEAR}{checkin_counter:06d}",
                    'booking_id': passenger['booking_id'],
                    'planning_id': planning_id,
                    'client_id': passenger['client_id'],
                    'passenger_name': passenger['passenger_name'],
                    'passenger_type': passenger['passenger_type'],
                    'checkin_status': 'checked_in',
                    'gate_number': flight_gates[planning_id],
                    'seat_allocation': seat_allocation,
                    'max_luggage': max_luggage,
                    'checkin_luggage': luggage,
                    'checkin_time': passenger['checkin_time'],
                    'checkin_channel': passenger['checkin_channel'],
                    'is_affidavit': is_affidavit,
                    'declared_item': declared_item
                }
                
                # Inject system and technical errors
                # 1. Check-in system downtime causing late check-ins
                if random.random() < 0.02:  # 2% chance
                    checkin['checkin_time'] = flight_info['scheduled_departure'] - timedelta(minutes=random.randint(15, 35))
                    checkin['checkin_channel'] = "counter"  # Forced to use counter due to system issues
                
                # 2. Duplicate check-in records from system glitches
                if random.random() < 0.015:  # 1.5% chance
                    duplicate_checkin = checkin.copy()
                    duplicate_checkin['checkin_id'] = f"CI{self.TARGET_YEAR}{checkin_counter + 999999:06d}"
                    duplicate_checkin['checkin_time'] = checkin['checkin_time'] + timedelta(minutes=random.randint(1, 15))
                    checkins.append(duplicate_checkin)
                
                # 3. Wrong flight assignments from barcode scanning errors
                if random.random() < 0.01:  # 1% chance
                    wrong_planning_ids = [p for p in self.unique_flights if p != planning_id]
                    if wrong_planning_ids:
                        checkin['planning_id'] = random.choice(wrong_planning_ids)
                
                # 4. Seat assignment conflicts from concurrent bookings
                if random.random() < 0.02:  # 2% chance
                    # Two passengers assigned same seat
                    existing_seats = [c['seat_allocation'] for c in checkins if c.get('planning_id') == planning_id and c['seat_allocation'] != 'Lap']
                    if existing_seats:
                        checkin['seat_allocation'] = random.choice(existing_seats)
                
                # Inject operational reality errors
                # 1. Gate changes not reflected in check-in data
                if random.random() < 0.08:  # 8% chance of gate change
                    old_gate = flight_gates[planning_id]
                    new_gate = self._generate_gate(passenger['origin_airport'])
                    if random.random() < 0.3:
                        checkin['gate_number'] = old_gate
                    else:
                        checkin['gate_number'] = new_gate
                
                # 2. Aircraft swaps causing capacity mismatches
                if random.random() < 0.03:  # 3% chance
                    if random.random() < 0.4:  # 40% of affected passengers don't get seats
                        checkin['seat_allocation'] = "WAITLIST"
                        checkin['checkin_status'] = "waitlisted"
                
                # 3. Weight restrictions forcing passenger removal
                if random.random() < 0.005:  # 0.5% chance
                    checkin['checkin_status'] = "denied_weight"
                    checkin['seat_allocation'] = None
                
                # 4. Last-minute crew requirements taking passenger seats
                if random.random() < 0.01:  # 1% chance
                    checkin['seat_allocation'] = "CREW_PRIORITY"
                    checkin['checkin_status'] = "bumped"
                
                # Inject customer service process errors
                # 1. Incorrect passenger name from typos or system errors
                if random.random() < 0.04:  # 4% chance
                    name_parts = checkin['passenger_name'].split()
                    if len(name_parts) > 1:
                        if random.random() < 0.5:
                            checkin['passenger_name'] = f"{name_parts[-1]} {name_parts[0]}"
                        else:
                            checkin['passenger_name'] = self._introduce_typo(checkin['passenger_name'])
                
                # 2. Missing special service requests
                if random.random() < 0.03:  # 3% chance
                    checkin['special_assistance'] = "WHEELCHAIR"
                    # But seat allocation doesn't reflect aisle requirement
                    if checkin['seat_allocation'] and 'A' not in checkin['seat_allocation'] and 'F' not in checkin['seat_allocation']:
                        pass  # Should be aisle seat but isn't
                
                # 3. Group bookings split across different check-in sessions
                if booking['num_adults'] > 1 and random.random() < 0.15:  # 15% for groups
                    checkin['checkin_time'] = checkin['checkin_time'] + timedelta(hours=random.randint(1, 12))
                    if (flight_info['scheduled_departure'] - checkin['checkin_time']) < timedelta(minutes=40 if is_domestic else 60):
                        checkin['checkin_status'] = "too_late"
                
                # 4. Baggage weight discrepancies
                if random.random() < 0.06:  # 6% chance
                    actual_weight = checkin['checkin_luggage']
                    checkin['checkin_luggage'] = actual_weight * random.uniform(0.8, 1.3)
                
                # Inject document and compliance errors
                # 1. Missing or incorrect affidavit documentation
                if checkin['is_affidavit'] and random.random() < 0.12:  # 12% chance
                    checkin['affidavit_status'] = "MISSING"
                
                # 2. International passengers missing required documents
                if not is_domestic and random.random() < 0.02:  # 2% chance
                    checkin['document_issue'] = "VISA_MISSING"
                    checkin['checkin_status'] = "document_review"
                
                # 3. Declared dangerous goods not properly flagged
                if checkin['declared_item'] == 'firearm' and random.random() < 0.05:  # 5% chance
                    checkin['declared_item'] = None
                    checkin['security_flag'] = "MISSED_DECLARATION"
                
                # 4. Age verification errors for children
                if passenger['passenger_type'] == 'child' and random.random() < 0.03:  # 3% chance
                    checkin['passenger_type'] = 'adult'
                
                # Inject mobile/digital check-in specific errors
                # 1. Mobile boarding pass barcode issues
                if checkin['checkin_channel'] == 'mobile' and random.random() < 0.04:  # 4% chance
                    checkin['boarding_pass_issue'] = "BARCODE_UNREADABLE"
                    checkin['checkin_status'] = "reprint_required"
                
                # 2. App crashes during seat selection
                if checkin['checkin_channel'] in ['mobile', 'website'] and random.random() < 0.03:  # 3% chance
                    checkin['seat_selection_method'] = "AUTO_ASSIGNED_TECH_ISSUE"
                
                # 3. Timezone confusion in check-in times
                if random.random() < 0.02:  # 2% chance
                    checkin['checkin_time'] = checkin['checkin_time'] + timedelta(hours=random.choice([-2, -1, 1, 2]))
                    if checkin['checkin_time'] > flight_info['scheduled_departure']:
                        checkin['checkin_time'] = flight_info['scheduled_departure'] - timedelta(minutes=30)
                
                # 4. Payment failures for seat upgrades
                if random.random() < 0.01:  # 1% chance
                    checkin['upgrade_payment_failed'] = True
                
                # Inject realistic operational patterns
                # 1. Weather-related check-in delays
                if flight_info['scheduled_departure'].month in [6, 7, 8] and random.random() < 0.1:  # Winter weather
                    checkin['checkin_time'] = checkin['checkin_time'] - timedelta(hours=random.randint(2, 8))
                
                # 2. Holiday travel chaos
                if flight_info['scheduled_departure'].month == 12 and flight_info['scheduled_departure'].day > 20:
                    if random.random() < 0.2:  # 20% chance during holidays
                        checkin['checkin_time'] = checkin['checkin_time'] + timedelta(minutes=random.randint(15, 60))
                
                # 3. Strike or labor action impacts
                if random.random() < 0.005:  # 0.5% chance
                    checkin['labor_action_delay'] = True
                    checkin['checkin_time'] = checkin['checkin_time'] + timedelta(hours=random.randint(2, 12))
                    checkin['checkin_channel'] = "counter"  # Manual processing only
                
                # 4. VIP or frequent flyer processing errors
                if random.random() < 0.02:  # 2% chance
                    checkin['status_recognition_failed'] = True
                
                checkins.append(checkin)
                checkin_counter += 1
        
        # Convert to DataFrame
        checkins_df = pd.DataFrame(checkins)
        
        # Optimize data types
        checkins_df['checkin_status'] = checkins_df['checkin_status'].astype('category')
        checkins_df['passenger_type'] = checkins_df['passenger_type'].astype('category')
        checkins_df['gate_number'] = checkins_df['gate_number'].astype('category')
        checkins_df['checkin_channel'] = checkins_df['checkin_channel'].astype('category')
        checkins_df['checkin_time'] = pd.to_datetime(checkins_df['checkin_time'])
        checkins_df['is_affidavit'] = checkins_df['is_affidavit'].astype(bool)
        checkins_df['declared_item'] = checkins_df['declared_item'].astype('category')
        
        self._validate_and_report_results(checkins_df)
        
        # Save to file
        output_path = f'airplane_data/checkins_{self.TARGET_YEAR}.parquet'
        checkins_df.to_parquet(output_path, index=False)
        print(f"\nCheck-ins data saved to {output_path}")
        
        return checkins_df

    def _validate_and_report_results(self, checkins_df: pd.DataFrame):
        """Validate and report results."""
        non_infant_checkins = checkins_df[
            (checkins_df['checkin_status'] == 'checked_in') & 
            (checkins_df['passenger_type'] != 'infant')
        ]
        
        checkins_per_flight = non_infant_checkins.groupby('planning_id').size()
        
        print(f"\n=== MINIMUM 50 CHECK-INS GENERATION COMPLETE ===")
        print(f"Total check-ins generated: {len(checkins_df):,}")
        print(f"Total flights: {checkins_df['planning_id'].nunique():,}")
        print(f"Unique bookings: {checkins_df['booking_id'].nunique():,}")
        print(f"Unique customers: {checkins_df['client_id'].nunique():,}")
        
        print(f"\nCheck-in status distribution:")
        for status, count in checkins_df['checkin_status'].value_counts().items():
            pct = count / len(checkins_df)
            print(f"  {status}: {count:,} ({pct:.1%})")
        
        print(f"\nPassenger type distribution:")
        for ptype, count in checkins_df['passenger_type'].value_counts().items():
            pct = count / len(checkins_df)
            print(f"  {ptype}: {count:,} ({pct:.1%})")
        
        print(f"\nCheck-in channel distribution:")
        for channel, count in checkins_df['checkin_channel'].value_counts().items():
            pct = count / len(checkins_df)
            print(f"  {channel}: {count:,} ({pct:.1%})")
        
        print(f"\nAffidavit requirement distribution:")
        for affidavit, count in checkins_df['is_affidavit'].value_counts().items():
            pct = count / len(checkins_df)
            print(f"  {affidavit}: {count:,} ({pct:.1%})")
        
        print(f"\nDeclared item distribution:")
        for item, count in checkins_df['declared_item'].value_counts(dropna=False).items():
            item_name = item if item else 'None'
            pct = count / len(checkins_df)
            print(f"  {item_name}: {count:,} ({pct:.1%})")
        
        print(f"\nFlight size validation (non-infant passengers):")
        print(f"  Minimum passengers per flight: {checkins_per_flight.min()}")
        print(f"  Maximum passengers per flight: {checkins_per_flight.max()}")
        print(f"  Average passengers per flight: {checkins_per_flight.mean():.1f}")
        print(f"  Flights with < {self.MIN_CHECKINS_PER_FLIGHT} passengers: {(checkins_per_flight < self.MIN_CHECKINS_PER_FLIGHT).sum()}")
        
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
            print(f"  SUCCESS: All flights meet minimum requirement!")
        
        seat_conflicts = checkins_df[
            (checkins_df['checkin_status'] == 'checked_in') & 
            (checkins_df['seat_allocation'].notna()) &
            (checkins_df['seat_allocation'] != 'Lap')
        ].groupby(['planning_id', 'seat_allocation']).size()
        conflicts = seat_conflicts[seat_conflicts > 1]
        
        if len(conflicts) > 0:
            print(f"\nWARNING: {len(conflicts)} seat conflicts detected!")
        else:
            print(f"\nSUCCESS: No seat conflicts detected!")

def generate_minimum_50_checkins(target_year: int = 2021):
    """Generate check-ins with minimum 50 passengers per flight."""
    print(f"Starting MINIMUM 50 CHECK-INS generation for {target_year}")
    print("=" * 60)
    print("GUARANTEE: Every flight will have at least 50 checked-in passengers")
    print("METHOD: Existing passengers (excluding cancellations) + synthetic passengers as needed")
    print("CHECK-IN RULES: 40 min (domestic) or 60 min (international) before departure")
    print("=" * 60)
    
    try:
        generator = Minimum50CheckInsGenerator(target_year=target_year)
        checkins_df = generator.generate_checkins()
        
        print(f"\nSuccessfully generated minimum 50 check-ins per flight for {target_year}!")
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
    checkins = generate_minimum_50_checkins(target_year=TARGET_YEAR)
    
    if not checkins.empty:
        print(f"\nSample check-ins:")
        print(checkins.head(10))
        
        non_infant_per_flight = checkins[
            checkins['passenger_type'] != 'infant'
        ].groupby('planning_id').size()
        
        min_passengers = non_infant_per_flight.min()
        max_passengers = non_infant_per_flight.max()
        avg_passengers = non_infant_per_flight.mean()
        
        print(f"\nFINAL VALIDATION:")
        print(f"   Minimum passengers per flight: {min_passengers}")
        print(f"   Maximum passengers per flight: {max_passengers}")
        print(f"   Average passengers per flight: {avg_passengers:.1f}")
        print(f"   Total flights: {len(non_infant_per_flight):,}")
        
        if min_passengers >= 50:
            print(f"\nSUCCESS: All flights have at least 50 passengers!")
        else:
            print(f"\nFAILURE: Some flights have fewer than 50 passengers!")
            problem_flights = non_infant_per_flight[non_infant_per_flight < 50]
            print(f"   Problem flights: {len(problem_flights)}")
            for flight_id, count in problem_flights.head().items():
                print(f"     {flight_id}: {count} passengers")
