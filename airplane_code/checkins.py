import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from typing import Dict, List, Optional, Set, Tuple
import uuid
from tqdm import tqdm
from faker import Faker
import warnings
warnings.filterwarnings('ignore')

class UltraRealisticFastCheckInsGenerator:
    def __init__(self, target_year: int = 2021):
        """
        Initialize with target year.
        
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
        
        # Prepare data
        self._prepare_data()
        
        # Realistic seat configurations with exact layouts
        self.seat_configs = {
            'Boeing 737-800': {
                'rows': 32, 'seats_per_row': 6, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 
                'business_rows': 4, 'premium_rows': [6, 7, 8, 9], 'exit_rows': [12, 13]
            },
            'Boeing 737-400': {
                'rows': 25, 'seats_per_row': 6, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 
                'business_rows': 3, 'premium_rows': [5, 6], 'exit_rows': [10, 11]
            },
            'Airbus A320': {
                'rows': 30, 'seats_per_row': 6, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 
                'business_rows': 4, 'premium_rows': [6, 7, 8], 'exit_rows': [11, 12]
            },
            'Airbus A319': {
                'rows': 26, 'seats_per_row': 6, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 
                'business_rows': 3, 'premium_rows': [5, 6], 'exit_rows': [9, 10]
            },
            'Embraer E190': {
                'rows': 25, 'seats_per_row': 4, 'layout': ['A', 'C', 'D', 'F'], 
                'business_rows': 5, 'premium_rows': [7, 8], 'exit_rows': [12, 13]
            },
            'Embraer E170': {
                'rows': 19, 'seats_per_row': 4, 'layout': ['A', 'C', 'D', 'F'], 
                'business_rows': 4, 'premium_rows': [6, 7], 'exit_rows': [9, 10]
            },
            'ATR 72': {
                'rows': 18, 'seats_per_row': 4, 'layout': ['A', 'C', 'D', 'F'], 
                'business_rows': 0, 'premium_rows': [], 'exit_rows': [8, 9]
            },
            'Bombardier Q400': {
                'rows': 20, 'seats_per_row': 4, 'layout': ['A', 'C', 'D', 'F'], 
                'business_rows': 0, 'premium_rows': [], 'exit_rows': [9, 10]
            },
            'default': {
                'rows': 25, 'seats_per_row': 6, 'layout': ['A', 'B', 'C', 'D', 'E', 'F'], 
                'business_rows': 3, 'premium_rows': [5, 6], 'exit_rows': [10, 11]
            }
        }
        
        # Ultra-realistic check-in status probabilities
        self.base_status_probs = {
            'checked_in': 0.88,
            'no_show': 0.08,
            'ticket_bumping': 0.025,
            'denied_boarding': 0.015
        }
        
        # Initialize Faker for realistic names
        self.faker = Faker(['en_US', 'en_GB', 'zu_ZA'])
        
        # Pre-generate optimized random values
        self._pregenerate_optimized_values()

    def _prepare_data(self):
        """Prepare and merge datasets with realistic flight loading."""
        # Convert date columns
        self.bookings_df['booking_date'] = pd.to_datetime(self.bookings_df['booking_date'])
        self.flight_schedule_df['scheduled_departure'] = pd.to_datetime(self.flight_schedule_df['scheduled_departure'])
        self.clients_df['date_of_registration'] = pd.to_datetime(self.clients_df['date_of_registration'])
        self.clients_df['dob'] = pd.to_datetime(self.clients_df['dob'])
        
        # Filter valid bookings (confirmed or rescheduled, not cancelled)
        self.valid_bookings = self.bookings_df[
            self.bookings_df['booking_status'].isin(['confirmed', 'rescheduled'])
        ].copy()
        
        # Handle group bookings properly - extract from seat_request if comma-separated
        def extract_passenger_count(row):
            if pd.isna(row['seat_request']):
                return row['num_adults'] + row['num_children']
            
            seat_request = str(row['seat_request'])
            if ',' in seat_request:
                return len(seat_request.split(','))
            else:
                return row['num_adults'] + row['num_children']
        
        self.valid_bookings['actual_passenger_count'] = self.valid_bookings.apply(extract_passenger_count, axis=1)
        
        # Merge with flight data
        self.flight_data = self.flight_schedule_df.merge(
            self.routes_df[['route_id', 'origin_airport', 'destination_airport']],
            on='route_id', how='left'
        ).merge(
            self.planes_df[['plane_id', 'aircraft_model', 'capacity']],
            on='plane_id', how='left'
        )
        
        # Calculate REALISTIC bookings per flight (using actual passenger count)
        bookings_per_flight = self.valid_bookings.groupby('planning_id').agg({
            'actual_passenger_count': 'sum',
            'num_infants': 'sum'
        }).reset_index()
        bookings_per_flight.columns = ['planning_id', 'total_passengers', 'total_infants']
        
        # Merge to get capacity and calculate REAL load factors
        flight_load_data = bookings_per_flight.merge(
            self.flight_data[['planning_id', 'capacity']], 
            on='planning_id', how='left'
        )
        flight_load_data['capacity'] = flight_load_data['capacity'].fillna(150)
        flight_load_data['load_factor'] = flight_load_data['total_passengers'] / flight_load_data['capacity']
        
        # Create load factor lookup
        self.flight_load_factors = dict(zip(
            flight_load_data['planning_id'], 
            flight_load_data['load_factor']
        ))
        
        # Merge bookings with flight data
        self.checkin_data = self.valid_bookings.merge(
            self.flight_data[['planning_id', 'route_id', 'plane_id', 'scheduled_departure', 'origin_airport', 'aircraft_model', 'capacity']],
            on='planning_id', how='left'
        )
        
        # Clean data
        self.checkin_data['aircraft_type'] = self.checkin_data['aircraft_model'].fillna('default')
        self.checkin_data['aircraft_capacity'] = self.checkin_data['capacity'].fillna(150)
        
        # Realistic flight filtering: no sampling needed - use actual bookings
        # Only filter out clearly invalid data
        self.checkin_data = self.checkin_data.dropna(subset=['planning_id', 'scheduled_departure'])
        
        # Create customer lookup for names
        self.customer_names = dict(zip(self.clients_df['client_id'], self.clients_df['name']))
        
        print(f"Loaded data for {self.TARGET_YEAR}:")
        print(f"- {len(self.valid_bookings):,} total valid bookings")
        print(f"- {len(self.checkin_data):,} check-in eligible records")
        print(f"- {self.checkin_data['planning_id'].nunique()} unique flights")
        
        # Show realistic load factor distribution
        load_factors = [self.flight_load_factors.get(pid, 0) for pid in self.checkin_data['planning_id'].unique()]
        print(f"Load factor stats: min={min(load_factors):.1%}, max={max(load_factors):.1%}, avg={np.mean(load_factors):.1%}")
        
        overbooked = sum(1 for lf in load_factors if lf > 1.0)
        print(f"Overbooked flights: {overbooked} ({overbooked/len(load_factors)*100:.1f}%)")

    def _pregenerate_optimized_values(self):
        """Pre-generate optimized random values for maximum speed."""
        sample_size = 50000
        
        # Pre-generate luggage weights with realistic distributions
        self.luggage_adult = np.clip(np.random.normal(18, 4, sample_size), 0, 32)
        self.luggage_child = np.clip(np.random.normal(12, 3, sample_size), 0, 25)
        self.luggage_infant = np.random.uniform(0, 8, sample_size)
        
        # Pre-generate check-in timing (more realistic patterns)
        self.online_checkin_hours = np.random.beta(2, 5, sample_size) * 22 + 2  # 2-24 hours, weighted toward later
        self.airport_checkin_hours = np.random.beta(5, 2, sample_size) * 1.5 + 0.5  # 0.5-2 hours, weighted toward earlier
        
        # Pre-generate seat preferences
        self.window_preference = np.random.random(sample_size) < 0.35  # 35% prefer window
        self.aisle_preference = np.random.random(sample_size) < 0.45  # 45% prefer aisle
        
        # Pre-generate special requirements
        self.wheelchair_assistance = np.random.random(sample_size) < 0.02
        self.dietary_requirements = np.random.random(sample_size) < 0.08
        
        print(f"Pre-generated {sample_size:,} random values for speed optimization")

    def _create_flight_seat_map(self, aircraft_type: str, capacity: int) -> Dict[str, bool]:
        """Create a complete seat map for the aircraft with realistic blocking."""
        config = self.seat_configs.get(aircraft_type, self.seat_configs['default'])
        seat_map = {}
        
        total_seats = config['rows'] * len(config['layout'])
        
        # Create all possible seats
        for row in range(1, config['rows'] + 1):
            for letter in config['layout']:
                seat = f"{row}{letter}"
                seat_map[seat] = True  # True = available, False = occupied/blocked
        
        # Block some seats realistically (maintenance, crew, etc.)
        blocked_seats = max(1, int(total_seats * 0.02))  # 2% blocked seats
        available_seats = list(seat_map.keys())
        blocked = np.random.choice(available_seats, size=blocked_seats, replace=False)
        
        for seat in blocked:
            seat_map[seat] = False
        
        return seat_map

    def _calculate_ultra_realistic_status_probs(self, planning_id: str, total_flight_passengers: int, checked_in_count: int, remaining_passengers: int) -> Dict[str, float]:
        """Calculate ultra-realistic status probabilities based on multiple factors including flight size and checked-in count."""
        load_factor = self.flight_load_factors.get(planning_id, 0.5)
        probs = self.base_status_probs.copy()
        
        # Ensure at least 50 checked-in passengers
        passengers_needed = max(0, 50 - checked_in_count)
        if remaining_passengers > 0 and passengers_needed > 0:
            # Force check-in for enough passengers to reach 50
            check_in_prob = min(1.0, passengers_needed / remaining_passengers)
            probs['checked_in'] = check_in_prob
            probs['no_show'] = (1 - check_in_prob) * 0.667  # Redistribute remaining probability
            probs['ticket_bumping'] = (1 - check_in_prob) * 0.167
            probs['denied_boarding'] = (1 - check_in_prob) * 0.166
        else:
            # Apply realistic adjustments based on load factor for remaining passengers
            if load_factor > 1.15:  # Severely overbooked
                probs['ticket_bumping'] = 0.20
                probs['denied_boarding'] = 0.12
                probs['no_show'] = 0.03
                probs['checked_in'] = 0.65
            elif load_factor > 1.05:  # Moderately overbooked
                probs['ticket_bumping'] = 0.12
                probs['denied_boarding'] = 0.08
                probs['no_show'] = 0.04
                probs['checked_in'] = 0.76
            elif load_factor > 0.95:  # Nearly full
                probs['ticket_bumping'] = 0.06
                probs['denied_boarding'] = 0.03
                probs['no_show'] = 0.06
                probs['checked_in'] = 0.85
            elif load_factor < 0.3:  # Very empty
                probs['no_show'] = 0.15
                probs['ticket_bumping'] = 0.005
                probs['denied_boarding'] = 0.002
                probs['checked_in'] = 0.843
        
        # Normalize probabilities
        total = sum(probs.values())
        return {k: v/total for k, v in probs.items()}

    def _assign_optimal_seat(self, seat_map: Dict[str, bool], aircraft_type: str, 
                           booking_class: str, passenger_type: str, group_size: int = 1,
                           prefer_window: bool = False, prefer_aisle: bool = False) -> Optional[str]:
        """Assign optimal seat using realistic airline algorithms."""
        config = self.seat_configs.get(aircraft_type, self.seat_configs['default'])
        available_seats = [seat for seat, available in seat_map.items() if available]
        
        if not available_seats:
            return None
        
        # Determine appropriate rows based on booking class
        if booking_class == 'business' and config['business_rows'] > 0:
            target_rows = list(range(1, config['business_rows'] + 1))
        else:
            target_rows = list(range(config['business_rows'] + 1, config['rows'] + 1))
        
        # Filter seats by appropriate rows
        class_appropriate_seats = [
            seat for seat in available_seats 
            if int(seat[:-1]) in target_rows
        ]
        
        if not class_appropriate_seats:
            class_appropriate_seats = available_seats  # Fallback if no class-appropriate seats
        
        # Apply seat preferences realistically
        preferred_seats = []
        
        if prefer_window:
            window_seats = [s for s in class_appropriate_seats if s[-1] in ['A', 'F']]
            if window_seats:
                preferred_seats.extend(window_seats)
        
        if prefer_aisle:
            if len(config['layout']) == 6:  # 3-3 config
                aisle_seats = [s for s in class_appropriate_seats if s[-1] in ['C', 'D']]
            else:  # 2-2 config
                aisle_seats = [s for s in class_appropriate_seats if s[-1] in ['C', 'D']]
            if aisle_seats:
                preferred_seats.extend(aisle_seats)
        
        # Choose best seat
        if preferred_seats:
            # Sort by row (prefer front of appropriate section)
            preferred_seats.sort(key=lambda x: int(x[:-1]))
            chosen_seat = preferred_seats[0]
        else:
            # Sort all appropriate seats by row
            class_appropriate_seats.sort(key=lambda x: int(x[:-1]))
            chosen_seat = class_appropriate_seats[0]
        
        return chosen_seat

    def _generate_ultra_realistic_checkin_time(self, scheduled_departure: datetime, booking_class: str, idx: int) -> datetime:
        """Generate ultra-realistic check-in time with class-based patterns."""
        # Business class tends to check in later (more confident about seats)
        if booking_class == 'business':
            if np.random.random() < 0.6:  # 60% online check-in for business
                hours_before = self.online_checkin_hours[idx % len(self.online_checkin_hours)] * 0.7  # Later check-in
            else:
                hours_before = self.airport_checkin_hours[idx % len(self.airport_checkin_hours)] * 1.2  # Slightly later
        else:
            if np.random.random() < 0.75:  # 75% online check-in for economy
                hours_before = self.online_checkin_hours[idx % len(self.online_checkin_hours)]
            else:
                hours_before = self.airport_checkin_hours[idx % len(self.airport_checkin_hours)]
        
        return scheduled_departure - timedelta(hours=hours_before)

    def _assign_realistic_luggage(self, passenger_type: str, booking_class: str, idx: int) -> Tuple[float, float]:
        """Assign ultra-realistic luggage weights with proper limits."""
        if passenger_type == 'infant':
            luggage = self.luggage_infant[idx % len(self.luggage_infant)]
            max_luggage = 10
        elif passenger_type == 'child':
            luggage = self.luggage_child[idx % len(self.luggage_child)]
            max_luggage = 32 if booking_class == 'business' else 23
        else:  # adult
            luggage = self.luggage_adult[idx % len(self.luggage_adult)]
            max_luggage = 32 if booking_class == 'business' else 23
        
        # Realistic weight distribution - some people pack light, others hit the limit
        if np.random.random() < 0.15:  # 15% pack very light
            luggage *= 0.4
        elif np.random.random() < 0.25:  # 25% pack close to limit
            luggage = max_luggage * np.random.uniform(0.9, 0.98)
        
        return round(max(0, luggage), 2), max_luggage

    def _generate_realistic_gate(self, origin_airport: str, aircraft_type: str) -> str:
        """Generate realistic gate based on airport and aircraft type."""
        large_airports = ['JNB', 'CPT', 'DUR', 'PLZ']
        medium_airports = ['BFN', 'ELS', 'GRJ', 'HDS', 'KIM', 'MQP', 'NTY', 'PZB', 'SBU', 'UTN', 'WEL']
        
        # Larger aircraft get specific gate types
        wide_body_aircraft = ['Boeing 777', 'Airbus A330', 'Airbus A340']
        regional_aircraft = ['ATR 72', 'Bombardier Q400', 'Embraer E170']
        
        if origin_airport in large_airports:
            if any(wb in aircraft_type for wb in wide_body_aircraft):
                gates = [f"A{num}" for num in range(1, 15)] + [f"B{num}" for num in range(1, 10)]
            elif any(ra in aircraft_type for ra in regional_aircraft):
                gates = [f"C{num}" for num in range(1, 20)]
            else:
                gates = [f"{letter}{num}" for letter in ['A', 'B', 'C'] for num in range(1, 25)]
        elif origin_airport in medium_airports:
            gates = [f"{letter}{num}" for letter in ['A', 'B'] for num in range(1, 15)]
        else:
            gates = [f"A{num}" for num in range(1, 8)]
        
        return random.choice(gates)

    def _generate_realistic_name(self, customer_id: str, passenger_idx: int, 
                                passenger_type: str, group_booking_type: str = 'individual') -> str:
        """Generate ultra-realistic passenger names."""
        main_name = self.customer_names.get(customer_id, f"Customer_{customer_id}")
        
        if passenger_idx == 0 and passenger_type != 'infant':
            return main_name
        
        if passenger_type == 'infant':
            surname = main_name.split()[-1] if ' ' in main_name else main_name
            return f"Infant {surname}"
        
        # Generate realistic name variations for group bookings
        if group_booking_type == 'sports_team':
            team_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor', 'Anderson']
            return f"{self.faker.first_name()} {random.choice(team_names)}"
        elif group_booking_type == 'corporate_event':
            return self.faker.name()
        elif group_booking_type == 'wedding_party':
            # Wedding parties often share surnames
            if np.random.random() < 0.4:  # 40% chance of shared surname
                surnames = main_name.split()[-1] if ' ' in main_name else main_name
                return f"{self.faker.first_name()} {surnames}"
            else:
                return self.faker.name()
        else:
            return self.faker.name()

    def generate_checkins(self):
        """Generate ultra-realistic check-ins dataset with minimum 50 checked-in passengers per flight."""
        print(f"Generating ULTRA-REALISTIC check-ins for {self.TARGET_YEAR}")
        print("Ultra-realistic features:")
        print("- Proper seat maps with blocking and conflict prevention")
        print("- Load factor based bumping/denial with realistic thresholds")
        print("- Minimum 50 checked-in passengers per flight")
        print("- Seat preference algorithms (window/aisle)")
        print("- Class-based check-in timing patterns")
        print("- Group booking aware name generation")
        print("- Aircraft-specific gate assignments")
        print("- Realistic luggage weight distributions")
        
        checkins = []
        checkin_counter = 1
        flight_seat_maps = {}
        flight_gates = {}
        
        # Process bookings by flight for realistic seat allocation
        flight_groups = self.checkin_data.groupby('planning_id')
        
        for planning_id, flight_bookings in tqdm(flight_groups, desc="Processing flights"):
            # Initialize flight-level data
            if len(flight_bookings) == 0:
                continue
                
            first_booking = flight_bookings.iloc[0]
            aircraft_type = first_booking['aircraft_type']
            aircraft_capacity = int(first_booking['aircraft_capacity'])
            scheduled_departure = first_booking['scheduled_departure']
            origin_airport = first_booking['origin_airport']
            
            # Calculate total passengers on this flight
            total_flight_passengers = flight_bookings['actual_passenger_count'].sum()
            
            # Create realistic seat map for this flight
            flight_seat_maps[planning_id] = self._create_flight_seat_map(aircraft_type, aircraft_capacity)
            flight_gates[planning_id] = self._generate_realistic_gate(origin_airport, aircraft_type)
            
            # Track checked-in passengers
            checked_in_count = 0
            total_passenger_count = flight_bookings['actual_passenger_count'].sum() + flight_bookings['num_infants'].sum()
            
            # Shuffle bookings to randomize check-in order
            flight_bookings = flight_bookings.sample(frac=1).reset_index(drop=True)
            
            # Process each booking on this flight
            for _, booking in flight_bookings.iterrows():
                booking_id = booking['booking_id']
                customer_id = booking['customer_id']
                booking_class = booking['booking_class']
                group_booking_type = booking.get('group_booking_type', 'individual')
                
                # Generate check-in time
                checkin_time = self._generate_ultra_realistic_checkin_time(
                    scheduled_departure, booking_class, checkin_counter
                )
                
                # Determine check-in status
                remaining_passengers = total_passenger_count - (checkin_counter - 1)
                status_probs = self._calculate_ultra_realistic_status_probs(
                    planning_id, total_flight_passengers, checked_in_count, remaining_passengers
                )
                
                # Generate passengers for this booking
                num_adults = booking['num_adults']
                num_children = booking['num_children'] 
                num_infants = booking['num_infants']
                total_passengers = num_adults + num_children + num_infants
                
                # Process each passenger
                passenger_idx = 0
                for passenger_type_count, passenger_type in [
                    (num_adults, 'adult'),
                    (num_children, 'child'),
                    (num_infants, 'infant')
                ]:
                    for i in range(passenger_type_count):
                        # Generate realistic name
                        passenger_name = self._generate_realistic_name(
                            customer_id, passenger_idx, passenger_type, group_booking_type
                        )
                        
                        # Assign check-in status for this passenger
                        checkin_status = np.random.choice(
                            list(status_probs.keys()), 
                            p=list(status_probs.values())
                        )
                        
                        # Assign seat with ultra-realistic logic
                        seat_allocation = None
                        if checkin_status == 'checked_in':
                            prefer_window = self.window_preference[checkin_counter % len(self.window_preference)]
                            prefer_aisle = self.aisle_preference[checkin_counter % len(self.aisle_preference)]
                            
                            seat_allocation = self._assign_optimal_seat(
                                flight_seat_maps[planning_id], aircraft_type, booking_class,
                                passenger_type, total_passengers, prefer_window, prefer_aisle
                            )
                            
                            if seat_allocation:
                                flight_seat_maps[planning_id][seat_allocation] = False  # Mark as occupied
                            else:
                                # No seat available - realistic bumping
                                checkin_status = 'ticket_bumping'
                        elif checkin_status == 'denied_boarding':
                            # Denied boarding passengers don't get seats
                            seat_allocation = None
                        elif passenger_type == 'infant':
                            # Infants don't get separate seats
                            seat_allocation = "Lap"
                        
                        # Assign realistic luggage
                        luggage, max_luggage = self._assign_realistic_luggage(
                            passenger_type, booking_class, checkin_counter
                        )
                        
                        # Create check-in record
                        checkin = {
                            'checkin_id': f"CI{self.TARGET_YEAR}{checkin_counter:06d}",
                            'booking_id': booking_id,
                            'planning_id': planning_id,
                            'customer_id': customer_id,
                            'passenger_name': passenger_name,
                            'passenger_type': passenger_type,
                            'checkin_status': checkin_status,
                            'gate_number': flight_gates[planning_id],
                            'seat_allocation': seat_allocation,
                            'max_luggage': max_luggage,
                            'checkin_luggage': luggage,
                            'checkin_time': checkin_time,
                            'booking_class': booking_class,
                            'group_booking_type': group_booking_type,
                            'total_flight_passengers': total_flight_passengers
                        }
                        
                        checkins.append(checkin)
                        if checkin_status == 'checked_in' and passenger_type != 'infant':
                            checked_in_count += 1
                        checkin_counter += 1
                        passenger_idx += 1
        
        # Create DataFrame with optimized dtypes
        checkins_df = pd.DataFrame(checkins)
        
        # Optimize memory usage
        checkins_df['checkin_status'] = checkins_df['checkin_status'].astype('category')
        checkins_df['passenger_type'] = checkins_df['passenger_type'].astype('category')
        checkins_df['booking_class'] = checkins_df['booking_class'].astype('category')
        checkins_df['group_booking_type'] = checkins_df['group_booking_type'].astype('category')
        checkins_df['gate_number'] = checkins_df['gate_number'].astype('category')
        checkins_df['checkin_time'] = pd.to_datetime(checkins_df['checkin_time'])
        
        # Validate minimum 50 checked-in passengers per flight
        checked_in_passengers = checkins_df[
            (checkins_df['checkin_status'] == 'checked_in') & 
            (checkins_df['passenger_type'] != 'infant')
        ].groupby('planning_id').size()
        underbooked_flights = checked_in_passengers[checked_in_passengers < 50]
        
        if not underbooked_flights.empty:
            print(f"\n❌ ERROR: Found {len(underbooked_flights)} flights with fewer than 50 checked-in passengers!")
            print("Underbooked flights (planning_id, checked-in passengers):")
            print(underbooked_flights.to_string())
        else:
            print("\n✅ SUCCESS: All flights have at least 50 checked-in passengers")
        
        print(f"\n=== ULTRA-REALISTIC CHECK-IN GENERATION COMPLETE ===")
        print(f"Total check-ins generated: {len(checkins_df):,}")
        print(f"Unique bookings: {checkins_df['booking_id'].nunique():,}")
        print(f"Unique customers: {checkins_df['customer_id'].nunique():,}")
        print(f"Unique flights: {checkins_df['planning_id'].nunique():,}")
        
        # Analyze small vs large flights
        small_flights = checkins_df[checkins_df['total_flight_passengers'] < 50]
        large_flights = checkins_df[checkins_df['total_flight_passengers'] >= 50]
        
        print(f"\nFlight size analysis:")
        print(f"Small flights (<50 passengers): {small_flights['planning_id'].nunique()} flights, {len(small_flights):,} check-ins")
        print(f"Large flights (≥50 passengers): {large_flights['planning_id'].nunique()} flights, {len(large_flights):,} check-ins")
        
        print(f"\nCheck-in status distribution:")
        for status, count in checkins_df['checkin_status'].value_counts().items():
            pct = count / len(checkins_df)
            print(f"  {status}: {count:,} ({pct:.1%})")
        
        # Verify small flights have 100% check-in rate
        if len(small_flights) > 0:
            small_flight_checkin_rate = (small_flights['checkin_status'] == 'checked_in').mean()
            print(f"\nSmall flights check-in rate: {small_flight_checkin_rate:.1%}")
            if small_flight_checkin_rate == 1.0:
                print("✅ SUCCESS: All passengers on small flights checked in!")
            else:
                print("❌ ERROR: Some passengers on small flights did not check in!")
        
        print(f"\nPassenger type distribution:")
        for ptype, count in checkins_df['passenger_type'].value_counts().items():
            pct = count / len(checkins_df)
            print(f"  {ptype}: {count:,} ({pct:.1%})")
        
        print(f"\nBooking class distribution:")
        for bclass, count in checkins_df['booking_class'].value_counts().items():
            pct = count / len(checkins_df)
            print(f"  {bclass}: {count:,} ({pct:.1%})")
        
        # Validate no seat conflicts
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
        
        # Save the generated check-ins to a parquet file for efficient storage
        output_path = f'airplane_data/checkins_{self.TARGET_YEAR}.parquet'
        checkins_df.to_parquet(output_path, index=False)
        print(f"\nCheck-ins data saved to {output_path}")
        
        return checkins_df

    def validate_checkins(self, checkins_df: pd.DataFrame) -> Dict[str, bool]:
        """Validate the generated check-ins for consistency and realism."""
        validation_results = {
            'no_duplicate_checkin_ids': True,
            'no_seat_conflicts': True,
            'valid_checkin_times': True,
            'valid_luggage_weights': True,
            'valid_seat_allocations': True,
            'realistic_status_distribution': True,
            'valid_gate_assignments': True,
            'minimum_50_checked_in_passengers': True
        }
        
        # Check for duplicate check-in IDs
        if checkins_df['checkin_id'].duplicated().any():
            validation_results['no_duplicate_checkin_ids'] = False
            print("Validation Error: Duplicate check-in IDs found")
        
        # Check for seat conflicts (excluding infants on lap)
        checked_in_with_seats = checkins_df[
            (checkins_df['checkin_status'] == 'checked_in') & 
            (checkins_df['seat_allocation'].notna()) &
            (checkins_df['seat_allocation'] != 'Lap')
        ]
        seat_conflicts = checked_in_with_seats.groupby(['planning_id', 'seat_allocation']).size()
        if (seat_conflicts > 1).any():
            validation_results['no_seat_conflicts'] = False
            print("Validation Error: Seat conflicts detected")
        
        # Validate check-in times
        flight_times = self.flight_data[['planning_id', 'scheduled_departure']].set_index('planning_id')
        checkins_with_flight = checkins_df.merge(flight_times, on='planning_id', how='left')
        time_diffs = (checkins_with_flight['scheduled_departure'] - checkins_with_flight['checkin_time']).dt.total_seconds() / 3600
        if (time_diffs < 0).any() or (time_diffs > 24).any():
            validation_results['valid_checkin_times'] = False
            print("Validation Error: Invalid check-in times (before flight or >24 hours)")
        
        # Validate luggage weights
        if (checkins_df['checkin_luggage'] > checkins_df['max_luggage']).any():
            validation_results['valid_luggage_weights'] = False
            print("Validation Error: Luggage weights exceed maximum allowed")
        
        # Validate seat allocations against aircraft configurations
        for planning_id, group in checkins_df[checkins_df['checkin_status'] == 'checked_in'].groupby('planning_id'):
            aircraft_type = self.checkin_data[self.checkin_data['planning_id'] == planning_id]['aircraft_type'].iloc[0]
            config = self.seat_configs.get(aircraft_type, self.seat_configs['default'])
            valid_seats = {f"{row}{letter}" for row in range(1, config['rows'] + 1) for letter in config['layout']}
            
            allocated_seats = group[group['seat_allocation'] != 'Lap']['seat_allocation']
            if not allocated_seats.isin(valid_seats).all():
                validation_results['valid_seat_allocations'] = False
                print(f"Validation Error: Invalid seat allocations for flight {planning_id}")
        
        # Validate status distribution realism
        status_dist = checkins_df['checkin_status'].value_counts(normalize=True)
        expected_ranges = {
            'checked_in': (0.7, 0.95),
            'no_show': (0.05, 0.15),
            'ticket_bumping': (0.0, 0.1),
            'denied_boarding': (0.0, 0.05)
        }
        for status, (min_val, max_val) in expected_ranges.items():
            if status in status_dist and not (min_val <= status_dist[status] <= max_val):
                validation_results['realistic_status_distribution'] = False
                print(f"Validation Error: Unrealistic {status} distribution ({status_dist.get(status, 0):.1%})")
        
        # Validate gate assignments
        valid_gates = set()
        for airport in self.checkin_data['origin_airport'].unique():
            for aircraft_type in self.checkin_data['aircraft_type'].unique():
                valid_gates.update([self._generate_realistic_gate(airport, aircraft_type)])
        if not checkins_df['gate_number'].isin(valid_gates).all():
            validation_results['valid_gate_assignments'] = False
            print("Validation Error: Invalid gate assignments detected")
        
        # Validate minimum 50 checked-in passengers per flight
        checked_in_passengers = checkins_df[
            (checkins_df['checkin_status'] == 'checked_in') & 
            (checkins_df['passenger_type'] != 'infant')
        ].groupby('planning_id').size()
        if (checked_in_passengers < 50).any():
            validation_results['minimum_50_checked_in_passengers'] = False
            print("Validation Error: Some flights have fewer than 50 checked-in passengers")
        
        print("\nValidation Summary:")
        for check, passed in validation_results.items():
            status = "✓" if passed else "✗"
            print(f"{status} {check}")
        
        return validation_results


# Example usage
if __name__ == "__main__":
    generator = UltraRealisticFastCheckInsGenerator(target_year=2021)
    checkins_df = generator.generate_checkins()
    validation_results = generator.validate_checkins(checkins_df)