import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from typing import Dict, List, Optional
import uuid
from tqdm import tqdm
from faker import Faker
import warnings
warnings.filterwarnings('ignore')

class RealisticFastCheckInsGenerator:
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
        
        # Seat configurations
        self.seat_configs = {
            'Boeing 737-800': {'rows': 32, 'seats_per_row': 6, 'layout': 'ABC DEF', 'business_rows': 4},
            'Boeing 737-400': {'rows': 25, 'seats_per_row': 6, 'layout': 'ABC DEF', 'business_rows': 3},
            'Airbus A320': {'rows': 30, 'seats_per_row': 6, 'layout': 'ABC DEF', 'business_rows': 4},
            'Airbus A319': {'rows': 26, 'seats_per_row': 6, 'layout': 'ABC DEF', 'business_rows': 3},
            'Embraer E190': {'rows': 25, 'seats_per_row': 4, 'layout': 'AC DF', 'business_rows': 5},
            'Embraer E170': {'rows': 19, 'seats_per_row': 4, 'layout': 'AC DF', 'business_rows': 4},
            'ATR 72': {'rows': 18, 'seats_per_row': 4, 'layout': 'AC DF', 'business_rows': 0},
            'Bombardier Q400': {'rows': 20, 'seats_per_row': 4, 'layout': 'AC DF', 'business_rows': 0},
            'default': {'rows': 25, 'seats_per_row': 6, 'layout': 'ABC DEF', 'business_rows': 3}
        }
        
        # Check-in status probabilities (keeping realistic logic)
        self.base_status_probs = {
            'checked_in': 0.90,
            'no_show': 0.07,
            'ticket_bumping': 0.02,
            'denied_boarding': 0.01
        }
        
        # Initialize Faker for realistic names
        self.faker = Faker(['en_US', 'en_GB', 'zu_ZA'])
        
        # Pre-generate some random values for speed (but keep realistic logic)
        self._pregenerate_some_values()

    def _prepare_data(self):
        """Prepare and merge datasets."""
        # Convert date columns
        self.bookings_df['booking_date'] = pd.to_datetime(self.bookings_df['booking_date'])
        self.flight_schedule_df['scheduled_departure'] = pd.to_datetime(self.flight_schedule_df['scheduled_departure'])
        self.clients_df['date_of_registration'] = pd.to_datetime(self.clients_df['date_of_registration'])
        self.clients_df['dob'] = pd.to_datetime(self.clients_df['dob'])
        
        # Filter valid bookings (confirmed or rescheduled, not cancelled)
        self.valid_bookings = self.bookings_df[
            self.bookings_df['booking_status'].isin(['confirmed', 'rescheduled'])
        ].copy()
        
        # Merge with flight data to get capacity and calculate load factors
        self.flight_data = self.flight_schedule_df.merge(
            self.routes_df[['route_id', 'origin_airport', 'destination_airport']],
            on='route_id', how='left'
        ).merge(
            self.planes_df[['plane_id', 'aircraft_model', 'capacity']],
            on='plane_id', how='left'
        )
        
        # Calculate bookings per flight and load factors
        bookings_per_flight = self.valid_bookings.groupby('planning_id').agg({
            'num_adults': 'sum',
            'num_children': 'sum',
            'num_infants': 'sum'
        }).reset_index()
        bookings_per_flight['total_passengers'] = (
            bookings_per_flight['num_adults'] + 
            bookings_per_flight['num_children']
        )
        
        # Merge to get capacity and calculate load factors
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
        
        # Merge bookings with flight schedule, routes, and planes
        self.checkin_data = self.valid_bookings.merge(
            self.flight_data[['planning_id', 'route_id', 'plane_id', 'scheduled_departure', 'origin_airport', 'aircraft_model', 'capacity']],
            on='planning_id', how='left'
        )
        
        # Clean data
        self.checkin_data['aircraft_type'] = self.checkin_data['aircraft_model'].fillna('default')
        self.checkin_data['aircraft_capacity'] = self.checkin_data['capacity'].fillna(150)
        
        # Apply load factor logic: 
        # - If load factor < 60%, keep ALL bookings
        # - If load factor >= 60%, sample to maintain realism
        filtered_bookings = []
        
        for planning_id, group in self.checkin_data.groupby('planning_id'):
            load_factor = self.flight_load_factors.get(planning_id, 0.5)
            
            if load_factor < 0.60:
                # Keep ALL bookings for low load factor flights
                filtered_bookings.append(group)
            else:
                # Sample bookings for realistic high load factor flights
                sample_rate = max(0.7, min(0.95, 0.6 / load_factor))  # Adaptive sampling
                sampled_group = group.sample(frac=sample_rate, random_state=42)
                filtered_bookings.append(sampled_group)
        
        self.checkin_data = pd.concat(filtered_bookings, ignore_index=True)
        
        # Create customer lookup for names
        self.customer_names = dict(zip(self.clients_df['client_id'], self.clients_df['name']))
        
        print(f"Loaded data for {self.TARGET_YEAR}:")
        print(f"- {len(self.valid_bookings):,} total valid bookings")
        print(f"- {len(self.checkin_data):,} check-in eligible records after load factor logic")
        print(f"- {self.checkin_data['planning_id'].nunique()} unique flights")
        
        # Show load factor distribution
        load_factors = [self.flight_load_factors.get(pid, 0) for pid in self.checkin_data['planning_id'].unique()]
        print(f"Load factor stats: min={min(load_factors):.1%}, max={max(load_factors):.1%}, avg={np.mean(load_factors):.1%}")

    def _pregenerate_some_values(self):
        """Pre-generate some values for speed while keeping realism."""
        # Pre-generate luggage weights
        self.luggage_adult = np.random.normal(18, 4, 10000)
        self.luggage_child = np.random.normal(12, 3, 10000)  
        self.luggage_infant = np.random.uniform(0, 5, 10000)
        
        # Pre-generate check-in timing
        self.online_checkin_hours = np.random.uniform(2, 24, 10000)
        self.airport_checkin_hours = np.random.uniform(0.5, 2, 10000)

    def _calculate_load_factor_adjusted_status_probs(self, planning_id):
        """Calculate realistic status probabilities based on load factor."""
        load_factor = self.flight_load_factors.get(planning_id, 0.5)
        probs = self.base_status_probs.copy()
        
        # Adjust probabilities based on load factor
        if load_factor > 1.0:  # Overbooked flights
            probs['ticket_bumping'] = min(0.15, 0.02 + (load_factor - 1.0) * 0.3)
            probs['denied_boarding'] = min(0.08, 0.01 + (load_factor - 1.0) * 0.15)
            probs['checked_in'] = 1.0 - probs['ticket_bumping'] - probs['denied_boarding'] - 0.03
            probs['no_show'] = 0.03  # Reduce no-shows on overbooked flights
        elif load_factor > 0.9:  # Nearly full flights
            probs['ticket_bumping'] = min(0.08, probs['ticket_bumping'] * 2)
            probs['denied_boarding'] = min(0.03, probs['denied_boarding'] * 2)
            probs['checked_in'] = 1.0 - probs['ticket_bumping'] - probs['denied_boarding'] - probs['no_show']
        elif load_factor < 0.4:  # Very low load factor
            probs['no_show'] = min(0.12, probs['no_show'] * 1.5)  # More no-shows on empty flights
            probs['ticket_bumping'] = 0.005
            probs['denied_boarding'] = 0.001
            probs['checked_in'] = 1.0 - probs['no_show'] - probs['ticket_bumping'] - probs['denied_boarding']
        
        # Normalize probabilities
        total = sum(probs.values())
        return {k: v/total for k, v in probs.items()}

    def _generate_realistic_checkin_time(self, scheduled_departure, idx):
        """Generate realistic check-in time."""
        if np.random.random() < 0.7:  # 70% online check-in
            hours_before = self.online_checkin_hours[idx % len(self.online_checkin_hours)]
        else:  # 30% airport check-in
            hours_before = self.airport_checkin_hours[idx % len(self.airport_checkin_hours)]
        
        return scheduled_departure - timedelta(hours=hours_before)

    def _generate_seat_allocation(self, aircraft_type, booking_class, existing_seats, is_infant=False, adult_seat=None):
        """Generate realistic seat allocation with conflict checking."""
        if is_infant and adult_seat:
            return f"{adult_seat}-Infant"
        
        config = self.seat_configs.get(aircraft_type, self.seat_configs['default'])
        seat_letters = config['layout'].replace(' ', '')
        
        # Choose appropriate rows based on class
        if booking_class == 'business' and config['business_rows'] > 0:
            available_rows = list(range(1, config['business_rows'] + 1))
        else:
            available_rows = list(range(config['business_rows'] + 1, config['rows'] + 1))
        
        # Try to find available seat (with conflict checking)
        for attempt in range(50):  # Reasonable number of attempts
            row = np.random.choice(available_rows)
            seat_letter = np.random.choice(list(seat_letters))
            seat = f"{row}{seat_letter}"
            
            if seat not in existing_seats:
                return seat
        
        # If no seat found, return None (will trigger bumping/denial)
        return None

    def _assign_realistic_luggage(self, is_adult, is_child, is_infant, booking_class, idx):
        """Assign realistic luggage weights."""
        if is_infant:
            luggage = self.luggage_infant[idx % len(self.luggage_infant)]
            max_luggage = 10
        elif is_child:
            luggage = self.luggage_child[idx % len(self.luggage_child)]
            max_luggage = 46 if booking_class == 'business' else 23
        else:
            luggage = self.luggage_adult[idx % len(self.luggage_adult)]
            max_luggage = 46 if booking_class == 'business' else 23
        
        luggage = max(0, luggage)  # No negative weights
        return round(luggage, 2), max_luggage

    def _generate_realistic_gate(self, origin_airport):
        """Generate realistic gate based on airport size."""
        large_airports = ['JNB', 'CPT', 'DUR']
        if origin_airport in large_airports:
            gates = [f"{letter}{num}" for letter in ['A', 'B', 'C'] for num in range(1, 21)]
        else:
            gates = [f"{letter}{num}" for letter in ['A', 'B'] for num in range(1, 11)]
        return random.choice(gates)

    def _generate_realistic_name(self, customer_id, passenger_idx, is_infant=False):
        """Generate realistic passenger names using faker."""
        main_name = self.customer_names.get(customer_id, f"Customer_{customer_id}")
        
        if passenger_idx == 0 and not is_infant:
            return main_name
        
        if is_infant:
            surname = main_name.split()[-1] if ' ' in main_name else main_name
            return f"Infant {surname}"
        
        # Generate realistic name variation
        return self.faker.name()

    def generate_checkins(self):
        """Generate realistic check-ins dataset optimized for speed."""
        print(f"Generating realistic check-ins for {self.TARGET_YEAR}")
        print("Logic: Load factor <60% = ALL bookings, ≥60% = sampled for realism")
        print("Realistic features: no-shows, bumping (esp. >100% load), seat conflicts, faker names")
        
        checkins = []
        checkin_counter = 1
        flight_seat_assignments = {}
        flight_gates = {}
        
        for _, booking in tqdm(self.checkin_data.iterrows(), total=len(self.checkin_data), desc="Processing bookings"):
            booking_id = booking['booking_id']
            planning_id = booking['planning_id']
            customer_id = booking['customer_id']
            aircraft_type = booking['aircraft_type']
            booking_class = booking['booking_class']
            scheduled_departure = booking['scheduled_departure']
            origin_airport = booking['origin_airport']
            
            # Initialize flight-level data
            if planning_id not in flight_seat_assignments:
                flight_seat_assignments[planning_id] = set()
            if planning_id not in flight_gates:
                flight_gates[planning_id] = self._generate_realistic_gate(origin_airport)
            
            gate_number = flight_gates[planning_id]
            
            # Generate check-in time
            checkin_time = self._generate_realistic_checkin_time(scheduled_departure, checkin_counter)
            
            # Get realistic status probabilities based on load factor
            status_probs = self._calculate_load_factor_adjusted_status_probs(planning_id)
            checkin_status = np.random.choice(
                list(status_probs.keys()), 
                p=list(status_probs.values())
            )
            
            # Generate passengers
            num_adults = booking['num_adults']
            num_children = booking['num_children'] 
            num_infants = booking['num_infants']
            total_passengers = num_adults + num_children + num_infants
            
            adult_seats = []  # Track adult seats for infant assignment
            
            # Process each passenger
            for i in range(total_passengers):
                is_adult = i < num_adults
                is_child = num_adults <= i < (num_adults + num_children)
                is_infant = i >= (num_adults + num_children)
                
                # Generate realistic name
                passenger_name = self._generate_realistic_name(customer_id, i, is_infant)
                
                # Assign seat with conflict checking
                if is_infant and adult_seats:
                    seat_allocation = self._generate_seat_allocation(
                        aircraft_type, booking_class, flight_seat_assignments[planning_id], 
                        is_infant=True, adult_seat=random.choice(adult_seats)
                    )
                else:
                    seat_allocation = self._generate_seat_allocation(
                        aircraft_type, booking_class, flight_seat_assignments[planning_id]
                    )
                
                # Handle seat conflicts realistically
                if seat_allocation is None and checkin_status == 'checked_in':
                    checkin_status = 'ticket_bumping'  # No seat available
                elif seat_allocation:
                    flight_seat_assignments[planning_id].add(seat_allocation)
                    if is_adult:
                        adult_seats.append(seat_allocation)
                
                # Assign realistic luggage
                luggage, max_luggage = self._assign_realistic_luggage(
                    is_adult, is_child, is_infant, booking_class, checkin_counter + i
                )
                
                # Create check-in record
                checkin = {
                    'checkin_id': f"CI{self.TARGET_YEAR}{checkin_counter:06d}",
                    'booking_id': booking_id,
                    'planning_id': planning_id,
                    'customer_id': customer_id,
                    'customer_name': passenger_name,
                    'checkin_status': checkin_status,
                    'gate_number': gate_number,
                    'seat_allocation': seat_allocation,
                    'max_luggage': max_luggage,
                    'checkin_luggage': luggage,
                    'checkin_time': checkin_time
                }
                
                checkins.append(checkin)
                checkin_counter += 1
        
        # Create DataFrame
        checkins_df = pd.DataFrame(checkins)
        
        # Optimize memory
        checkins_df['checkin_status'] = checkins_df['checkin_status'].astype('category')
        checkins_df['gate_number'] = checkins_df['gate_number'].astype('category')
        checkins_df['checkin_time'] = pd.to_datetime(checkins_df['checkin_time'])
        
        print(f"\n=== REALISTIC CHECK-IN GENERATION COMPLETE ===")
        print(f"Total check-ins generated: {len(checkins_df):,}")
        print(f"Unique bookings: {checkins_df['booking_id'].nunique():,}")
        print(f"Unique customers: {checkins_df['customer_id'].nunique():,}")
        print(f"Unique flights: {checkins_df['planning_id'].nunique():,}")
        print(f"Check-in status distribution:")
        for status, count in checkins_df['checkin_status'].value_counts().items():
            pct = count / len(checkins_df)
            print(f"  {status}: {count:,} ({pct:.1%})")
        print(f"Average luggage per check-in: {checkins_df['checkin_luggage'].mean():.2f} kg")
        
        return checkins_df

    def save_checkins(self, checkins_df, filename=None):
        """Save check-ins to parquet file."""
        if filename is None:
            filename = f'airplane_data/checkins_{self.TARGET_YEAR}.parquet'
        
        checkins_df.to_parquet(filename, index=False)
        print(f"Check-ins saved to: {filename}")
        return filename

def generate_realistic_fast_checkins(target_year=2021, save_file=True):
    """
    Main function to generate realistic check-ins dataset with speed optimizations.
    
    Args:
        target_year (int): Year to generate check-ins for
        save_file (bool): Whether to save the results to parquet
        
    Returns:
        pd.DataFrame: Generated check-ins dataset
    """
    print(f"Starting REALISTIC FAST check-ins generation for {target_year}")
    print("Features:")
    print("- Load factor <60%: ALL bookings get check-ins")
    print("- Load factor ≥60%: Sampled bookings for realism")
    print("- Realistic status logic: no-shows, bumping (heavy on >100% load)")
    print("- Seat conflict checking with realistic bumping")
    print("- Faker library for realistic names")
    print("- Speed optimizations: pre-generated values, efficient lookups")
    print("-" * 70)
    
    try:
        generator = RealisticFastCheckInsGenerator(target_year=target_year)
        checkins_df = generator.generate_checkins()

        if save_file:
            filename = generator.save_checkins(checkins_df)
            print(f"\nData saved to: {filename}")
            
        print(f"\nSuccessfully generated {len(checkins_df):,} realistic check-ins for {target_year}!")
        return checkins_df
        
    except Exception as e:
        print(f"Error generating check-ins: {str(e)}")
        raise

if __name__ == "__main__":
    TARGET_YEAR = 2022
    checkins = generate_realistic_fast_checkins(
        target_year=TARGET_YEAR,
        save_file=True
    )
    
    print("\nSample check-ins:")
    print(checkins.head())
    print(f"\nAll done! Realistic fast check-ins for {TARGET_YEAR} are ready to use.")