import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from typing import Dict, List, Tuple, Optional
import uuid
import os
import warnings
from tqdm import tqdm

warnings.filterwarnings('ignore')
BASE_YEAR = 2021

class FastAirlineBookingsGenerator:
    def __init__(self, target_year: int = BASE_YEAR):
        """
        Initialize with target year.
        
        Args:
            target_year (int): Year to generate bookings for (default: BASE_YEAR)
        """
        self.TARGET_YEAR = target_year
        
        # Load data dynamically based on target year
        try:
            self.flight_schedule_df = pd.read_parquet(f'airplane_data/flight_schedule_{target_year}.parquet')
            
            client_files = []
            for year in range(BASE_YEAR, target_year + 1):
                file_path = f'airplane_data/clients_{year}.parquet'
                if os.path.exists(file_path):
                    client_files.append(pd.read_parquet(file_path))
            
            if client_files:
                self.clients_df = pd.concat(client_files, ignore_index=True)
                self.clients_df = self.clients_df.drop_duplicates(subset=['client_id'], keep='last')
            else:
                raise FileNotFoundError(f"No plane data files found from {BASE_YEAR} to {target_year}")
            
            # For planes: combine from BASE_YEAR to target_year
            planes_files = []
            for year in range(BASE_YEAR, target_year + 1):
                file_path = f'airplane_data/planes_{year}.parquet'
                if os.path.exists(file_path):
                    planes_files.append(pd.read_parquet(file_path))
            
            if planes_files:
                self.planes_df = pd.concat(planes_files, ignore_index=True)
                self.planes_df = self.planes_df.drop_duplicates(subset=['plane_id'], keep='last')
            else:
                raise FileNotFoundError(f"No plane data files found from {BASE_YEAR} to {target_year}")
            
            # For routes: combine from BASE_YEAR to target_year
            routes_files = []
            for year in range(BASE_YEAR, target_year + 1):
                file_path = f'airplane_data/routes_{year}.parquet'
                if os.path.exists(file_path):
                    routes_files.append(pd.read_parquet(file_path))
            
            if routes_files:
                self.routes_df = pd.concat(routes_files, ignore_index=True)
                self.routes_df = self.routes_df.drop_duplicates(subset=['route_id'], keep='last')
            else:
                raise FileNotFoundError(f"No route data files found from {BASE_YEAR} to {target_year}")
            
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Missing data file: {str(e)}")
        except Exception as e:
            raise Exception(f"Error loading data: {str(e)}")
                
        # Prepare data
        self._prepare_data()
        
        # Pre-generate random values for speed
        self._pregenerate_random_values()

    def _prepare_data(self):
        """Prepare and merge all datasets."""
        # Convert date columns
        self.flight_schedule_df['scheduled_departure'] = pd.to_datetime(self.flight_schedule_df['scheduled_departure'])
        self.flight_schedule_df['scheduled_arrival'] = pd.to_datetime(self.flight_schedule_df['scheduled_arrival'])
        
        if 'actual_departure' in self.flight_schedule_df.columns:
            self.flight_schedule_df['actual_departure'] = pd.to_datetime(self.flight_schedule_df['actual_departure'])
        if 'actual_arrival' in self.flight_schedule_df.columns:
            self.flight_schedule_df['actual_arrival'] = pd.to_datetime(self.flight_schedule_df['actual_arrival'])
            
        self.clients_df['dob'] = pd.to_datetime(self.clients_df['dob'])
        self.clients_df['date_of_registration'] = pd.to_datetime(self.clients_df['date_of_registration'])
        
        # Filter only main account holders for booking
        self.main_holders = self.clients_df[self.clients_df['is_main_holder'] == True].copy()
        
        # Merge flight schedule with routes and planes
        self.flight_data = self.flight_schedule_df.merge(
            self.routes_df, on='route_id', how='left'
        ).merge(
            self.planes_df[['plane_id', 'aircraft_model', 'capacity']], 
            on='plane_id', 
            how='left'
        )
        
        # Filter flights for target year only
        self.flight_data = self.flight_data[
            self.flight_data['scheduled_departure'].dt.year == self.TARGET_YEAR
        ].copy()
        
        # Rename aircraft_model to aircraft_type for consistency
        self.flight_data['aircraft_type'] = self.flight_data['aircraft_model']
        
        # Fill missing aircraft types or capacities with default
        self.flight_data['aircraft_type'] = self.flight_data['aircraft_type'].fillna('default')
        self.flight_data['aircraft_capacity'] = self.flight_data['capacity'].fillna(150)
        
        # Drop the temporary capacity column from planes_df
        self.flight_data = self.flight_data.drop(columns=['capacity'], errors='ignore')
        
        print(f"Loaded data for {self.TARGET_YEAR}:")
        print(f"- {len(self.main_holders):,} main account holders")
        print(f"- {len(self.flight_data):,} scheduled flights")
        print(f"- {self.flight_data['route_id'].nunique()} unique routes")

    def _pregenerate_random_values(self):
        """Pre-generate random values for speed optimization."""
        num_flights = len(self.flight_data)
        
        # Pre-generate load factors (50% to 115%)
        self.load_factors = np.random.uniform(0.50, 1.15, num_flights)
        
        # Pre-generate passenger compositions (vectorized)
        passenger_probs = [0.65, 0.20, 0.06, 0.04, 0.02, 0.02, 0.01]  # Probabilities for different family sizes
        passenger_types = [
            (1, 0, 0), (2, 0, 0), (2, 1, 0), (2, 2, 0), 
            (2, 1, 1), (3, 0, 0), (2, 3, 0)
        ]
        
        # Pre-generate random choices for efficiency
        self.random_passenger_choices = np.random.choice(
            len(passenger_types), size=num_flights * 5, p=passenger_probs
        )
        self.random_passenger_types = [passenger_types[i] for i in self.random_passenger_choices]
        
        # Pre-generate other random values
        self.random_booking_classes = np.random.choice(
            ['economy', 'business'], size=num_flights * 5, p=[0.95, 0.05]
        )
        self.random_trip_types = np.random.choice(
            ['one-way', 'return'], size=num_flights * 5, p=[0.25, 0.75]
        )
        self.random_status_choices = np.random.choice(
            ['confirmed', 'cancelled', 'rescheduled', 'on-hold'], 
            size=num_flights * 5, p=[0.87, 0.08, 0.04, 0.01]
        )
        
        # Pre-generate booking timing offsets (in hours)
        self.booking_offsets = np.random.exponential(scale=168, size=num_flights * 5)  # ~1 week average
        
        # Pre-generate pricing multipliers
        self.price_multipliers = np.random.uniform(0.8, 1.8, num_flights * 5)
        
        # Pre-generate addon service flags
        self.addon_flags = {
            'is_priority': np.random.random(num_flights * 5) < 0.18,
            'is_assisted': np.random.random(num_flights * 5) < 0.025,
            'is_special_needs': np.random.random(num_flights * 5) < 0.015,
            'is_lounge_access': np.random.random(num_flights * 5) < 0.08,
            'is_cancellation_refundable': np.random.random(num_flights * 5) < 0.45,
            'is_travel_protection': np.random.random(num_flights * 5) < 0.28,
            'is_cheap_hotel_accommodation': np.random.random(num_flights * 5) < 0.06,
            'is_car_rental': np.random.random(num_flights * 5) < 0.05
        }

    def _simple_seat_assignment(self, aircraft_type):
        """Simple seat assignment without checking conflicts for speed."""
        # Default seat configuration
        max_row = 30
        seat_letters = ['A', 'B', 'C', 'D', 'E', 'F']
        
        row = np.random.randint(1, max_row + 1)
        seat_letter = np.random.choice(seat_letters)
        return f"{row}{seat_letter}"

    def _find_return_flight_fast(self, outbound_flight):
        """Fast return flight finding with simplified logic."""
        # Look for return flights within 1-7 days
        return_start = outbound_flight['scheduled_departure'] + timedelta(days=1)
        return_end = outbound_flight['scheduled_departure'] + timedelta(days=7)
        
        # Filter potential return flights
        return_candidates = self.flight_data[
            (self.flight_data['origin_city'] == outbound_flight['destination_city']) &
            (self.flight_data['destination_city'] == outbound_flight['origin_city']) &
            (self.flight_data['scheduled_departure'] >= return_start) &
            (self.flight_data['scheduled_departure'] <= return_end)
        ]
        
        if len(return_candidates) == 0:
            return None
            
        # Just pick a random return flight for speed
        return return_candidates.sample(n=1).iloc[0]

    def generate_bookings(self):
        """Generate bookings with very fast processing and random load factors."""
        print(f"Generating bookings for {self.TARGET_YEAR} with random load factors (50%-115%)")
        
        bookings = []
        booking_counter = 1
        random_idx = 0
        
        # Convert main_holders to numpy arrays for faster access
        customer_ids = self.main_holders['client_id'].values
        customer_cities = self.main_holders['city'].values
        
        # Use tqdm for progress bar
        for flight_idx, flight in tqdm(self.flight_data.iterrows(), total=len(self.flight_data), desc="Processing flights"):
            planning_id = flight['planning_id']
            aircraft_capacity = int(flight['aircraft_capacity'])
            aircraft_type = flight['aircraft_type']
            base_price = flight.get('final_price_zar', 800)
            
            # Use pre-generated random load factor
            target_load_factor = self.load_factors[flight_idx % len(self.load_factors)]
            
            # Calculate target bookings with simple overbooking
            overbooking_factor = 1.05 if target_load_factor > 0.9 else 1.0
            target_bookings = int(aircraft_capacity * target_load_factor * overbooking_factor)
            
            # Generate bookings for this flight
            current_bookings = 0
            
            while current_bookings < target_bookings:
                # Fast customer selection
                customer_idx = np.random.randint(0, len(customer_ids))
                customer_id = customer_ids[customer_idx]
                customer_city = customer_cities[customer_idx]
                
                # Use pre-generated passenger composition
                if random_idx >= len(self.random_passenger_types):
                    random_idx = 0
                    
                num_adults, num_children, num_infants = self.random_passenger_types[random_idx]
                total_passengers = num_adults + num_children
                
                # Check if we can fit these passengers
                if current_bookings + total_passengers > target_bookings:
                    break
                
                # Use pre-generated values
                booking_class = self.random_booking_classes[random_idx % len(self.random_booking_classes)]
                trip_type = self.random_trip_types[random_idx % len(self.random_trip_types)]
                status = self.random_status_choices[random_idx % len(self.random_status_choices)]
                
                # Simple booking date calculation
                hours_before = min(self.booking_offsets[random_idx % len(self.booking_offsets)], 2160)  # Max 90 days
                booking_date = flight['scheduled_departure'] - timedelta(hours=hours_before)
                
                # Simple pricing
                price_multiplier = self.price_multipliers[random_idx % len(self.price_multipliers)]
                price_per_ticket = base_price * price_multiplier
                
                if booking_class == 'business':
                    price_per_ticket *= 3.0
                
                # Simple seat assignment
                seat_request = self._simple_seat_assignment(aircraft_type)
                
                # Status-related dates (simplified)
                cancelled_date = None
                rescheduled_date = None
                on_hold_date = None
                on_hold_end_date = None
                
                if status == 'cancelled':
                    cancelled_date = booking_date + timedelta(hours=np.random.uniform(1, 240))
                elif status == 'on-hold':
                    on_hold_date = booking_date + timedelta(hours=np.random.uniform(1, 48))
                    on_hold_end_date = on_hold_date + timedelta(days=365)
                
                # Create outbound booking
                booking = {
                    'booking_id': f"BK{self.TARGET_YEAR}{booking_counter:06d}",
                    'customer_id': customer_id,
                    'planning_id': planning_id,
                    'booking_date': booking_date,
                    'trip_type': trip_type,
                    'num_adults': num_adults,
                    'num_children': num_children,
                    'num_infants': num_infants,
                    'booking_class': booking_class,
                    'booking_status': status,
                    'cancelled_date': cancelled_date,
                    'rescheduled_date': rescheduled_date,
                    'on_hold_date': on_hold_date,
                    'on_hold_end_date': on_hold_end_date,
                    'outbound_id': None,
                    'rescheduled_id': None,
                    'seat_request': seat_request,
                    'price_per_ticket': round(price_per_ticket, 2),
                    'is_priority': self.addon_flags['is_priority'][random_idx % len(self.addon_flags['is_priority'])],
                    'is_assisted': self.addon_flags['is_assisted'][random_idx % len(self.addon_flags['is_assisted'])],
                    'is_special_needs': self.addon_flags['is_special_needs'][random_idx % len(self.addon_flags['is_special_needs'])],
                    'is_lounge_access': self.addon_flags['is_lounge_access'][random_idx % len(self.addon_flags['is_lounge_access'])],
                    'is_cancellation_refundable': self.addon_flags['is_cancellation_refundable'][random_idx % len(self.addon_flags['is_cancellation_refundable'])],
                    'is_travel_protection': self.addon_flags['is_travel_protection'][random_idx % len(self.addon_flags['is_travel_protection'])],
                    'is_cheap_hotel_accommodation': self.addon_flags['is_cheap_hotel_accommodation'][random_idx % len(self.addon_flags['is_cheap_hotel_accommodation'])],
                    'is_car_rental': self.addon_flags['is_car_rental'][random_idx % len(self.addon_flags['is_car_rental'])]
                }
                
                bookings.append(booking)
                current_bookings += total_passengers
                booking_counter += 1
                
                # Handle return flight
                if trip_type == 'return':
                    return_flight = self._find_return_flight_fast(flight)
                    
                    if return_flight is not None:
                        return_price = price_per_ticket * np.random.uniform(0.9, 1.1)
                        return_seat = self._simple_seat_assignment(return_flight['aircraft_type'])
                        
                        return_booking = {
                            'booking_id': f"BK{self.TARGET_YEAR}{booking_counter:06d}",
                            'customer_id': customer_id,
                            'planning_id': return_flight['planning_id'],
                            'booking_date': booking_date,
                            'trip_type': 'return',
                            'num_adults': num_adults,
                            'num_children': num_children,
                            'num_infants': num_infants,
                            'booking_class': booking_class,
                            'booking_status': status,
                            'cancelled_date': cancelled_date,
                            'rescheduled_date': rescheduled_date,
                            'on_hold_date': on_hold_date,
                            'on_hold_end_date': on_hold_end_date,
                            'outbound_id': booking['booking_id'],
                            'rescheduled_id': None,
                            'seat_request': return_seat,
                            'price_per_ticket': round(return_price, 2),
                            'is_priority': booking['is_priority'],
                            'is_assisted': booking['is_assisted'],
                            'is_special_needs': booking['is_special_needs'],
                            'is_lounge_access': booking['is_lounge_access'],
                            'is_cancellation_refundable': booking['is_cancellation_refundable'],
                            'is_travel_protection': booking['is_travel_protection'],
                            'is_cheap_hotel_accommodation': booking['is_cheap_hotel_accommodation'],
                            'is_car_rental': booking['is_car_rental']
                        }
                        
                        bookings.append(return_booking)
                        booking_counter += 1
                
                random_idx += 1
                
        bookings_df = pd.DataFrame(bookings)
        
        print(f"\n=== FAST BOOKING GENERATION COMPLETE ===")
        print(f"Total bookings generated: {len(bookings_df):,}")
        print(f"Unique customers: {bookings_df['customer_id'].nunique():,}")
        print(f"Unique flights: {bookings_df['planning_id'].nunique():,}")
        print(f"Trip type distribution:")
        print(bookings_df['trip_type'].value_counts(normalize=True))
        print(f"Average price per ticket: R{bookings_df['price_per_ticket'].mean():.2f}")
        
        return bookings_df
        
    def save_bookings(self, bookings_df, filename=None):
        """Save bookings to parquet file."""
        if filename is None:
            filename = f'airplane_data/bookings_{self.TARGET_YEAR}.parquet'
            
        bookings_df.to_parquet(filename, index=False)
        print(f"Bookings saved to: {filename}")
        
        return filename

def generate_fast_airline_bookings(target_year=BASE_YEAR, save_file=True):
    """
    Main function to generate airline bookings super fast with random load factors.
    
    Args:
        target_year (int): Year to generate bookings for (default: BASE_YEAR)
        save_file (bool): Whether to save the results to parquet (default: True)
        
    Returns:
        pd.DataFrame: Generated bookings dataset
    """
    print(f"Starting FAST airline bookings generation for {target_year}")
    print("Using random load factors between 50% and 115%")
    print("Optimizations:")
    print("- Pre-generated random values")
    print("- Simplified seat assignment")
    print("- Fast return flight lookup")
    print("- Vectorized operations")
    print("-" * 70)
    
    try:
        generator = FastAirlineBookingsGenerator(target_year=target_year)
        bookings_df = generator.generate_bookings()
        
        if save_file:
            filename = generator.save_bookings(bookings_df)
            print(f"\nData saved to: {filename}")
            
        print(f"\nSuccessfully generated {len(bookings_df):,} bookings for {target_year} in record time!")
        return bookings_df
        
    except Exception as e:
        print(f"Error generating bookings: {str(e)}")
        raise

if __name__ == "__main__":
    seed_bytes = os.urandom(4)
    seed_int = int.from_bytes(seed_bytes, byteorder='big')
    random.seed(seed_int)
    np.random.seed(seed_int)
    TARGET_YEAR = 2024
    bookings = generate_fast_airline_bookings(
        target_year=TARGET_YEAR,
        save_file=True
    )
    
    print("\nSample bookings:")
    print(bookings.head())
    print(f"\nAll done! Fast bookings for {TARGET_YEAR} are ready to use.")