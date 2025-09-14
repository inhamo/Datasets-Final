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

class DynamicAirlineBookingsGenerator:
    def __init__(self, target_year: int = 2013):
        """
        Initialize with target year.
        
        Args:
            target_year (int): Year to generate bookings for (default: 2013)
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
                # Remove duplicates if necessary (planes might have updates over years)
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
                # Remove duplicates if routes might be repeated across years
                self.routes_df = self.routes_df.drop_duplicates(subset=['route_id'], keep='last')
            else:
                raise FileNotFoundError(f"No route data files found from {BASE_YEAR} to {target_year}")
            
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Missing data file: {str(e)}")
        except Exception as e:
            raise Exception(f"Error loading data: {str(e)}")
                
        # Prepare data
        self._prepare_data()
        
        # Seat configurations by aircraft model
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
        
        # Initialize realistic load factor system
        self.route_popularity = self._calculate_route_popularity()
        self.seasonal_factors = self._get_seasonal_factors()
        self.route_categories = self._categorize_routes()

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

    def _calculate_route_popularity(self):
        """Calculate route popularity based on frequency and destinations."""
        route_counts = self.flight_data['route_id'].value_counts()
        
        # Normalize to 0-1 scale
        min_count, max_count = route_counts.min(), route_counts.max()
        popularity = {}
        
        for route_id, count in route_counts.items():
            normalized = (count - min_count) / (max_count - min_count) if max_count > min_count else 0.5
            popularity[route_id] = normalized
            
        return popularity

    def _categorize_routes(self):
        """Categorize routes based on cities and characteristics."""
        route_categories = {}
        
        # Major hubs and business centers
        major_hubs = {'Johannesburg', 'Cape Town', 'Durban'}
        business_centers = {'Johannesburg', 'Cape Town', 'Durban', 'Pretoria'}
        leisure_destinations = {'Cape Town', 'Durban', 'Port Elizabeth', 'East London'}
        
        for _, route in self.routes_df.iterrows():
            route_id = route['route_id']
            origin = route['origin_city']
            destination = route['destination_city']
            
            # Determine route category
            if origin in major_hubs and destination in major_hubs:
                category = 'trunk'  # Major hub to major hub
            elif origin in business_centers or destination in business_centers:
                category = 'business'  # Business routes
            elif origin in leisure_destinations or destination in leisure_destinations:
                category = 'leisure'  # Leisure routes
            else:
                category = 'regional'  # Regional routes
                
            route_categories[route_id] = category
            
        return route_categories

    def _get_seasonal_factors(self):
        """Define seasonal load factor adjustments by month."""
        return {
            1: 1.15,  # January (summer holidays, high demand)
            2: 0.92,  # February (post-holiday lull)
            3: 1.05,  # March (Easter period)
            4: 1.12,  # April (Easter holidays)
            5: 0.98,  # May (moderate demand)
            6: 1.08,  # June (winter school holidays)
            7: 1.08,  # July (winter school holidays)
            8: 0.96,  # August (end of winter holidays)
            9: 1.06,  # September (spring school holidays)
            10: 1.02, # October (moderate demand)
            11: 1.08, # November (pre-summer rush)
            12: 1.22, # December (peak summer holidays)
        }

    def _calculate_realistic_load_factor(self, flight_row):
        """Calculate realistic load factor for each individual flight."""
        # Base load factors by route category
        base_load_factors = {
            'trunk': 0.88,      # Major routes (JNB-CPT, JNB-DUR)
            'business': 0.82,   # Business routes
            'leisure': 0.85,    # Leisure destinations
            'regional': 0.75    # Smaller regional routes
        }
        
        route_id = flight_row['route_id']
        departure_date = flight_row['scheduled_departure']
        aircraft_capacity = flight_row['aircraft_capacity']
        
        # Get base load factor
        route_category = self.route_categories.get(route_id, 'regional')
        base_lf = base_load_factors[route_category]
        
        # Seasonal adjustment
        month = departure_date.month
        seasonal_factor = self.seasonal_factors.get(month, 1.0)
        
        # Day of week effect
        dow = departure_date.weekday()  # 0=Monday, 6=Sunday
        if dow in [0, 4]:  # Monday, Friday - business travel
            dow_factor = 1.08 if route_category == 'business' else 1.05
        elif dow in [5, 6]:  # Saturday, Sunday - leisure travel
            dow_factor = 1.12 if route_category == 'leisure' else 0.92
        else:  # Tuesday-Thursday
            dow_factor = 1.02
            
        # Time of day effect
        hour = departure_date.hour
        if 6 <= hour <= 9:  # Early morning - business preferred
            time_factor = 1.10 if route_category in ['trunk', 'business'] else 1.05
        elif 17 <= hour <= 20:  # Evening - business return
            time_factor = 1.08 if route_category in ['trunk', 'business'] else 1.03
        elif 10 <= hour <= 16:  # Midday - moderate
            time_factor = 1.00
        else:  # Late night/very early - lower demand
            time_factor = 0.85
            
        # Aircraft size effect (larger planes harder to fill on some routes)
        if aircraft_capacity > 180:
            size_factor = 0.95 if route_category == 'regional' else 1.0
        elif aircraft_capacity < 100:
            size_factor = 1.05  # Smaller planes easier to fill
        else:
            size_factor = 1.0
            
        # Route popularity effect
        popularity = self.route_popularity.get(route_id, 0.5)
        popularity_factor = 0.9 + (popularity * 0.2)
        
        # Competition effect (random market conditions)
        competition_factor = np.random.uniform(0.95, 1.05)
        
        # Calculate final load factor
        final_lf = (base_lf * seasonal_factor * dow_factor * time_factor * 
                   size_factor * popularity_factor * competition_factor)
        
        # Apply realistic bounds and some randomness
        noise = np.random.normal(0, 0.05)  # Small random variation
        final_lf = final_lf + noise
        
        # Ensure reasonable bounds (40% to 105% - allowing slight overbooking)
        final_lf = np.clip(final_lf, 0.40, 1.05)
        
        return final_lf

    def _generate_booking_date(self, scheduled_departure, route_popularity=0.5):
        """Generate realistic booking dates."""
        if route_popularity > 0.7:  # Popular routes
            booking_windows = {
                'same_day': (0, 1, 0.03),
                'last_minute': (1, 7, 0.12),
                'normal': (7, 30, 0.55),
                'early': (30, 90, 0.25),
                'very_early': (90, 180, 0.05)
            }
        elif route_popularity < 0.3:  # Less popular routes
            booking_windows = {
                'same_day': (0, 1, 0.08),
                'last_minute': (1, 7, 0.22),
                'normal': (7, 30, 0.60),
                'early': (30, 90, 0.08),
                'very_early': (90, 180, 0.02)
            }
        else:  # Average routes
            booking_windows = {
                'same_day': (0, 1, 0.05),
                'last_minute': (1, 7, 0.17),
                'normal': (7, 30, 0.58),
                'early': (30, 90, 0.17),
                'very_early': (90, 180, 0.03)
            }
            
        # Sample booking window
        pattern = np.random.choice(
            list(booking_windows.keys()),
            p=[p[2] for p in booking_windows.values()]
        )
        
        min_days, max_days, _ = booking_windows[pattern]
        
        if pattern == 'same_day':
            hours_before = max(1.0, np.random.exponential(4))
            booking_date = scheduled_departure - timedelta(hours=hours_before)
        else:
            days_before = np.random.uniform(min_days, max_days)
            booking_date = scheduled_departure - timedelta(days=days_before)
            
            if np.random.random() < 0.7:  # 70% during business hours
                hour = np.random.randint(8, 18)
            else:
                hour = np.random.randint(0, 24)
            minute = np.random.randint(0, 60)
            booking_date = booking_date.replace(hour=hour, minute=minute)
            
        return booking_date
        
    def _calculate_dynamic_price(self, base_price, booking_date, scheduled_departure, 
                              current_bookings, total_capacity, route_id):
        """Calculate dynamic price with multiple factors."""
        month = scheduled_departure.month
        seasonal_factor = self.seasonal_factors.get(month, 1.0)
        
        days_ahead = (scheduled_departure - booking_date).days
        hours_ahead = (scheduled_departure - booking_date).total_seconds() / 3600
        
        if hours_ahead < 24:
            time_factor = 1.6
        elif days_ahead <= 3:
            time_factor = 1.4
        elif days_ahead <= 7:
            time_factor = 1.2
        elif days_ahead <= 21:
            time_factor = 1.0
        elif days_ahead <= 60:
            time_factor = 0.9
        else:
            time_factor = 0.85
            
        capacity_ratio = current_bookings / total_capacity
        if capacity_ratio >= 0.95:
            capacity_factor = 1.8
        elif capacity_ratio >= 0.85:
            capacity_factor = 1.5
        elif capacity_ratio >= 0.75:
            capacity_factor = 1.3
        elif capacity_ratio >= 0.6:
            capacity_factor = 1.1
        elif capacity_ratio >= 0.4:
            capacity_factor = 1.0
        else:
            capacity_factor = 0.9
            
        popularity = self.route_popularity.get(route_id, 0.5)
        popularity_factor = 0.9 + (popularity * 0.3)
        
        dow_factor = 1.1 if scheduled_departure.weekday() in [4, 5, 6] else 1.0
        market_factor = np.random.uniform(0.95, 1.05)
        
        final_price = (base_price * seasonal_factor * time_factor * 
                      capacity_factor * popularity_factor * dow_factor * market_factor)
        
        return round(final_price, 2)
        
    def _generate_passenger_composition(self):
        """Generate realistic passenger composition."""
        passenger_types = [
            (1, 0, 0, 0.65),  # Single traveler
            (2, 0, 0, 0.20),  # Couple
            (2, 1, 0, 0.06),  # Family with 1 child
            (2, 2, 0, 0.04),  # Family with 2 children
            (2, 1, 1, 0.02),  # Family with 1 child, 1 infant
            (3, 0, 0, 0.02),  # 3 adults
            (2, 3, 0, 0.01),  # Large family
        ]
        
        compositions, probabilities = zip(*[(comp[:3], comp[3]) for comp in passenger_types])
        adults, children, infants = compositions[np.random.choice(len(compositions), p=probabilities)]
        
        return adults, children, infants
        
    def _determine_trip_type(self, customer_city, origin_city, destination_city):
        """Determine if trip should be one-way or return."""
        if customer_city == origin_city:
            return 'return' if np.random.random() < 0.82 else 'one-way'
        elif customer_city == destination_city:
            return 'one-way' if np.random.random() < 0.65 else 'return'
        else:
            return 'return' if np.random.random() < 0.55 else 'one-way'
            
    def _find_return_flight(self, outbound_flight, customer_city):
        """Find appropriate return flight."""
        return_flights = self.flight_data[
            (self.flight_data['origin_city'] == outbound_flight['destination_city']) &
            (self.flight_data['destination_city'] == outbound_flight['origin_city']) &
            (self.flight_data['scheduled_departure'] > outbound_flight['scheduled_departure'])
        ].copy()
        
        if len(return_flights) == 0:
            return None
            
        outbound_date = outbound_flight['scheduled_departure']
        max_days = 3 if np.random.random() < 0.3 or outbound_date.weekday() >= 4 else 14
        min_return_date = outbound_date + timedelta(days=1)
        max_return_date = outbound_date + timedelta(days=max_days)
        
        return_flights = return_flights[
            (return_flights['scheduled_departure'] >= min_return_date) &
            (return_flights['scheduled_departure'] <= max_return_date)
        ]
        
        if len(return_flights) == 0:
            return None
            
        days_diff = (return_flights['scheduled_departure'] - outbound_date).dt.days
        weights = np.exp(-days_diff / 3) if customer_city == outbound_flight['origin_city'] else np.ones(len(return_flights))
        weights = weights / weights.sum()
        return_flight_idx = np.random.choice(len(return_flights), p=weights)
        
        return return_flights.iloc[return_flight_idx]
        
    def _generate_seat_assignment(self, aircraft_type, booking_class, existing_seats):
        """Generate valid seat assignment."""
        config = self.seat_configs.get(aircraft_type, self.seat_configs['default'])
        seat_letters = config['layout'].replace(' ', '')
        
        available_rows = list(range(1, config['business_rows'] + 1)) if booking_class == 'business' and config['business_rows'] > 0 else \
                         list(range(config['business_rows'] + 1, config['rows'] + 1))
        
        for _ in range(50):
            row = np.random.choice(available_rows)
            seat_letter = np.random.choice(list(seat_letters))
            seat = f"{row}{seat_letter}"
            
            if seat not in existing_seats:
                return seat
                
        return None
        
    def _assign_booking_status_and_dates(self, booking_date, scheduled_departure):
        """Assign booking status and related dates."""
        status_probs = {
            'confirmed': 0.87,
            'cancelled': 0.08,
            'rescheduled': 0.04,
            'on-hold': 0.01
        }
        
        status = np.random.choice(list(status_probs.keys()), p=list(status_probs.values()))
        
        cancelled_date = None
        rescheduled_date = None
        rescheduled_id = None
        on_hold_date = None
        on_hold_end_date = None
        
        if status == 'cancelled':
            if np.random.random() < 0.6:
                hours_after_booking = np.random.uniform(1, 48)
                cancelled_date = booking_date + timedelta(hours=hours_after_booking)
            else:
                days_before_flight = np.random.uniform(1, (scheduled_departure - booking_date).days)
                cancelled_date = scheduled_departure - timedelta(days=days_before_flight)
                
        elif status == 'rescheduled':
            max_days = min(7, (scheduled_departure - booking_date).days - 1)
            if max_days > 0:
                days_after_booking = np.random.uniform(1, max_days)
                rescheduled_date = booking_date + timedelta(days=days_after_booking)
                
        elif status == 'on-hold':
            days_after_booking = np.random.uniform(0.1, 2)
            on_hold_date = booking_date + timedelta(days=days_after_booking)
            on_hold_end_date = on_hold_date + timedelta(days=365)
            
        return status, cancelled_date, rescheduled_date, rescheduled_id, on_hold_date, on_hold_end_date
        
    def generate_bookings(self):
        """Generate the complete bookings dataset with realistic load factors."""
        print(f"Generating bookings for {self.TARGET_YEAR} with dynamic load factors")
        
        bookings = []
        booking_counter = 1
        flight_seat_assignments = {}
        flight_booking_counts = {}
        load_factor_stats = []
        
        # Use tqdm for progress bar
        for flight_idx, flight in tqdm(self.flight_data.iterrows(), total=len(self.flight_data), desc="Processing flights"):
            planning_id = flight['planning_id']
            route_id = flight['route_id']
            aircraft_capacity = int(flight['aircraft_capacity'])
            aircraft_type = flight['aircraft_type']
            base_price = flight.get('final_price_zar', 800)
            
            # Calculate realistic load factor for this specific flight
            target_load_factor = self._calculate_realistic_load_factor(flight)
            
            # Apply slight overbooking for realistic airline operations
            overbooking_factor = np.random.uniform(1.02, 1.08) if target_load_factor > 0.8 else 1.0
            target_bookings = int(aircraft_capacity * target_load_factor * overbooking_factor)
            
            # Store stats for reporting
            load_factor_stats.append({
                'planning_id': planning_id,
                'route_id': route_id,
                'route_category': self.route_categories.get(route_id, 'regional'),
                'departure_date': flight['scheduled_departure'],
                'target_load_factor': target_load_factor,
                'target_bookings': target_bookings,
                'capacity': aircraft_capacity
            })
            
            flight_seat_assignments[planning_id] = set()
            flight_booking_counts[planning_id] = 0
            
            current_bookings = 0
            attempts = 0
            max_attempts = target_bookings * 3
            
            while current_bookings < target_bookings and attempts < max_attempts:
                attempts += 1
                
                customer = self.main_holders.sample(n=1).iloc[0]
                customer_city = customer['city']
                origin_city = flight['origin_city']
                destination_city = flight['destination_city']
                
                if customer_city == 'Cape Town' and origin_city == 'Johannesburg':
                    continue
                    
                num_adults, num_children, num_infants = self._generate_passenger_composition()
                total_passengers = num_adults + num_children
                
                if current_bookings + total_passengers > target_bookings:
                    num_adults, num_children, num_infants = 1, 0, 0
                    total_passengers = 1
                    
                    if current_bookings + total_passengers > target_bookings:
                        continue
                        
                route_popularity = self.route_popularity.get(route_id, 0.5)
                booking_date = self._generate_booking_date(flight['scheduled_departure'], route_popularity)
                
                if booking_date >= flight['scheduled_departure']:
                    continue
                    
                trip_type = self._determine_trip_type(customer_city, origin_city, destination_city)
                booking_class = 'business' if np.random.random() < 0.05 else 'economy'
                
                price_per_ticket = self._calculate_dynamic_price(
                    base_price, booking_date, flight['scheduled_departure'],
                    current_bookings, aircraft_capacity, route_id
                )
                
                if booking_class == 'business':
                    price_per_ticket *= np.random.uniform(2.8, 3.5)
                    
                seat_request = self._generate_seat_assignment(
                    aircraft_type, booking_class, flight_seat_assignments[planning_id]
                )
                
                if seat_request:
                    flight_seat_assignments[planning_id].add(seat_request)
                    
                status, cancelled_date, rescheduled_date, rescheduled_id, on_hold_date, on_hold_end_date = \
                    self._assign_booking_status_and_dates(booking_date, flight['scheduled_departure'])
                    
                is_priority = np.random.random() < 0.18
                is_assisted = np.random.random() < 0.025
                is_special_needs = np.random.random() < 0.015
                is_lounge_access = np.random.random() < (0.08 if booking_class == 'economy' else 0.4)
                is_cancellation_refundable = np.random.random() < 0.45
                is_travel_protection = np.random.random() < 0.28
                is_cheap_hotel_accommodation = np.random.random() < 0.06
                is_car_rental = np.random.random() < 0.05
                
                booking = {
                    'booking_id': f"BK{self.TARGET_YEAR}{booking_counter:06d}",
                    'customer_id': customer['client_id'],
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
                    'rescheduled_id': rescheduled_id,
                    'seat_request': seat_request,
                    'price_per_ticket': round(price_per_ticket, 2),
                    'is_priority': is_priority,
                    'is_assisted': is_assisted,
                    'is_special_needs': is_special_needs,
                    'is_lounge_access': is_lounge_access,
                    'is_cancellation_refundable': is_cancellation_refundable,
                    'is_travel_protection': is_travel_protection,
                    'is_cheap_hotel_accommodation': is_cheap_hotel_accommodation,
                    'is_car_rental': is_car_rental
                }
                
                bookings.append(booking)
                current_bookings += total_passengers
                booking_counter += 1
                
                if trip_type == 'return':
                    return_flight = self._find_return_flight(flight, customer_city)
                    
                    if return_flight is not None:
                        return_planning_id = return_flight['planning_id']
                        
                        if return_planning_id not in flight_seat_assignments:
                            flight_seat_assignments[return_planning_id] = set()
                            
                        return_seat = self._generate_seat_assignment(
                            return_flight['aircraft_type'], booking_class, 
                            flight_seat_assignments[return_planning_id]
                        )
                        
                        if return_seat:
                            flight_seat_assignments[return_planning_id].add(return_seat)
                            
                        return_price = price_per_ticket * np.random.uniform(0.9, 1.1)
                        
                        return_booking = {
                            'booking_id': f"BK{self.TARGET_YEAR}{booking_counter:06d}",
                            'customer_id': customer['client_id'],
                            'planning_id': return_planning_id,
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
                            'rescheduled_id': rescheduled_id,
                            'seat_request': return_seat,
                            'price_per_ticket': round(return_price, 2),
                            'is_priority': is_priority,
                            'is_assisted': is_assisted,
                            'is_special_needs': is_special_needs,
                            'is_lounge_access': is_lounge_access,
                            'is_cancellation_refundable': is_cancellation_refundable,
                            'is_travel_protection': is_travel_protection,
                            'is_cheap_hotel_accommodation': is_cheap_hotel_accommodation,
                            'is_car_rental': is_car_rental
                        }
                        
                        bookings.append(return_booking)
                        booking_counter += 1
                
        bookings_df = pd.DataFrame(bookings)
        load_factor_df = pd.DataFrame(load_factor_stats)
        
        # Generate detailed load factor analysis
        self._generate_load_factor_report(load_factor_df)
        
        print(f"\n=== BOOKING GENERATION COMPLETE ===")
        print(f"Total bookings generated: {len(bookings_df):,}")
        print(f"Unique customers: {bookings_df['customer_id'].nunique():,}")
        print(f"Unique flights: {bookings_df['planning_id'].nunique():,}")
        print(f"Trip type distribution:")
        print(bookings_df['trip_type'].value_counts(normalize=True))
        print(f"Booking status distribution:")
        print(bookings_df['booking_status'].value_counts(normalize=True))
        print(f"Average price per ticket: R{bookings_df['price_per_ticket'].mean():.2f}")
        
        return bookings_df, load_factor_df
        
    def _generate_load_factor_report(self, load_factor_df):
        """Generate comprehensive load factor analysis."""
        print("\n" + "="*70)
        print("REALISTIC LOAD FACTOR ANALYSIS")
        print("="*70)
        
        # Overall statistics
        avg_lf = load_factor_df['target_load_factor'].mean()
        median_lf = load_factor_df['target_load_factor'].median()
        std_lf = load_factor_df['target_load_factor'].std()
        
        print(f"\nOVERALL LOAD FACTOR STATISTICS:")
        print(f"Average load factor: {avg_lf:.1%}")
        print(f"Median load factor: {median_lf:.1%}")
        print(f"Standard deviation: {std_lf:.1%}")
        print(f"Min load factor: {load_factor_df['target_load_factor'].min():.1%}")
        print(f"Max load factor: {load_factor_df['target_load_factor'].max():.1%}")
        
        # By route category
        print(f"\nLOAD FACTOR BY ROUTE CATEGORY:")
        category_stats = load_factor_df.groupby('route_category')['target_load_factor'].agg(['mean', 'std', 'count'])
        for category, stats in category_stats.iterrows():
            print(f"{category.title()}: {stats['mean']:.1%} ±{stats['std']:.1%} ({stats['count']} flights)")
            
        # By month
        print(f"\nSEASONAL LOAD FACTOR PATTERNS:")
        load_factor_df['month'] = load_factor_df['departure_date'].dt.month
        monthly_stats = load_factor_df.groupby('month')['target_load_factor'].mean()
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        for month, avg_lf in monthly_stats.items():
            print(f"{month_names[month-1]}: {avg_lf:.1%}")
            
        # By day of week
        print(f"\nLOAD FACTOR BY DAY OF WEEK:")
        load_factor_df['dow'] = load_factor_df['departure_date'].dt.dayofweek
        dow_stats = load_factor_df.groupby('dow')['target_load_factor'].mean()
        dow_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        for dow, avg_lf in dow_stats.items():
            print(f"{dow_names[dow]}: {avg_lf:.1%}")
            
        # By time of day
        print(f"\nLOAD FACTOR BY TIME OF DAY:")
        load_factor_df['hour'] = load_factor_df['departure_date'].dt.hour
        time_bins = [
            (5, 9, 'Early Morning (05:00-09:00)'),
            (9, 12, 'Morning (09:00-12:00)'),
            (12, 17, 'Afternoon (12:00-17:00)'),
            (17, 21, 'Evening (17:00-21:00)'),
            (21, 24, 'Night (21:00-24:00)'),
            (0, 5, 'Late Night (00:00-05:00)')
        ]
        
        for start, end, label in time_bins:
            if start < end:
                mask = (load_factor_df['hour'] >= start) & (load_factor_df['hour'] < end)
            else:  # Handle overnight period
                mask = (load_factor_df['hour'] >= start) | (load_factor_df['hour'] < end)
            
            if mask.sum() > 0:
                avg_lf = load_factor_df[mask]['target_load_factor'].mean()
                print(f"{label}: {avg_lf:.1%}")
        
        print("="*70)
        
    def save_bookings(self, bookings_df, load_factor_df=None, filename=None):
        """Save bookings and load factor data to parquet files."""
        if filename is None:
            filename = f'airplane_data/bookings_{self.TARGET_YEAR}.parquet'
            
        bookings_df.to_parquet(filename, index=False)
        print(f"Bookings saved to: {filename}")
        
        if load_factor_df is not None:
            lf_filename = f'airplane_data/load_factors_{self.TARGET_YEAR}.parquet'
            load_factor_df.to_parquet(lf_filename, index=False)
            print(f"Load factor analysis saved to: {lf_filename}")
            
        return filename
        
    def generate_summary_report(self, bookings_df):
        """Generate comprehensive summary report."""
        print("\n" + "="*60)
        print(f"AIRLINE BOOKINGS SUMMARY REPORT - {self.TARGET_YEAR}")
        print("="*60)
        
        print(f"\nBASIC STATISTICS")
        print(f"Total bookings: {len(bookings_df):,}")
        print(f"Unique customers: {bookings_df['customer_id'].nunique():,}")
        print(f"Unique flights: {bookings_df['planning_id'].nunique():,}")
        print(f"Date range: {bookings_df['booking_date'].min().date()} to {bookings_df['booking_date'].max().date()}")
        
        print(f"\nTRIP TYPE DISTRIBUTION")
        trip_dist = bookings_df['trip_type'].value_counts(normalize=True)
        for trip_type, pct in trip_dist.items():
            print(f"{trip_type.title()}: {pct:.1%}")
            
        print(f"\nBOOKING STATUS DISTRIBUTION")
        status_dist = bookings_df['booking_status'].value_counts(normalize=True)
        for status, pct in status_dist.items():
            print(f"{status.title()}: {pct:.1%}")
            
        print(f"\nPASSENGER COMPOSITION")
        print(f"Average adults per booking: {bookings_df['num_adults'].mean():.1f}")
        print(f"Average children per booking: {bookings_df['num_children'].mean():.1f}")
        print(f"Average infants per booking: {bookings_df['num_infants'].mean():.1f}")
        print(f"Bookings with children: {(bookings_df['num_children'] > 0).mean():.1%}")
        print(f"Bookings with infants: {(bookings_df['num_infants'] > 0).mean():.1%}")
        
        print(f"\nBOOKING CLASS DISTRIBUTION")
        class_dist = bookings_df['booking_class'].value_counts(normalize=True)
        for booking_class, pct in class_dist.items():
            print(f"{booking_class.title()}: {pct:.1%}")
            
        print(f"\nPRICING ANALYSIS")
        print(f"Average ticket price: R{bookings_df['price_per_ticket'].mean():.2f}")
        print(f"Median ticket price: R{bookings_df['price_per_ticket'].median():.2f}")
        print(f"Price range: R{bookings_df['price_per_ticket'].min():.2f} - R{bookings_df['price_per_ticket'].max():.2f}")
        
        economy_avg = bookings_df[bookings_df['booking_class'] == 'economy']['price_per_ticket'].mean()
        business_avg = bookings_df[bookings_df['booking_class'] == 'business']['price_per_ticket'].mean()
        print(f"Economy average: R{economy_avg:.2f}")
        print(f"Business average: R{business_avg:.2f}")
        
        print(f"\nADD-ON SERVICES ADOPTION")
        addons = ['is_priority', 'is_assisted', 'is_special_needs', 'is_lounge_access', 
                 'is_cancellation_refundable', 'is_travel_protection', 
                 'is_cheap_hotel_accommodation', 'is_car_rental']
        
        for addon in addons:
            if addon in bookings_df.columns:
                adoption_rate = bookings_df[addon].mean()
                service_name = addon.replace('is_', '').replace('_', ' ').title()
                print(f"{service_name}: {adoption_rate:.1%}")
                
        print(f"\nBOOKING TIMING ANALYSIS")
        bookings_df['days_ahead'] = (
            pd.to_datetime(bookings_df['planning_id'].map(
                dict(zip(self.flight_data['planning_id'], self.flight_data['scheduled_departure']))
            )) - bookings_df['booking_date']
        ).dt.days
        
        print(f"Average days booked ahead: {bookings_df['days_ahead'].mean():.1f}")
        print(f"Median days booked ahead: {bookings_df['days_ahead'].median():.1f}")
        
        same_day = (bookings_df['days_ahead'] < 1).mean()
        last_minute = ((bookings_df['days_ahead'] >= 1) & (bookings_df['days_ahead'] < 7)).mean()
        normal = ((bookings_df['days_ahead'] >= 7) & (bookings_df['days_ahead'] < 30)).mean()
        early = (bookings_df['days_ahead'] >= 30).mean()
        
        print(f"Same day bookings: {same_day:.1%}")
        print(f"Last minute (1-6 days): {last_minute:.1%}")
        print(f"Normal (7-29 days): {normal:.1%}")
        print(f"Early (30+ days): {early:.1%}")
        
        print(f"\nMONTHLY BOOKING PATTERNS")
        monthly_bookings = bookings_df['booking_date'].dt.month.value_counts().sort_index()
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        for month, count in monthly_bookings.items():
            pct = count / len(bookings_df)
            print(f"{month_names[month-1]}: {count:,} bookings ({pct:.1%})")
            
        print("\n" + "="*60)
        
        return {
            'total_bookings': len(bookings_df),
            'unique_customers': bookings_df['customer_id'].nunique(),
            'unique_flights': bookings_df['planning_id'].nunique(),
            'avg_price': bookings_df['price_per_ticket'].mean(),
            'trip_type_dist': trip_dist.to_dict(),
            'status_dist': status_dist.to_dict(),
            'avg_days_ahead': bookings_df['days_ahead'].mean()
        }

def generate_airline_bookings(target_year=2013, save_file=True):
    """
    Main function to generate airline bookings with realistic load factors.
    
    Args:
        target_year (int): Year to generate bookings for (default: 2013)
        save_file (bool): Whether to save the results to parquet (default: True)
        
    Returns:
        tuple: (bookings_df, load_factor_df) Generated datasets
    """
    print(f"Starting realistic airline bookings generation for {target_year}")
    print("Using dynamic load factors based on:")
    print("- Route category (trunk/business/leisure/regional)")
    print("- Seasonal patterns")
    print("- Day of week effects")
    print("- Time of day preferences")
    print("- Aircraft size considerations")
    print("- Route popularity")
    print("- Market competition")
    print("-" * 70)
    
    try:
        generator = DynamicAirlineBookingsGenerator(target_year=target_year)
        bookings_df, load_factor_df = generator.generate_bookings()
        summary = generator.generate_summary_report(bookings_df)
        
        if save_file:
            filename = generator.save_bookings(bookings_df, load_factor_df)
            print(f"\nData saved to: {filename}")
            
        print(f"\nSuccessfully generated {len(bookings_df):,} realistic bookings for {target_year}!")
        return bookings_df, load_factor_df
        
    except Exception as e:
        print(f"Error generating bookings: {str(e)}")
        raise

if __name__ == "__main__":
    seed_bytes = os.urandom(4)
    seed_int = int.from_bytes(seed_bytes, byteorder='big')
    random.seed(seed_int)
    np.random.seed(seed_int)
    TARGET_YEAR = 2024
    bookings, load_factors = generate_airline_bookings(
        target_year=TARGET_YEAR,
        save_file=True
    )
    
    print("\nSample bookings:")
    print(bookings.head())
    print("\nSample load factor analysis:")
    print(load_factors.head())
    print(f"\nAll done! Realistic bookings for {TARGET_YEAR} are ready to use.")
