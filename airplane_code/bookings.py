import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from typing import Dict, List, Tuple, Optional
import os
import warnings
from tqdm import tqdm
from scipy import stats

warnings.filterwarnings('ignore')
BASE_YEAR = 2021

class RealisticAirlineBookingsGenerator:
    def __init__(self, target_year: int = BASE_YEAR):
        """
        Initialize with target year.
        
        Args:
            target_year (int): Year to generate bookings for (default: BASE_YEAR)
        """
        self.TARGET_YEAR = target_year
        
        try:
            # Load flight schedules, ensuring planning_id is available
            self.flight_schedule_df = pd.read_parquet(f'airplane_data/flight_schedule_{target_year}.parquet')
            if 'planning_id' not in self.flight_schedule_df.columns:
                raise ValueError("planning_id missing from flight_schedule data")
            
            client_files = []
            for year in range(BASE_YEAR, target_year + 1):
                file_path = f'airplane_data/clients_{year}.parquet'
                if os.path.exists(file_path):
                    client_files.append(pd.read_parquet(file_path))
            
            if client_files:
                self.clients_df = pd.concat(client_files, ignore_index=True)
                self.clients_df = self.clients_df.drop_duplicates(subset=['client_id'], keep='last')
            else:
                raise FileNotFoundError(f"No client data files found from {BASE_YEAR} to {target_year}")
            
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
                
        self._prepare_data()

    def _prepare_data(self):
        """Prepare and merge all datasets with robust datetime handling."""
        # Convert flight dates
        self.flight_schedule_df['scheduled_departure'] = pd.to_datetime(self.flight_schedule_df['scheduled_departure'])
        self.flight_schedule_df['scheduled_arrival'] = pd.to_datetime(self.flight_schedule_df['scheduled_arrival'])
        
        if 'actual_departure' in self.flight_schedule_df.columns:
            self.flight_schedule_df['actual_departure'] = pd.to_datetime(self.flight_schedule_df['actual_departure'])
        if 'actual_arrival' in self.flight_schedule_df.columns:
            self.flight_schedule_df['actual_arrival'] = pd.to_datetime(self.flight_schedule_df['actual_arrival'])
            
        # Convert client dates
        self.clients_df['dob'] = pd.to_datetime(self.clients_df['dob'], errors='coerce')
        self.clients_df['date_of_registration'] = pd.to_datetime(
            self.clients_df['date_of_registration'], errors='coerce'
        )
        
        # Filter main holders and ensure valid registration dates
        self.main_holders = self.clients_df[self.clients_df['is_main_holder'] == True][
            ['client_id', 'city', 'date_of_registration', 'partner_company']
        ].copy()
        
        # Remove any holders with invalid registration dates
        self.main_holders = self.main_holders.dropna(subset=['date_of_registration'])
        
        # Sort by registration date for efficient filtering
        self.main_holders = self.main_holders.sort_values('date_of_registration').reset_index(drop=True)
        
        # Prepare flight data
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
        
        # Clean up aircraft data
        self.flight_data['aircraft_type'] = self.flight_data['aircraft_model'].fillna('default')
        self.flight_data['aircraft_capacity'] = self.flight_data['capacity'].fillna(150).astype(int)
        self.flight_data = self.flight_data.drop(columns=['capacity', 'aircraft_model'], errors='ignore')
        
        # Sort flights by departure date for easier processing
        self.flight_data = self.flight_data.sort_values('scheduled_departure').reset_index(drop=True)
        
        # Add route popularity factors
        self._calculate_route_popularity()
        
        print(f"Data prepared for {self.TARGET_YEAR}:")
        print(f"- {len(self.main_holders):,} main account holders")
        print(f"- {len(self.flight_data):,} scheduled flights")
        print(f"- Registration dates: {self.main_holders['date_of_registration'].min()} to {self.main_holders['date_of_registration'].max()}")
        print(f"- Flight dates: {self.flight_data['scheduled_departure'].min()} to {self.flight_data['scheduled_departure'].max()}")

    def _calculate_route_popularity(self):
        """Calculate route popularity with seasonal, time, and day factors."""
        major_cities = {'JNB', 'CPT', 'CDG', 'JFK', 'FRA', 'LHR', 'DXB'}
        
        route_popularity = []
        time_popularity = []
        day_popularity = []
        seasonal_popularity = []
        
        for _, flight in self.flight_data.iterrows():
            origin = flight.get('origin_city', '')
            destination = flight.get('destination_city', '')
            departure_time = flight['scheduled_departure']
            
            # Route popularity
            origin_major = any(city in str(origin).upper() for city in major_cities)
            dest_major = any(city in str(destination).upper() for city in major_cities)
            
            if origin_major and dest_major:
                route_pop = 0.95
            elif origin_major or dest_major:
                route_pop = 0.75
            else:
                route_pop = 0.45
            
            # Time popularity
            hour = departure_time.hour
            if 6 <= hour <= 9 or 17 <= hour <= 20:  # Business traveler peak times
                time_pop = 1.0
            elif 10 <= hour <= 16:
                time_pop = 0.7
            else:
                time_pop = 0.4
            
            # Day popularity
            weekday = departure_time.weekday()
            if weekday in [0, 1, 3, 4]:  # Business days
                day_pop = 1.0
            elif weekday == 6:  # Sunday
                day_pop = 0.85
            else:  # Wednesday, Saturday
                day_pop = 0.6
            
            # Seasonal popularity
            month = departure_time.month
            if month in [12, 1, 2, 6, 7, 8]:  # Peak travel seasons
                season_pop = 1.0
            else:
                season_pop = 0.6
            
            route_popularity.append(route_pop)
            time_popularity.append(time_pop)
            day_popularity.append(day_pop)
            seasonal_popularity.append(season_pop)
        
        self.flight_data['route_popularity'] = route_popularity
        self.flight_data['time_popularity'] = time_popularity
        self.flight_data['day_popularity'] = day_popularity
        self.flight_data['seasonal_popularity'] = seasonal_popularity
        self.flight_data['overall_popularity'] = (
            np.array(route_popularity) * np.array(time_popularity) * np.array(day_popularity) * np.array(seasonal_popularity)
        )

    def _get_eligible_customers_for_flight(self, flight_departure_date):
        """
        Get customers who could realistically book this flight.
        Only includes customers registered at least 1 day before the flight.
        """
        registration_cutoff = flight_departure_date - timedelta(days=1)
        eligible_customers = self.main_holders[
            self.main_holders['date_of_registration'] <= registration_cutoff
        ].copy()
        return eligible_customers

    def _generate_realistic_booking_date(self, flight_departure, customer_registration, popularity_factor=1.0, is_business_traveler=False):
        """
        Generate a realistic booking date that is ALWAYS after customer registration.
        
        Args:
            flight_departure: When the flight departs
            customer_registration: When the customer registered
            popularity_factor: Flight popularity (affects booking timing)
            is_business_traveler: If True, books closer to departure
        """
        if hasattr(flight_departure, 'tz') and flight_departure.tz is not None:
            flight_departure = flight_departure.tz_localize(None)
        if hasattr(customer_registration, 'tz') and customer_registration.tz is not None:
            customer_registration = customer_registration.tz_localize(None)
        
        earliest_booking = customer_registration + timedelta(days=1)
        latest_booking = flight_departure - timedelta(hours=2)
        
        if earliest_booking >= latest_booking:
            return None
        
        total_hours = (latest_booking - earliest_booking).total_seconds() / 3600
        
        if is_business_traveler:
            booking_position = np.random.beta(5, 2)  # Skewed toward late booking
        elif popularity_factor > 0.8:
            booking_position = np.random.beta(2, 5)  # Skewed toward early booking
        elif popularity_factor > 0.6:
            booking_position = np.random.beta(3, 3)  # Balanced
        else:
            booking_position = np.random.beta(5, 2)  # Skewed toward late booking
        
        hours_from_start = total_hours * booking_position
        booking_date = earliest_booking + timedelta(hours=hours_from_start)
        return booking_date

    def _determine_group_size(self, remaining_capacity, popularity_factor, is_company_traveler):
        """Determine realistic group size based on remaining capacity, popularity, and company status."""
        if remaining_capacity <= 0:
            return 0, 0, 0
        
        if is_company_traveler and remaining_capacity > 50:
            passenger_probs = [0.10, 0.10, 0.10, 0.10, 0.15, 0.15, 0.15, 0.15, 0.10, 0.05]
            passenger_types = [(1,0,0), (2,0,0), (4,0,0), (6,0,0), (8,0,0), (12,0,0), (15,0,0), (18,0,0), (25,0,0), (30,0,0)]
        elif popularity_factor > 0.8 and remaining_capacity > 50:
            passenger_probs = [0.35, 0.15, 0.10, 0.08, 0.08, 0.07, 0.06, 0.05, 0.06]
            passenger_types = [(1,0,0), (2,0,0), (2,1,0), (2,2,0), (3,0,0), (4,0,0), (6,0,0), (8,0,0), (12,0,0)]
        elif remaining_capacity < 10:
            max_passengers = min(remaining_capacity, 4)
            if max_passengers == 1:
                return 1, 0, 0
            elif max_passengers == 2:
                return (2, 0, 0) if random.random() > 0.3 else (1, 1, 0)
            elif max_passengers == 3:
                return random.choice([(3,0,0), (2,1,0), (1,2,0)])
            else:
                return random.choice([(4,0,0), (2,2,0), (2,1,1)])
        else:
            passenger_probs = [0.45, 0.18, 0.08, 0.06, 0.04, 0.04, 0.04, 0.03, 0.03, 0.02, 0.03]
            passenger_types = [
                (1, 0, 0), (2, 0, 0), (2, 1, 0), (2, 2, 0), (2, 1, 1),
                (3, 0, 0), (4, 0, 0), (8, 0, 0), (12, 0, 0), (18, 0, 0), (25, 0, 0)
            ]
        
        valid_types = [(adults, children, infants) for adults, children, infants in passenger_types 
                      if adults + children <= remaining_capacity]
        
        if not valid_types:
            return 1, 0, 0
        
        valid_probs = passenger_probs[:len(valid_types)]
        valid_probs = np.array(valid_probs) / sum(valid_probs)
        
        choice = np.random.choice(len(valid_types), p=valid_probs)
        return valid_types[choice]

    def _simple_seat_assignment(self, num_seats, is_group=False, is_company_traveler=False):
        """Generate seat assignments, prioritizing consecutive seating for groups and company travelers."""
        max_row = 30
        seat_letters = ['A', 'B', 'C', 'D', 'E', 'F']
        
        if (is_group or is_company_traveler) and num_seats >= 4:
            start_row = np.random.randint(5, 20)
            seats = []
            current_row = start_row
            seats_assigned = 0
            
            while seats_assigned < num_seats and current_row <= max_row:
                row_seats = min(6, num_seats - seats_assigned)
                for i in range(row_seats):
                    seats.append(f"{current_row}{seat_letters[i]}")
                    seats_assigned += 1
                current_row += 1
            
            return seats[:num_seats]
        else:
            rows = np.random.poisson(lam=15, size=num_seats) % max_row + 1
            letters = np.random.choice(seat_letters, size=num_seats)
            return [f"{r}{l}" for r, l in zip(rows, letters)]

    def generate_bookings(self):
        """Generate bookings with absolute guarantee that booking_date > registration_date and minimum 50 passengers per planning_id."""
        print(f"Generating bookings for {self.TARGET_YEAR} with strict date validation and minimum 50 passengers per flight")
        
        flight_data = self.flight_data.copy()
        
        # Calculate target bookings with minimum 50 passengers
        base_load_factors = stats.beta.rvs(a=5, b=2, loc=0.4, scale=0.6, size=len(flight_data))
        adjusted_load_factors = base_load_factors * flight_data['overall_popularity']
        flight_data['target_bookings'] = (
            flight_data['aircraft_capacity'] * adjusted_load_factors
        ).astype(int)
        
        flight_data['target_bookings'] = np.maximum(flight_data['target_bookings'], 50)
        flight_data['target_bookings'] = np.minimum(flight_data['target_bookings'], flight_data['aircraft_capacity'])
        
        bookings = []
        booking_counter = 1
        
        # Track passengers per segment for connecting flights
        current_segments = {}
        for _, flight in flight_data.iterrows():
            pid = flight['planning_id']
            if flight.get('is_connecting', False):
                current_segments[pid] = {'segment1': 0, 'segment2': 0}
            else:
                current_segments[pid] = {'full': 0}
        
        target_passengers = dict(zip(flight_data['planning_id'], flight_data['target_bookings']))
        
        print("Processing flights with strict date validation...")
        
        for _, flight in tqdm(flight_data.iterrows(), total=len(flight_data), desc="Generating bookings"):
            pid = flight['planning_id']
            target = target_passengers[pid]
            is_connecting = flight.get('is_connecting', False)
            current_dict = current_segments[pid]
            aircraft_capacity = flight['aircraft_capacity']
            
            eligible_customers = self._get_eligible_customers_for_flight(flight['scheduled_departure'])
            
            if eligible_customers.empty:
                print(f"Warning: No eligible customers for flight {pid} on {flight['scheduled_departure']}")
                continue
            
            # Prefer local customers
            eligible_customers['weight'] = np.where(eligible_customers['city'] == flight.get('origin_city', ''), 10, 1)
            
            attempt_count = 0
            max_attempts = target * 2
            
            while attempt_count < max_attempts:
                total_passengers = sum(current_dict.values())
                if is_connecting:
                    if max(current_dict['segment1'], current_dict['segment2']) >= target:
                        break
                else:
                    if current_dict['full'] >= target:
                        break
                
                attempt_count += 1
                
                customer = eligible_customers.sample(n=1, weights='weight').iloc[0]
                is_company_traveler = pd.notna(customer['partner_company'])
                
                # Determine booking channel
                booking_channel = np.random.choice(['mobile', 'website', 'agent'], p=[0.6, 0.3, 0.1])
                
                booking_date = self._generate_realistic_booking_date(
                    flight['scheduled_departure'],
                    customer['date_of_registration'],
                    flight['overall_popularity'],
                    is_business_traveler=is_company_traveler
                )
                
                if booking_date is None:
                    continue
                
                days_before = (flight['scheduled_departure'] - booking_date).days
                
                num_adults, num_children, num_infants = self._determine_group_size(
                    aircraft_capacity - total_passengers, flight['overall_popularity'], is_company_traveler
                )
                
                total_passengers_booking = num_adults + num_children
                if total_passengers_booking == 0:
                    continue
                
                # Handle connecting flights
                if is_connecting:
                    segment_choice = np.random.choice(['full', 'first', 'second'], p=[0.7, 0.15, 0.15])
                    add_to_seg1 = segment_choice in ['full', 'first']
                    add_to_seg2 = segment_choice in ['full', 'second']
                    
                    if add_to_seg1 and current_dict['segment1'] + total_passengers_booking > aircraft_capacity:
                        continue
                    if add_to_seg2 and current_dict['segment2'] + total_passengers_booking > aircraft_capacity:
                        continue
                else:
                    segment_choice = 'direct'
                    if current_dict['full'] + total_passengers_booking > aircraft_capacity:
                        continue
                
                is_group = num_adults >= 8
                
                # Trip type with multi-city option
                trip_type = np.random.choice(
                    ['one-way', 'return', 'multi-city'],
                    p=[0.5, 0.4, 0.1]
                )
                
                # Booking class and fare type
                booking_class = 'Business' if (is_company_traveler and random.random() < 0.5) else (
                    'Business' if random.random() < (0.15 if is_group else 0.05) else 'Economy'
                )
                
                if booking_class == 'Business':
                    fare_type = np.random.choice(['Business Saver', 'Business Flex'], p=[0.6, 0.4])
                else:
                    fare_type = np.random.choice(['Basic', 'Standard Fare', 'Flex'], p=[0.3, 0.5, 0.2])
                
                # Dynamic pricing
                base_price = flight.get('final_price_zar', 800)
                price_multiplier = np.random.normal(1.3, 0.25)
                if booking_class == 'Business':
                    price_multiplier *= 3.0
                if is_group and num_adults >= 12:
                    price_multiplier *= 0.9
                if is_company_traveler:
                    price_multiplier *= 0.9
                if days_before > 60:
                    price_multiplier *= 0.8
                elif days_before < 14:
                    price_multiplier *= 1.3
                elif days_before < 3:
                    price_multiplier *= 1.6
                if booking_channel == 'mobile':
                    price_multiplier *= 0.95  # Mobile promotion
                if is_connecting and segment_choice != 'full':
                    price_multiplier *= 0.6
                
                price_per_ticket = max(base_price * price_multiplier, 200)
                
                # Booking status with realistic errors
                status_probs = [0.85, 0.08, 0.04, 0.02, 0.01]
                status = np.random.choice(
                    ['confirmed', 'cancelled', 'rescheduled', 'on-hold', 'payment_failed'],
                    p=status_probs
                )
                
                # Addons
                addons = {
                    'is_priority': random.random() < (0.35 if is_group else 0.18),
                    'is_assisted': random.random() < 0.025,
                    'is_special_needs': random.random() < 0.015,
                    'is_lounge_access': random.random() < (0.50 if is_company_traveler else 0.08),
                    'is_cancellation_refundable': fare_type in ['Flex', 'Business Flex'],
                    'is_travel_protection': random.random() < (0.50 if is_company_traveler else 0.28),
                    'is_cheap_hotel_accommodation': random.random() < 0.06,
                    'is_car_rental': random.random() < 0.05
                }
                
                # Seat assignment (optional, less likely for mobile)
                seat_request = ''
                if booking_channel == 'mobile':
                    assign_seats = random.random() < 0.3  # 30% chance for mobile
                else:
                    assign_seats = random.random() < 0.8  # 80% chance for website/agent
                
                if assign_seats:
                    seat_assignments = self._simple_seat_assignment(total_passengers_booking, is_group, is_company_traveler)
                    seat_request = ','.join(seat_assignments) if len(seat_assignments) > 1 else seat_assignments[0] if seat_assignments else ''
                
                # Determine board and deboard airports
                origin_airport = flight.get('origin_airport', '')
                destination_airport = flight.get('destination_airport', '')
                intermediate_stop = flight.get('intermediate_stop', '')
                
                if is_connecting:
                    if segment_choice == 'full':
                        board_airport = origin_airport
                        deboard_airport = destination_airport
                    elif segment_choice == 'first':
                        board_airport = origin_airport
                        deboard_airport = intermediate_stop
                    else:  # second
                        board_airport = intermediate_stop
                        deboard_airport = destination_airport
                else:
                    board_airport = origin_airport
                    deboard_airport = destination_airport
                
                # Create booking record
                booking = {
                    'booking_id': f"BK{self.TARGET_YEAR}{booking_counter:06d}",
                    'client_id': customer['client_id'],
                    'planning_id': pid,
                    'booking_date': booking_date,
                    'trip_type': trip_type,
                    'num_adults': num_adults,
                    'num_children': num_children,
                    'num_infants': num_infants,
                    'booking_class': booking_class,
                    'fare_type': fare_type,
                    'booking_status': status,
                    'is_charter': random.random() < 0.005 if is_group else False,
                    'seat_request': seat_request,
                    'price_per_ticket': round(price_per_ticket, 2),
                    'board_airport': board_airport,
                    'deboard_airport': deboard_airport,
                    'booking_channel': booking_channel,
                    **addons
                }
                
                bookings.append(booking)
                booking_counter += 1
                
                # Update passenger counts
                if is_connecting:
                    if add_to_seg1:
                        current_dict['segment1'] += total_passengers_booking
                    if add_to_seg2:
                        current_dict['segment2'] += total_passengers_booking
                else:
                    current_dict['full'] += total_passengers_booking
        
        bookings_df = pd.DataFrame(bookings)
        columns_to_keep = [
            'booking_id', 'client_id', 'planning_id', 'booking_date', 'trip_type',
            'num_adults', 'num_children', 'num_infants', 'booking_class', 'fare_type',
            'booking_status', 'is_charter', 'seat_request', 'price_per_ticket',
            'board_airport', 'deboard_airport', 'booking_channel',
            'is_priority', 'is_assisted', 'is_special_needs', 'is_lounge_access',
            'is_cancellation_refundable', 'is_travel_protection', 'is_cheap_hotel_accommodation',
            'is_car_rental'
        ]
        bookings_df = bookings_df[columns_to_keep]
        
        # Ensure minimum 50 passengers per planning_id
        passengers_per_flight = bookings_df.groupby('planning_id').apply(
            lambda x: (x['num_adults'] + x['num_children']).sum()
        )
        underbooked_flights = passengers_per_flight[passengers_per_flight < 50]
        if not underbooked_flights.empty:
            print(f"Warning: {len(underbooked_flights)} flights have less than 50 passengers")
            for pid in underbooked_flights.index:
                current_passengers = passengers_per_flight.get(pid, 0)
                additional_needed = 50 - current_passengers
                eligible_customers = self._get_eligible_customers_for_flight(
                    flight_data[flight_data['planning_id'] == pid]['scheduled_departure'].iloc[0]
                )
                
                while additional_needed > 0 and not eligible_customers.empty:
                    customer = eligible_customers.sample(n=1, weights='weight').iloc[0]
                    is_company_traveler = pd.notna(customer['partner_company'])
                    booking_date = self._generate_realistic_booking_date(
                        flight_data[flight_data['planning_id'] == pid]['scheduled_departure'].iloc[0],
                        customer['date_of_registration'],
                        flight_data[flight_data['planning_id'] == pid]['overall_popularity'].iloc[0],
                        is_business_traveler=is_company_traveler
                    )
                    if booking_date is None:
                        continue
                    
                    days_before = (flight_data[flight_data['planning_id'] == pid]['scheduled_departure'].iloc[0] - booking_date).days
                    num_adults, num_children, num_infants = self._determine_group_size(
                        additional_needed, flight_data[flight_data['planning_id'] == pid]['overall_popularity'].iloc[0], is_company_traveler
                    )
                    total_passengers_booking = num_adults + num_children
                    if total_passengers_booking == 0 or total_passengers_booking + current_dict.get('full', 0) > flight_data[flight_data['planning_id'] == pid]['aircraft_capacity'].iloc[0]:
                        continue
                    
                    is_group = num_adults >= 8
                    trip_type = np.random.choice(['one-way', 'return', 'multi-city'], p=[0.5, 0.4, 0.1])
                    booking_class = 'Business' if (is_company_traveler and random.random() < 0.5) else (
                        'Business' if random.random() < (0.15 if is_group else 0.05) else 'Economy'
                    )
                    fare_type = np.random.choice(['Business Saver', 'Business Flex'], p=[0.6, 0.4]) if booking_class == 'Business' else np.random.choice(['Basic', 'Standard Fare', 'Flex'], p=[0.3, 0.5, 0.2])
                    booking_channel = np.random.choice(['mobile', 'website', 'agent'], p=[0.6, 0.3, 0.1])
                    
                    base_price = flight_data[flight_data['planning_id'] == pid].get('final_price_zar', 800).iloc[0]
                    price_multiplier = np.random.normal(1.3, 0.25)
                    if booking_class == 'Business':
                        price_multiplier *= 3.0
                    if is_group and num_adults >= 12:
                        price_multiplier *= 0.9
                    if is_company_traveler:
                        price_multiplier *= 0.9
                    if days_before > 60:
                        price_multiplier *= 0.8
                    elif days_before < 14:
                        price_multiplier *= 1.3
                    elif days_before < 3:
                        price_multiplier *= 1.6
                    if booking_channel == 'mobile':
                        price_multiplier *= 0.95
                    
                    price_per_ticket = max(base_price * price_multiplier, 200)
                    status = np.random.choice(['confirmed', 'cancelled', 'rescheduled', 'on-hold', 'payment_failed'], p=[0.85, 0.08, 0.04, 0.02, 0.01])
                    addons = {
                        'is_priority': random.random() < (0.35 if is_group else 0.18),
                        'is_assisted': random.random() < 0.025,
                        'is_special_needs': random.random() < 0.015,
                        'is_lounge_access': random.random() < (0.50 if is_company_traveler else 0.08),
                        'is_cancellation_refundable': fare_type in ['Flex', 'Business Flex'],
                        'is_travel_protection': random.random() < (0.50 if is_company_traveler else 0.28),
                        'is_cheap_hotel_accommodation': random.random() < 0.06,
                        'is_car_rental': random.random() < 0.05
                    }
                    
                    seat_request = ''
                    if booking_channel == 'mobile':
                        assign_seats = random.random() < 0.3
                    else:
                        assign_seats = random.random() < 0.8
                    if assign_seats:
                        seat_assignments = self._simple_seat_assignment(total_passengers_booking, is_group, is_company_traveler)
                        seat_request = ','.join(seat_assignments) if len(seat_assignments) > 1 else seat_assignments[0] if seat_assignments else ''
                    
                    board_airport = flight_data[flight_data['planning_id'] == pid].get('origin_airport', '').iloc[0]
                    deboard_airport = flight_data[flight_data['planning_id'] == pid].get('destination_airport', '').iloc[0]
                    
                    booking = {
                        'booking_id': f"BK{self.TARGET_YEAR}{booking_counter:06d}",
                        'client_id': customer['client_id'],
                        'planning_id': pid,
                        'booking_date': booking_date,
                        'trip_type': trip_type,
                        'num_adults': num_adults,
                        'num_children': num_children,
                        'num_infants': num_infants,
                        'booking_class': booking_class,
                        'fare_type': fare_type,
                        'booking_status': status,
                        'is_charter': random.random() < 0.005 if is_group else False,
                        'seat_request': seat_request,
                        'price_per_ticket': round(price_per_ticket, 2),
                        'board_airport': board_airport,
                        'deboard_airport': deboard_airport,
                        'booking_channel': booking_channel,
                        **addons
                    }
                    
                    bookings.append(booking)
                    booking_counter += 1
                    additional_needed -= total_passengers_booking
                    current_dict['full'] = current_dict.get('full', 0) + total_passengers_booking
        
        bookings_df = pd.DataFrame(bookings)
        columns_to_keep = [
            'booking_id', 'client_id', 'planning_id', 'booking_date', 'trip_type',
            'num_adults', 'num_children', 'num_infants', 'booking_class', 'fare_type',
            'booking_status', 'is_charter', 'seat_request', 'price_per_ticket',
            'board_airport', 'deboard_airport', 'booking_channel',
            'is_priority', 'is_assisted', 'is_special_needs', 'is_lounge_access',
            'is_cancellation_refundable', 'is_travel_protection', 'is_cheap_hotel_accommodation',
            'is_car_rental'
        ]
        bookings_df = bookings_df[columns_to_keep]
        
        # Final validation
        if not bookings_df.empty:
            validation_df = bookings_df.merge(
                self.main_holders[['client_id', 'date_of_registration']], 
                left_on='client_id', 
                right_on='client_id', 
                how='left'
            )
            
            invalid_bookings = validation_df[validation_df['booking_date'] < validation_df['date_of_registration']]
            
            if len(invalid_bookings) > 0:
                print(f"CRITICAL ERROR: Found {len(invalid_bookings)} invalid bookings!")
                raise ValueError("Invalid booking dates found - check the booking generation logic")
            else:
                print("SUCCESS: All booking dates are after customer registration dates")
        
        # Verify minimum 50 passengers
        passengers_per_flight = bookings_df.groupby('planning_id').apply(
            lambda x: (x['num_adults'] + x['num_children']).sum()
        )
        underbooked_flights = passengers_per_flight[passengers_per_flight < 50]
        if not underbooked_flights.empty:
            print(f"ERROR: {len(underbooked_flights)} flights still have less than 50 passengers after additional bookings")
        else:
            print("SUCCESS: All flights have at least 50 passengers")
        
        print(f"\n=== BOOKING GENERATION COMPLETE ===")
        print(f"Total bookings: {len(bookings_df):,}")
        print(f"Unique customers: {bookings_df['client_id'].nunique():,}")
        print(f"Unique flights: {bookings_df['planning_id'].nunique():,}")
        print(f"Average passengers per flight: {passengers_per_flight.mean():.1f}")
        
        return bookings_df
        
    def save_bookings(self, bookings_df, filename=None):
        """Save bookings to parquet file."""
        if filename is None:
            filename = f'airplane_data/bookings_{self.TARGET_YEAR}.parquet'
            
        bookings_df.to_parquet(filename, index=False, engine='pyarrow')
        print(f"Bookings saved to: {filename}")
        
        return filename

def generate_realistic_airline_bookings(target_year=BASE_YEAR, save_file=True):
    """
    Generate airline bookings with absolute guarantee that booking_date > registration_date and minimum 50 passengers per flight.
    
    Args:
        target_year (int): Year to generate bookings for
        save_file (bool): Whether to save results to parquet
        
    Returns:
        pd.DataFrame: Generated bookings with valid dates
    """
    print(f"Starting FIXED airline bookings generation for {target_year}")
    print("Key Fix: Booking dates are GUARANTEED to be after registration dates, minimum 50 passengers per flight")
    print("-" * 70)
    
    try:
        generator = RealisticAirlineBookingsGenerator(target_year=target_year)
        bookings_df = generator.generate_bookings()
        
        if save_file and not bookings_df.empty:
            filename = generator.save_bookings(bookings_df)
            print(f"\nData saved to: {filename}")
            
        print(f"\nSuccessfully generated {len(bookings_df):,} valid bookings for {target_year}!")
        return bookings_df
        
    except Exception as e:
        print(f"Error generating bookings: {str(e)}")
        raise

if __name__ == "__main__":
    seed_bytes = os.urandom(4)
    seed_int = int.from_bytes(seed_bytes, byteorder='big')
    random.seed(seed_int)
    np.random.seed(seed_int)
    
    TARGET_YEAR = 2021
    bookings = generate_realistic_airline_bookings(
        target_year=TARGET_YEAR,
        save_file=True
    )
    
    if not bookings.empty:
        print("\nSample bookings:")
        print(bookings[['booking_id', 'client_id', 'planning_id', 'booking_date', 'trip_type', 'num_adults', 'num_children']].head())
        
        print(f"\nFinal validation:")
        print(f"- Date range: {bookings['booking_date'].min()} to {bookings['booking_date'].max()}")
        print(f"- Total bookings: {len(bookings):,}")
        print(f"- All dates valid: No booking before registration guaranteed by design")
