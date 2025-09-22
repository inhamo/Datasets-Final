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
        """Prepare and merge all datasets with robust datetime handling and realistic registration dates."""
        self.flight_schedule_df['scheduled_departure'] = pd.to_datetime(self.flight_schedule_df['scheduled_departure'])
        self.flight_schedule_df['scheduled_arrival'] = pd.to_datetime(self.flight_schedule_df['scheduled_arrival'])
        
        if 'actual_departure' in self.flight_schedule_df.columns:
            self.flight_schedule_df['actual_departure'] = pd.to_datetime(self.flight_schedule_df['actual_departure'])
        if 'actual_arrival' in self.flight_schedule_df.columns:
            self.flight_schedule_df['actual_arrival'] = pd.to_datetime(self.flight_schedule_df['actual_arrival'])
            
        self.clients_df['dob'] = pd.to_datetime(self.clients_df['dob'], errors='coerce')
        
        # More robust registration date handling - ensure all dates are in target year
        self.clients_df['date_of_registration'] = pd.to_datetime(
            self.clients_df['date_of_registration'], errors='coerce', format='mixed'
        )
        
        # Handle invalid registration dates more realistically
        invalid_dates_mask = self.clients_df['date_of_registration'].isna()
        
        # Get date ranges for realistic registration dates
        target_year_start = pd.to_datetime(f"{self.TARGET_YEAR}-01-01")
        target_year_end = pd.to_datetime(f"{self.TARGET_YEAR}-12-31")
        earliest_flight = self.flight_schedule_df['scheduled_departure'].min()
        
        # CRITICAL FIX: Ensure registration dates are within the target year
        if invalid_dates_mask.any():
            print(f"Warning: {invalid_dates_mask.sum()} invalid registration dates found. Generating realistic dates...")
            
            # Generate realistic registration dates within the target year
            reg_start = target_year_start
            reg_end = min(earliest_flight - timedelta(days=1), target_year_end)
            
            if reg_start >= reg_end:
                # Fallback: use first 6 months of target year
                reg_end = target_year_start + timedelta(days=180)
            
            date_range = (reg_end - reg_start).days
            if date_range > 0:
                random_days = np.random.randint(0, max(1, date_range), size=invalid_dates_mask.sum())
                random_dates = [reg_start + timedelta(days=int(days)) for days in random_days]
                self.clients_df.loc[invalid_dates_mask, 'date_of_registration'] = random_dates
        
        # CRITICAL FIX: Ensure no registration dates are after the target year
        future_registrations = self.clients_df['date_of_registration'] > target_year_end
        if future_registrations.any():
            print(f"Adjusting {future_registrations.sum()} registration dates from after {self.TARGET_YEAR}...")
            # Move these to random dates within the target year
            days_in_year = (target_year_end - target_year_start).days
            random_days = np.random.randint(0, days_in_year, size=future_registrations.sum())
            adjusted_dates = [target_year_start + timedelta(days=int(days)) for days in random_days]
            self.clients_df.loc[future_registrations, 'date_of_registration'] = adjusted_dates
        
        # Ensure all registration dates are before the target year's flights
        late_registration_mask = self.clients_df['date_of_registration'] >= earliest_flight
        if late_registration_mask.any():
            print(f"Warning: {late_registration_mask.sum()} registration dates were too late. Adjusting...")
            # Move these registrations to be 1-30 days before the earliest flight
            days_before = np.random.randint(1, 31, size=late_registration_mask.sum())
            adjusted_dates = [earliest_flight - timedelta(days=int(days)) for days in days_before]
            self.clients_df.loc[late_registration_mask, 'date_of_registration'] = adjusted_dates
        
        self.main_holders = self.clients_df[self.clients_df['is_main_holder'] == True][
            ['client_id', 'city', 'date_of_registration']
        ].copy()
        
        # Sort customers by registration date for easier filtering
        self.main_holders = self.main_holders.sort_values('date_of_registration').reset_index(drop=True)
        
        self.flight_data = self.flight_schedule_df.merge(
            self.routes_df, on='route_id', how='left'
        ).merge(
            self.planes_df[['plane_id', 'aircraft_model', 'capacity']], 
            on='plane_id', 
            how='left'
        )
        
        self.flight_data = self.flight_data[
            self.flight_data['scheduled_departure'].dt.year == self.TARGET_YEAR
        ].copy()
        
        self.flight_data['aircraft_type'] = self.flight_data['aircraft_model'].fillna('default')
        self.flight_data['aircraft_capacity'] = self.flight_data['capacity'].fillna(150).astype(int)
        self.flight_data = self.flight_data.drop(columns=['capacity', 'aircraft_model'], errors='ignore')
        
        # Add route popularity factors
        self._calculate_route_popularity()
        
        print(f"Loaded data for {self.TARGET_YEAR}:")
        print(f"- {len(self.main_holders):,} main account holders")
        print(f"- {len(self.flight_data):,} scheduled flights")
        print(f"- {self.flight_data['route_id'].nunique()} unique routes")
        print(f"- Registration dates range: {self.main_holders['date_of_registration'].min()} to {self.main_holders['date_of_registration'].max()}")
        print(f"- Flight dates range: {self.flight_data['scheduled_departure'].min()} to {self.flight_data['scheduled_departure'].max()}")

    def _calculate_route_popularity(self):
        """Calculate route popularity based on city pairs and add time-of-day factors."""
        # Major city codes (adjust based on your data)
        major_cities = {'JNB', 'CPT', 'DBN', 'PLZ', 'ELS', 'GRJ'}  # Add your major airports
        
        route_popularity = []
        time_popularity = []
        day_popularity = []
        
        for _, flight in self.flight_data.iterrows():
            origin = flight.get('origin_city', '')
            destination = flight.get('destination_city', '')
            departure_time = flight['scheduled_departure']
            
            # Route popularity based on city importance
            origin_major = any(city in str(origin).upper() for city in major_cities)
            dest_major = any(city in str(destination).upper() for city in major_cities)
            
            if origin_major and dest_major:
                route_pop = 0.95  # Major to major (highest demand)
            elif origin_major or dest_major:
                route_pop = 0.75  # One major city
            else:
                route_pop = 0.45  # Regional routes
            
            # Time of day popularity
            hour = departure_time.hour
            if 6 <= hour <= 9:  # Morning business hours
                time_pop = 1.0
            elif 17 <= hour <= 20:  # Evening return flights
                time_pop = 0.9
            elif 10 <= hour <= 16:  # Daytime
                time_pop = 0.7
            else:  # Early morning/late night
                time_pop = 0.4
            
            # Day of week popularity
            weekday = departure_time.weekday()
            if weekday in [0, 1, 3, 4]:  # Mon, Tue, Thu, Fri (business travel)
                day_pop = 1.0
            elif weekday == 6:  # Sunday (return travel)
                day_pop = 0.85
            elif weekday == 2:  # Wednesday
                day_pop = 0.75
            else:  # Saturday
                day_pop = 0.6
            
            route_popularity.append(route_pop)
            time_popularity.append(time_pop)
            day_popularity.append(day_pop)
        
        self.flight_data['route_popularity'] = route_popularity
        self.flight_data['time_popularity'] = time_popularity
        self.flight_data['day_popularity'] = day_popularity
        self.flight_data['overall_popularity'] = (
            np.array(route_popularity) * np.array(time_popularity) * np.array(day_popularity)
        )

    def _get_available_customers_at_date(self, booking_date):
        """
        Get customers who were already registered by the booking date.
        
        Args:
            booking_date: The date when the booking is made
            
        Returns:
            DataFrame of customers available for booking at that date
        """
        # Ensure booking_date is timezone-naive if registration dates are timezone-naive
        if hasattr(booking_date, 'tz') and booking_date.tz is not None:
            booking_date = booking_date.tz_localize(None)
        
        available_customers = self.main_holders[
            self.main_holders['date_of_registration'] <= booking_date
        ].copy()
        
        return available_customers

    def _generate_booking_date(self, flight_departure, popularity_factor=1.0):
        """Generate realistic booking date based on flight departure and popularity."""
        # Ensure booking dates don't go before the target year
        target_year_start = pd.to_datetime(f"{self.TARGET_YEAR}-01-01")
        
        # Popular flights get booked earlier on average
        if popularity_factor > 0.8:
            # High popularity: book 2-12 weeks in advance (peak at 4 weeks)
            scale_hours = 24 * 28  # 4 weeks in hours
            max_hours = 24 * 84   # 12 weeks
        elif popularity_factor > 0.6:
            # Medium popularity: book 1-8 weeks in advance (peak at 2 weeks)
            scale_hours = 24 * 14  # 2 weeks in hours
            max_hours = 24 * 56   # 8 weeks
        else:
            # Low popularity: book 3 days to 4 weeks in advance
            scale_hours = 24 * 7   # 1 week in hours
            max_hours = 24 * 28   # 4 weeks
        
        # Use exponential distribution for realistic booking patterns
        hours_before = min(stats.expon.rvs(scale=scale_hours), max_hours)
        
        # Ensure minimum 2 hours before departure
        hours_before = max(hours_before, 2)
        
        booking_date = flight_departure - timedelta(hours=hours_before)
        
        # CRITICAL FIX: Don't allow booking dates before the target year
        if booking_date < target_year_start:
            # If booking would be before target year, adjust to be within target year
            days_into_year = min((flight_departure - target_year_start).days, 30)
            if days_into_year > 0:
                booking_date = target_year_start + timedelta(days=np.random.randint(0, days_into_year))
            else:
                # If flight is very early in the year, book just 1-7 days before
                booking_date = flight_departure - timedelta(days=np.random.randint(1, 8))
        
        return booking_date

    def _determine_group_size(self, available_customers, remaining_capacity, popularity_factor):
        """Determine realistic group size based on flight capacity and popularity."""
        if remaining_capacity <= 0:
            return 0, 0, 0
        
        # Adjust probabilities based on popularity and remaining capacity
        if popularity_factor > 0.8 and remaining_capacity > 50:
            # Popular flights more likely to have business groups
            passenger_probs = [0.35, 0.15, 0.10, 0.08, 0.08, 0.07, 0.06, 0.05, 0.06]
            passenger_types = [(1,0,0), (2,0,0), (2,1,0), (2,2,0), (3,0,0), (4,0,0), (6,0,0), (8,0,0), (12,0,0)]
        elif remaining_capacity < 10:
            # Fill remaining seats with small groups/individuals
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
            # Standard distribution
            passenger_probs = [0.45, 0.18, 0.08, 0.06, 0.04, 0.04, 0.04, 0.03, 0.03, 0.02, 0.03]
            passenger_types = [
                (1, 0, 0),    # Solo traveler
                (2, 0, 0),    # Couple
                (2, 1, 0),    # Small family
                (2, 2, 0),    # Family with 2 kids
                (2, 1, 1),    # Family with baby
                (3, 0, 0),    # Friends/colleagues
                (4, 0, 0),    # Small group
                (8, 0, 0),    # Small sports team/corporate
                (12, 0, 0),   # Medium sports team
                (18, 0, 0),   # Large sports team/tour group
                (25, 0, 0)    # Large corporate/conference group
            ]
        
        # Filter passenger types that fit in remaining capacity
        valid_types = [(adults, children, infants) for adults, children, infants in passenger_types 
                      if adults + children <= remaining_capacity]
        
        if not valid_types:
            return 1, 0, 0  # Fallback to single passenger
        
        # Adjust probabilities for valid types
        valid_probs = passenger_probs[:len(valid_types)]
        valid_probs = np.array(valid_probs) / sum(valid_probs)  # Normalize
        
        choice = np.random.choice(len(valid_types), p=valid_probs)
        return valid_types[choice]

    def _simple_seat_assignment(self, num_seats, is_group=False, group_type='individual'):
        """Vectorized seat assignment with group seating logic."""
        max_row = 30
        seat_letters = ['A', 'B', 'C', 'D', 'E', 'F']
        
        if is_group and num_seats > 6:
            # For large groups, try to assign consecutive seats/rows
            if group_type in ['sports_team', 'corporate_event']:
                # Sports teams and corporate groups often sit together
                start_row = np.random.randint(5, 20)  # Avoid very front/back
                seats = []
                current_row = start_row
                seats_assigned = 0
                
                while seats_assigned < num_seats and current_row <= max_row:
                    row_seats = min(6, num_seats - seats_assigned)  # Max 6 per row
                    for i in range(row_seats):
                        seats.append(f"{current_row}{seat_letters[i]}")
                        seats_assigned += 1
                    current_row += 1
                
                return seats[:num_seats]
            else:
                # Other groups more scattered
                rows = np.random.poisson(lam=15, size=num_seats) % max_row + 1
                letters = np.random.choice(seat_letters, size=num_seats)
                return [f"{r}{l}" for r, l in zip(rows, letters)]
        else:
            # Individual/small group assignment
            rows = np.random.poisson(lam=15, size=num_seats) % max_row + 1
            letters = np.random.choice(seat_letters, size=num_seats)
            return [f"{r}{l}" for r, l in zip(rows, letters)]

    def _find_return_flights(self, flight_data, bookings_df):
        """Optimized return flight assignment."""
        return_bookings = bookings_df[bookings_df['trip_type'] == 'return'].copy()
        if return_bookings.empty:
            return bookings_df
        
        return_flights = []
        max_booking_id = int(bookings_df['booking_id'].str[6:].max()) if not bookings_df.empty else 0
        
        for idx, booking in return_bookings.iterrows():
            return_start = booking['scheduled_departure'] + timedelta(days=1)
            return_end = booking['scheduled_departure'] + timedelta(days=7)
            
            candidates = flight_data[
                (flight_data['origin_city'] == booking['destination_city']) &
                (flight_data['destination_city'] == booking['origin_city']) &
                (flight_data['scheduled_departure'] >= return_start) &
                (flight_data['scheduled_departure'] <= return_end)
            ]
            
            if not candidates.empty:
                geom_idx = min(stats.geom.rvs(p=0.3, size=1)[0] - 1, len(candidates) - 1)
                return_flight = candidates.iloc[geom_idx]
                
                return_booking = booking.copy()
                max_booking_id += 1
                return_booking['booking_id'] = f"BK{self.TARGET_YEAR}{max_booking_id:06d}"
                return_booking['planning_id'] = return_flight['planning_id']
                return_booking['outbound_id'] = booking['booking_id']
                return_booking['price_per_ticket'] = round(booking['price_per_ticket'] * np.random.uniform(0.9, 1.1), 2)
                
                # Update seat assignments for return flight
                num_passengers = booking['num_adults'] + booking['num_children']
                is_group = booking.get('group_booking_type', 'individual') != 'individual'
                return_booking['seat_request'] = ','.join(self._simple_seat_assignment(
                    num_passengers, is_group, booking.get('group_booking_type', 'individual')
                ))
                
                # Update scheduled departure for return flight
                return_booking['scheduled_departure'] = return_flight['scheduled_departure']
                return_booking['origin_city'] = return_flight['origin_city']
                return_booking['destination_city'] = return_flight['destination_city']
                
                return_flights.append(return_booking)
        
        if return_flights:
            return_df = pd.DataFrame(return_flights)
            bookings_df = pd.concat([bookings_df, return_df], ignore_index=True)
        
        return bookings_df

    def generate_bookings(self):
        """Generate bookings with proper customer registration filtering and minimum 75 passengers per flight."""
        print(f"Generating bookings for {self.TARGET_YEAR} with proper customer registration filtering")
        
        flight_data = self.flight_data.copy()
        
        # Calculate target bookings based on capacity and popularity, ensuring minimum 75 passengers
        base_load_factors = stats.beta.rvs(a=5, b=2, loc=0.6, scale=0.4, size=len(flight_data))  # Shifted to higher load factors
        adjusted_load_factors = base_load_factors * flight_data['overall_popularity']
        flight_data['target_bookings'] = (
            flight_data['aircraft_capacity'] * adjusted_load_factors
        ).astype(int)
        
        # Enforce minimum of 75 passengers per flight, capped at aircraft capacity
        min_passengers = 75
        flight_data['target_bookings'] = np.maximum(flight_data['target_bookings'], min_passengers)
        flight_data['target_bookings'] = np.minimum(flight_data['target_bookings'], flight_data['aircraft_capacity'])
        
        bookings = []
        booking_counter = 1
        
        print("Processing flights with proper customer filtering...")
        
        for _, flight in tqdm(flight_data.iterrows(), total=len(flight_data), desc="Generating bookings"):
            target_bookings = flight['target_bookings']
            current_capacity = 0
            attempt_count = 0
            max_attempts = target_bookings * 3  # Prevent infinite loops
            
            while current_capacity < target_bookings and attempt_count < max_attempts:
                attempt_count += 1
                
                # Generate a realistic booking date for this flight
                booking_date = self._generate_booking_date(
                    flight['scheduled_departure'], 
                    flight['overall_popularity']
                )
                
                # Get customers who were registered by this booking date
                available_customers = self._get_available_customers_at_date(booking_date)
                
                if available_customers.empty:
                    # If no customers available, try a booking date closer to flight departure
                    # This ensures we use customers who registered later
                    hours_before = np.random.randint(24, 168)  # 1-7 days before
                    booking_date = flight['scheduled_departure'] - timedelta(hours=hours_before)
                    available_customers = self._get_available_customers_at_date(booking_date)
                    
                    if available_customers.empty:
                        continue  # Skip this attempt if still no customers
                
                # Select a random customer from available customers
                customer = available_customers.sample(n=1).iloc[0]
                
                # Determine group size based on remaining capacity and popularity
                remaining_capacity = target_bookings - current_capacity
                num_adults, num_children, num_infants = self._determine_group_size(
                    available_customers, remaining_capacity, flight['overall_popularity']
                )
                
                total_passengers = num_adults + num_children
                if total_passengers == 0 or current_capacity + total_passengers > flight['aircraft_capacity']:
                    continue
                
                # Determine booking characteristics
                is_group = num_adults >= 8
                group_types = ['individual', 'family', 'friends', 'corporate_event', 'sports_team', 'tour_group']
                group_type = np.random.choice(group_types, p=[0.6, 0.2, 0.1, 0.05, 0.03, 0.02]) if is_group else 'individual'
                
                # Trip type (return vs one-way)
                trip_type = 'return' if random.random() < 0.75 else 'one-way'
                
                # Booking class
                business_prob = 0.15 if is_group else 0.05
                booking_class = 'business' if random.random() < business_prob else 'economy'
                
                # Calculate price
                base_price = flight.get('final_price_zar', 800)
                price_multiplier = np.random.normal(1.3, 0.25)
                if booking_class == 'business':
                    price_multiplier *= 3.0
                if is_group and num_adults >= 12:
                    price_multiplier *= 0.9  # Group discount
                
                price_per_ticket = max(base_price * price_multiplier, 200)  # Minimum price
                
                # Booking status
                status_probs = [0.87, 0.08, 0.04, 0.01]
                status = np.random.choice(['confirmed', 'cancelled', 'rescheduled', 'on-hold'], p=status_probs)
                
                # Generate addon services
                addons = {
                    'is_priority': random.random() < (0.35 if is_group else 0.18),
                    'is_assisted': random.random() < (0.08 if group_type == 'tour_group' else 0.025),
                    'is_special_needs': random.random() < 0.015,
                    'is_lounge_access': random.random() < (0.25 if group_type == 'corporate_event' else 0.08),
                    'is_cancellation_refundable': random.random() < 0.45,
                    'is_travel_protection': random.random() < 0.28,
                    'is_cheap_hotel_accommodation': random.random() < 0.06,
                    'is_car_rental': random.random() < 0.05
                }
                
                # Generate seat assignments
                seat_assignments = self._simple_seat_assignment(total_passengers, is_group, group_type)
                seat_request = ','.join(seat_assignments) if len(seat_assignments) > 1 else seat_assignments[0]
                
                # Create booking record
                booking = {
                    'booking_id': f"BK{self.TARGET_YEAR}{booking_counter:06d}",
                    'customer_id': customer['client_id'],
                    'planning_id': flight['planning_id'],
                    'booking_date': booking_date,
                    'trip_type': trip_type,
                    'num_adults': num_adults,
                    'num_children': num_children,
                    'num_infants': num_infants,
                    'booking_class': booking_class,
                    'booking_status': status,
                    'group_booking_type': group_type,
                    'is_charter': random.random() < 0.005 if is_group else False,
                    'cancelled_date': booking_date + timedelta(hours=random.randint(1, 240)) if status == 'cancelled' else None,
                    'rescheduled_date': None,
                    'on_hold_date': booking_date + timedelta(hours=random.randint(1, 48)) if status == 'on-hold' else None,
                    'on_hold_end_date': None,
                    'outbound_id': None,
                    'rescheduled_id': None,
                    'seat_request': seat_request,
                    'price_per_ticket': round(price_per_ticket, 2),
                    'scheduled_departure': flight['scheduled_departure'],
                    'origin_city': flight['origin_city'],
                    'destination_city': flight['destination_city'],
                    **addons
                }
                
                bookings.append(booking)
                current_capacity += total_passengers
                booking_counter += 1
        
        bookings_df = pd.DataFrame(bookings)
        
        # Add return flights
        bookings_df = self._find_return_flights(flight_data, bookings_df)
        
        # Clean up temporary columns
        bookings_df = bookings_df.drop(columns=['scheduled_departure', 'origin_city', 'destination_city'], errors='ignore')
        
        # Validate booking dates vs registration dates
        validation_df = bookings_df.merge(
            self.main_holders[['client_id', 'date_of_registration']], 
            left_on='customer_id', 
            right_on='client_id', 
            how='left'
        )
        
        invalid_bookings = validation_df[validation_df['booking_date'] < validation_df['date_of_registration']]
        
        if len(invalid_bookings) > 0:
            print(f"❌ ERROR: Found {len(invalid_bookings)} bookings with dates before registration!")
            print("Fixing invalid bookings...")
            
            # Fix invalid bookings by adjusting booking dates to be after registration
            for idx, invalid_booking in invalid_bookings.iterrows():
                # Set booking date to be at least 1 day after registration
                min_booking_date = invalid_booking['date_of_registration'] + timedelta(days=1)
                current_booking_date = invalid_booking['booking_date']
                
                # If current booking date is before registration, adjust it
                if current_booking_date < min_booking_date:
                    # Find the original flight departure for this booking
                    flight_match = flight_data[flight_data['planning_id'] == invalid_booking['planning_id']]
                    if not flight_match.empty:
                        flight_departure = flight_match.iloc[0]['scheduled_departure']
                        
                        # Ensure we have a valid window
                        if min_booking_date < flight_departure:
                            days_available = (flight_departure - min_booking_date).days
                            if days_available > 0:
                                random_days = np.random.randint(0, min(days_available, 30))
                                new_booking_date = min_booking_date + timedelta(days=random_days)
                                bookings_df.loc[bookings_df['booking_id'] == invalid_booking['booking_id'], 'booking_date'] = new_booking_date
            
            # Re-validate after fixing
            validation_df = bookings_df.merge(
                self.main_holders[['client_id', 'date_of_registration']], 
                left_on='customer_id', 
                right_on='client_id', 
                how='left'
            )
            invalid_bookings = validation_df[validation_df['booking_date'] < validation_df['date_of_registration']]
            
            if len(invalid_bookings) > 0:
                print(f"❌ Still found {len(invalid_bookings)} invalid bookings after fix!")
                print("Sample of remaining invalid bookings:")
                print(invalid_bookings[['booking_id', 'booking_date', 'date_of_registration']].head())
            else:
                print("✅ SUCCESS: All booking dates are after customer registration dates")
        else:
            print("✅ SUCCESS: All booking dates are after customer registration dates")
        
        # Validate minimum 75 passengers per flight
        passengers_per_flight = bookings_df.groupby('planning_id').apply(
            lambda x: (x['num_adults'] + x['num_children']).sum()
        )
        underbooked_flights = passengers_per_flight[passengers_per_flight < 75]
        
        if not underbooked_flights.empty:
            print(f"❌ ERROR: Found {len(underbooked_flights)} flights with fewer than 75 passengers!")
            print("First few underbooked flights:")
            print(underbooked_flights.head())
        else:
            print("✅ SUCCESS: All flights have at least 75 passengers")
        
        # Statistics
        print(f"\n=== REALISTIC BOOKING GENERATION COMPLETE ===")
        print(f"Total bookings generated: {len(bookings_df):,}")
        print(f"Unique customers: {bookings_df['customer_id'].nunique():,}")
        print(f"Unique flights: {bookings_df['planning_id'].nunique():,}")
        print(f"Average bookings per flight: {len(bookings_df) / bookings_df['planning_id'].nunique():.1f}")
        print(f"Average passengers per flight: {passengers_per_flight.mean():.1f}")
        
        # Bookings per flight distribution
        bookings_per_flight = bookings_df['planning_id'].value_counts()
        print(f"Flights with 1 booking: {(bookings_per_flight == 1).sum()} ({(bookings_per_flight == 1).mean()*100:.1f}%)")
        print(f"Flights with 2-5 bookings: {((bookings_per_flight >= 2) & (bookings_per_flight <= 5)).sum()}")
        print(f"Flights with 6+ bookings: {(bookings_per_flight >= 6).sum()}")
        
        print(f"Trip type distribution:")
        print(bookings_df['trip_type'].value_counts(normalize=True))
        
        group_bookings = bookings_df[bookings_df['num_adults'] >= 8]
        print(f"Group bookings (8+ adults): {len(group_bookings):,} ({len(group_bookings)/len(bookings_df)*100:.1f}%)")
        
        print(f"Average price per ticket: R{bookings_df['price_per_ticket'].mean():.2f}")
        print(f"Booking date range: {bookings_df['booking_date'].min()} to {bookings_df['booking_date'].max()}")
        
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
    Main function to generate airline bookings with proper customer registration filtering.
    
    Args:
        target_year (int): Year to generate bookings for (default: BASE_YEAR)
        save_file (bool): Whether to save the results to parquet (default: True)
        
    Returns:
        pd.DataFrame: Generated bookings dataset with proper temporal filtering
    """
    print(f"Starting REALISTIC airline bookings generation for {target_year}")
    print("Key Features:")
    print("✅ Proper customer registration date filtering")
    print("✅ Only customers registered BEFORE booking date can book")
    print("✅ Multiple realistic booking attempts per flight")
    print("✅ Route popularity modeling (major vs regional routes)")
    print("✅ Time-of-day and day-of-week booking patterns")
    print("✅ Minimum 75 passengers per flight")
    print("✅ Group booking logic with consecutive seating")
    print("✅ Realistic booking timing (popular flights book earlier)")
    print("-" * 70)
    
    try:
        generator = RealisticAirlineBookingsGenerator(target_year=target_year)
        bookings_df = generator.generate_bookings()
        
        if save_file:
            filename = generator.save_bookings(bookings_df)
            print(f"\nData saved to: {filename}")
            
        print(f"\nSuccessfully generated {len(bookings_df):,} realistic bookings for {target_year}!")
        return bookings_df
        
    except Exception as e:
        print(f"Error generating bookings: {str(e)}")
        raise

if __name__ == "__main__":
    # Set random seed for reproducibility
    seed_bytes = os.urandom(4)
    seed_int = int.from_bytes(seed_bytes, byteorder='big')
    random.seed(seed_int)
    np.random.seed(seed_int)
    
    TARGET_YEAR = 2021
    bookings = generate_realistic_airline_bookings(
        target_year=TARGET_YEAR,
        save_file=True
    )
    
    print("\nSample bookings:")
    print(bookings.head())
    
    # Additional validation
    print(f"\nValidation Results:")
    print(f"- Flights with only 1 booking: {(bookings['planning_id'].value_counts() == 1).sum()}")
    print(f"- Most bookings on single flight: {bookings['planning_id'].value_counts().max()}")
    print(f"- Average bookings per flight: {len(bookings) / bookings['planning_id'].nunique():.1f}")
    passengers_per_flight = bookings.groupby('planning_id').apply(
        lambda x: (x['num_adults'] + x['num_children']).sum()
    )
    print(f"- Flights with fewer than 75 passengers: {(passengers_per_flight < 75).sum()}")
    print(f"- Average passengers per flight: {passengers_per_flight.mean():.1f}")
    
    print(f"\nAll done! Realistic bookings for {TARGET_YEAR} are ready to use.")