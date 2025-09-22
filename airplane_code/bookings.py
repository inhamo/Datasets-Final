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

class FastAirlineBookingsGenerator:
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
                
                # Convert date_of_registration early to avoid comparison issues
                self.clients_df['date_of_registration'] = pd.to_datetime(
                    self.clients_df['date_of_registration'], errors='coerce'
                )
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
            
            # Additional validation for registration dates
            target_year_start = pd.to_datetime(f"{target_year}-01-01")
            if (self.clients_df['date_of_registration'] > target_year_start).any():
                print("Warning: Some registration dates are after the target year start. These will be adjusted in _prepare_data.")
            
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Missing data file: {str(e)}")
        except Exception as e:
            raise Exception(f"Error loading data: {str(e)}")
                
        self._prepare_data()
        self._pregenerate_random_values()

    def _prepare_data(self):
        """Prepare and merge all datasets with robust datetime handling and realistic registration dates."""
        self.flight_schedule_df['scheduled_departure'] = pd.to_datetime(self.flight_schedule_df['scheduled_departure'])
        self.flight_schedule_df['scheduled_arrival'] = pd.to_datetime(self.flight_schedule_df['scheduled_arrival'])
        
        if 'actual_departure' in self.flight_schedule_df.columns:
            self.flight_schedule_df['actual_departure'] = pd.to_datetime(self.flight_schedule_df['actual_departure'])
        if 'actual_arrival' in self.flight_schedule_df.columns:
            self.flight_schedule_df['actual_arrival'] = pd.to_datetime(self.flight_schedule_df['actual_arrival'])
            
        self.clients_df['dob'] = pd.to_datetime(self.clients_df['dob'], errors='coerce')
        
        # More robust registration date handling (already converted in __init__, but re-do for safety)
        self.clients_df['date_of_registration'] = pd.to_datetime(
            self.clients_df['date_of_registration'], errors='coerce', format='mixed'
        )
        
        # Handle invalid registration dates more realistically
        invalid_dates_mask = self.clients_df['date_of_registration'].isna()
        
        # Get the earliest flight date for the target year to set realistic bounds
        target_year_start = pd.to_datetime(f"{self.TARGET_YEAR}-01-01")
        earliest_flight = self.flight_schedule_df['scheduled_departure'].min()
        
        if invalid_dates_mask.any():
            print(f"Warning: {invalid_dates_mask.sum()} invalid registration dates found. Generating realistic dates...")
            
            # Generate realistic registration dates between 6 months before target year and earliest flight
            reg_start = target_year_start - timedelta(days=180)  # 6 months before target year
            reg_end = min(earliest_flight - timedelta(days=1), target_year_start + timedelta(days=30))  # Before earliest flight or early in target year
            
            # Generate random dates in this range
            date_range = (reg_end - reg_start).days
            random_days = np.random.randint(0, max(1, date_range), size=invalid_dates_mask.sum())
            random_dates = [reg_start + timedelta(days=int(days)) for days in random_days]
            
            self.clients_df.loc[invalid_dates_mask, 'date_of_registration'] = random_dates
        
        # Ensure all registration dates are before the target year's flights
        max_registration_date = earliest_flight - timedelta(hours=1)
        late_registration_mask = self.clients_df['date_of_registration'] > max_registration_date
        if late_registration_mask.any():
            print(f"Warning: {late_registration_mask.sum()} registration dates after earliest flight. Adjusting...")
            days_before = np.random.randint(1, 31, size=late_registration_mask.sum())
            adjusted_dates = [earliest_flight - timedelta(days=int(days)) for days in days_before]
            self.clients_df.loc[late_registration_mask, 'date_of_registration'] = adjusted_dates
        
        self.main_holders = self.clients_df[self.clients_df['is_main_holder'] == True][['client_id', 'city', 'date_of_registration']].copy()
        
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
        
        # Pre-calculate min booking offset for each customer to ensure realistic booking dates
        self.main_holders['min_booking_offset_hours'] = (
            self.flight_data['scheduled_departure'].min() - self.main_holders['date_of_registration']
        ).dt.total_seconds() / 3600
        
        # Ensure minimum offset is at least 1 hour
        self.main_holders['min_booking_offset_hours'] = np.maximum(
            self.main_holders['min_booking_offset_hours'], 1
        )
        
        print(f"Loaded data for {self.TARGET_YEAR}:")
        print(f"- {len(self.main_holders):,} main account holders")
        print(f"- {len(self.flight_data):,} scheduled flights")
        print(f"- {self.flight_data['route_id'].nunique()} unique routes")
        print(f"- Registration dates range: {self.main_holders['date_of_registration'].min()} to {self.main_holders['date_of_registration'].max()}")

    def _pregenerate_random_values(self):
        """Pre-generate random values using various statistical distributions."""
        num_flights = len(self.flight_data)
        self.num_samples = num_flights * 5
        
        self.load_factors = stats.beta.rvs(a=5, b=2, loc=0.5, scale=0.65, size=num_flights)
        
        # Enhanced passenger types to include group bookings
        passenger_probs = [0.55, 0.18, 0.08, 0.06, 0.04, 0.03, 0.02, 0.015, 0.01, 0.008, 0.007]
        passenger_types = [
            # Individual/Family bookings (adults, children, infants)
            (1, 0, 0),    # Solo traveler
            (2, 0, 0),    # Couple
            (2, 1, 0),    # Small family
            (2, 2, 0),    # Family with 2 kids
            (2, 1, 1),    # Family with baby
            (3, 0, 0),    # Friends/colleagues
            (4, 0, 0),    # Small group
            # Group bookings
            (8, 0, 0),    # Small sports team/corporate
            (12, 0, 0),   # Medium sports team
            (18, 0, 0),   # Large sports team/tour group
            (25, 0, 0)    # Large corporate/conference group
        ]
        choices = np.random.choice(len(passenger_types), size=self.num_samples, p=passenger_probs)
        self.random_passenger_types = np.array([passenger_types[i] for i in choices])
        
        # Group booking indicators for special handling
        self.is_group_booking = np.array([adults >= 8 for adults, _, _ in self.random_passenger_types])
        
        # Group booking types for special handling
        group_booking_types = np.random.choice([
            'sports_team', 'corporate_event', 'wedding_party', 'tour_group', 
            'conference', 'school_trip', 'military', 'emergency_services'
        ], size=self.num_samples)
        self.group_booking_types = np.where(self.is_group_booking, group_booking_types, 'individual')
        
        # Adjust booking class probabilities for groups (more economy, some charter)
        business_prob = np.where(self.is_group_booking, 0.15, 0.05)  # Groups less likely business class
        self.random_booking_classes = np.where(
            stats.binom.rvs(n=1, p=business_prob, size=self.num_samples) == 1, 'business', 'economy'
        )
        
        # Charter bookings for very large groups (rare but realistic)
        charter_prob = np.where([adults >= 18 for adults, _, _ in self.random_passenger_types], 0.05, 0.0)
        self.is_charter = stats.binom.rvs(n=1, p=charter_prob, size=self.num_samples).astype(bool)
        
        # Group bookings more likely to be one-way (especially sports teams)
        group_oneway_prob = np.where(
            np.isin(self.group_booking_types, ['sports_team', 'military', 'emergency_services']), 
            0.6, 0.25  # Sports/military often one-way, others more likely return
        )
        return_prob = np.where(self.is_group_booking, 1 - group_oneway_prob, 0.75)
        self.random_trip_types = np.where(
            stats.binom.rvs(n=1, p=return_prob, size=self.num_samples) == 1, 'return', 'one-way'
        )
        
        status_probs = [0.87, 0.08, 0.04, 0.01]
        status_choices = ['confirmed', 'cancelled', 'rescheduled', 'on-hold']
        self.random_status_choices = np.random.choice(status_choices, size=self.num_samples, p=status_probs)
        
        # Group bookings have different timing patterns
        # Sports teams often book closer to event, corporate further out
        booking_scales = np.where(
            np.isin(self.group_booking_types, ['sports_team', 'emergency_services']), 
            48,    # 2 days average for urgent bookings
            np.where(
                np.isin(self.group_booking_types, ['corporate_event', 'conference', 'wedding_party']),
                336,   # 2 weeks average for planned events
                168    # 1 week average for others
            )
        )
        self.booking_offsets = np.minimum(
            stats.expon.rvs(scale=booking_scales, size=self.num_samples),
            720  # Cap at 30 days to avoid extreme offsets
        )
        
        # Group discounts and pricing variations
        group_discount = np.where(self.is_group_booking, 
                                np.random.uniform(0.85, 0.95, size=self.num_samples), 1.0)
        self.price_multipliers = stats.norm.rvs(loc=1.3, scale=0.25, size=self.num_samples) * group_discount
        self.price_multipliers = np.clip(self.price_multipliers, 0.6, 1.8)
        
        # Adjust addon probabilities for group bookings
        priority_prob = np.where(self.is_group_booking, 0.35, 0.18)  # Groups more likely priority
        assisted_prob = np.where(
            np.isin(self.group_booking_types, ['school_trip', 'tour_group']), 0.08, 
            np.where(self.is_group_booking, 0.05, 0.025)
        )
        lounge_prob = np.where(
            np.isin(self.group_booking_types, ['corporate_event', 'conference']), 0.25, 
            np.where(self.is_group_booking, 0.03, 0.08)  # Corporate groups more lounge access
        )
        
        self.addon_flags = {
            'is_priority': stats.binom.rvs(n=1, p=priority_prob, size=self.num_samples).astype(bool),
            'is_assisted': stats.binom.rvs(n=1, p=assisted_prob, size=self.num_samples).astype(bool),
            'is_special_needs': stats.binom.rvs(n=1, p=0.015, size=self.num_samples).astype(bool),
            'is_lounge_access': stats.binom.rvs(n=1, p=lounge_prob, size=self.num_samples).astype(bool),
            'is_cancellation_refundable': stats.binom.rvs(n=1, p=0.45, size=self.num_samples).astype(bool),
            'is_travel_protection': stats.binom.rvs(n=1, p=0.28, size=self.num_samples).astype(bool),
            'is_cheap_hotel_accommodation': stats.binom.rvs(n=1, p=0.06, size=self.num_samples).astype(bool),
            'is_car_rental': stats.binom.rvs(n=1, p=0.05, size=self.num_samples).astype(bool)
        }

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

    def _calculate_realistic_booking_date(self, customer_reg_date, flight_departure, booking_offset_hours):
        """Calculate a realistic booking date that ensures registration comes before booking."""
        # Convert to datetime if needed
        if isinstance(customer_reg_date, str):
            customer_reg_date = pd.to_datetime(customer_reg_date)
        if isinstance(flight_departure, str):
            flight_departure = pd.to_datetime(flight_departure)
        
        min_booking_date = customer_reg_date + timedelta(hours=1)
        max_hours_before = (flight_departure - min_booking_date).total_seconds() / 3600
        
        if max_hours_before <= 0:
            # If registration is too close to or after flight, set booking date to just after registration
            return min_booking_date + timedelta(hours=np.random.uniform(0.5, 1))
        
        # Cap the booking offset to the available time window
        adjusted_offset = min(booking_offset_hours, max_hours_before)
        proposed_booking_date = flight_departure - timedelta(hours=adjusted_offset)
        
        # Double-check that the booking date is after registration
        if proposed_booking_date < min_booking_date:
            proposed_booking_date = min_booking_date + timedelta(hours=np.random.uniform(0.5, 1))
        
        return proposed_booking_date

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

    def _introduce_data_errors(self, bookings_df: pd.DataFrame) -> pd.DataFrame:
        """Introduce deliberate errors into the bookings DataFrame."""
        bookings_with_errors = bookings_df.copy()
        
        num_duplicates = int(len(bookings_df) * 0.02)
        duplicate_rows = bookings_df.sample(n=num_duplicates, replace=True).copy()
        max_booking_id = int(bookings_df['booking_id'].str[6:].max()) if not bookings_df.empty else 0
        duplicate_rows['booking_id'] = [
            f"BK{self.TARGET_YEAR}{i:06d}" 
            for i in range(max_booking_id + 1, max_booking_id + num_duplicates + 1)
        ]
        bookings_with_errors = pd.concat([bookings_with_errors, duplicate_rows], ignore_index=True)
        
        for column in ['seat_request', 'price_per_ticket']:
            mask = stats.binom.rvs(n=1, p=0.01, size=len(bookings_with_errors)).astype(bool)
            bookings_with_errors.loc[mask, column] = np.nan
        
        mask = stats.binom.rvs(n=1, p=0.005, size=len(bookings_with_errors)).astype(bool)
        bookings_with_errors.loc[mask, 'booking_status'] = 'invalid_status'
        
        mask = stats.binom.rvs(n=1, p=0.002, size=len(bookings_with_errors)).astype(bool)
        bookings_with_errors.loc[mask, 'price_per_ticket'] = -bookings_with_errors.loc[mask, 'price_per_ticket']
        
        print(f"Introduced errors:")
        print(f"- {num_duplicates} duplicate bookings")
        print(f"- {int(mask.sum())} rows with invalid status")
        print(f"- Missing values and negative prices added to ~1% and ~0.2% of rows respectively")
        
        return bookings_with_errors

    def generate_bookings(self):
        """Generate bookings with vectorized operations and realistic date constraints."""
        print(f"Generating bookings for {self.TARGET_YEAR} with varied statistical distributions")
        
        flight_data = self.flight_data.copy()
        flight_data['target_bookings'] = (flight_data['aircraft_capacity'] * 
                                        self.load_factors[:len(flight_data)] * 
                                        np.where(self.load_factors[:len(flight_data)] > 0.9, 1.05, 1.0)).astype(int)
        
        # Pre-select customers and their registration dates
        customer_indices = np.random.randint(0, len(self.main_holders), size=self.num_samples)
        selected_customers = self.main_holders.iloc[customer_indices].reset_index(drop=True)
        
        bookings = []
        booking_counter = 1
        idx = 0
        
        for _, flight in tqdm(flight_data.iterrows(), total=len(flight_data), desc="Processing flights"):
            target_bookings = flight['target_bookings']
            current_bookings = 0
            
            # Filter customers with registration dates before this flight
            valid_customers = selected_customers[
                selected_customers['date_of_registration'] < flight['scheduled_departure'] - timedelta(hours=1)
            ].reset_index(drop=True)
            if valid_customers.empty:
                continue
            
            customer_idx = 0
            while current_bookings < target_bookings and customer_idx < len(valid_customers) and idx < len(selected_customers):
                customer = valid_customers.iloc[customer_idx]
                num_adults, num_children, num_infants = self.random_passenger_types[idx]
                total_passengers = num_adults + num_children
                
                if current_bookings + total_passengers > target_bookings:
                    break
                
                # Calculate realistic booking date
                booking_date = self._calculate_realistic_booking_date(
                    customer['date_of_registration'],
                    flight['scheduled_departure'],
                    self.booking_offsets[idx] if idx < len(self.booking_offsets) else 168
                )
                
                # Determine if this is a group booking
                is_group = self.is_group_booking[idx] if idx < len(self.is_group_booking) else False
                group_type = self.group_booking_types[idx] if idx < len(self.group_booking_types) else 'individual'
                
                price_per_ticket = flight.get('final_price_zar', 800) * self.price_multipliers[idx]
                if self.random_booking_classes[idx] == 'business':
                    price_per_ticket *= 3.0
                elif is_group and num_adults >= 12:
                    # Additional group discount for large bookings
                    price_per_ticket *= 0.9
                
                status = self.random_status_choices[idx]
                cancelled_date = (booking_date + timedelta(hours=stats.uniform.rvs(loc=1, scale=239))) if status == 'cancelled' else None
                on_hold_date = (booking_date + timedelta(hours=stats.uniform.rvs(loc=1, scale=47))) if status == 'on-hold' else None
                on_hold_end_date = (on_hold_date + timedelta(days=365)) if status == 'on-hold' else None
                
                # Generate appropriate seat assignments
                seat_assignments = self._simple_seat_assignment(total_passengers, is_group, group_type)
                seat_request = ','.join(seat_assignments) if len(seat_assignments) > 1 else seat_assignments[0]
                
                booking = {
                    'booking_id': f"BK{self.TARGET_YEAR}{booking_counter:06d}",
                    'customer_id': customer['client_id'],
                    'planning_id': flight['planning_id'],
                    'booking_date': booking_date,
                    'trip_type': self.random_trip_types[idx],
                    'num_adults': num_adults,
                    'num_children': num_children,
                    'num_infants': num_infants,
                    'booking_class': self.random_booking_classes[idx],
                    'booking_status': status,
                    'group_booking_type': group_type,
                    'is_charter': self.is_charter[idx] if idx < len(self.is_charter) else False,
                    'cancelled_date': cancelled_date,
                    'rescheduled_date': None,
                    'on_hold_date': on_hold_date,
                    'on_hold_end_date': on_hold_end_date,
                    'outbound_id': None,
                    'rescheduled_id': None,
                    'seat_request': seat_request,
                    'price_per_ticket': round(price_per_ticket, 2),
                    'is_priority': self.addon_flags['is_priority'][idx],
                    'is_assisted': self.addon_flags['is_assisted'][idx],
                    'is_special_needs': self.addon_flags['is_special_needs'][idx],
                    'is_lounge_access': self.addon_flags['is_lounge_access'][idx],
                    'is_cancellation_refundable': self.addon_flags['is_cancellation_refundable'][idx],
                    'is_travel_protection': self.addon_flags['is_travel_protection'][idx],
                    'is_cheap_hotel_accommodation': self.addon_flags['is_cheap_hotel_accommodation'][idx],
                    'is_car_rental': self.addon_flags['is_car_rental'][idx],
                    'scheduled_departure': flight['scheduled_departure'],
                    'origin_city': flight['origin_city'],
                    'destination_city': flight['destination_city']
                }
                
                bookings.append(booking)
                current_bookings += total_passengers
                booking_counter += 1
                customer_idx += 1
                idx += 1
        
        bookings_df = pd.DataFrame(bookings)
        bookings_df = self._find_return_flights(flight_data, bookings_df)
        
        # Post-process to fix any remaining invalid bookings
        validation_df = bookings_df.merge(
            self.main_holders[['client_id', 'date_of_registration']], 
            left_on='customer_id', 
            right_on='client_id', 
            how='left'
        )
        invalid_bookings = validation_df[validation_df['booking_date'] < validation_df['date_of_registration']]
        
        if len(invalid_bookings) > 0:
            print(f"Fixing {len(invalid_bookings)} invalid bookings...")
            for idx in invalid_bookings.index:
                reg_date = validation_df.loc[idx, 'date_of_registration']
                flight_dep = bookings_df.loc[idx, 'scheduled_departure']
                bookings_df.loc[idx, 'booking_date'] = self._calculate_realistic_booking_date(
                    reg_date, flight_dep, stats.expon.rvs(scale=168)
                )
        
        bookings_df = self._introduce_data_errors(bookings_df)
        
        # Final validation
        validation_df = bookings_df.merge(
            self.main_holders[['client_id', 'date_of_registration']], 
            left_on='customer_id', 
            right_on='client_id', 
            how='left'
        )
        invalid_bookings = validation_df[validation_df['booking_date'] < validation_df['date_of_registration']]
        if len(invalid_bookings) > 0:
            print(f"Error: {len(invalid_bookings)} bookings still invalid after correction!")
        else:
            print("✓ All booking dates are after customer registration dates")
        
        bookings_df = bookings_df.drop(columns=['scheduled_departure', 'origin_city', 'destination_city'], errors='ignore')
        
        print(f"\n=== FAST BOOKING GENERATION COMPLETE ===")
        print(f"Total bookings generated: {len(bookings_df):,}")
        print(f"Unique customers: {bookings_df['customer_id'].nunique():,}")
        print(f"Unique flights: {bookings_df['planning_id'].nunique():,}")
        print(f"Trip type distribution:")
        print(bookings_df['trip_type'].value_counts(normalize=True))
        print(f"Group booking types distribution:")
        if 'group_booking_type' in bookings_df.columns:
            group_stats = bookings_df['group_booking_type'].value_counts()
            print(group_stats)
            print(f"Group bookings: {(group_stats.sum() - group_stats.get('individual', 0)):,} ({((group_stats.sum() - group_stats.get('individual', 0))/len(bookings_df)*100):.1f}%)")
        print(f"Average price per ticket: R{bookings_df['price_per_ticket'].mean():.2f}")
        print(f"Booking date range: {bookings_df['booking_date'].min()} to {bookings_df['booking_date'].max()}")
        
        # Show large group bookings stats
        large_groups = bookings_df[bookings_df['num_adults'] >= 10]
        if len(large_groups) > 0:
            print(f"Large group bookings (10+ people): {len(large_groups):,}")
            print(f"Largest group: {large_groups['num_adults'].max()} people")
        
        return bookings_df
        
    def save_bookings(self, bookings_df, filename=None):
        """Save bookings to parquet file."""
        if filename is None:
            filename = f'airplane_data/bookings_{self.TARGET_YEAR}.parquet'
            
        bookings_df.to_parquet(filename, index=False, engine='pyarrow')
        print(f"Bookings saved to: {filename}")
        
        return filename

def generate_fast_airline_bookings(target_year=BASE_YEAR, save_file=True):
    """
    Main function to generate airline bookings with optimized performance and realistic date constraints.
    
    Args:
        target_year (int): Year to generate bookings for (default: BASE_YEAR)
        save_file (bool): Whether to save the results to parquet (default: True)
        
    Returns:
        pd.DataFrame: Generated bookings dataset with introduced errors
    """
    print(f"Starting FAST airline bookings generation for {target_year}")
    print("Using various statistical distributions (beta, normal, binomial, etc.)")
    print("Optimizations:")
    print("- Vectorized booking generation")
    print("- Simplified seat assignment with Poisson distribution")
    print("- Optimized return flight lookup")
    print("- Robust datetime handling with realistic registration dates")
    print("- Guaranteed booking date > registration date validation")
    print("- Introduced data errors (duplicates, missing values, invalid data)")
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
    TARGET_YEAR = 2021
    bookings = generate_fast_airline_bookings(
        target_year=TARGET_YEAR,
        save_file=True
    )
    
    print("\nSample bookings:")
    print(bookings.head())
    print(f"\nAll done! Fast bookings for {TARGET_YEAR} are ready to use.")