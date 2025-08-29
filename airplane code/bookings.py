import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from tqdm import tqdm
import os
from scipy.stats import poisson, weibull_min, expon, norm, binom
import warnings
warnings.filterwarnings('ignore')

class BookingGenerator:
    def __init__(self, data_folder="airline_data"):
        self.data_folder = data_folder
        self.customers = None
        self.flights = None
        self.routes = None
        self.schedules = None
        self.bookings = []
        self.booking_counter = 1
        self.flight_occupancy = {}
        self.holiday_periods = self._get_holiday_periods()
        self.customer_lookup = None
        
    def _get_holiday_periods(self):
        """Define holiday periods for pricing adjustments"""
        holidays = []
        for year in range(2013, 2019):
            holidays.append((f"{year}-07-01", f"{year}-07-07"))  # Durban July
            holidays.append((f"{year}-04-01", f"{year}-04-15"))  # Easter
            holidays.append((f"{year}-12-15", f"{year}-12-31"))  # Year-end
            if year > 2013:
                holidays.append((f"{year}-01-01", f"{year}-01-10"))  # New Year
        return holidays
    
    def load_data(self):
        """Load all required datasets with optimizations"""
        print("Loading datasets...")
        
        # Load data with optimized dtypes
        self.customers = pd.read_parquet(f"{self.data_folder}/customers.parquet")
        self.flights = pd.read_parquet(f"{self.data_folder}/flights.parquet")
        self.routes = pd.read_parquet(f"{self.data_folder}/routes.parquet")
        self.schedules = pd.read_parquet(f"{self.data_folder}/flight_schedules.parquet")
        
        # Convert date columns to datetime with cache
        self.customers['Date_Registered'] = pd.to_datetime(self.customers['Date_Registered'], cache=True)
        self.schedules['scheduled_departure'] = pd.to_datetime(self.schedules['scheduled_departure'], cache=True)
        self.schedules['scheduled_arrival'] = pd.to_datetime(self.schedules['scheduled_arrival'], cache=True)
        
        # Create lookup dictionaries for faster access
        self.flights_dict = self.flights.set_index('Flight_ID').to_dict('index')
        self.routes_dict = self.routes.set_index('route_id').to_dict('index')
        self.schedules_dict = self.schedules.set_index('planning_id').to_dict('index')
        
        # Create reverse lookup for route pair relationships
        self.route_pairs = {}
        for route_id, route_info in self.routes_dict.items():
            pair_id = route_info['route_pair_id']
            if pair_id not in self.route_pairs:
                self.route_pairs[pair_id] = []
            self.route_pairs[pair_id].append(route_id)
        
        # Create customer lookup with registration dates
        self.customer_lookup = self.customers.set_index('Customer_ID')['Date_Registered'].to_dict()
        
        # Filter out canceled flights and create date index
        self.valid_schedules = self.schedules[self.schedules['cancellation_note'].isna()].copy()
        self.valid_schedules['scheduled_departure_date'] = self.valid_schedules['scheduled_departure'].dt.date
        
        # Create date-based index for faster schedule lookup
        self.schedules_by_date = {}
        for date, group in self.valid_schedules.groupby('scheduled_departure_date'):
            self.schedules_by_date[date] = group
        
        # Create route-based schedule index
        self.schedules_by_route = {}
        for route_id, group in self.valid_schedules.groupby('route_id'):
            self.schedules_by_route[route_id] = group
        
        print(f"Loaded {len(self.customers)} customers, {len(self.flights)} flights, "
              f"{len(self.routes)} routes, {len(self.schedules)} schedules")
    
    def is_holiday(self, date):
        """Check if a date falls within any holiday period - optimized"""
        date_str = date.strftime('%Y-%m-%d')
        for start, end in self.holiday_periods:
            if start <= date_str <= end:
                return True
        return False
    
    def get_season_multiplier(self, date):
        """Get seasonal pricing multiplier - optimized"""
        month = date.month
        return 1.2 if month in [6, 7, 12] else 1.0
    
    def get_urgency_multiplier(self, days_to_departure):
        """Get pricing multiplier based on booking urgency - optimized"""
        if days_to_departure < 7:
            return 1.2
        elif days_to_departure > 30:
            return 0.9
        return 1.0
    
    def calculate_price(self, route_info, booking_class, flight_date, days_to_departure, fill_ratio):
        """Calculate ticket price with all adjustments - optimized"""
        base_price = route_info['base_price_ZAR']
        
        # Class multiplier
        class_multiplier = {
            'Economy': 1.0,
            'Business': 2.0,
            'First': 3.5
        }[booking_class]
        
        # Combined multipliers
        holiday_multiplier = 1.3 if self.is_holiday(flight_date) else 1.0
        urgency_multiplier = self.get_urgency_multiplier(days_to_departure)
        season_multiplier = self.get_season_multiplier(flight_date)
        demand_multiplier = 0.8 + (fill_ratio * 0.4)
        random_multiplier = norm.rvs(loc=1.0, scale=0.1)
        
        # Calculate final price in one operation
        price = (base_price * class_multiplier * holiday_multiplier * 
                urgency_multiplier * season_multiplier * demand_multiplier * random_multiplier)
        
        return max(round(price, 2), 100.0)
    
    def get_passenger_counts(self, customer_id):
        """Generate passenger counts based on customer type - optimized"""
        if customer_id.startswith('COM'):
            return max(1, poisson.rvs(3)), 0, 0  # Company customer
        else:
            return (max(1, poisson.rvs(1)),  # Individual customer
                    poisson.rvs(0.2), 
                    poisson.rvs(0.1))
    
    def get_booking_status(self, flight_datetime, booking_datetime):
        """Determine booking status with heavy skew towards checked-in"""
        # Time difference between booking and flight
        time_diff = (flight_datetime - booking_datetime).total_seconds() / 3600  # hours
        
        # Status probabilities (heavily skewed towards checked-in)
        if time_diff < 0:
            # Booking after flight departure - should be very rare (error case)
            return 'cancelled'
        
        # Normal cases - skewed distribution
        status_probs = {
            'checked-in': 0.65,    # 65% checked-in (heavily skewed)
            'on-hold': 0.05,       # 5% on-hold
            'rescheduled': 0.03,    # 3% rescheduled
            'cancelled': 0.02,      # 2% cancelled
            'no-show': 0.05,        # 5% no-show
            'confirmed': 0.20       # 20% confirmed (not yet checked in)
        }
        
        # Random selection based on probabilities
        statuses = list(status_probs.keys())
        probs = list(status_probs.values())
        return np.random.choice(statuses, p=probs)
    
    def get_available_capacity(self, planning_id, schedule_row):
        """Get available capacity for a flight - optimized"""
        if planning_id not in self.flight_occupancy:
            flight_id = schedule_row['flight_id']
            flight_info = self.flights_dict.get(flight_id, {})
            capacity = flight_info.get('Capacity', 100) if flight_info else 100
            self.flight_occupancy[planning_id] = {
                'capacity': capacity,
                'occupied': 0
            }
        return self.flight_occupancy[planning_id]['capacity'] - self.flight_occupancy[planning_id]['occupied']
    
    def update_capacity(self, planning_id, num_passengers):
        """Update flight occupancy - optimized"""
        if planning_id in self.flight_occupancy:
            self.flight_occupancy[planning_id]['occupied'] += num_passengers
    
    def generate_booking_id(self):
        """Generate unique booking ID - optimized"""
        booking_id = f"BKG{self.booking_counter:06d}"
        self.booking_counter += 1
        return booking_id
    
    def find_opposite_route_schedule(self, current_route_id, return_date):
        """Find the opposite route for return trips - optimized"""
        current_route_info = self.routes_dict.get(current_route_id)
        if not current_route_info:
            return None
            
        route_pair_id = current_route_info['route_pair_id']
        
        # Find opposite routes in the same pair
        opposite_routes = self.route_pairs.get(route_pair_id, [])
        opposite_routes = [r for r in opposite_routes if r != current_route_id]
        
        if not opposite_routes:
            return None
            
        # Try each opposite route until we find an available schedule
        for opposite_route_id in opposite_routes:
            # Check if there are schedules for this route on the return date
            opposite_schedules = self.schedules_by_date.get(return_date.date(), pd.DataFrame())
            if len(opposite_schedules) == 0:
                continue
                
            matching_schedules = opposite_schedules[opposite_schedules['route_id'] == opposite_route_id]
            if len(matching_schedules) > 0:
                # Check capacity for the first available schedule
                return_schedule = matching_schedules.iloc[0]
                return_planning_id = return_schedule['planning_id']
                available_capacity = self.get_available_capacity(return_planning_id, return_schedule)
                
                if available_capacity > 0:
                    return return_planning_id
        
        return None
    
    def generate_booking_datetime(self, flight_datetime, reg_datetime=None):
        """Generate booking datetime that is BEFORE flight departure time"""
        # Generate days before flight (exponential distribution, mean 30 days)
        days_before = max(0, int(expon.rvs(scale=30)))
        
        if days_before == 0:
            # Same day booking - MUST be before flight time
            # Generate random time 1-6 hours BEFORE flight departure
            hours_before = random.randint(1, 6)
            minutes_before = random.randint(0, 59)
            
            booking_datetime = flight_datetime - timedelta(hours=hours_before, minutes=minutes_before)
            
            # Double-check: ensure booking is definitely before flight
            if booking_datetime >= flight_datetime:
                booking_datetime = flight_datetime - timedelta(hours=1)  # Force 1 hour before
        else:
            # Different day booking
            booking_date = flight_datetime - timedelta(days=days_before)
            
            # Generate random time during business hours (8 AM to 8 PM)
            hour = random.randint(8, 20)
            minute = random.randint(0, 59)
            booking_datetime = booking_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        # Ensure booking is after registration date
        if reg_datetime and booking_datetime < reg_datetime:
            booking_datetime = reg_datetime + timedelta(hours=random.randint(1, 12))
        
        # Final validation: booking MUST be before flight
        if booking_datetime >= flight_datetime:
            booking_datetime = flight_datetime - timedelta(minutes=30)  # Emergency fallback
        
        return booking_datetime
    
    def generate_regular_bookings(self):
        """Generate regular bookings with optimizations"""
        print("First pass: Regular bookings...")
        
        # Pre-sample customers for faster access
        customer_ids = self.customers['Customer_ID'].values
        customer_count = len(customer_ids)
        
        # Process each day in the date range
        date_range = pd.date_range('2013-01-01', '2018-12-31')
        
        for current_date in tqdm(date_range, desc="Processing days"):
            date_str = current_date.date()
            date_schedules = self.schedules_by_date.get(date_str, pd.DataFrame())
            
            if len(date_schedules) == 0:
                continue
                
            for _, schedule in date_schedules.iterrows():
                planning_id = schedule['planning_id']
                route_id = schedule['route_id']
                flight_datetime = schedule['scheduled_departure']
                
                # Get route info from dictionary
                route_info = self.routes_dict.get(route_id)
                if not route_info:
                    continue
                
                # Determine fill ratio
                fill_ratio = min(0.99, max(0.45, weibull_min.rvs(2, scale=0.54)))
                if self.is_holiday(flight_datetime):
                    fill_ratio = 0.99
                
                available_capacity = self.get_available_capacity(planning_id, schedule)
                if available_capacity <= 0:
                    continue
                
                # Calculate target number of passengers
                flight_capacity = self.flight_occupancy[planning_id]['capacity']
                target_passengers = min(available_capacity, int(flight_capacity * fill_ratio))
                
                # Determine number of bookings to create
                avg_passengers_per_booking = 1.5  # Conservative estimate
                num_bookings = min(target_passengers, int(target_passengers / avg_passengers_per_booking))
                num_bookings = max(1, num_bookings)
                
                for _ in range(num_bookings):
                    # Random customer selection
                    customer_idx = random.randint(0, customer_count - 1)
                    customer_id = customer_ids[customer_idx]
                    
                    # Check registration date
                    reg_datetime = self.customer_lookup.get(customer_id)
                    if not reg_datetime or reg_datetime > flight_datetime:
                        continue
                    
                    # Generate booking datetime (ensuring it's BEFORE flight time)
                    booking_datetime = self.generate_booking_datetime(flight_datetime, reg_datetime)
                    
                    # Get passenger counts
                    num_adults, num_children, num_infants = self.get_passenger_counts(customer_id)
                    total_passengers = num_adults + num_children
                    
                    if total_passengers > available_capacity:
                        continue
                    
                    # Get booking status
                    booking_status = self.get_booking_status(flight_datetime, booking_datetime)
                    
                    # Create booking
                    self._create_booking(
                        customer_id, planning_id, route_id, route_info, booking_datetime, 
                        flight_datetime, fill_ratio, num_adults, num_children, num_infants, booking_status
                    )
                    
                    self.update_capacity(planning_id, total_passengers)
                    available_capacity -= total_passengers
    
    def _create_booking(self, customer_id, planning_id, route_id, route_info, booking_datetime, 
                       flight_datetime, fill_ratio, num_adults, num_children, num_infants, booking_status):
        """Helper method to create booking - optimized"""
        # Determine trip type and booking class
        trip_type = "Return" if random.random() < 0.15 else "One-way"
        booking_class = random.choice(['Economy', 'Business', 'First'])
        
        # Calculate price
        days_to_departure = (flight_datetime - booking_datetime).days
        price_per_ticket = self.calculate_price(
            route_info, booking_class, flight_datetime, days_to_departure, fill_ratio
        )
        
        total_amount = price_per_ticket * (num_adults + num_children)
        
        # On-hold logic (only if status is on-hold)
        on_hold = booking_status == 'on-hold'
        on_hold_end_datetime = booking_datetime + timedelta(days=365) if on_hold else None
        
        # Rescheduled date logic
        rescheduled_datetime = None
        if booking_status == 'rescheduled':
            reschedule_days = max(7, int(expon.rvs(scale=60)))
            rescheduled_datetime = flight_datetime + timedelta(days=reschedule_days)
        
        # Return date logic
        return_datetime_val = None
        if trip_type == "Return":
            return_days = max(2, int(expon.rvs(scale=7)))
            return_datetime_val = flight_datetime + timedelta(days=return_days)
        
        # Create booking
        booking = {
            'booking_id': self.generate_booking_id(),
            'customer_id': customer_id,
            'planning_id': planning_id,
            'booking_date': booking_datetime,
            'trip_type': trip_type,
            'num_adults': num_adults,
            'num_children': num_children,
            'num_infants': num_infants,
            'booking_class': booking_class,
            'price_per_ticket': price_per_ticket,
            'total_amount_paid': round(total_amount, 2),
            'booking_status': booking_status,  # New booking status field
            'on_hold': on_hold,
            'on_hold_end_date': on_hold_end_datetime if on_hold_end_datetime else None,
            'rescheduled_date': rescheduled_datetime if rescheduled_datetime else None,
            'return_date': return_datetime_val if return_datetime_val else None,
            'is_return': False
        }
        
        self.bookings.append(booking)
        
        # Create return booking if needed
        if trip_type == "Return" and return_datetime_val:
            return_planning_id = self.find_opposite_route_schedule(route_id, return_datetime_val)
            if return_planning_id:
                return_booking = booking.copy()
                return_booking['booking_id'] = self.generate_booking_id()
                return_booking['planning_id'] = return_planning_id
                return_booking['return_date'] = None
                return_booking['is_return'] = True
                # Set appropriate status for return flight
                return_booking['booking_status'] = self.get_booking_status(return_datetime_val, booking_datetime)
                self.bookings.append(return_booking)
    
    def ensure_all_customers_have_bookings(self):
        """Ensure all customers have at least one booking - optimized"""
        print("Second pass: Ensuring all customers have bookings...")
        
        # Get customers without bookings
        booked_customers = {b['customer_id'] for b in self.bookings}
        customers_without_bookings = [
            cust_id for cust_id in self.customer_lookup.keys() 
            if cust_id not in booked_customers
        ]
        
        if not customers_without_bookings:
            return
            
        # Get available schedules with capacity
        available_schedules = []
        for date_str, schedules in self.schedules_by_date.items():
            for _, schedule in schedules.iterrows():
                planning_id = schedule['planning_id']
                capacity = self.get_available_capacity(planning_id, schedule)
                if capacity > 0:
                    available_schedules.append((planning_id, schedule, capacity))
        
        if not available_schedules:
            return
            
        for customer_id in tqdm(customers_without_bookings, desc="Processing customers without bookings"):
            reg_datetime = self.customer_lookup.get(customer_id)
            if not reg_datetime:
                continue
                
            # Find suitable schedule
            suitable_schedules = [
                (pid, sched, cap) for pid, sched, cap in available_schedules 
                if sched['scheduled_departure'] >= reg_datetime and cap >= 1
            ]
            
            if not suitable_schedules:
                continue
                
            planning_id, schedule, capacity = random.choice(suitable_schedules)
            route_id = schedule['route_id']
            flight_datetime = schedule['scheduled_departure']
            route_info = self.routes_dict.get(route_id)
            
            if not route_info:
                continue
                
            # Create booking with proper datetime
            booking_datetime = self.generate_booking_datetime(flight_datetime, reg_datetime)
            
            num_adults, num_children, num_infants = self.get_passenger_counts(customer_id)
            total_passengers = num_adults + num_children
            
            if total_passengers > capacity:
                continue
                
            # Get booking status
            booking_status = self.get_booking_status(flight_datetime, booking_datetime)
                
            booking_class = random.choice(['Economy', 'Business', 'First'])
            days_to_departure = (flight_datetime - booking_datetime).days
            price_per_ticket = self.calculate_price(route_info, booking_class, flight_datetime, days_to_departure, 0.7)
            total_amount = price_per_ticket * (num_adults + num_children)
            
            booking = {
                'booking_id': self.generate_booking_id(),
                'customer_id': customer_id,
                'planning_id': planning_id,
                'booking_date': booking_datetime,
                'trip_type': "One-way",
                'num_adults': num_adults,
                'num_children': num_children,
                'num_infants': num_infants,
                'booking_class': booking_class,
                'price_per_ticket': price_per_ticket,
                'total_amount_paid': round(total_amount, 2),
                'booking_status': booking_status,
                'on_hold': booking_status == 'on-hold',
                'on_hold_end_date': booking_datetime + timedelta(days=365) if booking_status == 'on-hold' else None,
                'rescheduled_date': None,
                'return_date': None,
                'is_return': False
            }
            
            self.bookings.append(booking)
            self.update_capacity(planning_id, total_passengers)
    
    def generate_bookings(self):
        """Main method to generate all bookings"""
        self.generate_regular_bookings()
        self.ensure_all_customers_have_bookings()
    
    def save_bookings(self):
        """Save bookings to parquet file - optimized"""
        if not self.bookings:
            print("No bookings generated!")
            return
            
        bookings_df = pd.DataFrame(self.bookings)
        
        # Convert date columns efficiently
        date_cols = ['booking_date', 'on_hold_end_date', 'rescheduled_date', 'return_date']
        for col in date_cols:
            if col in bookings_df.columns and bookings_df[col].notna().any():
                bookings_df[col] = pd.to_datetime(bookings_df[col], errors='coerce')
        
        # Validate booking datetime constraints
        print("\nValidating booking datetime constraints...")
        valid_count = 0
        total_count = len(bookings_df)
        
        for _, booking in bookings_df.iterrows():
            if pd.notna(booking['booking_date']):
                # Get flight departure time from schedule
                schedule_info = self.schedules_dict.get(booking['planning_id'], {})
                flight_departure = schedule_info.get('scheduled_departure')
                
                if flight_departure and booking['booking_date'] < flight_departure:
                    valid_count += 1
        
        print(f"Valid bookings (booking before flight): {valid_count}/{total_count} ({valid_count/total_count*100:.1f}%)")
        
        # Show booking status distribution
        status_distribution = bookings_df['booking_status'].value_counts(normalize=True) * 100
        print("\nBooking Status Distribution:")
        for status, percentage in status_distribution.items():
            print(f"  {status}: {percentage:.1f}%")
        
        # Save to file
        output_path = f"{self.data_folder}/bookings.parquet"
        bookings_df.to_parquet(output_path, index=False, compression='snappy')
        
        # Print summary
        print(f"\nSaved {len(bookings_df)} bookings to {output_path}")
        print(f"Customers with bookings: {bookings_df['customer_id'].nunique()}")
        print(f"Coverage: {(bookings_df['customer_id'].nunique() / len(self.customers) * 100):.1f}%")
        print(f"Total revenue: ZAR {bookings_df['total_amount_paid'].sum():,.2f}")

def main():
    """Main function to generate bookings"""
    generator = BookingGenerator()
    generator.load_data()
    generator.generate_bookings()
    generator.save_bookings()

if __name__ == "__main__":
    main()