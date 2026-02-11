import pandas as pd
import numpy as np
import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

class FastHotelBookingGenerator:
    def __init__(self, customers_df: pd.DataFrame, rooms_df: pd.DataFrame, 
                 hotels_df: pd.DataFrame, staff_df: pd.DataFrame = None):
        """
        Optimized and robust version
        """
        self.customers_df = customers_df.copy()
        self.rooms_df = rooms_df.copy()
        self.hotels_df = hotels_df.copy()
        self.staff_df = staff_df.copy() if staff_df is not None else None
        
        # Pre-compute for faster access
        self._preprocess_data()
        
        # Distributions with exact sums
        self.status_dist = ['CONFIRMED', 'CHECKED_IN', 'CHECKED_OUT', 'CANCELLED', 'NO_SHOW', 'PENDING']
        self.status_weights = np.array([0.35, 0.15, 0.30, 0.12, 0.05, 0.03])
        self.status_weights /= self.status_weights.sum()  # Ensure sum = 1
        
        self.booking_sources = ['Website Direct', 'Mobile App', 'OTA (Booking.com)', 
                               'OTA (Expedia)', 'Phone', 'Travel Agent', 'Walk-in']
        self.booking_source_weights = np.array([0.30, 0.25, 0.20, 0.15, 0.05, 0.03, 0.02])
        self.booking_source_weights /= self.booking_source_weights.sum()
        
        self.stay_purposes = ['Leisure', 'Business', 'Family Visit', 'Event/Conference', 'Medical', 'Other']
        self.stay_purpose_weights = np.array([0.45, 0.35, 0.10, 0.05, 0.03, 0.02])
        self.stay_purpose_weights /= self.stay_purpose_weights.sum()
        
        # COVID impact
        self.covid_impact = {2019: 1.0, 2020: 0.4, 2021: 0.6, 2022: 0.9, 2023: 1.1, 2024: 1.2}
        
        # Cancellation reasons
        self.cancellation_reasons = [
            'Change of plans', 'Found better deal', 'Travel restrictions', 'Personal emergency',
            'Weather issues', 'Flight cancellation', 'Dissatisfied with hotel',
            'Financial reasons', 'Work commitment', 'Family emergency'
        ]
        
        # Room type preferences
        self.room_preferences = {
            'Business': {'Executive': 0.4, 'Deluxe': 0.4, 'Standard': 0.2},
            'Leisure': {'Deluxe': 0.5, 'Suite': 0.3, 'Standard': 0.2},
            'Family': {'Family': 0.6, 'Suite': 0.3, 'Deluxe': 0.1}
        }
    
    def _preprocess_data(self):
        """Preprocess data with robust weight calculation"""
        # Create customer weights
        loyalty_multipliers = {'Non-Member': 0.8, 'Basic': 1.0, 'Silver': 1.5, 
                              'Gold': 2.0, 'Platinum': 3.0}
        
        if 'loyalty_tier' in self.customers_df.columns:
            self.customers_df['weight'] = self.customers_df['loyalty_tier'].map(
                lambda x: loyalty_multipliers.get(str(x), 1.0)
            )
        else:
            self.customers_df['weight'] = 1.0
        
        # Business customers get higher weight
        if 'customer_type' in self.customers_df.columns:
            self.customers_df['weight'] *= np.where(
                self.customers_df['customer_type'] == 'Business', 1.2, 1.0
            )
        
        # Ensure no NaN weights
        self.customers_df['weight'] = self.customers_df['weight'].fillna(1.0)
        
        # Ensure all weights are positive
        self.customers_df['weight'] = np.maximum(self.customers_df['weight'], 0.001)
        
        # Normalize weights with high precision
        weights_sum = self.customers_df['weight'].sum()
        if weights_sum > 0:
            self.customers_df['weight'] = self.customers_df['weight'] / weights_sum
        else:
            self.customers_df['weight'] = 1.0 / len(self.customers_df)
        
        # Verify weights sum to 1 (within tolerance)
        weight_sum = self.customers_df['weight'].sum()
        if abs(weight_sum - 1.0) > 1e-10:
            # Force normalization
            self.customers_df['weight'] = self.customers_df['weight'] / weight_sum
        
        # Create room lookup dictionary
        self.room_lookup = {}
        for (hotel_id, room_type), group in self.rooms_df.groupby(['hotel_id', 'room_type']):
            self.room_lookup[(hotel_id, room_type)] = group['room_id'].tolist()
        
        # Create hotel staff lookup
        self.staff_lookup = {}
        if self.staff_df is not None:
            for hotel_id, group in self.staff_df[self.staff_df['is_active']].groupby('hotel_id'):
                self.staff_lookup[hotel_id] = {
                    'Front Desk': group[group['department'] == 'Front Desk']['staff_id'].tolist(),
                    'Housekeeping': group[group['department'] == 'Housekeeping']['staff_id'].tolist()
                }
    
    def _get_customer_sample(self, n: int):
        """Robust customer sampling"""
        weights = self.customers_df['weight'].values
        
        # Ensure weights are valid
        if len(weights) == 0 or np.any(weights < 0) or not np.isfinite(weights).all():
            # Fallback to uniform sampling
            indices = np.random.choice(len(self.customers_df), size=n, replace=True)
        else:
            # Normalize to ensure sum = 1
            weights = weights / weights.sum()
            indices = np.random.choice(len(self.customers_df), size=n, p=weights, replace=True)
        
        return self.customers_df.iloc[indices].reset_index(drop=True)
    
    def _get_room_for_hotel(self, hotel_id: str, preferred_type: str):
        """Get a room for a hotel with fallback logic"""
        # Try preferred type first
        key = (hotel_id, preferred_type)
        if key in self.room_lookup and len(self.room_lookup[key]) > 0:
            return np.random.choice(self.room_lookup[key]), preferred_type
        
        # Try alternative types
        alternatives = {
            'Standard': ['Deluxe', 'Executive'],
            'Deluxe': ['Standard', 'Executive'],
            'Executive': ['Deluxe', 'Suite'],
            'Suite': ['Executive', 'Presidential'],
            'Family': ['Suite', 'Deluxe'],
            'Presidential': ['Suite']
        }
        
        for alt_type in alternatives.get(preferred_type, ['Standard']):
            key = (hotel_id, alt_type)
            if key in self.room_lookup and len(self.room_lookup[key]) > 0:
                return np.random.choice(self.room_lookup[key]), alt_type
        
        # Last resort: any room in the hotel
        hotel_rooms = self.rooms_df[self.rooms_df['hotel_id'] == hotel_id]
        if len(hotel_rooms) > 0:
            room = hotel_rooms.sample(1).iloc[0]
            return room['room_id'], room['room_type']
        
        return None, None
    
    def _get_staff_for_booking(self, hotel_id: str):
        """Get staff members for a booking"""
        result = {
            'booking_agent_id': '',
            'check_in_staff_id': '',
            'check_out_staff_id': '',
            'housekeeping_staff_id': ''
        }
        
        if hotel_id in self.staff_lookup:
            hotel_staff = self.staff_lookup[hotel_id]
            
            # Assign front desk staff
            if hotel_staff['Front Desk']:
                front_desk = hotel_staff['Front Desk']
                result['booking_agent_id'] = np.random.choice(front_desk)
                result['check_in_staff_id'] = np.random.choice(front_desk)
                result['check_out_staff_id'] = np.random.choice(front_desk)
            
            # Assign housekeeping staff
            if hotel_staff['Housekeeping']:
                result['housekeeping_staff_id'] = np.random.choice(hotel_staff['Housekeeping'])
        
        return result
    
    def _generate_batch_bookings(self, batch_size: int, year: int):
        """Generate a batch of bookings"""
        # Sample customers
        customers_batch = self._get_customer_sample(batch_size)
        n = len(customers_batch)
        
        # Generate months with seasonal distribution
        month_probs = np.array([0.08, 0.09, 0.11, 0.12, 0.13, 0.15, 
                               0.16, 0.14, 0.12, 0.11, 0.09, 0.13])
        month_probs /= month_probs.sum()
        months = np.random.choice(range(1, 13), size=n, p=month_probs)
        days = np.random.randint(2, 28, size=n)
        
        # Create check-in dates
        checkin_dates = []
        for i in range(n):
            try:
                checkin_dates.append(datetime(year, months[i], days[i]))
            except:
                # Handle invalid dates (e.g., Feb 30)
                checkin_dates.append(datetime(year, months[i], min(days[i], 28)))
        
        # Get customer types
        customer_types = customers_batch.get('customer_type', 'Leisure').fillna('Leisure').values
        
        # Calculate lead times
        lead_times = np.zeros(n, dtype=int)
        for i, ct in enumerate(customer_types):
            if ct == 'Business':
                lt = np.random.normal(7, 5)
                lead_times[i] = np.clip(int(round(lt)), 1, 30)
            elif ct == 'Family':
                lt = np.random.normal(60, 40)
                lead_times[i] = np.clip(int(round(lt)), 7, 180)
            else:
                lt = np.random.normal(45, 30)
                lead_times[i] = np.clip(int(round(lt)), 1, 365)
        
        # Calculate booking dates
        booking_dates = [cd - timedelta(days=int(lt)) for cd, lt in zip(checkin_dates, lead_times)]
        
        # Stay purposes and durations
        stay_purposes = np.random.choice(
            self.stay_purposes, 
            size=n, 
            p=self.stay_purpose_weights
        )
        
        durations = np.ones(n, dtype=int)
        for i, purpose in enumerate(stay_purposes):
            if purpose == 'Business':
                durations[i] = np.clip(int(round(np.random.normal(2.5, 1.5))), 1, 7)
            elif purpose == 'Family Visit':
                durations[i] = np.clip(int(round(np.random.normal(5, 3))), 2, 21)
            elif purpose == 'Event/Conference':
                durations[i] = np.clip(int(round(np.random.normal(3, 1))), 1, 7)
            else:
                durations[i] = np.clip(int(round(np.random.normal(4, 3))), 1, 14)
        
        # Calculate checkout dates
        checkout_dates = [cd + timedelta(days=int(d)) for cd, d in zip(checkin_dates, durations)]
        
        # Select hotels
        hotel_indices = np.random.choice(len(self.hotels_df), size=n, replace=True)
        hotels_batch = self.hotels_df.iloc[hotel_indices].reset_index(drop=True)
        
        # Select room types based on customer preferences
        room_types = []
        for ct in customer_types:
            if ct in self.room_preferences:
                preferences = self.room_preferences[ct]
                room_types.append(np.random.choice(
                    list(preferences.keys()), 
                    p=list(preferences.values())
                ))
            else:
                room_types.append(np.random.choice(['Standard', 'Deluxe'], p=[0.5, 0.5]))
        
        # Get rooms
        room_ids = []
        final_room_types = []
        
        for i in range(n):
            hotel_id = hotels_batch.iloc[i]['hotel_id']
            room_type = room_types[i]
            
            room_id, final_type = self._get_room_for_hotel(hotel_id, room_type)
            room_ids.append(room_id)
            final_room_types.append(final_type)
        
        # Get room prices
        room_prices = []
        for room_id in room_ids:
            if room_id:
                price = self.rooms_df.loc[self.rooms_df['room_id'] == room_id, 'price_per_night']
                if len(price) > 0:
                    room_prices.append(price.values[0])
                else:
                    room_prices.append(150.0)
            else:
                room_prices.append(150.0)
        
        # Determine status
        current_date = datetime.now()
        statuses = []
        for i in range(n):
            if booking_dates[i] > current_date:
                statuses.append(np.random.choice(['CONFIRMED', 'PENDING'], p=[0.83, 0.17]))
            elif checkin_dates[i] > current_date:
                statuses.append(np.random.choice(self.status_dist, p=self.status_weights))
            else:
                statuses.append(np.random.choice(['CHECKED_OUT', 'NO_SHOW', 'CANCELLED'], p=[0.7, 0.15, 0.15]))
        
        # Calculate pricing
        base_prices = np.array(room_prices, dtype=float)
        durations_arr = np.array(durations, dtype=float)
        
        # Loyalty discounts
        loyalty_tiers = customers_batch.get('loyalty_tier', 'Non-Member').fillna('Non-Member').values
        loyalty_discounts = np.zeros(n)
        for i, tier in enumerate(loyalty_tiers):
            if tier == 'Basic':
                loyalty_discounts[i] = 0.05
            elif tier == 'Silver':
                loyalty_discounts[i] = 0.10
            elif tier == 'Gold':
                loyalty_discounts[i] = 0.15
            elif tier == 'Platinum':
                loyalty_discounts[i] = 0.20
        
        # Seasonal factors
        seasonal_factors = np.array([self._get_seasonal_factor(m) for m in months])
        
        # Calculate amounts
        discounted_rates = base_prices * seasonal_factors * (1 - loyalty_discounts)
        room_totals = discounted_rates * durations_arr
        tax_rates = np.random.uniform(0.08, 0.20, size=n)
        tax_amounts = room_totals * tax_rates
        service_fees = room_totals * np.random.uniform(0, 0.05, size=n)
        total_amounts = room_totals + tax_amounts + service_fees
        
        # Round everything
        base_prices = np.round(base_prices, 2)
        discounted_rates = np.round(discounted_rates, 2)
        room_totals = np.round(room_totals, 2)
        tax_amounts = np.round(tax_amounts, 2)
        service_fees = np.round(service_fees, 2)
        total_amounts = np.round(total_amounts, 2)
        
        # Generate bookings
        bookings = []
        for i in range(n):
            if not room_ids[i]:  # Skip if no room found
                continue
            
            # Get staff assignments
            hotel_id = hotels_batch.iloc[i]['hotel_id']
            staff_assignment = self._get_staff_for_booking(hotel_id)
            
            # Create booking
            booking = {
                'booking_id': str(uuid.uuid4()),
                'customer_id': customers_batch.iloc[i]['customer_id'],
                'hotel_id': hotel_id,
                'room_id': room_ids[i],
                'booking_agent_id': staff_assignment['booking_agent_id'],
                'check_in_staff_id': staff_assignment['check_in_staff_id'],
                'check_out_staff_id': staff_assignment['check_out_staff_id'],
                'housekeeping_staff_id': staff_assignment['housekeeping_staff_id'],
                'booking_date': booking_dates[i].strftime('%Y-%m-%d'),
                'check_in_date': checkin_dates[i].strftime('%Y-%m-%d'),
                'check_out_date': checkout_dates[i].strftime('%Y-%m-%d'),
                'check_in_time': f"{np.random.choice([14, 15, 16])}:{np.random.choice(['00', '15', '30', '45'])}:00",
                'check_out_time': f"{np.random.choice([10, 11, 12])}:{np.random.choice(['00', '15', '30', '45'])}:00",
                'total_nights': int(durations[i]),
                'total_guests': np.random.choice([1, 2, 3, 4], p=[0.3, 0.4, 0.2, 0.1]),
                'room_type': final_room_types[i] if final_room_types[i] else 'Standard',
                'status': statuses[i],
                'stay_purpose': stay_purposes[i],
                'booking_source': np.random.choice(self.booking_sources, p=self.booking_source_weights),
                'special_requests': self._generate_special_request(customer_types[i]),
                'base_rate_per_night': float(base_prices[i]),
                'discounted_rate_per_night': float(discounted_rates[i]),
                'room_total': float(room_totals[i]),
                'tax_amount': float(tax_amounts[i]),
                'service_fee': float(service_fees[i]),
                'total_amount': float(total_amounts[i]),
                'deposit_amount': float(np.round(total_amounts[i] * np.random.uniform(0.1, 0.3), 2)),
                'payment_status': self._get_payment_status(statuses[i]),
                'discount_percent': float(np.round(loyalty_discounts[i] * 100, 1)),
                'tax_rate_percent': float(np.round(tax_rates[i] * 100, 1)),
                'lead_time_days': int(lead_times[i]),
                'loyalty_tier_at_booking': str(loyalty_tiers[i]),
                'customer_type_at_booking': str(customer_types[i]),
                'created_at': booking_dates[i].strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': (booking_dates[i] + timedelta(hours=np.random.randint(1, 24))).strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Add cancellation details if needed
            if statuses[i] == 'CANCELLED':
                cancellation = self._generate_cancellation_details(
                    booking_dates[i], checkin_dates[i], total_amounts[i]
                )
                booking.update(cancellation)
            else:
                booking.update({
                    'cancellation_date': '',
                    'cancellation_reason': '',
                    'refund_percent': 0.0,
                    'refund_amount': 0.0,
                    'cancellation_fee_percent': 0.0,
                    'cancellation_fee': 0.0
                })
            
            bookings.append(booking)
        
        return pd.DataFrame(bookings)
    
    def _get_seasonal_factor(self, month: int) -> float:
        """Get seasonal price factor"""
        factors = {
            1: 0.8, 2: 0.9, 3: 1.1, 4: 1.2, 5: 1.3,
            6: 1.5, 7: 1.6, 8: 1.4, 9: 1.2, 10: 1.1,
            11: 0.9, 12: 1.3
        }
        return factors.get(month, 1.0)
    
    def _generate_special_request(self, customer_type: str) -> str:
        """Generate special requests"""
        if np.random.random() < 0.6:  # 40% have no requests
            return 'None'
        
        requests = []
        if customer_type == 'Business':
            if np.random.random() < 0.3:
                requests.append('Early check-in')
            if np.random.random() < 0.2:
                requests.append('Late check-out')
        elif customer_type == 'Family':
            if np.random.random() < 0.2:
                requests.append('Extra bed')
            if np.random.random() < 0.1:
                requests.append('Baby crib')
        
        if np.random.random() < 0.3:
            requests.append('Non-smoking room')
        if np.random.random() < 0.2:
            requests.append('High floor')
        
        return ', '.join(requests[:3]) if requests else 'None'
    
    def _get_payment_status(self, booking_status: str) -> str:
        """Determine payment status"""
        if booking_status == 'CANCELLED':
            return np.random.choice(['REFUNDED', 'PARTIAL', 'PENDING'], p=[0.6, 0.3, 0.1])
        elif booking_status == 'NO_SHOW':
            return 'PENDING'
        elif booking_status in ['CONFIRMED', 'CHECKED_IN', 'CHECKED_OUT']:
            return np.random.choice(['PAID', 'PENDING', 'PARTIAL'], p=[0.65, 0.25, 0.10])
        else:
            return 'PENDING'
    
    def _generate_cancellation_details(self, booking_date: datetime, 
                                      checkin_date: datetime, total_amount: float) -> Dict:
        """Generate cancellation details"""
        days_before = max(1, (checkin_date - booking_date).days)
        
        if days_before > 7:
            refund_percent = np.random.uniform(0.7, 1.0)
        elif days_before > 1:
            refund_percent = np.random.uniform(0.3, 0.7)
        else:
            refund_percent = 0.0
        
        cancellation_fee_percent = 1 - refund_percent
        
        cancellation_days = np.random.randint(1, max(2, days_before))
        cancellation_date = booking_date + timedelta(days=cancellation_days)
        
        return {
            'cancellation_date': cancellation_date.strftime('%Y-%m-%d'),
            'cancellation_reason': np.random.choice(self.cancellation_reasons),
            'refund_percent': float(np.round(refund_percent * 100, 1)),
            'refund_amount': float(np.round(total_amount * refund_percent, 2)),
            'cancellation_fee_percent': float(np.round(cancellation_fee_percent * 100, 1)),
            'cancellation_fee': float(np.round(total_amount * cancellation_fee_percent, 2))
        }
    
    def generate_bookings(self, num_bookings: int = 50000,
                         start_year: int = 2019, end_year: int = 2024):
        """Generate bookings efficiently"""
        print(f"Generating {num_bookings:,} bookings from {start_year} to {end_year}...")
        
        # Calculate distribution by year
        years = list(range(start_year, end_year + 1))
        year_weights = np.array([self.covid_impact.get(year, 1.0) for year in years])
        year_weights = year_weights / year_weights.sum()
        
        bookings_per_year = (year_weights * num_bookings).astype(int)
        total_allocated = bookings_per_year.sum()
        
        # Adjust for rounding
        if total_allocated < num_bookings:
            bookings_per_year[-1] += (num_bookings - total_allocated)
        elif total_allocated > num_bookings:
            bookings_per_year[-1] = max(0, bookings_per_year[-1] - (total_allocated - num_bookings))
        
        # Generate bookings year by year
        all_bookings = []
        
        for year_idx, year in enumerate(tqdm(years, desc="Processing years")):
            year_target = bookings_per_year[year_idx]
            if year_target <= 0:
                continue
            
            # Generate in manageable batches
            batch_size = 2000
            batches_needed = (year_target + batch_size - 1) // batch_size
            
            for batch in tqdm(range(batches_needed), desc=f"Year {year}", leave=False):
                current_batch_size = min(batch_size, year_target - (batch * batch_size))
                if current_batch_size <= 0:
                    break
                
                batch_df = self._generate_batch_bookings(current_batch_size, year)
                if len(batch_df) > 0:
                    all_bookings.append(batch_df)
        
        # Combine all bookings
        if all_bookings:
            final_df = pd.concat(all_bookings, ignore_index=True)
            
            # Ensure we have the right number
            if len(final_df) > num_bookings:
                final_df = final_df.sample(n=num_bookings, random_state=42).reset_index(drop=True)
            
            return final_df
        else:
            print("Warning: No bookings were generated!")
            return pd.DataFrame()

# Main execution
if __name__ == "__main__":
    import time
    
    print("="*60)
    print("FAST HOTEL BOOKING GENERATOR")
    print("="*60)
    
    # Load data
    print("\nLoading data...")
    start_load = time.time()
    
    customers_df = pd.read_csv("hotel_data/hotel_customers.csv")
    rooms_df = pd.read_csv("hotel_data/hotel_chain_rooms.csv")
    hotels_df = pd.read_csv("hotel_data/hotel_chain_hotels.csv")
    staff_df = pd.read_csv("hotel_data/hotel_chain_staff.csv")
    
    print(f"Data loaded in {time.time() - start_load:.1f} seconds")
    print(f"  Customers: {len(customers_df):,}")
    print(f"  Rooms: {len(rooms_df):,}")
    print(f"  Hotels: {len(hotels_df):,}")
    print(f"  Staff: {len(staff_df):,}")
    
    # Check for required columns
    print("\nChecking data structure...")
    required_customer_cols = ['customer_id']
    missing_customer = [col for col in required_customer_cols if col not in customers_df.columns]
    if missing_customer:
        print(f"Missing columns in customers: {missing_customer}")
        # Create customer_id if missing
        if 'customer_id' not in customers_df.columns:
            customers_df['customer_id'] = [str(uuid.uuid4()) for _ in range(len(customers_df))]
            print(f"  Created customer_id column")
    
    # Initialize generator
    print("\nInitializing generator...")
    generator = FastHotelBookingGenerator(customers_df, rooms_df, hotels_df, staff_df)
    
    # Generate bookings
    print("\n" + "="*60)
    num_bookings = 50000
    print(f"Generating {num_bookings:,} bookings...")
    
    start_gen = time.time()
    bookings_df = generator.generate_bookings(
        num_bookings=num_bookings,
        start_year=2019,
        end_year=2024
    )
    
    gen_time = time.time() - start_gen
    print(f"\nGeneration completed in {gen_time:.1f} seconds")
    print(f"  Speed: {len(bookings_df)/gen_time:,.0f} bookings/second")
    
    if len(bookings_df) == 0:
        print("\nERROR: No bookings were generated!")
        print("Check your input data and try again.")
        exit(1)
    
    # Basic analysis
    print("\n" + "="*60)
    print("GENERATION SUMMARY")
    print("="*60)
    
    print(f"\nTotal bookings generated: {len(bookings_df):,}")
    print(f"Unique customers: {bookings_df['customer_id'].nunique():,}")
    print(f"Unique hotels: {bookings_df['hotel_id'].nunique():,}")
    print(f"Unique rooms booked: {bookings_df['room_id'].nunique():,}")
    
    # Status distribution
    print(f"\nBooking Status Distribution:")
    status_counts = bookings_df['status'].value_counts()
    for status, count in status_counts.items():
        percentage = (count / len(bookings_df)) * 100
        print(f"  {status:<12} {count:>6,} ({percentage:>5.1f}%)")
    
    # Financial summary
    print(f"\nFinancial Summary:")
    total_revenue = bookings_df['total_amount'].sum()
    avg_booking = bookings_df['total_amount'].mean()
    print(f"  Total Revenue: ${total_revenue:,.2f}")
    print(f"  Average Booking: ${avg_booking:,.2f}")
    print(f"  Max Booking: ${bookings_df['total_amount'].max():,.2f}")
    print(f"  Min Booking: ${bookings_df['total_amount'].min():,.2f}")
    
    # Room type popularity
    print(f"\nTop 5 Room Types:")
    room_counts = bookings_df['room_type'].value_counts().head(5)
    for room_type, count in room_counts.items():
        percentage = (count / len(bookings_df)) * 100
        print(f"  {room_type:<12} {count:>6,} ({percentage:>5.1f}%)")
    
    # Check for issues
    print(f"\nData Quality Check:")
    missing_data = {}
    for col in ['booking_id', 'customer_id', 'room_id', 'check_in_date']:
        missing = bookings_df[col].isnull().sum()
        if missing > 0:
            missing_data[col] = missing
    
    if missing_data:
        print("  WARNING: Missing data found:")
        for col, count in missing_data.items():
            print(f"    {col}: {count:,} missing")
    else:
        print(" No missing critical data")
    
    # Sample output
    print(f"\nSample Bookings (first 3):")
    sample_cols = ['booking_id', 'customer_id', 'check_in_date', 'check_out_date', 
                  'room_type', 'status', 'total_amount']
    print(bookings_df[sample_cols].head(3).to_string(index=False))
    
    # Save to CSV
    print(f"\n" + "="*60)
    print("Saving data...")
    
    output_file = 'hotel_data/hotel_chain_bookings.csv'
    start_save = time.time()
    bookings_df.to_csv(output_file, index=False)
    save_time = time.time() - start_save
    
    # Check file size
    import os
    if os.path.exists(output_file):
        file_size = os.path.getsize(output_file) / (1024**2)
        print(f"Saved to: {output_file}")
        print(f"  File size: {file_size:.1f} MB")
        print(f"  Save time: {save_time:.1f} seconds")
        print(f"  Rows written: {len(bookings_df):,}")
    else:
        print(f"ERROR: File not saved!")
    
    print("\n" + "="*60)
    print("PROCESS COMPLETE")
    print("="*60)
    print(f"Total time: {time.time() - start_load:.1f} seconds")
    print(f"Bookings generated: {len(bookings_df):,}")
    print(f"Overall speed: {len(bookings_df)/(time.time() - start_load):,.0f} bookings/second")