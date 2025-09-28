import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
from tqdm import tqdm
from collections import defaultdict

# Set random seed for reproducibility
seed_bytes = os.urandom(4)
seed_int = int.from_bytes(seed_bytes, byteorder='big')
random.seed(seed_int)
np.random.seed(seed_int)

# Constants
TARGET_YEAR = 2021
CANCELLATION_REASONS = [
    "Personal Reasons", "Schedule Change", "Financial Issues", 
    "Health Concerns", "Flight Disruption", "Other"
]
CANCELLATION_CHANNELS = ["Mobile", "Website", "Agent"]
CHANNEL_PROBS = [0.6, 0.3, 0.1]  # Aligned with bookings.py

def generate_cancellations():
    """Generate a cancellations table based on bookings data with realistic errors."""
    print(f"Generating cancellations for {TARGET_YEAR}...")
    
    # Load bookings and flight schedule data
    try:
        bookings_df = pd.read_parquet(f'airplane_data/bookings_{TARGET_YEAR}.parquet')
        flight_schedule_df = pd.read_parquet(f'airplane_data/flight_schedule_{TARGET_YEAR}.parquet')
    except FileNotFoundError as e:
        print(f"Error: Missing data file: {str(e)}")
        return None
    
    # Convert datetimes in flight_schedule_df
    flight_schedule_df['scheduled_departure'] = pd.to_datetime(flight_schedule_df['scheduled_departure'])
    
    # Create a dictionary of flights by route_id, list of (planning_id, scheduled_departure) sorted by date
    flights_by_route = defaultdict(list)
    for _, row in flight_schedule_df.iterrows():
        flights_by_route[row['route_id']].append((row['planning_id'], row['scheduled_departure']))
    for route in flights_by_route:
        flights_by_route[route].sort(key=lambda x: x[1])
    
    # Filter bookings with status 'cancelled', 'rescheduled', or 'on-hold'
    cancellations_df = bookings_df[bookings_df['booking_status'].isin(['cancelled', 'rescheduled', 'on-hold'])].copy()
    
    if cancellations_df.empty:
        print("No cancellations, reschedulings, or on-hold bookings found.")
        return None
    
    # Merge with flight schedule to get scheduled_departure and route_id
    cancellations_df = cancellations_df.merge(
        flight_schedule_df[['planning_id', 'route_id', 'scheduled_departure']],
        on='planning_id',
        how='left'
    )
    
    # Initialize cancellation records
    cancellation_records = []
    for idx, row in tqdm(cancellations_df.iterrows(), total=len(cancellations_df), desc="Generating cancellations"):
        booking_id = row['booking_id']
        client_id = row['client_id']
        booking_date = pd.to_datetime(row['booking_date'])
        scheduled_departure = pd.to_datetime(row['scheduled_departure'])
        status = row['booking_status']
        route_id = row['route_id']
        booking_class = row['booking_class']
        num_adults = row['num_adults']
        num_children = row['num_children']
        num_infants = row['num_infants']
        
        # Initialize ref_booking_id
        ref_booking_id = booking_id
        
        # Ensure timezone-naive dates
        if booking_date.tz is not None:
            booking_date = booking_date.tz_localize(None)
        if scheduled_departure.tz is not None:
            scheduled_departure = scheduled_departure.tz_localize(None)
        
        # Generate action date (between booking_date and scheduled_departure)
        time_window_hours = (scheduled_departure - booking_date).total_seconds() / 3600
        if time_window_hours <= 0:
            continue  # Skip if invalid time window
        action_hours = random.uniform(1, time_window_hours)
        action_date = booking_date + timedelta(hours=action_hours)
        
        # Set mutually exclusive flags
        is_cancelled = status == 'cancelled'
        is_rescheduled = status == 'rescheduled'
        is_onhold = status == 'on-hold'
        
        cancellation_date = action_date if is_cancelled else None
        rescheduled_date = None
        on_hold_date = None
        on_hold_end_date = None
        rescheduled_planning_id = None
        
        # Handle rescheduling
        if is_rescheduled:
            rescheduled_date = action_date
            candidates = flights_by_route.get(route_id, [])
            if candidates:
                later_flights = [p for p, d in candidates if d > rescheduled_date]
                if later_flights:
                    rescheduled_planning_id = random.choice(later_flights)
                else:
                    closest = min(candidates, key=lambda x: abs(x[1] - rescheduled_date))
                    rescheduled_planning_id = closest[0]
        
        # Handle on-hold bookings
        elif is_onhold:
            on_hold_date = action_date
            on_hold_end_date = on_hold_date + timedelta(days=365)
            exercise_days = random.randint(1, 365)
            rescheduled_date = on_hold_date + timedelta(days=exercise_days)
            if rescheduled_date.year <= TARGET_YEAR:
                candidates = flights_by_route.get(route_id, [])
                if candidates:
                    later_flights = [p for p, d in candidates if d > rescheduled_date]
                    if later_flights:
                        rescheduled_planning_id = random.choice(later_flights)
                    else:
                        closest = min(candidates, key=lambda x: abs(x[1] - rescheduled_date))
                        rescheduled_planning_id = closest[0]
        
        # Inject realistic customer behavior patterns
        # 1. Weather-related clustering
        if scheduled_departure.month in [6, 7, 8] and random.random() < 0.15:  # Winter weather
            cancellation_reason = "Weather Concerns"
            if random.random() < 0.3:
                action_date = scheduled_departure - timedelta(days=1)  # Last-minute weather cancellations
        
        # 2. Holiday cancellation spikes
        elif scheduled_departure.month == 12 and scheduled_departure.day > 20:
            if random.random() < 0.25:  # Higher cancellation rate around Christmas
                cancellation_reason = "Personal Reasons"
        
        # 3. Business travel patterns
        elif booking_date.weekday() < 5 and booking_class == 'Business' and random.random() < 0.15:  # Weekday business booking
            cancellation_reason = "Schedule Change"
        
        # 4. Non-refundable fare cancellations
        elif row['is_cancellation_refundable'] == False and random.random() < 0.1:
            cancellation_reason = "Financial Issues"  # Non-refundable fares lead to financial complaints
        
        # Generate cancellation reason (20% chance of None), but after patterns
        if 'cancellation_reason' not in locals():
            cancellation_reason = random.choice(CANCELLATION_REASONS) if random.random() < 0.80 else None
        
        # Generate cancellation channel
        cancellation_channel = random.choices(CANCELLATION_CHANNELS, weights=CHANNEL_PROBS, k=1)[0]
        
        # Inject errors
        # Data Quality Issues:
        # 1. Inconsistent cancellation_date formats (use invalid datetime instead of string)
        if random.random() < 0.03:  # 3% chance
            cancellation_date = datetime(9999, 12, 31) if is_cancelled else None
        
        # 2. Future cancellation dates
        if random.random() < 0.01:  # 1% chance
            cancellation_date = action_date + timedelta(days=random.randint(1, 30)) if is_cancelled else None
        
        # 3. Missing or duplicate cancel_ids
        cancel_id = f"CN{TARGET_YEAR}{idx + 1:06d}"
        if random.random() < 0.02:  # 2% chance
            if random.random() < 0.5:
                cancel_id = None  # Missing ID
            else:
                cancel_id = f"CN{TARGET_YEAR}{random.randint(1, len(cancellations_df)):06d}"  # Duplicate ID
        
        # 4. Incorrect channel mappings
        if random.random() < 0.05:  # 5% chance
            if cancellation_channel == "Agent":
                cancellation_channel = "Website"
        
        # Business Logic Violations:
        # 1. On-hold bookings with immediate cancellation
        if is_onhold and random.random() < 0.02:  # 2% chance
            is_cancelled = True
            is_onhold = False
            cancellation_date = action_date
        
        # 2. Rescheduled to non-existent flights
        if is_rescheduled and random.random() < 0.03:  # 3% chance
            rescheduled_planning_id = f"PLN{TARGET_YEAR}9999"
        
        # 3. Same-day reschedule to past flight
        if is_rescheduled and random.random() < 0.02:  # 2% chance
            candidates = flights_by_route.get(route_id, [])
            if candidates:
                past_flights = [p for p, d in candidates if d < rescheduled_date]
                if past_flights:  # Only assign if past flights exist
                    rescheduled_planning_id = random.choice(past_flights)
        
        # 4. On-hold end dates before start dates
        if is_onhold and random.random() < 0.01:  # 1% chance
            on_hold_end_date = on_hold_date - timedelta(days=random.randint(1, 30)) if on_hold_date else None
        
        # Customer Service Process Errors:
        # 1. Multiple cancellation records for same booking
        if random.random() < 0.01:  # 1% chance
            duplicate_record = {
                'cancel_id': f"CN{TARGET_YEAR}{len(cancellation_records) + 1:06d}",
                'ref_booking_id': booking_id,
                'is_cancelled': is_cancelled,
                'is_rescheduled': is_rescheduled,
                'is_onhold': is_onhold,
                'cancellation_date': cancellation_date,
                'rescheduled_date': rescheduled_date,
                'on_hold_date': on_hold_date,
                'on_hold_end_date': on_hold_end_date,
                'rescheduled_planning_id': rescheduled_planning_id,
                'cancellation_reason': cancellation_reason,
                'cancellation_channel': random.choice(CANCELLATION_CHANNELS),
                'refund_amount': refund_amount if 'refund_amount' in locals() else None
            }
            cancellation_records.append(duplicate_record)
        
        # 2. Cancelled bookings marked as rescheduled
        if is_cancelled and random.random() < 0.02:  # 2% chance
            is_cancelled = False
            is_rescheduled = True
            rescheduled_date = action_date
            candidates = flights_by_route.get(route_id, [])
            if candidates:
                later_flights = [p for p, d in candidates if d > rescheduled_date]
                rescheduled_planning_id = random.choice(later_flights) if later_flights else min(candidates, key=lambda x: abs(x[1] - rescheduled_date))[0]
        
        # 3. Orphaned cancellation records
        if random.random() < 0.005:  # 0.5% chance
            ref_booking_id = f"BK{TARGET_YEAR}999999"
        
        # 4. Agent notes in reason field
        if random.random() < 0.03:  # 3% chance
            cancellation_reason = "Customer called upset - processed refund - see notes in system"
        
        # Temporal Logic Errors:
        # 1. Cancellation after flight departure
        if random.random() < 0.01:  # 1% chance
            cancellation_date = scheduled_departure + timedelta(hours=random.randint(1, 24)) if is_cancelled else None
        
        # 2. Rescheduled date before original booking
        if is_rescheduled and random.random() < 0.015:  # 1.5% chance
            rescheduled_date = booking_date - timedelta(days=random.randint(1, 10))
        
        # 3. On-hold exercised after expiry
        if is_onhold and random.random() < 0.02:  # 2% chance
            rescheduled_date = (on_hold_end_date + timedelta(days=random.randint(1, 90))) if on_hold_end_date else None
        
        # 4. Time zone confusion
        if random.random() < 0.02:  # 2% chance
            if cancellation_date and not isinstance(cancellation_date, str):  # Ensure datetime
                cancellation_date = cancellation_date + timedelta(hours=random.choice([-3, 3, -8, 8]))
        
        # System Integration Errors:
        # 1. Partial record updates
        if random.random() < 0.02:  # 2% chance
            if is_rescheduled:
                rescheduled_planning_id = None
        
        # 2. Channel attribution errors
        if random.random() < 0.04:  # 4% chance
            if cancellation_channel == "Mobile":
                cancellation_channel = "Agent"
        
        # 3. Reason code mapping errors
        if random.random() < 0.03:  # 3% chance
            cancellation_reason = "Code: 404"
        
        # 4. Currency/refund calculation errors
        refund_amount = None
        if row['is_cancellation_refundable'] and random.random() < 0.5:  # 50% of refundable bookings have refund
            refund_amount = round(random.uniform(0, row['price_per_ticket'] * (num_adults + num_children + num_infants)), 2)
        if random.random() < 0.01:  # 1% chance
            refund_amount = random.choice([-100, 999999, 0.01])
        
        # Group booking cascades
        if (num_adults + num_children >= 8) and random.random() < 0.05:  # 5% chance for group bookings
            # Find other bookings by same client within 1 hour of booking_date
            related_bookings = bookings_df[
                (bookings_df['client_id'] == client_id) &
                (abs((pd.to_datetime(bookings_df['booking_date']) - booking_date).dt.total_seconds()) < 3600) &
                (bookings_df['booking_id'] != booking_id)
            ]
            for _, related in related_bookings.iterrows():
                if related['booking_status'] in ['cancelled', 'rescheduled', 'on-hold']:
                    continue
                related_cancel_id = f"CN{TARGET_YEAR}{len(cancellation_records) + 1:06d}"
                related_action_date = action_date + timedelta(minutes=random.randint(-30, 30))
                related_status = random.choice(['cancelled', 'rescheduled', 'on-hold'])
                related_cancellation_date = related_action_date if related_status == 'cancelled' else None
                related_rescheduled_date = None
                related_on_hold_date = None
                related_on_hold_end_date = None
                related_rescheduled_planning_id = None
                
                if related_status == 'rescheduled':
                    related_rescheduled_date = related_action_date
                    candidates = flights_by_route.get(route_id, [])
                    if candidates:
                        later_flights = [p for p, d in candidates if d > related_rescheduled_date]
                        if later_flights:
                            related_rescheduled_planning_id = random.choice(later_flights)
                        else:
                            closest = min(candidates, key=lambda x: abs(x[1] - related_rescheduled_date))
                            related_rescheduled_planning_id = closest[0]
                elif related_status == 'on-hold':
                    related_on_hold_date = related_action_date
                    related_on_hold_end_date = related_on_hold_date + timedelta(days=365)
                    related_rescheduled_date = related_on_hold_date + timedelta(days=random.randint(1, 365))
                    if related_rescheduled_date.year <= TARGET_YEAR:
                        candidates = flights_by_route.get(route_id, [])
                        if candidates:
                            later_flights = [p for p, d in candidates if d > related_rescheduled_date]
                            if later_flights:
                                related_rescheduled_planning_id = random.choice(later_flights)
                            else:
                                closest = min(candidates, key=lambda x: abs(x[1] - related_rescheduled_date))
                                related_rescheduled_planning_id = closest[0]
                
                related_refund_amount = None
                if related['is_cancellation_refundable'] and random.random() < 0.5:
                    related_refund_amount = round(random.uniform(0, related['price_per_ticket'] * (related['num_adults'] + related['num_children'] + related['num_infants'])), 2)
                if random.random() < 0.01:
                    related_refund_amount = random.choice([-100, 999999, 0.01])
                
                cancellation_records.append({
                    'cancel_id': related_cancel_id,
                    'ref_booking_id': related['booking_id'],
                    'is_cancelled': related_status == 'cancelled',
                    'is_rescheduled': related_status == 'rescheduled',
                    'is_onhold': related_status == 'on-hold',
                    'cancellation_date': related_cancellation_date,
                    'rescheduled_date': related_rescheduled_date,
                    'on_hold_date': related_on_hold_date,
                    'on_hold_end_date': related_on_hold_end_date,
                    'rescheduled_planning_id': related_rescheduled_planning_id,
                    'cancellation_reason': cancellation_reason,
                    'cancellation_channel': random.choice(CANCELLATION_CHANNELS),
                    'refund_amount': related_refund_amount
                })
        
        # Create cancellation record
        cancellation_record = {
            'cancel_id': cancel_id,
            'ref_booking_id': ref_booking_id,
            'is_cancelled': is_cancelled,
            'is_rescheduled': is_rescheduled,
            'is_onhold': is_onhold,
            'cancellation_date': cancellation_date,
            'rescheduled_date': rescheduled_date,
            'on_hold_date': on_hold_date,
            'on_hold_end_date': on_hold_end_date,
            'rescheduled_planning_id': rescheduled_planning_id,
            'cancellation_reason': cancellation_reason,
            'cancellation_channel': cancellation_channel,
            'refund_amount': refund_amount
        }
        cancellation_records.append(cancellation_record)
    
    # Create DataFrame
    cancellations_df = pd.DataFrame(cancellation_records)
    
    # Ensure date columns are datetime or None
    for col in ['cancellation_date', 'rescheduled_date', 'on_hold_date', 'on_hold_end_date']:
        cancellations_df[col] = pd.to_datetime(cancellations_df[col], errors='coerce')
    
    # Validate mutual exclusivity of flags (but allow some violations for realism)
    status_sum = cancellations_df[['is_cancelled', 'is_rescheduled', 'is_onhold']].sum(axis=1)
    violations = (status_sum != 1).sum()
    if violations > 0:
        print(f"Warning: Found {violations} records with non-mutually exclusive statuses (realistic error)")
    
    # Save to parquet
    os.makedirs('airplane_data', exist_ok=True)
    output_file = f'airplane_data/cancellations_{TARGET_YEAR}.parquet'
    cancellations_df.to_parquet(output_file, index=False, engine='pyarrow')
    
    # Print summary
    print(f"\nCancellations Generation Complete:")
    print("=" * 50)
    print(f"Total cancellation records: {len(cancellations_df):,}")
    print(f"Cancelled: {cancellations_df['is_cancelled'].sum():,} ({cancellations_df['is_cancelled'].mean()*100:.1f}%)")
    print(f"Rescheduled: {cancellations_df['is_rescheduled'].sum():,} ({cancellations_df['is_rescheduled'].mean()*100:.1f}%)")
    print(f"On Hold: {cancellations_df['is_onhold'].sum():,} ({cancellations_df['is_onhold'].mean()*100:.1f}%)")
    
    print("\nCancellation Reasons:")
    for reason, count in cancellations_df['cancellation_reason'].value_counts(dropna=False).items():
        print(f"  {reason if reason else 'None'}: {count:,}")
    
    print("\nCancellation Channels:")
    for channel, count in cancellations_df['cancellation_channel'].value_counts().items():
        print(f"  {channel}: {count:,}")
    
    print("\nSample cancellation data:")
    print("=" * 60)
    print(cancellations_df.head(10).to_string(index=False))
    
    return cancellations_df

if __name__ == "__main__":
    cancellations_data = generate_cancellations()
