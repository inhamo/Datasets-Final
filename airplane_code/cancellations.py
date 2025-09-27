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
CANCELLATION_CHANNELS = ["Website", "Mobile App", "Call Center", "Travel Agent"]
CHANNEL_PROBS = [0.50, 0.30, 0.15, 0.05]

def generate_cancellations():
    """Generate a cancellations table based on bookings data."""
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
        booking_date = pd.to_datetime(row['booking_date'])
        scheduled_departure = pd.to_datetime(row['scheduled_departure'])
        status = row['booking_status']
        route_id = row['route_id']
        
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
        
        if is_rescheduled:
            rescheduled_date = action_date
            # Pick a new flight on the same route, preferably after rescheduled_date
            candidates = flights_by_route.get(route_id, [])
            if candidates:
                later_flights = [p for p, d in candidates if d > rescheduled_date]
                if later_flights:
                    rescheduled_planning_id = random.choice(later_flights)
                else:
                    # Pick closest if no later
                    closest = min(candidates, key=lambda x: abs(x[1] - rescheduled_date))
                    rescheduled_planning_id = closest[0]
        
        elif is_onhold:
            on_hold_date = action_date
            on_hold_end_date = on_hold_date + timedelta(days=365)
            # Generate rescheduled_date (exercise date) between on_hold_date and on_hold_end_date
            exercise_days = random.randint(1, 365)
            rescheduled_date = on_hold_date + timedelta(days=exercise_days)
            # Pick a new flight near rescheduled_date if within 2021
            if rescheduled_date.year <= TARGET_YEAR:
                candidates = flights_by_route.get(route_id, [])
                if candidates:
                    later_flights = [p for p, d in candidates if d > rescheduled_date]
                    if later_flights:
                        rescheduled_planning_id = random.choice(later_flights)
                    else:
                        closest = min(candidates, key=lambda x: abs(x[1] - rescheduled_date))
                        rescheduled_planning_id = closest[0]
            # If future year, set to None
        
        # Generate cancellation reason (20% chance of None)
        cancellation_reason = random.choice(CANCELLATION_REASONS) if random.random() < 0.80 else None
        
        # Generate cancellation channel
        cancellation_channel = random.choices(CANCELLATION_CHANNELS, weights=CHANNEL_PROBS, k=1)[0]
        
        # Create cancellation record
        cancellation_records.append({
            'cancel_id': f"CN{TARGET_YEAR}{idx + 1:06d}",
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
            'cancellation_channel': cancellation_channel
        })
    
    # Create DataFrame
    cancellations_df = pd.DataFrame(cancellation_records)
    
    # Validate mutual exclusivity of flags
    status_sum = cancellations_df[['is_cancelled', 'is_rescheduled', 'is_onhold']].sum(axis=1)
    if not (status_sum == 1).all():
        print("CRITICAL ERROR: Non-mutually exclusive statuses found!")
        raise ValueError("Each cancellation record must have exactly one of is_cancelled, is_rescheduled, or is_onhold set to True")
    
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