import pandas as pd
import numpy as np
import uuid
import random
from typing import List, Dict, Tuple
from datetime import datetime

class HotelRoomGenerator:
    def __init__(self, num_hotels=50):
        self.num_hotels = num_hotels
        
        # Hotel chain properties with global distribution
        self.hotel_chains = {
            'Luxury Collection': {'regions': ['North America', 'Europe', 'Middle East'], 'luxury_level': 5},
            'Grand Hotels': {'regions': ['Europe', 'Asia Pacific'], 'luxury_level': 4},
            'Urban Stays': {'regions': ['North America', 'Europe'], 'luxury_level': 3},
            'Budget Inn': {'regions': ['North America', 'Europe', 'Asia Pacific'], 'luxury_level': 2},
            'Express Hotels': {'regions': ['Global'], 'luxury_level': 2},
            'Resort World': {'regions': ['Asia Pacific', 'Middle East', 'Africa'], 'luxury_level': 4},
            'Boutique Collection': {'regions': ['Europe', 'South America'], 'luxury_level': 4},
            'Business Suites': {'regions': ['North America', 'Europe', 'Asia Pacific'], 'luxury_level': 3}
        }
        
        # Room types with properties
        self.room_types = {
            'Standard': {
                'size_range': (20, 30),  # sqm
                'bed_types': ['1 Queen', '2 Singles', '1 King'],
                'amenities': ['TV', 'WiFi', 'Safe', 'AC'],
                'view_types': ['City', 'Garden', 'Street'],
                'floor_range': (1, 10),
                'distribution': 0.40
            },
            'Deluxe': {
                'size_range': (30, 45),
                'bed_types': ['1 King', '2 Queens'],
                'amenities': ['TV', 'WiFi', 'Safe', 'AC', 'Minibar', 'Coffee Machine'],
                'view_types': ['City', 'Garden', 'Partial Sea'],
                'floor_range': (3, 15),
                'distribution': 0.25
            },
            'Executive': {
                'size_range': (40, 55),
                'bed_types': ['1 King', '2 Kings'],
                'amenities': ['TV', 'WiFi', 'Safe', 'AC', 'Minibar', 'Coffee Machine', 'Work Desk', 'Lounge Access'],
                'view_types': ['City', 'Harbor', 'Executive Floor'],
                'floor_range': (8, 20),
                'distribution': 0.15
            },
            'Suite': {
                'size_range': (55, 120),
                'bed_types': ['1 King + Living', '2 Bedroom Suite'],
                'amenities': ['TV', 'WiFi', 'Safe', 'AC', 'Minibar', 'Coffee Machine', 'Work Desk', 'Kitchenette', 'Separate Living', 'Jacuzzi'],
                'view_types': ['Panoramic', 'Sea View', 'City Skyline', 'Executive'],
                'floor_range': (10, 25),
                'distribution': 0.10
            },
            'Presidential': {
                'size_range': (120, 300),
                'bed_types': ['Multiple Bedrooms'],
                'amenities': ['Everything', 'Butler Service', 'Private Pool', 'Multiple Bathrooms', 'Private Elevator'],
                'view_types': ['Best Available', 'Penthouse View'],
                'floor_range': (20, 30),
                'special_floor': ['Penthouse', 'Top Floor'],
                'distribution': 0.02
            },
            'Family': {
                'size_range': (35, 50),
                'bed_types': ['2 Queens', 'Bunk Beds + Queen'],
                'amenities': ['TV', 'WiFi', 'Safe', 'AC', 'Minibar', 'Kitchenette', 'Extra Beds Available'],
                'view_types': ['Garden', 'Pool', 'Family Floor'],
                'floor_range': (1, 8),
                'distribution': 0.08
            }
        }
        
        # Room status distribution
        self.room_status_dist = {
            'AVAILABLE': 0.60,
            'OCCUPIED': 0.25,
            'MAINTENANCE': 0.06,
            'CLEANING': 0.05,
            'RESERVED': 0.03,
            'OUT_OF_ORDER': 0.01
        }
        
        # Regional price multipliers
        self.regional_multipliers = {
            'North America': {'Standard': 1.0, 'Deluxe': 1.0, 'Suite': 1.0, 'Executive': 1.0},
            'Europe': {'Standard': 1.2, 'Deluxe': 1.3, 'Suite': 1.4, 'Executive': 1.3},
            'Asia Pacific': {'Standard': 0.9, 'Deluxe': 1.1, 'Suite': 1.2, 'Executive': 1.1},
            'Middle East': {'Standard': 1.1, 'Deluxe': 1.4, 'Suite': 1.6, 'Executive': 1.4},
            'South America': {'Standard': 0.8, 'Deluxe': 0.9, 'Suite': 1.1, 'Executive': 1.0},
            'Africa': {'Standard': 0.7, 'Deluxe': 0.9, 'Suite': 1.0, 'Executive': 0.9}
        }
        
        # Base prices by room type (USD)
        self.base_prices = {
            'Standard': {'min': 80, 'max': 180},
            'Deluxe': {'min': 150, 'max': 350},
            'Executive': {'min': 250, 'max': 500},
            'Suite': {'min': 400, 'max': 1000},
            'Presidential': {'min': 1500, 'max': 5000},
            'Family': {'min': 120, 'max': 280}
        }
        
        # Hotel cities by region
        self.hotel_cities = {
            'North America': ['New York', 'Los Angeles', 'Chicago', 'Toronto', 'Vancouver', 'Miami', 'Las Vegas'],
            'Europe': ['London', 'Paris', 'Berlin', 'Rome', 'Barcelona', 'Amsterdam', 'Vienna'],
            'Asia Pacific': ['Tokyo', 'Singapore', 'Sydney', 'Bangkok', 'Hong Kong', 'Shanghai', 'Seoul'],
            'Middle East': ['Dubai', 'Abu Dhabi', 'Doha', 'Riyadh'],
            'South America': ['São Paulo', 'Buenos Aires', 'Rio de Janeiro', 'Lima'],
            'Africa': ['Cape Town', 'Cairo', 'Marrakech', 'Nairobi']
        }
        
        # Amenities distribution
        self.amenities_list = [
            'WiFi', 'TV', 'AC', 'Heating', 'Minibar', 'Safe', 'Coffee Machine',
            'Iron', 'Hairdryer', 'Desk', 'Balcony', 'Bathtub', 'Shower',
            'Room Service', 'Wake-up Service', 'Newspaper'
        ]
    
    def _generate_hotel_id(self) -> str:
        """Generate UUID for hotel"""
        return str(uuid.uuid4())
    
    def _generate_room_id(self) -> str:
        """Generate UUID for room"""
        return str(uuid.uuid4())
    
    def _generate_hotel_info(self) -> Dict:
        """Generate hotel information"""
        # Select hotel chain
        chains = list(self.hotel_chains.keys())
        chain = random.choice(chains)
        
        # Select region based on chain's presence
        possible_regions = self.hotel_chains[chain]['regions']
        if 'Global' in possible_regions:
            region = random.choice(list(self.regional_multipliers.keys()))
        else:
            region = random.choice(possible_regions)
        
        # Select city within region
        city = random.choice(self.hotel_cities[region])
        
        # Determine hotel size based on chain and region
        if self.hotel_chains[chain]['luxury_level'] >= 4:
            # Luxury hotels are smaller
            num_floors = random.randint(5, 25)
            rooms_per_floor = random.randint(10, 30)
        elif self.hotel_chains[chain]['luxury_level'] <= 2:
            # Budget hotels can be larger
            num_floors = random.randint(3, 15)
            rooms_per_floor = random.randint(20, 50)
        else:
            # Mid-range
            num_floors = random.randint(4, 20)
            rooms_per_floor = random.randint(15, 40)
        
        # Some hotels have special features
        has_pool = random.random() < 0.7
        has_gym = random.random() < 0.6
        has_spa = self.hotel_chains[chain]['luxury_level'] >= 4 and random.random() < 0.8
        
        return {
            'hotel_id': self._generate_hotel_id(),
            'hotel_name': f"{chain} {city}",
            'chain': chain,
            'city': city,
            'region': region,
            'country': self._get_country_from_city(city),
            'star_rating': self.hotel_chains[chain]['luxury_level'],
            'num_floors': num_floors,
            'total_rooms': num_floors * rooms_per_floor,
            'has_pool': has_pool,
            'has_gym': has_gym,
            'has_spa': has_spa,
            'year_opened': random.randint(1990, 2023)
        }
    
    def _get_country_from_city(self, city: str) -> str:
        """Map city to country"""
        city_country_map = {
            'New York': 'USA', 'Los Angeles': 'USA', 'Chicago': 'USA',
            'Toronto': 'Canada', 'Vancouver': 'Canada',
            'London': 'UK', 'Paris': 'France', 'Berlin': 'Germany',
            'Rome': 'Italy', 'Barcelona': 'Spain', 'Amsterdam': 'Netherlands',
            'Tokyo': 'Japan', 'Singapore': 'Singapore', 'Sydney': 'Australia',
            'Bangkok': 'Thailand', 'Hong Kong': 'China', 'Shanghai': 'China',
            'Dubai': 'UAE', 'Abu Dhabi': 'UAE', 'Doha': 'Qatar',
            'São Paulo': 'Brazil', 'Buenos Aires': 'Argentina',
            'Cape Town': 'South Africa', 'Cairo': 'Egypt'
        }
        return city_country_map.get(city, 'Unknown')
    
    def _generate_room_number(self, floor: int, room_index: int) -> str:
        """Generate realistic room number"""
        # Different numbering styles
        style = random.choice(['simple', 'european', 'luxury'])
        
        if style == 'simple':
            # 101, 102, 201, 202
            return f"{floor}{room_index:02d}"
        elif style == 'european':
            # Sometimes skip 13th floor or room 13
            if floor == 13 and random.random() < 0.7:
                floor = 14  # Skip 13th floor
            if room_index == 13 and random.random() < 0.5:
                room_index = 14  # Skip room 13
            return f"{floor}{room_index:02d}"
        else:  # luxury
            # Sometimes use letters or special numbers
            if random.random() < 0.2:
                return f"{floor}{chr(64 + room_index)}"  # 1A, 1B, etc.
            else:
                return f"{floor}{room_index:02d}"
    
    def _generate_room_type(self) -> str:
        """Generate room type based on distribution"""
        room_types = list(self.room_types.keys())
        weights = [self.room_types[rt]['distribution'] for rt in room_types]
        return random.choices(room_types, weights=weights, k=1)[0]
    
    def _calculate_price(self, room_type: str, region: str, hotel_luxury: int) -> float:
        """Calculate realistic room price"""
        base_min = self.base_prices[room_type]['min']
        base_max = self.base_prices[room_type]['max']
        
        # Base random price
        base_price = random.uniform(base_min, base_max)
        
        # Apply regional multiplier
        if room_type in self.regional_multipliers[region]:
            regional_mult = self.regional_multipliers[region][room_type]
        else:
            # For room types not specifically listed, use average
            regional_mult = np.mean(list(self.regional_multipliers[region].values()))
        
        # Luxury level multiplier (1-5 stars)
        luxury_mult = 0.8 + (hotel_luxury * 0.15)
        
        # Weekend multiplier (some rooms have weekend pricing)
        weekend_mult = 1.0
        if random.random() < 0.3:  # 30% of rooms have weekend rates
            weekend_mult = random.uniform(1.1, 1.5)
        
        # Seasonal adjustment baked into base
        final_price = base_price * regional_mult * luxury_mult
        
        # Add VAT/tax component (8-20% depending on region)
        tax_rates = {
            'North America': 0.10,
            'Europe': 0.20,
            'Asia Pacific': 0.08,
            'Middle East': 0.05,
            'South America': 0.12,
            'Africa': 0.15
        }
        tax_mult = 1 + tax_rates.get(region, 0.10)
        
        final_price *= tax_mult
        
        # Round to nearest 5 or 9 (psychological pricing)
        rounding_strategy = random.choice(['5', '9', '0'])
        if rounding_strategy == '5':
            final_price = round(final_price / 5) * 5
        elif rounding_strategy == '9':
            # e.g., 199, 299
            final_price = round((final_price - 1) / 100) * 100 + 99
        else:
            final_price = round(final_price, 2)
        
        return final_price
    
    def _generate_room_status(self, hotel_info: Dict) -> str:
        """Generate room status with realistic distribution"""
        # Adjust status based on hotel type
        adjusted_dist = self.room_status_dist.copy()
        
        if hotel_info['star_rating'] >= 4:
            # Luxury hotels have more available rooms (better management)
            adjusted_dist['AVAILABLE'] *= 1.1
            adjusted_dist['MAINTENANCE'] *= 0.8  # Better maintenance
        elif hotel_info['star_rating'] <= 2:
            # Budget hotels might have more maintenance issues
            adjusted_dist['MAINTENANCE'] *= 1.3
            adjusted_dist['CLEANING'] *= 1.2
        
        # Time of day effect (simulated)
        current_hour = datetime.now().hour
        if 8 <= current_hour <= 11:
            # Morning - more cleaning
            adjusted_dist['CLEANING'] *= 1.5
        elif 14 <= current_hour <= 18:
            # Afternoon - more occupied (check-in time)
            adjusted_dist['OCCUPIED'] *= 1.2
        
        # Normalize
        total = sum(adjusted_dist.values())
        normalized = {k: v/total for k, v in adjusted_dist.items()}
        
        statuses = list(normalized.keys())
        weights = list(normalized.values())
        
        return random.choices(statuses, weights=weights, k=1)[0]
    
    def _generate_room_features(self, room_type: str, floor: int) -> Dict:
        """Generate room features and amenities"""
        room_specs = self.room_types[room_type]
        
        # Room size
        size_min, size_max = room_specs['size_range']
        size = random.uniform(size_min, size_max)
        
        # Bed type
        bed_type = random.choice(room_specs['bed_types'])
        
        # Max occupancy
        if '1 King' in bed_type or '1 Queen' in bed_type:
            max_occupancy = 2
        elif '2 Singles' in bed_type or '2 Queens' in bed_type:
            max_occupancy = random.choice([2, 3, 4])
        elif 'Suite' in room_type or 'Presidential' in room_type:
            max_occupancy = random.choice([2, 3, 4, 6])
        elif 'Family' in room_type:
            max_occupancy = random.choice([3, 4, 5])
        else:
            max_occupancy = 2
        
        # View type
        view_type = random.choice(room_specs['view_types'])
        
        # Accessibility features
        is_accessible = random.random() < 0.08  # 8% of rooms are accessible
        if is_accessible:
            accessibility_features = ['Wheelchair Access', 'Grab Bars', 'Roll-in Shower']
        else:
            accessibility_features = []
        
        # Smoking preference
        is_smoking = random.random() < 0.15  # 15% smoking rooms (where allowed)
        
        # Select amenities
        base_amenities = room_specs['amenities']
        # Add random extra amenities
        extra_amenities = []
        if random.random() < 0.3:
            available_extras = [a for a in self.amenities_list if a not in base_amenities]
            if available_extras:
                num_extras = random.randint(1, 3)
                extra_amenities = random.sample(available_extras, min(num_extras, len(available_extras)))
        
        all_amenities = base_amenities + extra_amenities
        
        # Room condition (1-5, 5 being best)
        condition = random.randint(3, 5)
        if random.random() < 0.1:  # 10% chance of poorer condition
            condition = random.randint(1, 3)
        
        # Last renovation year
        current_year = datetime.now().year
        last_renovated = random.randint(max(1990, current_year - 20), current_year)
        
        return {
            'size_sqm': round(size, 1),
            'bed_type': bed_type,
            'max_occupancy': max_occupancy,
            'view_type': view_type,
            'is_accessible': is_accessible,
            'accessibility_features': ', '.join(accessibility_features) if accessibility_features else '',
            'is_smoking': is_smoking,
            'amenities': ', '.join(all_amenities),
            'condition_rating': condition,
            'last_renovated': last_renovated,
            'has_balcony': random.random() < 0.4,
            'has_bathtub': random.random() < 0.7 if room_type in ['Deluxe', 'Suite', 'Executive'] else random.random() < 0.3
        }
    
    def generate_hotels_and_rooms(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Generate hotels and their rooms"""
        hotels_data = []
        rooms_data = []
        
        print(f"Generating {self.num_hotels} hotels with rooms...")
        
        for hotel_idx in range(self.num_hotels):
            # Generate hotel
            hotel_info = self._generate_hotel_info()
            hotels_data.append(hotel_info)
            
            # Generate rooms for this hotel
            num_floors = hotel_info['num_floors']
            rooms_per_floor = hotel_info['total_rooms'] // num_floors
            
            print(f"  Hotel {hotel_idx+1}/{self.num_hotels}: {hotel_info['hotel_name']} - {hotel_info['total_rooms']} rooms")
            
            for floor in range(1, num_floors + 1):
                # Determine room types per floor (higher floors have better rooms)
                floor_multiplier = floor / num_floors  # 0 to 1
                
                for room_idx in range(1, rooms_per_floor + 1):
                    # Adjust room type probability based on floor
                    room_type = self._generate_room_type()
                    
                    # Higher floors have more premium rooms
                    if floor_multiplier > 0.7 and room_type in ['Standard', 'Family']:
                        # On high floors, upgrade some standard rooms
                        if random.random() < 0.6:
                            room_type = random.choice(['Deluxe', 'Executive'])
                    
                    # Generate room number
                    room_number = self._generate_room_number(floor, room_idx)
                    
                    # Generate room features
                    room_features = self._generate_room_features(room_type, floor)
                    
                    # Calculate price
                    price = self._calculate_price(room_type, hotel_info['region'], hotel_info['star_rating'])
                    
                    # Generate status
                    status = self._generate_room_status(hotel_info)
                    
                    # Create room record
                    room = {
                        'room_id': self._generate_room_id(),
                        'hotel_id': hotel_info['hotel_id'],
                        'room_number': room_number,
                        'room_type': room_type,
                        'floor': floor,
                        'size_sqm': room_features['size_sqm'],
                        'bed_type': room_features['bed_type'],
                        'max_occupancy': room_features['max_occupancy'],
                        'price_per_night': round(price, 2),
                        'status': status,
                        'view_type': room_features['view_type'],
                        'is_accessible': room_features['is_accessible'],
                        'is_smoking': room_features['is_smoking'],
                        'amenities': room_features['amenities'],
                        'condition_rating': room_features['condition_rating'],
                        'last_renovated': room_features['last_renovated'],
                        'has_balcony': room_features['has_balcony'],
                        'has_bathtub': room_features['has_bathtub'],
                        'accessibility_features': room_features['accessibility_features']
                    }
                    
                    rooms_data.append(room)
            
            # Add some variation in rooms per floor
            if hotel_idx % 10 == 0 and len(rooms_data) > 0:
                # Occasionally add a special room
                special_room = rooms_data[-1].copy()
                special_room['room_id'] = self._generate_room_id()
                special_room['room_number'] = 'PH'  # Penthouse
                special_room['room_type'] = 'Presidential'
                special_room['price_per_night'] = random.uniform(2000, 5000)
                rooms_data.append(special_room)
        
        hotels_df = pd.DataFrame(hotels_data)
        rooms_df = pd.DataFrame(rooms_data)
        
        return hotels_df, rooms_df
    
    def analyze_room_distribution(self, rooms_df: pd.DataFrame, hotels_df: pd.DataFrame):
        """Analyze room distribution"""
        print("\n=== Room Distribution Analysis ===")
        print(f"Total rooms generated: {len(rooms_df)}")
        print(f"Total hotels: {len(hotels_df)}")
        
        print("\nRoom Type Distribution:")
        room_type_dist = rooms_df['room_type'].value_counts(normalize=True).round(3)
        print(room_type_dist)
        
        print("\nStatus Distribution:")
        status_dist = rooms_df['status'].value_counts(normalize=True).round(3)
        print(status_dist)
        
        print("\nAverage Prices by Room Type:")
        avg_prices = rooms_df.groupby('room_type')['price_per_night'].agg(['mean', 'min', 'max']).round(2)
        print(avg_prices)
        
        print("\nRoom Features Summary:")
        print(f"Accessible rooms: {rooms_df['is_accessible'].mean():.2%}")
        print(f"Smoking rooms: {rooms_df['is_smoking'].mean():.2%}")
        print(f"Rooms with balcony: {rooms_df['has_balcony'].mean():.2%}")
        
        print("\nCondition Rating Distribution:")
        condition_dist = rooms_df['condition_rating'].value_counts().sort_index()
        print(condition_dist)
        
        # Merge with hotels for regional analysis
        merged_df = rooms_df.merge(hotels_df[['hotel_id', 'region', 'star_rating']], on='hotel_id')
        
        print("\nAverage Price by Region:")
        region_prices = merged_df.groupby('region')['price_per_night'].mean().round(2)
        print(region_prices)

# Usage example
if __name__ == "__main__":
    # Initialize generator
    generator = HotelRoomGenerator(num_hotels=20)  # Generate 20 hotels
    
    # Generate hotels and rooms
    hotels_df, rooms_df = generator.generate_hotels_and_rooms()
    
    # Save to CSV
    hotels_df.to_csv('hotel data/hotel_chain_hotels.csv', index=False)
    rooms_df.to_csv('hotel data/hotel_chain_rooms.csv', index=False)
    
    # Analyze distribution
    generator.analyze_room_distribution(rooms_df, hotels_df)
    
    # Display samples
    print("\n=== Sample Hotels (First 5) ===")
    pd.set_option('display.max_columns', None)
    print(hotels_df.head(5))
    
    print("\n=== Sample Rooms (First 10) ===")
    print(rooms_df[['room_id', 'hotel_id', 'room_number', 'room_type', 'floor', 
                    'max_occupancy', 'price_per_night', 'status']].head(10))
    
    # Generate summary statistics
    print("\n=== Summary Statistics ===")
    print(f"Total room inventory value: ${rooms_df['price_per_night'].sum():,.2f}")
    print(f"Average rooms per hotel: {len(rooms_df)/len(hotels_df):.1f}")
    
    # Room availability analysis
    available_rooms = rooms_df[rooms_df['status'] == 'AVAILABLE']
    print(f"\nAvailable rooms: {len(available_rooms)} ({len(available_rooms)/len(rooms_df):.1%})")
    
    # Price range by hotel star rating
    merged = rooms_df.merge(hotels_df[['hotel_id', 'star_rating']], on='hotel_id')
    print("\nAverage Price by Star Rating:")
    star_prices = merged.groupby('star_rating')['price_per_night'].agg(['mean', 'min', 'max']).round(2)
    print(star_prices)