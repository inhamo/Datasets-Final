import pandas as pd
import numpy as np
import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

class HotelReviewGenerator:
    def __init__(self, bookings_df: pd.DataFrame, customers_df: pd.DataFrame = None):
        """
        Initialize with existing booking and customer data
        """
        self.bookings_df = bookings_df.copy()
        self.customers_df = customers_df.copy() if customers_df is not None else None
        
        # Review rates by booking status
        self.review_rates_by_status = {
            'CHECKED_OUT': 0.45,    # 45% of checked-out guests leave reviews
            'CHECKED_IN': 0.10,     # 10% of current guests leave reviews
            'CONFIRMED': 0.05,      # 5% of future bookings (pre-arrival reviews)
            'CANCELLED': 0.15,      # 15% of cancelled bookings leave reviews
            'NO_SHOW': 0.08,        # 8% of no-shows leave reviews
            'PENDING': 0.02         # 2% of pending bookings leave reviews
        }
        
        # Rating distributions by booking status
        self.rating_dist_by_status = {
            'CHECKED_OUT': {'mean': 4.2, 'std': 0.8, 'min': 1, 'max': 5},
            'CHECKED_IN': {'mean': 4.0, 'std': 0.9, 'min': 1, 'max': 5},
            'CONFIRMED': {'mean': 4.5, 'std': 0.6, 'min': 3, 'max': 5},  # Pre-arrival are optimistic
            'CANCELLED': {'mean': 2.0, 'std': 1.2, 'min': 1, 'max': 5},  # Cancelled guests are unhappy
            'NO_SHOW': {'mean': 1.8, 'std': 1.0, 'min': 1, 'max': 5},    # No-shows often leave bad reviews
            'PENDING': {'mean': 4.0, 'std': 0.7, 'min': 3, 'max': 5}
        }
        
        # Rating correlation with booking value
        self.rating_by_value = {
            'high': {'mean': 4.5, 'std': 0.6},   # High value bookings -> better reviews
            'medium': {'mean': 4.1, 'std': 0.8},
            'low': {'mean': 3.8, 'std': 1.0}    # Low value bookings -> worse reviews
        }
        
        # Review category weights
        self.category_weights = {
            'cleanliness': 0.25,
            'staff_service': 0.25,
            'comfort': 0.20,
            'location': 0.15,
            'value': 0.15
        }
        
        # Rating correlations between categories
        self.category_correlations = {
            'cleanliness': {'staff_service': 0.6, 'comfort': 0.7, 'location': 0.3, 'value': 0.4},
            'staff_service': {'cleanliness': 0.6, 'comfort': 0.5, 'location': 0.2, 'value': 0.3},
            'comfort': {'cleanliness': 0.7, 'staff_service': 0.5, 'location': 0.4, 'value': 0.6},
            'location': {'cleanliness': 0.3, 'staff_service': 0.2, 'comfort': 0.4, 'value': 0.8},
            'value': {'cleanliness': 0.4, 'staff_service': 0.3, 'comfort': 0.6, 'location': 0.8}
        }
        
        # Review sentiment by rating
        self.sentiment_by_rating = {
            5: {'positive': 0.95, 'neutral': 0.04, 'negative': 0.01},
            4: {'positive': 0.85, 'neutral': 0.12, 'negative': 0.03},
            3: {'positive': 0.30, 'neutral': 0.50, 'negative': 0.20},
            2: {'positive': 0.10, 'neutral': 0.30, 'negative': 0.60},
            1: {'positive': 0.02, 'neutral': 0.08, 'negative': 0.90}
        }
        
        # Review templates by sentiment and category
        self.review_templates = {
            'positive': {
                'cleanliness': [
                    "The room was spotless and very well maintained.",
                    "Impeccably clean with attention to every detail.",
                    "The cleanliness was outstanding throughout our stay.",
                    "Housekeeping did an excellent job keeping everything tidy.",
                    "One of the cleanest hotels I've ever stayed at."
                ],
                'staff_service': [
                    "The staff went above and beyond to make our stay perfect.",
                    "Exceptionally friendly and helpful staff at every turn.",
                    "Outstanding service from check-in to check-out.",
                    "The concierge was incredibly knowledgeable and helpful.",
                    "Staff made us feel like VIPs throughout our stay."
                ],
                'comfort': [
                    "The bed was incredibly comfortable and we slept great.",
                    "Very comfortable room with perfect temperature control.",
                    "Loved the comfortable bedding and peaceful atmosphere.",
                    "The room was cozy and had everything we needed.",
                    "Perfectly comfortable for both work and relaxation."
                ],
                'location': [
                    "Perfect location with easy access to everything.",
                    "Great neighborhood with plenty of dining options nearby.",
                    "Conveniently located close to attractions and transportation.",
                    "Loved the area - felt safe and had great views.",
                    "Perfect base for exploring the city."
                ],
                'value': [
                    "Excellent value for the quality provided.",
                    "Worth every penny for the experience we had.",
                    "Great value compared to other hotels in the area.",
                    "The amenities and service made it a great value.",
                    "Would definitely return for the price and quality."
                ]
            },
            'neutral': {
                'cleanliness': [
                    "The room was clean for the most part.",
                    "Generally clean with a few minor issues.",
                    "Cleanliness was adequate for our needs.",
                    "Room was clean but could use some updates.",
                    "Acceptable cleanliness for a short stay."
                ],
                'staff_service': [
                    "Staff were polite and did their job.",
                    "Service was standard for a hotel of this type.",
                    "Staff were helpful when we asked for assistance.",
                    "Service was adequate but nothing exceptional.",
                    "Staff were present but not particularly engaging."
                ],
                'comfort': [
                    "The room was comfortable enough for our stay.",
                    "Bed was okay, room temperature was fine.",
                    "Adequately comfortable for the price point.",
                    "Room served its purpose for sleeping.",
                    "Comfort was reasonable for what we needed."
                ],
                'location': [
                    "Location was convenient but nothing special.",
                    "The area was okay, nothing remarkable.",
                    "Decent location for our business needs.",
                    "Location worked for our purposes.",
                    "Area was fine, not particularly exciting."
                ],
                'value': [
                    "Reasonable value for what we paid.",
                    "Price was fair for what we received.",
                    "Not a bargain but not overpriced either.",
                    "Value was average for this type of hotel.",
                    "Decent value but could be better."
                ]
            },
            'negative': {
                'cleanliness': [
                    "The room wasn't properly cleaned before our arrival.",
                    "Found stains and dust in several areas.",
                    "Bathroom cleanliness was below standards.",
                    "Housekeeping missed several spots during cleaning.",
                    "Overall cleanliness was disappointing."
                ],
                'staff_service': [
                    "Staff seemed indifferent and unhelpful.",
                    "Service was slow and unprofessional.",
                    "Front desk was rude when we had questions.",
                    "Staff seemed poorly trained and overwhelmed.",
                    "Terrible service experience from start to finish."
                ],
                'comfort': [
                    "The bed was uncomfortable and we had trouble sleeping.",
                    "Room temperature was impossible to regulate.",
                    "Noise from hallway was constant and disruptive.",
                    "Room felt cramped and poorly designed.",
                    "Very uncomfortable stay overall."
                ],
                'location': [
                    "Location was inconvenient and noisy.",
                    "Area felt unsafe especially at night.",
                    "Too far from everything we wanted to do.",
                    "Constant traffic noise made it hard to sleep.",
                    "Terrible location choice for our needs."
                ],
                'value': [
                    "Definitely not worth the price we paid.",
                    "Overpriced for the quality provided.",
                    "Would not recommend based on value.",
                    "Felt like we were overcharged for what we got.",
                    "Poor value compared to alternatives."
                ]
            }
        }
        
        # Common issues that affect ratings
        self.common_issues = {
            'noise': ['street noise', 'neighbor noise', 'elevator noise', 'hallway noise', 'construction'],
            'cleanliness': ['stains', 'dust', 'mold', 'hair', 'odor'],
            'service': ['slow check-in', 'rude staff', 'unhelpful', 'mistakes', 'delays'],
            'amenities': ['broken equipment', 'missing items', 'poor wifi', 'no hot water', 'power outage'],
            'room': ['small room', 'uncomfortable bed', 'poor lighting', 'old furniture', 'broken AC']
        }
        
        # Time to review after checkout (days)
        self.review_timing = {
            'immediate': {'prob': 0.25, 'days_range': (0, 2)},
            'prompt': {'prob': 0.40, 'days_range': (3, 7)},
            'delayed': {'prob': 0.25, 'days_range': (8, 30)},
            'very_late': {'prob': 0.10, 'days_range': (31, 90)}
        }
        
        # Platform distribution
        self.review_platforms = {
            'Hotel Website': 0.35,
            'Booking.com': 0.25,
            'TripAdvisor': 0.15,
            'Google Reviews': 0.10,
            'Expedia': 0.08,
            'Agoda': 0.04,
            'Other': 0.03
        }
        
        # Review helpfulness distribution
        self.helpfulness_dist = {
            'very_helpful': 0.15,
            'helpful': 0.35,
            'somewhat_helpful': 0.30,
            'not_helpful': 0.20
        }
        
        # Preprocess data
        self._preprocess_data()
    
    def _preprocess_data(self):
        """Preprocess booking data for faster access"""
        # Convert dates
        self.bookings_df['booking_date'] = pd.to_datetime(self.bookings_df['booking_date'])
        self.bookings_df['check_in_date'] = pd.to_datetime(self.bookings_df['check_in_date'])
        self.bookings_df['check_out_date'] = pd.to_datetime(self.bookings_df['check_out_date'])
        
        # Calculate booking value category
        booking_value_quantiles = self.bookings_df['total_amount'].quantile([0.33, 0.67])
        self.bookings_df['value_category'] = pd.cut(
            self.bookings_df['total_amount'],
            bins=[-np.inf, booking_value_quantiles[0.33], booking_value_quantiles[0.67], np.inf],
            labels=['low', 'medium', 'high']
        )
        
        # Calculate if booking had issues (for correlation with reviews)
        # High value bookings are less likely to have issues
        self.bookings_df['likely_issues'] = np.where(
            self.bookings_df['value_category'] == 'high',
            np.random.random(len(self.bookings_df)) < 0.1,
            np.where(
                self.bookings_df['value_category'] == 'medium',
                np.random.random(len(self.bookings_df)) < 0.25,
                np.random.random(len(self.bookings_df)) < 0.4
            )
        )
        
        # Group bookings by status for faster lookup
        self.bookings_by_status = self.bookings_df.groupby('status')
        
        print(f"Preprocessed {len(self.bookings_df):,} bookings")
    
    def _generate_review_id(self) -> str:
        """Generate UUID for review"""
        return str(uuid.uuid4())
    
    def _should_generate_review(self, booking_status: str, booking_value: str, 
                               likely_issues: bool) -> bool:
        """Determine if a booking should have a review"""
        
        # Base probability from status
        base_prob = self.review_rates_by_status.get(booking_status, 0.3)
        
        # Adjust by booking value (higher value = more likely to review)
        if booking_value == 'high':
            base_prob *= 1.3
        elif booking_value == 'low':
            base_prob *= 0.7
        
        # Adjust by likely issues (unhappy guests more likely to review)
        if likely_issues:
            base_prob *= 1.5
        
        # Cap probability
        base_prob = min(base_prob, 0.9)
        
        return np.random.random() < base_prob
    
    def _generate_overall_rating(self, booking_status: str, booking_value: str, 
                               likely_issues: bool, customer_loyalty: str = None) -> float:
        """Generate overall rating based on multiple factors"""
        
        # Base distribution from booking status
        status_dist = self.rating_dist_by_status.get(booking_status, self.rating_dist_by_status['CHECKED_OUT'])
        
        # Adjust mean based on booking value
        value_adjustment = self.rating_by_value.get(booking_value, {'mean': 4.0, 'std': 0.8})
        adjusted_mean = (status_dist['mean'] + value_adjustment['mean']) / 2
        
        # Adjust for likely issues
        if likely_issues:
            adjusted_mean -= 1.5  # Issues significantly lower ratings
        
        # Adjust for loyalty (loyal customers rate higher)
        if customer_loyalty:
            loyalty_multiplier = {
                'Non-Member': 1.0,
                'Basic': 1.05,
                'Silver': 1.10,
                'Gold': 1.15,
                'Platinum': 1.20
            }.get(customer_loyalty, 1.0)
            adjusted_mean *= loyalty_multiplier
        
        # Generate rating with normal distribution
        rating = np.random.normal(adjusted_mean, status_dist['std'])
        
        # Clip to valid range and round to nearest 0.5
        rating = np.clip(rating, status_dist['min'], status_dist['max'])
        rating = round(rating * 2) / 2  # Round to nearest 0.5
        
        return rating
    
    def _generate_category_ratings(self, overall_rating: float, 
                                 booking_status: str, likely_issues: bool) -> Dict:
        """Generate individual category ratings"""
        
        # Base ratings correlated with overall rating
        categories = list(self.category_weights.keys())
        base_ratings = {}
        
        for category in categories:
            # Start with overall rating as base
            base = overall_rating
            
            # Add some variation
            variation = np.random.normal(0, 0.3)
            base += variation
            
            # Adjust based on correlations with other categories
            for other_cat, correlation in self.category_correlations[category].items():
                if other_cat in base_ratings:
                    # If other category is rated, influence this one
                    influence = (base_ratings[other_cat] - overall_rating) * correlation * 0.5
                    base += influence
            
            # Adjust for likely issues
            if likely_issues:
                # Randomly select which categories are affected
                if np.random.random() < 0.6:
                    issue_impact = np.random.uniform(-1.5, -0.5)
                    base += issue_impact
            
            # Clip and round
            rating = np.clip(base, 1, 5)
            rating = round(rating)  # Category ratings are typically integers
            
            base_ratings[category] = rating
        
        # Ensure at least one category matches overall rating sentiment
        rating_int = int(round(overall_rating))
        matching_cat = np.random.choice(categories)
        base_ratings[matching_cat] = rating_int
        
        return base_ratings
    
    def _determine_sentiment(self, rating: float) -> str:
        """Determine sentiment from rating"""
        rating_int = int(round(rating))
        if rating_int in self.sentiment_by_rating:
            sentiments = list(self.sentiment_by_rating[rating_int].keys())
            probs = list(self.sentiment_by_rating[rating_int].values())
            return np.random.choice(sentiments, p=probs)
        return 'neutral'
    
    def _generate_review_text(self, overall_rating: float, category_ratings: Dict, 
                            booking_status: str, likely_issues: bool) -> str:
        """Generate realistic review text"""
        
        sentiment = self._determine_sentiment(overall_rating)
        
        # Select 2-3 categories to mention in review
        n_categories = np.random.choice([2, 3], p=[0.6, 0.4])
        selected_categories = np.random.choice(
            list(category_ratings.keys()), 
            size=n_categories, 
            replace=False,
            p=list(self.category_weights.values())
        )
        
        # Generate sentences for each selected category
        sentences = []
        for category in selected_categories:
            cat_rating = category_ratings[category]
            cat_sentiment = self._determine_sentiment(cat_rating)
            
            if cat_sentiment in self.review_templates and category in self.review_templates[cat_sentiment]:
                template = np.random.choice(self.review_templates[cat_sentiment][category])
                sentences.append(template)
        
        # Add specific issue mention if likely_issues and negative sentiment
        if likely_issues and sentiment in ['negative', 'neutral']:
            if np.random.random() < 0.7:
                issue_type = np.random.choice(list(self.common_issues.keys()))
                specific_issue = np.random.choice(self.common_issues[issue_type])
                issue_sentence = f"Unfortunately, we experienced {specific_issue} during our stay."
                sentences.append(issue_sentence)
        
        # Add positive highlight if high rating
        if overall_rating >= 4.5 and len(sentences) < 4:
            highlight = np.random.choice([
                "We will definitely be returning!",
                "Highly recommended to anyone visiting the area.",
                "One of our best hotel experiences ever.",
                "Already planning our next visit.",
                "Exceeded all our expectations."
            ])
            sentences.append(highlight)
        
        # Add recommendation based on rating
        if overall_rating >= 4:
            rec_sentence = "Would recommend to friends and family."
        elif overall_rating >= 3:
            rec_sentence = "Might consider returning if improvements are made."
        else:
            rec_sentence = "Would not recommend based on our experience."
        
        if np.random.random() < 0.8:  # 80% chance to include recommendation
            sentences.append(rec_sentence)
        
        # Combine sentences
        if sentences:
            review_text = " ".join(sentences)
        else:
            # Fallback generic review
            if sentiment == 'positive':
                review_text = "Great stay, would recommend."
            elif sentiment == 'negative':
                review_text = "Disappointing experience overall."
            else:
                review_text = "Average stay, met basic expectations."
        
        # Add quotes and proper punctuation
        if not review_text.endswith(('.', '!', '?')):
            review_text += '.'
        
        # Limit length
        if len(review_text) > 500:
            review_text = review_text[:497] + "..."
        
        return review_text
    
    def _generate_review_date(self, checkout_date: datetime, booking_status: str) -> datetime:
        """Generate realistic review date"""
        
        if booking_status in ['CONFIRMED', 'PENDING']:
            # Pre-arrival reviews happen before check-in
            days_before = np.random.randint(1, 30)
            return checkout_date - timedelta(days=days_before)
        elif booking_status == 'CHECKED_IN':
            # Current guests review during stay
            days_during = np.random.randint(0, 7)
            return checkout_date + timedelta(days=days_during)
        else:
            # Post-stay reviews
            timing_type = np.random.choice(
                list(self.review_timing.keys()),
                p=[self.review_timing[t]['prob'] for t in self.review_timing]
            )
            days_range = self.review_timing[timing_type]['days_range']
            days_after = np.random.randint(days_range[0], days_range[1] + 1)
            return checkout_date + timedelta(days=days_after)
    
    def _generate_platform(self, booking_source: str = None) -> str:
        """Generate review platform"""
        platforms = list(self.review_platforms.keys())
        probs = list(self.review_platforms.values())
        
        # Adjust based on booking source if available
        if booking_source:
            if 'Website' in booking_source or 'Direct' in booking_source:
                # More likely to review on hotel website
                adjusted_probs = [p * 1.5 if platform == 'Hotel Website' else p * 0.8 
                                 for p, platform in zip(probs, platforms)]
                total = sum(adjusted_probs)
                adjusted_probs = [p/total for p in adjusted_probs]
                return np.random.choice(platforms, p=adjusted_probs)
            elif 'OTA' in booking_source or 'Booking.com' in booking_source:
                # More likely to review on OTA platform
                adjusted_probs = []
                for p, platform in zip(probs, platforms):
                    if platform in ['Booking.com', 'Expedia', 'Agoda']:
                        adjusted_probs.append(p * 1.5)
                    else:
                        adjusted_probs.append(p * 0.7)
                total = sum(adjusted_probs)
                adjusted_probs = [p/total for p in adjusted_probs]
                return np.random.choice(platforms, p=adjusted_probs)
        
        return np.random.choice(platforms, p=probs)
    
    def _calculate_helpfulness(self, review_text: str, overall_rating: float) -> Dict:
        """Calculate review helpfulness metrics"""
        
        # Base helpfulness distribution
        helpfulness_level = np.random.choice(
            list(self.helpfulness_dist.keys()),
            p=list(self.helpfulness_dist.values())
        )
        
        # Convert to score
        helpfulness_scores = {
            'very_helpful': 0.9,
            'helpful': 0.7,
            'somewhat_helpful': 0.4,
            'not_helpful': 0.1
        }
        
        base_score = helpfulness_scores[helpfulness_level]
        
        # Adjust based on review characteristics
        # Longer reviews are often more helpful
        text_length = len(review_text)
        if text_length > 200:
            base_score *= 1.2
        elif text_length < 50:
            base_score *= 0.7
        
        # Extreme ratings (1 or 5) get more attention
        if overall_rating == 1 or overall_rating == 5:
            base_score *= 1.3
        
        # Clip score
        helpfulness_score = np.clip(base_score, 0, 1)
        
        # Calculate helpful votes
        max_votes = np.random.randint(0, 50)
        helpful_votes = int(max_votes * helpfulness_score)
        unhelpful_votes = max_votes - helpful_votes
        
        return {
            'helpfulness_score': helpfulness_score,
            'helpful_votes': helpful_votes,
            'unhelpful_votes': unhelpful_votes,
            'total_votes': max_votes,
            'helpfulness_level': helpfulness_level
        }
    
    def generate_reviews(self, target_review_rate: float = 0.35):
        """
        Generate review records for bookings
        
        Args:
            target_review_rate: Target percentage of bookings that have reviews
        """
        print(f"Generating reviews for {len(self.bookings_df):,} bookings...")
        print(f"Target review rate: {target_review_rate:.1%}")
        
        reviews_data = []
        
        # Get customer loyalty data if available
        customer_loyalty = {}
        if self.customers_df is not None and 'customer_id' in self.customers_df.columns:
            if 'loyalty_tier' in self.customers_df.columns:
                customer_loyalty = self.customers_df.set_index('customer_id')['loyalty_tier'].to_dict()
        
        # Process bookings
        for idx, booking in tqdm(self.bookings_df.iterrows(), total=len(self.bookings_df), desc="Processing bookings"):
            booking_id = booking['booking_id']
            customer_id = booking['customer_id']
            booking_status = booking['status']
            booking_value = booking.get('value_category', 'medium')
            likely_issues = booking.get('likely_issues', False)
            checkout_date = booking['check_out_date']
            booking_source = booking.get('booking_source', 'Website Direct')
            
            # Get customer loyalty tier
            loyalty_tier = customer_loyalty.get(customer_id, 'Non-Member')
            
            # Determine if this booking gets a review
            if not self._should_generate_review(booking_status, booking_value, likely_issues):
                continue
            
            # Generate overall rating
            overall_rating = self._generate_overall_rating(
                booking_status, booking_value, likely_issues, loyalty_tier
            )
            
            # Generate category ratings
            category_ratings = self._generate_category_ratings(
                overall_rating, booking_status, likely_issues
            )
            
            # Generate review text
            review_text = self._generate_review_text(
                overall_rating, category_ratings, booking_status, likely_issues
            )
            
            # Generate review date
            review_date = self._generate_review_date(checkout_date, booking_status)
            
            # Generate platform
            platform = self._generate_platform(booking_source)
            
            # Calculate helpfulness metrics
            helpfulness = self._calculate_helpfulness(review_text, overall_rating)
            
            # Determine if response from hotel (higher ratings less likely to get responses)
            response_prob = 0.3 if overall_rating < 3 else 0.1
            has_response = np.random.random() < response_prob
            
            # Create review record
            review = {
                'review_id': self._generate_review_id(),
                'booking_id': booking_id,
                'customer_id': customer_id,
                'hotel_id': booking['hotel_id'],
                'room_id': booking.get('room_id', ''),
                'overall_rating': overall_rating,
                'cleanliness_rating': category_ratings.get('cleanliness', overall_rating),
                'staff_service_rating': category_ratings.get('staff_service', overall_rating),
                'comfort_rating': category_ratings.get('comfort', overall_rating),
                'location_rating': category_ratings.get('location', overall_rating),
                'value_rating': category_ratings.get('value', overall_rating),
                'review_text': review_text,
                'review_date': review_date.strftime('%Y-%m-%d'),
                'review_timestamp': review_date.strftime('%Y-%m-%d %H:%M:%S'),
                'platform': platform,
                'booking_status_at_review': booking_status,
                'helpfulness_score': helpfulness['helpfulness_score'],
                'helpful_votes': helpfulness['helpful_votes'],
                'unhelpful_votes': helpfulness['unhelpful_votes'],
                'total_votes': helpfulness['total_votes'],
                'helpfulness_level': helpfulness['helpfulness_level'],
                'has_response': has_response,
                'response_date': '',
                'response_text': '',
                'response_by': '',
                'sentiment': self._determine_sentiment(overall_rating),
                'review_length': len(review_text),
                'verification_status': np.random.choice(['Verified', 'Unverified'], p=[0.7, 0.3]),
                'trip_type': np.random.choice(['Business', 'Leisure', 'Family', 'Couple'], 
                                             p=[0.3, 0.4, 0.2, 0.1]),
                'created_at': review_date.strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': (review_date + timedelta(minutes=np.random.randint(1, 60))).strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Add hotel response if applicable
            if has_response:
                days_to_response = np.random.randint(1, 7)
                response_date = review_date + timedelta(days=days_to_response)
                
                # Generate response text based on rating
                if overall_rating >= 4:
                    response_text = np.random.choice([
                        "Thank you for your wonderful feedback! We're delighted you enjoyed your stay.",
                        "We appreciate your kind words and look forward to welcoming you back soon.",
                        "Thank you for choosing us and for sharing your positive experience."
                    ])
                elif overall_rating >= 3:
                    response_text = np.random.choice([
                        "Thank you for your feedback. We're glad you enjoyed aspects of your stay.",
                        "We appreciate your comments and will use them to improve our services.",
                        "Thank you for taking the time to share your experience with us."
                    ])
                else:
                    response_text = np.random.choice([
                        "We sincerely apologize for not meeting your expectations. Please contact our manager to discuss your concerns.",
                        "Thank you for bringing these issues to our attention. We take all feedback seriously and will address them.",
                        "We're sorry to hear about your experience. We'd like to make things right - please contact our guest relations team."
                    ])
                
                review.update({
                    'response_date': response_date.strftime('%Y-%m-%d'),
                    'response_text': response_text,
                    'response_by': np.random.choice(['Hotel Manager', 'Guest Relations', 'General Manager'])
                })
            
            reviews_data.append(review)
        
        reviews_df = pd.DataFrame(reviews_data)
        
        # Add some reviews without booking_id (direct reviews from website)
        if len(reviews_df) > 0:
            n_direct_reviews = int(len(reviews_df) * 0.1)  # 10% of reviews are direct
            if n_direct_reviews > 0:
                direct_reviews = []
                for _ in range(n_direct_reviews):
                    # Create a modified review without booking_id
                    source_review = reviews_df.sample(1).iloc[0].copy()
                    source_review['review_id'] = self._generate_review_id()
                    source_review['booking_id'] = ''
                    source_review['platform'] = 'Hotel Website'
                    source_review['verification_status'] = 'Unverified'
                    direct_reviews.append(source_review)
                
                if direct_reviews:
                    direct_df = pd.DataFrame(direct_reviews)
                    reviews_df = pd.concat([reviews_df, direct_df], ignore_index=True)
        
        print(f"\nGenerated {len(reviews_df):,} review records")
        print(f"Actual review rate: {len(reviews_df)/len(self.bookings_df):.1%}")
        
        return reviews_df
    
    def analyze_reviews(self, reviews_df: pd.DataFrame):
        """Analyze review data"""
        print("\n" + "="*60)
        print("REVIEW ANALYSIS")
        print("="*60)
        
        print(f"\nTotal Reviews: {len(reviews_df):,}")
        print(f"Review Rate: {len(reviews_df)/len(self.bookings_df):.1%}")
        
        # Rating distribution
        print(f"\nOverall Rating Distribution:")
        rating_counts = reviews_df['overall_rating'].value_counts().sort_index()
        for rating, count in rating_counts.items():
            percentage = (count / len(reviews_df)) * 100
            print(f"  {rating:<4} stars: {count:>6,} ({percentage:>5.1f}%)")
        
        # Average ratings
        print(f"\nAverage Ratings:")
        print(f"  Overall: {reviews_df['overall_rating'].mean():.2f}")
        print(f"  Cleanliness: {reviews_df['cleanliness_rating'].mean():.2f}")
        print(f"  Staff Service: {reviews_df['staff_service_rating'].mean():.2f}")
        print(f"  Comfort: {reviews_df['comfort_rating'].mean():.2f}")
        print(f"  Location: {reviews_df['location_rating'].mean():.2f}")
        print(f"  Value: {reviews_df['value_rating'].mean():.2f}")
        
        # Sentiment analysis
        print(f"\nSentiment Distribution:")
        sentiment_counts = reviews_df['sentiment'].value_counts()
        for sentiment, count in sentiment_counts.items():
            percentage = (count / len(reviews_df)) * 100
            print(f"  {sentiment:<10} {count:>6,} ({percentage:>5.1f}%)")
        
        # Platform distribution
        print(f"\nReview Platform Distribution:")
        platform_counts = reviews_df['platform'].value_counts().head(5)
        for platform, count in platform_counts.items():
            percentage = (count / len(reviews_df)) * 100
            print(f"  {platform:<15} {count:>6,} ({percentage:>5.1f}%)")
        
        # Helpfulness analysis
        print(f"\nHelpfulness Analysis:")
        print(f"  Average Helpfulness Score: {reviews_df['helpfulness_score'].mean():.3f}")
        print(f"  Total Helpful Votes: {reviews_df['helpful_votes'].sum():,}")
        print(f"  Total Unhelpful Votes: {reviews_df['unhelpful_votes'].sum():,}")
        
        # Response rate
        response_rate = reviews_df['has_response'].mean()
        print(f"\nHotel Response Rate: {response_rate:.1%}")
        
        # Review length statistics
        print(f"\nReview Length Statistics:")
        print(f"  Average Length: {reviews_df['review_length'].mean():.0f} characters")
        print(f"  Max Length: {reviews_df['review_length'].max():,} characters")
        print(f"  Min Length: {reviews_df['review_length'].min():,} characters")
        
        # Reviews without booking_id
        direct_reviews = reviews_df[reviews_df['booking_id'] == '']
        print(f"\nDirect Reviews (no booking): {len(direct_reviews):,} ({len(direct_reviews)/len(reviews_df):.1%})")
        
        # Correlation analysis
        print(f"\nRating Correlations:")
        rating_cols = ['overall_rating', 'cleanliness_rating', 'staff_service_rating', 
                      'comfort_rating', 'location_rating', 'value_rating']
        corr_matrix = reviews_df[rating_cols].corr()
        
        print(f"  Cleanliness vs Overall: {corr_matrix.loc['cleanliness_rating', 'overall_rating']:.3f}")
        print(f"  Staff Service vs Overall: {corr_matrix.loc['staff_service_rating', 'overall_rating']:.3f}")
        print(f"  Value vs Overall: {corr_matrix.loc['value_rating', 'overall_rating']:.3f}")
        
        return {
            'total_reviews': len(reviews_df),
            'avg_rating': reviews_df['overall_rating'].mean(),
            'response_rate': response_rate,
            'review_rate': len(reviews_df) / len(self.bookings_df)
        }

# Main execution
if __name__ == "__main__":
    import time
    
    print("="*60)
    print("HOTEL REVIEW DATA GENERATOR")
    print("="*60)
    
    # Load data
    print("\nLoading data...")
    start_load = time.time()
    
    bookings_df = pd.read_csv("hotel_data/hotel_chain_bookings.csv")
    customers_df = pd.read_csv("hotel_data/hotel_customers.csv")
    
    print(f"Data loaded in {time.time() - start_load:.1f} seconds")
    print(f"  Bookings: {len(bookings_df):,}")
    print(f"  Customers: {len(customers_df):,}")
    
    # Check data
    required_cols = ['booking_id', 'customer_id', 'hotel_id', 'status', 
                    'check_out_date', 'total_amount']
    missing_cols = [col for col in required_cols if col not in bookings_df.columns]
    
    if missing_cols:
        print(f"\nMissing columns in bookings data: {missing_cols}")
        print("Please check your bookings CSV file.")
        exit(1)
    
    # Initialize generator
    print("\nInitializing review generator...")
    generator = HotelReviewGenerator(bookings_df, customers_df)
    
    # Generate reviews
    print("\n" + "="*60)
    print("Generating review records...")
    
    start_gen = time.time()
    reviews_df = generator.generate_reviews(target_review_rate=0.35)
    
    gen_time = time.time() - start_gen
    print(f"\nReviews generated in {gen_time:.1f} seconds")
    print(f"  Speed: {len(reviews_df)/gen_time:,.0f} reviews/second")
    
    if len(reviews_df) == 0:
        print("\nERROR: No reviews were generated!")
        exit(1)
    
    # Analyze reviews
    analysis = generator.analyze_reviews(reviews_df)
    
    # Sample output
    print(f"\n" + "="*60)
    print("SAMPLE REVIEW RECORDS")
    print("="*60)
    
    sample_cols = ['review_id', 'booking_id', 'customer_id', 'overall_rating', 
                  'cleanliness_rating', 'staff_service_rating', 'review_date',
                  'sentiment', 'platform']
    print(reviews_df[sample_cols].head(10).to_string(index=False))
    
    # Sample review texts
    print(f"\n" + "="*60)
    print("SAMPLE REVIEW TEXTS")
    print("="*60)
    
    sample_reviews = reviews_df[['overall_rating', 'review_text']].head(5)
    for idx, (rating, text) in enumerate(zip(sample_reviews['overall_rating'], sample_reviews['review_text'])):
        print(f"\n{rating} stars:")
        print(f"\"{text}\"")
    
    # Save to CSV
    print(f"\n" + "="*60)
    print("Saving data...")
    
    output_file = 'hotel_data/hotel_chain_reviews.csv'
    start_save = time.time()
    reviews_df.to_csv(output_file, index=False)
    save_time = time.time() - start_save
    
    # Check file size
    import os
    if os.path.exists(output_file):
        file_size = os.path.getsize(output_file) / (1024**2)
        print(f"Saved to: {output_file}")
        print(f"  File size: {file_size:.1f} MB")
        print(f"  Save time: {save_time:.1f} seconds")
        print(f"  Rows written: {len(reviews_df):,}")
        
        # Create summary report
        summary_file = 'hotel_data/reviews_summary.txt'
        with open(summary_file, 'w') as f:
            f.write("REVIEW DATA SUMMARY\n")
            f.write("="*50 + "\n\n")
            f.write(f"Total Reviews: {len(reviews_df):,}\n")
            f.write(f"Average Rating: {analysis['avg_rating']:.2f}\n")
            f.write(f"Review Rate: {analysis['review_rate']:.1%}\n")
            f.write(f"Response Rate: {analysis['response_rate']:.1%}\n")
            f.write(f"File: {output_file}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        print(f"Summary saved to: {summary_file}")
    else:
        print(f"ERROR: File not saved!")
    
    print("\n" + "="*60)
    print("PROCESS COMPLETE")
    print("="*60)
    print(f"Total time: {time.time() - start_load:.1f} seconds")
    print(f"Reviews generated: {len(reviews_df):,}")
    print(f"Overall speed: {len(reviews_df)/(time.time() - start_load):,.0f} reviews/second")