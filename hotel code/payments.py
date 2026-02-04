import pandas as pd
import numpy as np
import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

class HotelPaymentGenerator:
    def __init__(self, bookings_df: pd.DataFrame, customers_df: pd.DataFrame = None):
        """
        Initialize with existing booking and customer data
        """
        self.bookings_df = bookings_df.copy()
        self.customers_df = customers_df.copy() if customers_df is not None else None
        
        # Payment methods distribution
        self.payment_methods = {
            'CREDIT_CARD': {'Visa': 0.45, 'Mastercard': 0.30, 'American Express': 0.15, 'Discover': 0.05, 'Other': 0.05},
            'DEBIT_CARD': {'Visa Debit': 0.60, 'Mastercard Debit': 0.30, 'Other Debit': 0.10},
            'DIGITAL_WALLET': {'PayPal': 0.50, 'Apple Pay': 0.25, 'Google Pay': 0.15, 'Samsung Pay': 0.05, 'Other Wallet': 0.05},
            'BANK_TRANSFER': {'Direct Transfer': 0.70, 'Wire Transfer': 0.20, 'ACH': 0.10},
            'CASH': {'Cash': 1.0},
            'CHECK': {'Check': 1.0},
            'LOYALTY_POINTS': {'Points': 1.0},
            'CRYPTO': {'Bitcoin': 0.60, 'Ethereum': 0.25, 'Other Crypto': 0.15}
        }
        
        # Payment method distribution by booking source
        self.payment_method_by_source = {
            'Website Direct': {'CREDIT_CARD': 0.60, 'DIGITAL_WALLET': 0.25, 'DEBIT_CARD': 0.10, 'OTHER': 0.05},
            'Mobile App': {'DIGITAL_WALLET': 0.55, 'CREDIT_CARD': 0.35, 'DEBIT_CARD': 0.08, 'OTHER': 0.02},
            'OTA (Booking.com)': {'CREDIT_CARD': 0.70, 'DIGITAL_WALLET': 0.20, 'OTHER': 0.10},
            'OTA (Expedia)': {'CREDIT_CARD': 0.65, 'DIGITAL_WALLET': 0.25, 'OTHER': 0.10},
            'Phone': {'CREDIT_CARD': 0.50, 'DEBIT_CARD': 0.20, 'CASH': 0.20, 'OTHER': 0.10},
            'Travel Agent': {'CREDIT_CARD': 0.40, 'BANK_TRANSFER': 0.30, 'CHECK': 0.20, 'OTHER': 0.10},
            'Walk-in': {'CASH': 0.40, 'CREDIT_CARD': 0.35, 'DEBIT_CARD': 0.20, 'OTHER': 0.05}
        }
        
        # Payment status distribution
        self.payment_status_dist = {
            'COMPLETED': 0.75,
            'PENDING': 0.12,
            'FAILED': 0.05,
            'REFUNDED': 0.04,
            'PARTIALLY_REFUNDED': 0.02,
            'CANCELLED': 0.01,
            'DISPUTED': 0.01
        }
        
        # Payment status by booking status
        self.payment_status_by_booking = {
            'CONFIRMED': {'COMPLETED': 0.60, 'PENDING': 0.30, 'PARTIAL': 0.10},
            'CHECKED_IN': {'COMPLETED': 0.80, 'PARTIAL': 0.15, 'PENDING': 0.05},
            'CHECKED_OUT': {'COMPLETED': 0.95, 'REFUNDED': 0.03, 'PARTIALLY_REFUNDED': 0.02},
            'CANCELLED': {'REFUNDED': 0.60, 'PARTIALLY_REFUNDED': 0.25, 'PENDING': 0.10, 'COMPLETED': 0.05},
            'NO_SHOW': {'COMPLETED': 0.70, 'REFUNDED': 0.20, 'PENDING': 0.10},
            'PENDING': {'PENDING': 0.80, 'COMPLETED': 0.15, 'CANCELLED': 0.05}
        }
        
        # Regional payment method preferences
        self.regional_payment_preferences = {
            'North America': {'CREDIT_CARD': 0.55, 'DIGITAL_WALLET': 0.25, 'DEBIT_CARD': 0.15, 'OTHER': 0.05},
            'Europe': {'CREDIT_CARD': 0.45, 'DEBIT_CARD': 0.35, 'DIGITAL_WALLET': 0.15, 'BANK_TRANSFER': 0.05},
            'Asia Pacific': {'DIGITAL_WALLET': 0.40, 'CREDIT_CARD': 0.30, 'DEBIT_CARD': 0.20, 'OTHER': 0.10},
            'Middle East': {'CREDIT_CARD': 0.50, 'CASH': 0.25, 'DEBIT_CARD': 0.15, 'OTHER': 0.10},
            'South America': {'CREDIT_CARD': 0.40, 'DIGITAL_WALLET': 0.25, 'CASH': 0.20, 'OTHER': 0.15},
            'Africa': {'CASH': 0.35, 'MOBILE_MONEY': 0.30, 'CREDIT_CARD': 0.20, 'OTHER': 0.15}
        }
        
        # Payment gateway providers
        self.payment_gateways = {
            'STRIPE': 0.35,
            'PAYPAL': 0.25,
            'SQUARE': 0.15,
            'BRAINTREE': 0.10,
            'AUTHORIZE.NET': 0.08,
            'ADYEN': 0.04,
            'WORLDPAY': 0.03
        }
        
        # Transaction fee structure
        self.transaction_fees = {
            'CREDIT_CARD': {'percentage': 2.9, 'fixed': 0.30},
            'DEBIT_CARD': {'percentage': 1.5, 'fixed': 0.25},
            'DIGITAL_WALLET': {'percentage': 2.5, 'fixed': 0.20},
            'BANK_TRANSFER': {'percentage': 0.5, 'fixed': 1.00},
            'CASH': {'percentage': 0.0, 'fixed': 0.0},
            'CHECK': {'percentage': 0.0, 'fixed': 1.50},
            'LOYALTY_POINTS': {'percentage': 0.0, 'fixed': 0.0},
            'CRYPTO': {'percentage': 1.0, 'fixed': 0.50}
        }
        
        # Currency codes by region
        self.currency_by_region = {
            'North America': ['USD', 'CAD'],
            'Europe': ['EUR', 'GBP', 'CHF'],
            'Asia Pacific': ['JPY', 'CNY', 'AUD', 'SGD'],
            'Middle East': ['AED', 'SAR', 'QAR'],
            'South America': ['BRL', 'ARS', 'CLP'],
            'Africa': ['ZAR', 'EGP', 'NGN']
        }
        
        # Exchange rates (simplified)
        self.exchange_rates = {
            'USD': 1.0, 'EUR': 0.92, 'GBP': 0.79, 'CAD': 1.35,
            'JPY': 148.0, 'CNY': 7.18, 'AUD': 1.52, 'SGD': 1.34,
            'AED': 3.67, 'SAR': 3.75, 'QAR': 3.64,
            'BRL': 4.95, 'ARS': 350.0, 'CLP': 850.0,
            'ZAR': 18.5, 'EGP': 30.9, 'NGN': 780.0
        }
        
        # Fraud patterns
        self.fraud_patterns = {
            'HIGH_RISK': {'probability': 0.02, 'indicators': ['high_amount', 'last_minute', 'new_customer']},
            'MEDIUM_RISK': {'probability': 0.08, 'indicators': ['international', 'multiple_cards']},
            'LOW_RISK': {'probability': 0.90, 'indicators': []}
        }
        
        # Credit card issuers
        self.card_issuers = {
            'Visa': ['Chase', 'Bank of America', 'Citi', 'Wells Fargo', 'Capital One'],
            'Mastercard': ['Chase', 'Bank of America', 'Citi', 'Barclays', 'HSBC'],
            'American Express': ['American Express'],
            'Discover': ['Discover']
        }
        
        # Preprocess data
        self._preprocess_data()
    
    def _preprocess_data(self):
        """Preprocess booking data for faster access"""
        # Extract year and month from booking dates
        self.bookings_df['booking_year'] = pd.to_datetime(self.bookings_df['booking_date']).dt.year
        self.bookings_df['booking_month'] = pd.to_datetime(self.bookings_df['booking_date']).dt.month
        
        # Add region information if available
        if self.customers_df is not None and 'region' in self.customers_df.columns:
            # Create customer region mapping
            customer_regions = self.customers_df.set_index('customer_id')['region'].to_dict()
            self.bookings_df['customer_region'] = self.bookings_df['customer_id'].map(customer_regions)
        else:
            # Assign random regions for demo purposes
            regions = list(self.regional_payment_preferences.keys())
            self.bookings_df['customer_region'] = np.random.choice(
                regions, size=len(self.bookings_df), 
                p=[0.35, 0.30, 0.25, 0.05, 0.03, 0.02]
            )
        
        # Calculate if booking is high value (top 20%)
        self.bookings_df['is_high_value'] = self.bookings_df['total_amount'] > self.bookings_df['total_amount'].quantile(0.8)
        
        # Calculate if booking is last minute (< 3 days lead time)
        self.bookings_df['is_last_minute'] = self.bookings_df['lead_time_days'] < 3
        
        # Group bookings for faster lookup
        self.bookings_by_status = self.bookings_df.groupby('status')
        
        print(f"Preprocessed {len(self.bookings_df):,} bookings")
    
    def _generate_payment_id(self) -> str:
        """Generate UUID for payment"""
        return str(uuid.uuid4())
    
    def _determine_payment_method(self, booking_source: str, customer_region: str, 
                                booking_amount: float, booking_status: str) -> Tuple[str, str]:
        """Determine payment method based on multiple factors"""
        
        # Base on booking source
        if booking_source in self.payment_method_by_source:
            source_methods = self.payment_method_by_source[booking_source]
        else:
            source_methods = self.payment_method_by_source['Website Direct']
        
        # Adjust based on region
        if customer_region in self.regional_payment_preferences:
            region_methods = self.regional_payment_preferences[customer_region]
            
            # Blend source and region preferences
            blended_methods = {}
            all_methods = set(source_methods.keys()).union(set(region_methods.keys()))
            
            for method in all_methods:
                source_weight = source_methods.get(method, 0) * 0.6
                region_weight = region_methods.get(method, 0) * 0.4
                blended_methods[method] = source_weight + region_weight
        else:
            blended_methods = source_methods.copy()
        
        # Adjust for high value bookings (more likely to use credit cards)
        if booking_amount > 1000:
            if 'CREDIT_CARD' in blended_methods:
                blended_methods['CREDIT_CARD'] *= 1.5
            if 'CASH' in blended_methods:
                blended_methods['CASH'] *= 0.5
        
        # Adjust for cancelled bookings (more likely to be refunded to original method)
        if booking_status == 'CANCELLED':
            # When refunding, use original method or digital wallet
            if 'DIGITAL_WALLET' in blended_methods:
                blended_methods['DIGITAL_WALLET'] *= 1.3
            if 'CREDIT_CARD' in blended_methods:
                blended_methods['CREDIT_CARD'] *= 1.2
        
        # Normalize probabilities
        total = sum(blended_methods.values())
        if total > 0:
            for method in blended_methods:
                blended_methods[method] /= total
        
        # Select method
        methods = list(blended_methods.keys())
        probabilities = list(blended_methods.values())
        
        payment_method = np.random.choice(methods, p=probabilities)
        
        # Select specific type within the method
        if payment_method in self.payment_methods:
            subtypes = self.payment_methods[payment_method]
            subtype = np.random.choice(list(subtypes.keys()), p=list(subtypes.values()))
        else:
            subtype = payment_method
        
        return payment_method, subtype
    
    def _determine_payment_status(self, booking_status: str, payment_method: str, 
                                booking_date: datetime, checkin_date: datetime) -> str:
        """Determine payment status based on booking and timing"""
        
        if booking_status in self.payment_status_by_booking:
            status_dist = self.payment_status_by_booking[booking_status]
        else:
            status_dist = self.payment_status_dist
        
        # Adjust based on timing
        days_before_checkin = (checkin_date - booking_date).days if checkin_date > booking_date else 0
        
        if days_before_checkin < 1:
            # Last minute payments more likely to be pending or fail
            status_dist = status_dist.copy()
            if 'PENDING' in status_dist:
                status_dist['PENDING'] *= 1.5
            if 'FAILED' in status_dist:
                status_dist['FAILED'] *= 2.0
        
        # Adjust for payment method
        if payment_method == 'CASH':
            status_dist = {'COMPLETED': 0.95, 'PENDING': 0.05}
        elif payment_method == 'CHECK':
            status_dist = {'PENDING': 0.60, 'COMPLETED': 0.35, 'FAILED': 0.05}
        
        # Normalize and select
        statuses = list(status_dist.keys())
        probabilities = list(status_dist.values())
        total = sum(probabilities)
        
        if total > 0:
            probabilities = [p/total for p in probabilities]
        else:
            probabilities = [1/len(statuses)] * len(statuses)
        
        return np.random.choice(statuses, p=probabilities)
    
    def _calculate_payment_amounts(self, booking_total: float, booking_status: str, 
                                 payment_status: str, lead_time: int) -> Dict:
        """Calculate payment amounts including fees and splits"""
        
        if payment_status in ['REFUNDED', 'CANCELLED']:
            # Full refund
            return {
                'amount': booking_total,
                'transaction_fee': 0.0,
                'net_amount': booking_total,
                'is_full_payment': True,
                'is_deposit': False
            }
        
        elif payment_status == 'PARTIALLY_REFUNDED':
            # Partial refund
            refund_percent = np.random.uniform(0.2, 0.8)
            refund_amount = booking_total * refund_percent
            return {
                'amount': refund_amount,
                'transaction_fee': 0.0,
                'net_amount': refund_amount,
                'is_full_payment': False,
                'is_deposit': False,
                'refund_percent': refund_percent * 100
            }
        
        else:
            # Regular payment
            is_deposit = False
            
            if lead_time > 30:
                # Early booking: deposit + final payment
                if np.random.random() < 0.7:
                    deposit_percent = np.random.uniform(0.1, 0.3)
                    deposit_amount = booking_total * deposit_percent
                    is_deposit = True
                    amount = deposit_amount
                else:
                    amount = booking_total
            else:
                # Last minute: usually full payment
                if np.random.random() < 0.9:
                    amount = booking_total
                else:
                    deposit_percent = np.random.uniform(0.5, 1.0)
                    amount = booking_total * deposit_percent
            
            # Add transaction fee based on method (will be set later)
            transaction_fee = 0.0  # Will be calculated based on payment method
            
            return {
                'amount': amount,
                'transaction_fee': transaction_fee,
                'net_amount': amount - transaction_fee,
                'is_full_payment': amount == booking_total,
                'is_deposit': is_deposit
            }
    
    def _generate_payment_date(self, booking_date: datetime, checkin_date: datetime, 
                             payment_status: str, lead_time: int) -> datetime:
        """Generate realistic payment date"""
        
        if payment_status in ['REFUNDED', 'PARTIALLY_REFUNDED']:
            # Refunds happen after check-in date
            days_after_checkin = np.random.randint(1, 14)
            return checkin_date + timedelta(days=days_after_checkin)
        
        elif payment_status == 'FAILED':
            # Failed payments might have retries
            base_date = booking_date
            if np.random.random() < 0.5:
                # First attempt failed, retry later
                days_after_booking = np.random.randint(1, 3)
                return booking_date + timedelta(days=days_after_booking)
            else:
                return booking_date
        
        else:
            # Regular payments
            if lead_time > 60:
                # Early booking: payment at booking or soon after
                if np.random.random() < 0.7:
                    return booking_date
                else:
                    days_after_booking = np.random.randint(1, 7)
                    return booking_date + timedelta(days=days_after_booking)
            
            elif lead_time > 7:
                # Moderate lead time
                days_after_booking = np.random.randint(0, 14)
                return booking_date + timedelta(days=days_after_booking)
            
            else:
                # Last minute: payment at booking
                return booking_date
    
    def _generate_payment_details(self, payment_method: str, subtype: str, 
                                amount: float, currency: str) -> Dict:
        """Generate detailed payment information"""
        
        details = {
            'payment_gateway': np.random.choice(list(self.payment_gateways.keys()), 
                                               p=list(self.payment_gateways.values())),
            'currency': currency,
            'exchange_rate': self.exchange_rates.get(currency, 1.0),
            'amount_in_usd': round(amount / self.exchange_rates.get(currency, 1.0), 2),
            'fraud_score': np.random.randint(0, 100),
            'is_international': np.random.random() < 0.15,
            'ip_country': np.random.choice(['US', 'GB', 'DE', 'FR', 'JP', 'AU', 'CA', 'BR']),
            'device_type': np.random.choice(['Desktop', 'Mobile', 'Tablet'], p=[0.5, 0.4, 0.1])
        }
        
        # Add method-specific details
        if payment_method == 'CREDIT_CARD':
            issuer = np.random.choice(self.card_issuers.get(subtype, ['Unknown Bank']))
            last_four = str(np.random.randint(1000, 9999))
            expiry_month = np.random.randint(1, 13)
            expiry_year = np.random.randint(2023, 2030)
            
            details.update({
                'card_issuer': issuer,
                'last_four_digits': last_four,
                'expiry_date': f"{expiry_month:02d}/{expiry_year}",
                'card_type': subtype,
                'is_tokenized': np.random.random() < 0.7,
                'avs_result': np.random.choice(['Y', 'N', 'A', 'U'], p=[0.8, 0.1, 0.05, 0.05]),
                'cvv_result': np.random.choice(['M', 'N', 'P'], p=[0.9, 0.05, 0.05])
            })
            
            # Calculate transaction fee
            fee_percent = self.transaction_fees['CREDIT_CARD']['percentage']
            fee_fixed = self.transaction_fees['CREDIT_CARD']['fixed']
            transaction_fee = round((amount * fee_percent/100) + fee_fixed, 2)
            details['transaction_fee'] = transaction_fee
            
        elif payment_method == 'DIGITAL_WALLET':
            details.update({
                'wallet_email': f"user{np.random.randint(10000, 99999)}@example.com",
                'wallet_id': str(uuid.uuid4())[:12],
                'is_verified': np.random.random() < 0.9
            })
            
            fee_percent = self.transaction_fees['DIGITAL_WALLET']['percentage']
            fee_fixed = self.transaction_fees['DIGITAL_WALLET']['fixed']
            transaction_fee = round((amount * fee_percent/100) + fee_fixed, 2)
            details['transaction_fee'] = transaction_fee
            
        elif payment_method == 'BANK_TRANSFER':
            details.update({
                'bank_name': np.random.choice(['Chase', 'Bank of America', 'Wells Fargo', 'HSBC', 'Barclays']),
                'account_last_four': str(np.random.randint(1000, 9999)),
                'routing_number': str(np.random.randint(100000000, 999999999)),
                'transfer_reference': f"TRF{np.random.randint(100000, 999999)}"
            })
            
            fee_percent = self.transaction_fees['BANK_TRANSFER']['percentage']
            fee_fixed = self.transaction_fees['BANK_TRANSFER']['fixed']
            transaction_fee = round((amount * fee_percent/100) + fee_fixed, 2)
            details['transaction_fee'] = transaction_fee
            
        elif payment_method == 'CASH':
            details.update({
                'received_by': f"Staff_{np.random.randint(100, 999)}",
                'receipt_number': f"CASH{np.random.randint(10000, 99999)}",
                'is_receipt_issued': np.random.random() < 0.8
            })
            details['transaction_fee'] = 0.0
            
        elif payment_method == 'LOYALTY_POINTS':
            points_per_dollar = np.random.randint(80, 120)
            points_used = int(amount * points_per_dollar)
            details.update({
                'points_used': points_used,
                'points_balance': points_used + np.random.randint(1000, 10000),
                'loyalty_tier': np.random.choice(['Silver', 'Gold', 'Platinum'], p=[0.5, 0.3, 0.2])
            })
            details['transaction_fee'] = 0.0
        else:
            # For other payment methods, set default transaction fee
            if payment_method in self.transaction_fees:
                fee_percent = self.transaction_fees[payment_method]['percentage']
                fee_fixed = self.transaction_fees[payment_method]['fixed']
                transaction_fee = round((amount * fee_percent/100) + fee_fixed, 2)
            else:
                transaction_fee = 0.0
            details['transaction_fee'] = transaction_fee
        
        # Calculate net amount
        details['net_amount'] = round(amount - details['transaction_fee'], 2)
        
        return details
    
    def _determine_fraud_risk(self, amount: float, is_new_customer: bool, 
                            is_international: bool, payment_method: str) -> Dict:
        """Determine fraud risk level"""
        
        risk_score = 0
        
        # High amount increases risk
        if amount > 5000:
            risk_score += 40
        elif amount > 2000:
            risk_score += 20
        elif amount > 1000:
            risk_score += 10
        
        # New customer increases risk
        if is_new_customer:
            risk_score += 25
        
        # International transaction increases risk
        if is_international:
            risk_score += 30
        
        # Certain payment methods are riskier
        if payment_method == 'CRYPTO':
            risk_score += 35
        elif payment_method == 'CASH':
            risk_score += 15
        
        # Determine risk level
        if risk_score >= 60:
            risk_level = 'HIGH_RISK'
            is_flagged = True
        elif risk_score >= 30:
            risk_level = 'MEDIUM_RISK'
            is_flagged = np.random.random() < 0.3
        else:
            risk_level = 'LOW_RISK'
            is_flagged = np.random.random() < 0.05
        
        return {
            'risk_level': risk_level,
            'risk_score': risk_score,
            'is_flagged': is_flagged,
            'fraud_indicators': self.fraud_patterns.get(risk_level, {}).get('indicators', [])
        }
    
    def generate_payments(self, payments_per_booking: float = 1.2):
        """
        Generate payment records for bookings
        
        Args:
            payments_per_booking: Average number of payments per booking
                                 (e.g., 1.2 = 20% of bookings have multiple payments)
        """
        print(f"Generating payments for {len(self.bookings_df):,} bookings...")
        print(f"Target: ~{int(len(self.bookings_df) * payments_per_booking):,} payments")
        
        payments_data = []
        booking_payment_counts = {}
        
        # Process bookings
        for idx, booking in tqdm(self.bookings_df.iterrows(), total=len(self.bookings_df), desc="Processing bookings"):
            booking_id = booking['booking_id']
            booking_status = booking['status']
            booking_total = booking['total_amount']
            booking_date = pd.to_datetime(booking['booking_date'])
            checkin_date = pd.to_datetime(booking['check_in_date'])
            lead_time = booking['lead_time_days']
            booking_source = booking.get('booking_source', 'Website Direct')
            customer_region = booking.get('customer_region', 'North America')
            
            # Determine currency based on region
            if customer_region in self.currency_by_region:
                currency = np.random.choice(self.currency_by_region[customer_region])
            else:
                currency = 'USD'
            
            # Determine number of payments for this booking
            if np.random.random() < (payments_per_booking - 1):
                num_payments = 2
            else:
                num_payments = 1
            
            booking_payment_counts[booking_id] = num_payments
            remaining_amount = booking_total
            
            for payment_num in range(num_payments):
                # Determine if this is the last payment
                is_last_payment = (payment_num == num_payments - 1)
                
                # Determine payment method
                payment_method, subtype = self._determine_payment_method(
                    booking_source, customer_region, booking_total, booking_status
                )
                
                # Determine payment status
                payment_status = self._determine_payment_status(
                    booking_status, payment_method, booking_date, checkin_date
                )
                
                # Calculate payment amount
                if is_last_payment and remaining_amount > 0:
                    # Last payment covers remaining amount
                    amount = remaining_amount
                else:
                    # Partial payment
                    if num_payments == 1:
                        amount = booking_total
                    else:
                        max_amount = remaining_amount * 0.8 if remaining_amount > booking_total * 0.5 else remaining_amount
                        amount = np.random.uniform(booking_total * 0.2, max_amount)
                        amount = round(amount, 2)
                
                remaining_amount -= amount
                if remaining_amount < 0.01:
                    remaining_amount = 0
                
                # Calculate payment details
                amounts = self._calculate_payment_amounts(
                    amount, booking_status, payment_status, lead_time
                )
                
                # Generate payment date
                payment_date = self._generate_payment_date(
                    booking_date, checkin_date, payment_status, lead_time
                )
                
                # Generate payment details
                payment_details = self._generate_payment_details(
                    payment_method, subtype, amounts['amount'], currency
                )
                
                # Determine fraud risk
                is_new_customer = np.random.random() < 0.15  # 15% of customers are new
                fraud_risk = self._determine_fraud_risk(
                    amounts['amount'], is_new_customer, 
                    payment_details['is_international'], payment_method
                )
                
                # Create payment record
                payment = {
                    'payment_id': self._generate_payment_id(),
                    'booking_id': booking_id,
                    'customer_id': booking['customer_id'],
                    'hotel_id': booking['hotel_id'],
                    'payment_number': payment_num + 1,
                    'total_payments': num_payments,
                    'amount': amounts['amount'],
                    'currency': payment_details['currency'],
                    'amount_in_usd': payment_details['amount_in_usd'],
                    'payment_method': payment_method,
                    'payment_subtype': subtype,
                    'payment_status': payment_status,
                    'payment_date': payment_date.strftime('%Y-%m-%d'),
                    'payment_timestamp': payment_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'transaction_fee': payment_details['transaction_fee'],
                    'net_amount': payment_details['net_amount'],
                    'payment_gateway': payment_details['payment_gateway'],
                    'gateway_transaction_id': f"TXN{np.random.randint(100000000, 999999999)}",
                    'is_deposit': amounts.get('is_deposit', False),
                    'is_full_payment': amounts.get('is_full_payment', False),
                    'is_refund': payment_status in ['REFUNDED', 'PARTIALLY_REFUNDED'],
                    'refund_amount': amounts.get('amount', 0) if payment_status in ['REFUNDED', 'PARTIALLY_REFUNDED'] else 0,
                    'refund_percent': amounts.get('refund_percent', 0),
                    'fraud_risk_level': fraud_risk['risk_level'],
                    'fraud_risk_score': fraud_risk['risk_score'],
                    'is_flagged': fraud_risk['is_flagged'],
                    'fraud_indicators': ', '.join(fraud_risk['fraud_indicators']) if fraud_risk['fraud_indicators'] else '',
                    'is_international': payment_details['is_international'],
                    'ip_country': payment_details['ip_country'],
                    'device_type': payment_details['device_type'],
                    'avs_result': payment_details.get('avs_result', ''),
                    'cvv_result': payment_details.get('cvv_result', ''),
                    'card_last_four': payment_details.get('last_four_digits', ''),
                    'card_issuer': payment_details.get('card_issuer', ''),
                    'card_expiry': payment_details.get('expiry_date', ''),
                    'bank_name': payment_details.get('bank_name', ''),
                    'account_last_four': payment_details.get('account_last_four', ''),
                    'wallet_email': payment_details.get('wallet_email', ''),
                    'receipt_number': payment_details.get('receipt_number', ''),
                    'created_at': payment_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': (payment_date + timedelta(minutes=np.random.randint(1, 60))).strftime('%Y-%m-%d %H:%M:%S')
                }
                
                payments_data.append(payment)
        
        payments_df = pd.DataFrame(payments_data)
        
        # Add some failed payment retries
        failed_payments = payments_df[payments_df['payment_status'] == 'FAILED']
        if len(failed_payments) > 0:
            retry_payments = []
            for _, failed_payment in failed_payments.sample(min(100, len(failed_payments))).iterrows():
                retry_payment = failed_payment.copy()
                retry_payment['payment_id'] = self._generate_payment_id()
                retry_payment['payment_number'] += 1
                retry_payment['total_payments'] = max(retry_payment['total_payments'], retry_payment['payment_number'])
                retry_payment['payment_status'] = np.random.choice(['COMPLETED', 'PENDING'], p=[0.8, 0.2])
                retry_payment['payment_date'] = (pd.to_datetime(failed_payment['payment_date']) + 
                                                timedelta(days=np.random.randint(1, 3))).strftime('%Y-%m-%d')
                retry_payment['gateway_transaction_id'] = f"TXN{np.random.randint(100000000, 999999999)}"
                retry_payment['fraud_risk_score'] = min(100, retry_payment['fraud_risk_score'] + 10)
                
                retry_payments.append(retry_payment)
            
            if retry_payments:
                retry_df = pd.DataFrame(retry_payments)
                payments_df = pd.concat([payments_df, retry_df], ignore_index=True)
        
        print(f"\nGenerated {len(payments_df):,} payment records")
        return payments_df
    
    def analyze_payments(self, payments_df: pd.DataFrame):
        """Analyze payment data"""
        print("\n" + "="*60)
        print("PAYMENT ANALYSIS")
        print("="*60)
        
        print(f"\nTotal Payments: {len(payments_df):,}")
        print(f"Total Amount Processed: ${payments_df['amount_in_usd'].sum():,.2f}")
        print(f"Total Fees Collected: ${payments_df['transaction_fee'].sum():,.2f}")
        print(f"Net Revenue: ${payments_df['net_amount'].sum():,.2f}")
        
        # Payment method distribution
        print(f"\nPayment Method Distribution:")
        method_counts = payments_df['payment_method'].value_counts()
        for method, count in method_counts.head(10).items():
            percentage = (count / len(payments_df)) * 100
            print(f"  {method:<20} {count:>6,} ({percentage:>5.1f}%)")
        
        # Payment status distribution
        print(f"\nPayment Status Distribution:")
        status_counts = payments_df['payment_status'].value_counts()
        for status, count in status_counts.items():
            percentage = (count / len(payments_df)) * 100
            print(f"  {status:<20} {count:>6,} ({percentage:>5.1f}%)")
        
        # Fraud analysis
        print(f"\nFraud Risk Analysis:")
        fraud_counts = payments_df['fraud_risk_level'].value_counts()
        for level, count in fraud_counts.items():
            percentage = (count / len(payments_df)) * 100
            flagged = payments_df[payments_df['fraud_risk_level'] == level]['is_flagged'].sum()
            print(f"  {level:<15} {count:>6,} ({percentage:>5.1f}%) - {flagged:>4,} flagged")
        
        # Payment gateway distribution
        print(f"\nPayment Gateway Distribution:")
        gateway_counts = payments_df['payment_gateway'].value_counts().head(5)
        for gateway, count in gateway_counts.items():
            percentage = (count / len(payments_df)) * 100
            print(f"  {gateway:<15} {count:>6,} ({percentage:>5.1f}%)")
        
        # Average transaction amounts
        print(f"\nTransaction Amount Statistics:")
        print(f"  Average Payment: ${payments_df['amount'].mean():,.2f}")
        print(f"  Median Payment: ${payments_df['amount'].median():,.2f}")
        print(f"  Max Payment: ${payments_df['amount'].max():,.2f}")
        print(f"  Min Payment: ${payments_df['amount'].min():,.2f}")
        
        # Deposit vs full payment
        deposits = payments_df[payments_df['is_deposit']]
        full_payments = payments_df[payments_df['is_full_payment']]
        refunds = payments_df[payments_df['is_refund']]
        
        print(f"\nPayment Types:")
        print(f"  Deposits: {len(deposits):,} (${deposits['amount'].sum():,.2f})")
        print(f"  Full Payments: {len(full_payments):,} (${full_payments['amount'].sum():,.2f})")
        print(f"  Refunds: {len(refunds):,} (${refunds['amount'].sum():,.2f})")
        
        # Success rate
        successful = payments_df[payments_df['payment_status'].isin(['COMPLETED', 'REFUNDED', 'PARTIALLY_REFUNDED'])]
        failed = payments_df[payments_df['payment_status'] == 'FAILED']
        
        print(f"\nPayment Success Rate:")
        print(f"  Successful: {len(successful):,} ({len(successful)/len(payments_df)*100:.1f}%)")
        print(f"  Failed: {len(failed):,} ({len(failed)/len(payments_df)*100:.1f}%)")
        
        # Multiple payments analysis
        multi_payment_bookings = payments_df[payments_df['total_payments'] > 1]['booking_id'].nunique()
        print(f"\nMultiple Payment Bookings: {multi_payment_bookings:,}")
        
        return {
            'total_payments': len(payments_df),
            'total_amount': payments_df['amount_in_usd'].sum(),
            'success_rate': len(successful) / len(payments_df),
            'fraud_rate': payments_df['is_flagged'].sum() / len(payments_df)
        }

# Main execution
if __name__ == "__main__":
    import time
    
    print("="*60)
    print("HOTEL PAYMENT DATA GENERATOR")
    print("="*60)
    
    # Load data
    print("\nLoading data...")
    start_load = time.time()
    
    bookings_df = pd.read_csv("hotel data/hotel_chain_bookings.csv")
    customers_df = pd.read_csv("hotel data/hotel_customers.csv")
    
    print(f"Data loaded in {time.time() - start_load:.1f} seconds")
    print(f"  Bookings: {len(bookings_df):,}")
    print(f"  Customers: {len(customers_df):,}")
    
    # Check data
    required_cols = ['booking_id', 'customer_id', 'hotel_id', 'total_amount', 
                    'booking_date', 'check_in_date', 'status', 'lead_time_days']
    missing_cols = [col for col in required_cols if col not in bookings_df.columns]
    
    if missing_cols:
        print(f"\nMissing columns in bookings data: {missing_cols}")
        print("Please check your bookings CSV file.")
        exit(1)
    
    # Initialize generator
    print("\nInitializing payment generator...")
    generator = HotelPaymentGenerator(bookings_df, customers_df)
    
    # Generate payments
    print("\n" + "="*60)
    print("Generating payment records...")
    
    start_gen = time.time()
    payments_df = generator.generate_payments(payments_per_booking=1.2)
    
    gen_time = time.time() - start_gen
    print(f"\nPayments generated in {gen_time:.1f} seconds")
    print(f"  Speed: {len(payments_df)/gen_time:,.0f} payments/second")
    
    if len(payments_df) == 0:
        print("\nERROR: No payments were generated!")
        exit(1)
    
    # Analyze payments
    analysis = generator.analyze_payments(payments_df)
    
    # Sample output
    print(f"\n" + "="*60)
    print("SAMPLE PAYMENT RECORDS")
    print("="*60)
    
    sample_cols = ['payment_id', 'booking_id', 'amount', 'currency', 
                  'payment_method', 'payment_status', 'payment_date', 
                  'fraud_risk_level']
    print(payments_df[sample_cols].head(10).to_string(index=False))
    
    # Save to CSV
    print(f"\n" + "="*60)
    print("Saving data...")
    
    output_file = 'hotel data/hotel_chain_payments.csv'
    start_save = time.time()
    payments_df.to_csv(output_file, index=False)
    save_time = time.time() - start_save
    
    # Check file size
    import os
    if os.path.exists(output_file):
        file_size = os.path.getsize(output_file) / (1024**2)
        print(f"Saved to: {output_file}")
        print(f"  File size: {file_size:.1f} MB")
        print(f"  Save time: {save_time:.1f} seconds")
        print(f"  Rows written: {len(payments_df):,}")
        
        # Create summary report
        summary_file = 'hotel data/payments_summary.txt'
        with open(summary_file, 'w') as f:
            f.write("PAYMENT DATA SUMMARY\n")
            f.write("="*50 + "\n\n")
            f.write(f"Total Payments: {len(payments_df):,}\n")
            f.write(f"Total Amount Processed: ${payments_df['amount_in_usd'].sum():,.2f}\n")
            f.write(f"Total Fees: ${payments_df['transaction_fee'].sum():,.2f}\n")
            f.write(f"Success Rate: {analysis['success_rate']*100:.1f}%\n")
            f.write(f"Fraud Flag Rate: {analysis['fraud_rate']*100:.1f}%\n")
            f.write(f"File: {output_file}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        print(f"Summary saved to: {summary_file}")
    else:
        print(f"ERROR: File not saved!")
    
    print("\n" + "="*60)
    print("PROCESS COMPLETE")
    print("="*60)
    print(f"Total time: {time.time() - start_load:.1f} seconds")
    print(f"Payments generated: {len(payments_df):,}")
    print(f"Overall speed: {len(payments_df)/(time.time() - start_load):,.0f} payments/second")