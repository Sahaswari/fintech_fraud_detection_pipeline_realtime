"""
Fraud Detection Pipeline - Transaction Producer
Generates realistic transaction data with occasional fraud patterns
"""

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import uuid

class TransactionProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Initialize Kafka producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Merchant categories
        self.categories = [
            'RESTAURANT', 'ELECTRONICS', 'GROCERY', 'FUEL',
            'PHARMACY', 'CLOTHING', 'ENTERTAINMENT', 'TRAVEL'
        ]
        
        # Countries for location
        self.countries = [
            'Sri Lanka', 'India', 'USA', 'UK', 'Singapore',
            'UAE', 'Australia', 'Canada', 'Germany', 'Japan'
        ]
        
        # Track user locations for impossible travel detection
        self.user_last_location = {}
        self.user_last_timestamp = {}
        
        print("=" * 70)
        print("🚀 FRAUD DETECTION - TRANSACTION PRODUCER STARTED")
        print("=" * 70)
        print(f"📡 Connected to Kafka: {bootstrap_servers}")
        print(f"📋 Generating transactions with fraud patterns...")
        print("=" * 70)
        print()
    
    def generate_normal_transaction(self, user_id):
        """Generate a normal, legitimate transaction"""
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'user_id': user_id,
            'timestamp': datetime.now().isoformat(),
            'merchant_category': random.choice(self.categories),
            'amount': round(random.uniform(5, 500), 2),
            'location': random.choice(['Sri Lanka', 'India', 'Singapore'])  # Nearby countries
        }
        
        # Update user's last location
        self.user_last_location[user_id] = transaction['location']
        self.user_last_timestamp[user_id] = datetime.now()
        
        return transaction
    
    def generate_impossible_travel_fraud(self, user_id):
        """Generate impossible travel fraud: same user, different countries, <10 mins"""
        # First transaction in one country
        first_location = random.choice(['Sri Lanka', 'India'])
        first_trans = {
            'transaction_id': str(uuid.uuid4()),
            'user_id': user_id,
            'timestamp': datetime.now().isoformat(),
            'merchant_category': random.choice(self.categories),
            'amount': round(random.uniform(20, 200), 2),
            'location': first_location
        }
        
        # Send first transaction
        self.send_transaction(first_trans, is_fraud=True, fraud_type="IMPOSSIBLE_TRAVEL (Part 1)")
        time.sleep(2)  # Small delay
        
        # Second transaction in distant country (within 10 minutes)
        distant_location = random.choice(['USA', 'UK', 'Australia', 'Germany'])
        second_trans = {
            'transaction_id': str(uuid.uuid4()),
            'user_id': user_id,
            'timestamp': (datetime.now()).isoformat(),  # Within 10 minutes
            'merchant_category': random.choice(self.categories),
            'amount': round(random.uniform(30, 300), 2),
            'location': distant_location
        }
        
        return second_trans
    
    def generate_high_value_fraud(self, user_id):
        """Generate high-value fraud: transaction > $5000"""
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'user_id': user_id,
            'timestamp': datetime.now().isoformat(),
            'merchant_category': random.choice(['ELECTRONICS', 'TRAVEL', 'ENTERTAINMENT']),
            'amount': round(random.uniform(5001, 15000), 2),  # Over $5000
            'location': random.choice(self.countries)
        }
        return transaction
    
    def send_transaction(self, transaction, is_fraud=False, fraud_type="NORMAL"):
        """Send transaction to Kafka"""
        try:
            self.producer.send(
                'transactions',
                key=transaction['user_id'],
                value=transaction
            )
            
            # Color coding for terminal output
            if is_fraud:
                status = f"🚨 FRAUD - {fraud_type}"
                color = "\033[91m"  # Red
            else:
                status = "✓ NORMAL"
                color = "\033[92m"  # Green
            
            reset = "\033[0m"
            
            print(f"{color}{status}{reset} | "
                  f"User: {transaction['user_id'][:8]}... | "
                  f"Amount: ${transaction['amount']:>8.2f} | "
                  f"Category: {transaction['merchant_category']:<15} | "
                  f"Location: {transaction['location']:<15} | "
                  f"Time: {transaction['timestamp']}")
            
        except Exception as e:
            print(f"❌ Error sending transaction: {e}")
    
    def run(self, duration_seconds=300, transactions_per_second=2):
        """
        Run the producer for specified duration
        
        Args:
            duration_seconds: How long to run (default 5 minutes)
            transactions_per_second: Rate of transaction generation
        """
        start_time = time.time()
        transaction_count = 0
        fraud_count = 0
        
        # Generate pool of user IDs
        user_pool = [f"user_{i:04d}" for i in range(1, 51)]  # 50 users
        
        print(f"⏱️  Running for {duration_seconds} seconds...")
        print(f"📊 Transaction rate: {transactions_per_second} per second")
        print()
        
        try:
            while time.time() - start_time < duration_seconds:
                user_id = random.choice(user_pool)
                
                # Decide if this should be a fraud transaction (5% chance)
                fraud_probability = random.random()
                
                if fraud_probability < 0.02:  # 2% impossible travel
                    transaction = self.generate_impossible_travel_fraud(user_id)
                    self.send_transaction(transaction, is_fraud=True, fraud_type="IMPOSSIBLE_TRAVEL (Part 2)")
                    fraud_count += 1
                    
                elif fraud_probability < 0.05:  # 3% high value
                    transaction = self.generate_high_value_fraud(user_id)
                    self.send_transaction(transaction, is_fraud=True, fraud_type="HIGH_VALUE")
                    fraud_count += 1
                    
                else:  # 95% normal transactions
                    transaction = self.generate_normal_transaction(user_id)
                    self.send_transaction(transaction)
                
                transaction_count += 1
                
                # Control transaction rate
                time.sleep(1.0 / transactions_per_second)
            
        except KeyboardInterrupt:
            print("\n⚠️  Producer stopped by user")
        
        finally:
            # Summary statistics
            elapsed = time.time() - start_time
            print()
            print("=" * 70)
            print("📈 PRODUCER SUMMARY")
            print("=" * 70)
            print(f"⏱️  Duration: {elapsed:.2f} seconds")
            print(f"📊 Total Transactions: {transaction_count}")
            print(f"✓ Normal Transactions: {transaction_count - fraud_count}")
            print(f"🚨 Fraud Transactions: {fraud_count}")
            fraud_rate = (fraud_count / transaction_count * 100) if transaction_count > 0 else 0.0
            print(f"📉 Fraud Rate: {fraud_rate:.2f}%")
            print("=" * 70)
            
            self.producer.flush()
            self.producer.close()
            print("👋 Producer shut down successfully")


if __name__ == "__main__":
    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
    RUN_DURATION = 300  # 5 minutes (adjust as needed)
    TPS = 2  # Transactions per second
    
    # Create and run producer
    producer = TransactionProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    producer.run(duration_seconds=RUN_DURATION, transactions_per_second=TPS)