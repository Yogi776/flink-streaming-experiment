#!/usr/bin/env python3
"""
Kafka Data Generator for Flink Streaming Demo
Generates mock e-commerce transaction data and sends to Kafka
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import os

class MockDataGenerator:
    def __init__(self, kafka_servers="localhost:9092"):
        self.fake = Faker()
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Product categories and their price ranges
        self.categories = {
            "Electronics": (50, 2000),
            "Clothing": (20, 300),
            "Books": (10, 80),
            "Home & Garden": (25, 500),
            "Sports": (30, 400),
            "Beauty": (15, 150)
        }
        
        self.payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
        self.user_segments = ["premium", "regular", "new"]

    def generate_transaction(self):
        """Generate a single transaction record"""
        category = random.choice(list(self.categories.keys()))
        min_price, max_price = self.categories[category]
        
        transaction = {
            "transaction_id": self.fake.uuid4(),
            "user_id": f"user_{random.randint(1000, 9999)}",
            "user_segment": random.choice(self.user_segments),
            "product_id": f"prod_{random.randint(10000, 99999)}",
            "product_name": self.fake.catch_phrase(),
            "category": category,
            "price": round(random.uniform(min_price, max_price), 2),
            "quantity": random.randint(1, 5),
            "payment_method": random.choice(self.payment_methods),
            "country": self.fake.country_code(),
            "city": self.fake.city(),
            "timestamp": datetime.now().isoformat(),
            "is_fraud": random.random() < 0.02  # 2% fraud rate
        }
        
        # Calculate total amount
        transaction["total_amount"] = round(transaction["price"] * transaction["quantity"], 2)
        
        return transaction

    def generate_user_activity(self):
        """Generate user activity events"""
        activities = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "search"]
        
        activity = {
            "event_id": self.fake.uuid4(),
            "user_id": f"user_{random.randint(1000, 9999)}",
            "session_id": self.fake.uuid4()[:8],
            "activity_type": random.choice(activities),
            "page_url": self.fake.url(),
            "user_agent": self.fake.user_agent(),
            "ip_address": self.fake.ipv4(),
            "timestamp": datetime.now().isoformat(),
            "duration_seconds": random.randint(5, 300)
        }
        
        return activity

    def generate_sensor_data(self):
        """Generate IoT sensor data"""
        sensor_types = ["temperature", "humidity", "pressure", "light"]
        locations = ["warehouse_a", "warehouse_b", "store_front", "storage_room"]
        
        sensor_data = {
            "sensor_id": f"sensor_{random.randint(100, 999)}",
            "sensor_type": random.choice(sensor_types),
            "location": random.choice(locations),
            "value": round(random.uniform(0, 100), 2),
            "unit": "celsius" if random.choice(sensor_types) == "temperature" else "percent",
            "timestamp": datetime.now().isoformat(),
            "battery_level": random.randint(10, 100),
            "status": "normal" if random.random() > 0.1 else "warning"
        }
        
        return sensor_data

    def run(self):
        """Main loop to generate and send data"""
        print("Starting data generation...")
        print(f"Kafka servers: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
        
        try:
            while True:
                # Generate different types of events with different frequencies
                
                # Transactions (most frequent)
                if random.random() < 0.6:
                    transaction = self.generate_transaction()
                    self.producer.send('transactions', 
                                     key=transaction['user_id'], 
                                     value=transaction)
                    print(f"Sent transaction: {transaction['transaction_id']} - ${transaction['total_amount']}")
                
                # User activities
                if random.random() < 0.8:
                    activity = self.generate_user_activity()
                    self.producer.send('user_activities', 
                                     key=activity['user_id'], 
                                     value=activity)
                    print(f"Sent activity: {activity['activity_type']} by {activity['user_id']}")
                
                # Sensor data
                if random.random() < 0.4:
                    sensor = self.generate_sensor_data()
                    self.producer.send('sensor_data', 
                                     key=sensor['sensor_id'], 
                                     value=sensor)
                    print(f"Sent sensor data: {sensor['sensor_id']} - {sensor['value']} {sensor['unit']}")
                
                # Flush producer to ensure delivery
                self.producer.flush()
                
                # Wait before next batch
                time.sleep(random.uniform(0.5, 2.0))
                
        except KeyboardInterrupt:
            print("\nStopping data generation...")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            self.producer.close()

if __name__ == "__main__":
    # Get Kafka servers from environment variable
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    # Wait for Kafka to be ready
    print("Waiting for Kafka to be ready...")
    time.sleep(30)
    
    generator = MockDataGenerator(kafka_servers)
    generator.run()
