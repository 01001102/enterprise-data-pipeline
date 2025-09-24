#!/usr/bin/env python3
"""
Enterprise Data Producer for AWS Kinesis Streams

This module provides a comprehensive data generator for simulating enterprise-grade
data streams including customer events, transactions, product interactions, and
system logs. Designed for testing and development of real-time data pipelines.

Author: Ivan de França
Version: 1.0.0
License: MIT

Example:
    Basic usage:
        >>> producer = EnterpriseDataProducer()
        >>> producer.create_streams()
        >>> producer.start_streaming(duration_minutes=30)

Attributes:
    STREAM_NAMES (list): Default Kinesis stream names
    EVENT_TYPES (dict): Mapping of event types to generation methods
"""

import json
import boto3
import time
import random
from datetime import datetime, timedelta
from faker import Faker
import uuid
from typing import Dict, Any, Optional, List


class EnterpriseDataProducer:
    """
    Enterprise-grade data producer for AWS Kinesis streams.
    
    Generates realistic business data including customer events, financial transactions,
    product interactions, and system logs for testing data pipelines.
    
    Args:
        endpoint_url (str): AWS endpoint URL (default: LocalStack)
        region_name (str): AWS region (default: us-east-1)
        
    Attributes:
        kinesis: Boto3 Kinesis client
        fake: Faker instance for generating realistic data
        stream_names: List of Kinesis stream names
    """
    
    STREAM_NAMES = [
        'customer-events',
        'transaction-events', 
        'product-events',
        'system-logs'
    ]
    
    def __init__(self, endpoint_url: str = 'http://localhost:4566', 
                 region_name: str = 'us-east-1') -> None:
        """
        Initialize the data producer with AWS Kinesis client.
        
        Args:
            endpoint_url: AWS endpoint (LocalStack for local development)
            region_name: AWS region name
        """
        self.kinesis = boto3.client(
            'kinesis',
            endpoint_url=endpoint_url,
            region_name=region_name,
            aws_access_key_id='test',
            aws_secret_access_key='test'
        )
        self.fake = Faker()
        self.stream_names = self.STREAM_NAMES
        
    def create_streams(self, shard_count: int = 2) -> None:
        """
        Create Kinesis streams for data ingestion.
        
        Creates all required streams with specified shard count for parallel processing.
        Handles existing streams gracefully.
        
        Args:
            shard_count: Number of shards per stream (default: 2)
            
        Raises:
            Exception: If stream creation fails for reasons other than already existing
        """
        for stream in self.stream_names:
            try:
                self.kinesis.create_stream(
                    StreamName=stream,
                    ShardCount=shard_count
                )
                print(f"✓ Stream {stream} created successfully")
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"ℹ Stream {stream} already exists")
                else:
                    print(f"✗ Error creating stream {stream}: {e}")
    
    def generate_customer_event(self) -> Dict[str, Any]:
        """
        Generate realistic customer interaction events.
        
        Creates events for user lifecycle activities including registration,
        authentication, profile management, and session activities.
        
        Returns:
            Dict containing customer event data with fields:
            - event_id: Unique event identifier
            - customer_id: Customer identifier
            - action: Type of customer action
            - timestamp: Event timestamp in ISO format
            - metadata: Device, location, and session information
        """
        actions = ['signup', 'login', 'profile_update', 'logout', 'password_reset']
        
        return {
            'event_id': str(uuid.uuid4()),
            'event_type': 'customer_event',
            'timestamp': datetime.now().isoformat(),
            'customer_id': f"cust_{random.randint(1000, 9999)}",
            'action': random.choice(actions),
            'email': self.fake.email(),
            'country': self.fake.country_code(),
            'device_type': random.choice(['mobile', 'desktop', 'tablet']),
            'session_id': str(uuid.uuid4())[:8],
            'ip_address': self.fake.ipv4(),
            'user_agent': self.fake.user_agent()
        }
    
    def generate_transaction_event(self) -> Dict[str, Any]:
        """
        Generate realistic financial transaction events.
        
        Creates comprehensive transaction data including payment processing,
        fraud detection scores, and merchant information for financial analytics.
        
        Returns:
            Dict containing transaction event data with fields:
            - transaction_id: Unique transaction identifier
            - amount: Transaction amount with currency
            - payment_method: Payment processing method
            - status: Transaction processing status
            - fraud_score: ML-based fraud detection score (0-1)
            - merchant_info: Merchant and category details
        """
        status_options = ['completed', 'pending', 'failed', 'cancelled']
        payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']
        
        return {
            'event_id': str(uuid.uuid4()),
            'event_type': 'transaction_event',
            'timestamp': datetime.now().isoformat(),
            'transaction_id': f"txn_{random.randint(100000, 999999)}",
            'customer_id': f"cust_{random.randint(1000, 9999)}",
            'amount': round(random.uniform(10.0, 5000.0), 2),
            'currency': random.choice(['USD', 'EUR', 'BRL', 'GBP']),
            'status': random.choice(status_options),
            'payment_method': random.choice(payment_methods),
            'merchant_id': f"merchant_{random.randint(100, 999)}",
            'category': random.choice(['electronics', 'clothing', 'food', 'books', 'travel']),
            'fraud_score': round(random.uniform(0.0, 1.0), 3)
        }
    
    def generate_product_event(self):
        """Gera eventos de produtos"""
        actions = ['view', 'add_to_cart', 'remove_from_cart', 'purchase', 'review']
        
        return {
            'event_id': str(uuid.uuid4()),
            'event_type': 'product_event',
            'timestamp': datetime.now().isoformat(),
            'product_id': f"prod_{random.randint(1000, 9999)}",
            'customer_id': f"cust_{random.randint(1000, 9999)}",
            'action': random.choice(actions),
            'category': random.choice(['electronics', 'clothing', 'home', 'sports', 'books']),
            'price': round(random.uniform(5.0, 2000.0), 2),
            'quantity': random.randint(1, 10),
            'rating': random.randint(1, 5) if random.random() > 0.7 else None,
            'session_id': str(uuid.uuid4())[:8]
        }
    
    def generate_system_log(self):
        """Gera logs de sistema"""
        log_levels = ['INFO', 'WARN', 'ERROR', 'DEBUG']
        services = ['api-gateway', 'user-service', 'payment-service', 'inventory-service']
        
        return {
            'event_id': str(uuid.uuid4()),
            'event_type': 'system_log',
            'timestamp': datetime.now().isoformat(),
            'service': random.choice(services),
            'level': random.choice(log_levels),
            'message': self.fake.sentence(),
            'request_id': str(uuid.uuid4())[:8],
            'response_time_ms': random.randint(10, 5000),
            'status_code': random.choice([200, 201, 400, 401, 404, 500]),
            'endpoint': f"/api/v1/{self.fake.word()}",
            'user_id': f"user_{random.randint(1000, 9999)}" if random.random() > 0.3 else None
        }
    
    def send_to_kinesis(self, stream_name: str, data: Dict[str, Any]) -> Optional[Dict]:
        """
        Send event data to specified Kinesis stream.
        
        Handles JSON serialization and partition key selection for optimal
        data distribution across shards.
        
        Args:
            stream_name: Target Kinesis stream name
            data: Event data dictionary to send
            
        Returns:
            Kinesis response dict if successful, None if failed
            
        Raises:
            Exception: If Kinesis put_record operation fails
        """
        try:
            response = self.kinesis.put_record(
                StreamName=stream_name,
                Data=json.dumps(data, default=str),
                PartitionKey=data.get('customer_id', data.get('event_id', 'default'))
            )
            return response
        except Exception as e:
            print(f"✗ Error sending to {stream_name}: {e}")
            return None
    
    def start_streaming(self, duration_minutes: int = 60, 
                       events_per_minute: int = 100) -> None:
        """
        Start continuous data streaming to Kinesis streams.
        
        Generates and sends realistic business events at configurable rates
        with built-in randomization to simulate real-world data patterns.
        
        Args:
            duration_minutes: How long to run the streaming (default: 60)
            events_per_minute: Target events per minute (default: 100)
            
        Note:
            - Events are distributed across different streams
            - Includes realistic timing variations
            - Provides real-time progress feedback
        """
        print(f"🚀 Starting data streaming for {duration_minutes} minutes...")
        print(f"📊 Target rate: ~{events_per_minute} events/minute")
        
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        event_count = 0
        
        while datetime.now() < end_time:
            # Generate different types of events
            events = [
                (self.generate_customer_event(), 'customer-events'),
                (self.generate_transaction_event(), 'transaction-events'),
                (self.generate_product_event(), 'product-events'),
                (self.generate_system_log(), 'system-logs')
            ]
            
            # Send events with different frequencies
            for event_data, stream_name in events:
                if random.random() < 0.8:  # 80% chance to send
                    response = self.send_to_kinesis(stream_name, event_data)
                    if response:
                        event_count += 1
                        print(f"✓ [{event_count:04d}] {event_data['event_type']} → {stream_name}")
            
            # Variable interval between batches
            sleep_time = 60.0 / events_per_minute * random.uniform(0.5, 1.5)
            time.sleep(sleep_time)
        
        print(f"🏁 Streaming completed! Total events sent: {event_count}")

def main() -> None:
    """
    Main execution function for the data producer.
    
    Initializes the producer, creates streams, and starts data generation
    with enterprise-grade error handling and logging.
    """
    try:
        print("🏭 Enterprise Data Producer v1.0.0")
        print("📡 Initializing Kinesis connection...")
        
        producer = EnterpriseDataProducer()
        
        # Create streams
        print("🔧 Creating Kinesis streams...")
        producer.create_streams()
        time.sleep(5)  # Wait for stream creation
        
        # Start streaming
        print("🎯 Starting data generation...")
        producer.start_streaming(duration_minutes=30)
        
    except KeyboardInterrupt:
        print("\n⏹️ Streaming stopped by user")
    except Exception as e:
        print(f"❌ Error in data producer: {e}")
        raise


if __name__ == "__main__":
    main()