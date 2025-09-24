import json
import boto3
import time
import random
from datetime import datetime, timedelta
from faker import Faker
import uuid

class EnterpriseDataProducer:
    def __init__(self, endpoint_url='http://localhost:4566'):
        self.kinesis = boto3.client(
            'kinesis',
            endpoint_url=endpoint_url,
            region_name='us-east-1',
            aws_access_key_id='test',
            aws_secret_access_key='test'
        )
        self.fake = Faker()
        
    def create_streams(self):
        """Cria streams Kinesis"""
        streams = [
            'customer-events',
            'transaction-events', 
            'product-events',
            'system-logs'
        ]
        
        for stream in streams:
            try:
                self.kinesis.create_stream(
                    StreamName=stream,
                    ShardCount=2
                )
                print(f"Stream {stream} criado")
            except Exception as e:
                print(f"Stream {stream} já existe ou erro: {e}")
    
    def generate_customer_event(self):
        """Gera eventos de clientes"""
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
    
    def generate_transaction_event(self):
        """Gera eventos de transações"""
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
    
    def send_to_kinesis(self, stream_name, data):
        """Envia dados para Kinesis"""
        try:
            response = self.kinesis.put_record(
                StreamName=stream_name,
                Data=json.dumps(data),
                PartitionKey=data.get('customer_id', data.get('event_id'))
            )
            return response
        except Exception as e:
            print(f"Erro ao enviar para {stream_name}: {e}")
            return None
    
    def start_streaming(self, duration_minutes=60):
        """Inicia streaming de dados"""
        print(f"Iniciando streaming por {duration_minutes} minutos...")
        
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        
        while datetime.now() < end_time:
            # Gera diferentes tipos de eventos
            events = [
                (self.generate_customer_event(), 'customer-events'),
                (self.generate_transaction_event(), 'transaction-events'),
                (self.generate_product_event(), 'product-events'),
                (self.generate_system_log(), 'system-logs')
            ]
            
            # Envia eventos com frequências diferentes
            for event_data, stream_name in events:
                if random.random() < 0.8:  # 80% chance de enviar
                    response = self.send_to_kinesis(stream_name, event_data)
                    if response:
                        print(f"✓ {event_data['event_type']} → {stream_name}")
            
            # Intervalo entre batches
            time.sleep(random.uniform(0.5, 2.0))
        
        print("Streaming finalizado!")

if __name__ == "__main__":
    producer = EnterpriseDataProducer()
    
    # Criar streams
    producer.create_streams()
    time.sleep(5)  # Aguarda criação
    
    # Iniciar streaming
    producer.start_streaming(duration_minutes=30)