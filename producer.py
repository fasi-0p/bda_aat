from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# --- PASTE YOUR AIVEN CONNECTION STRING HERE ---
producer = KafkaProducer(
    bootstrap_servers='kafka-1eb56695-bda-aat1.h.aivencloud.com:14992',
    security_protocol='SSL',
    ssl_cafile='ca.pem',
    ssl_certfile='service.cert',
    ssl_keyfile='service.key',
    api_version=(2, 8, 1), 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

price = 65000.00
print("Sending live BTC data & VOLUME to Aiven... (Press Ctrl+C to stop)")

while True:
    price += random.uniform(-15, 15) 
    volume = random.randint(1, 100) # Generating fake volume
    
    data = {
        "symbol": "BTC",
        "price": round(price, 2),
        "volume": volume, # Added volume to the payload
        "timestamp": datetime.now().isoformat()
    }
    
    future = producer.send('crypto_ticks', value=data) 
    future.get(timeout=10) 
    
    print(f"Sent: {data}")
    time.sleep(1)