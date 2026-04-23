from kafka import KafkaConsumer
import json
import csv
import os

# --- PASTE YOUR AIVEN CONNECTION STRING HERE ---
consumer = KafkaConsumer(
    'crypto_ticks', 
    bootstrap_servers='kafka-1eb56695-bda-aat1.h.aivencloud.com:14992',
    security_protocol='SSL',
    ssl_cafile='ca.pem',
    ssl_certfile='service.cert',
    ssl_keyfile='service.key',
    api_version=(2, 8, 1), 
    group_id='my_aat_demo_group',     
    auto_offset_reset='earliest',     
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Native Python Processor connected. Processing stream with Volume...")

os.makedirs('./output_data', exist_ok=True)
csv_file = './output_data/stream.csv'

with open(csv_file, 'a', newline='') as f:
    writer = csv.writer(f)
    for message in consumer:
        data = message.value
        
        # --- THE FIX: Data Validation ---
        # If it's an old message missing the volume data, just skip it!
        if 'volume' not in data:
            continue
            
        # Writing 4 columns now: Symbol, Price, Volume, Timestamp
        writer.writerow([data['symbol'], data['price'], data['volume'], data['timestamp']])
        f.flush() 
        print(f"Processed: {data['symbol']} @ ${data['price']} | Vol: {data['volume']}")