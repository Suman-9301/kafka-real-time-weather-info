from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv

load_dotenv()

TOPIC = os.getenv('TOPIC')
KAFKA_SERVER = os.getenv('KAFKA_SERVER')

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='weather-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for weather data...")

for message in consumer:
    data = message.value
    city = data.get('name')
    temp = data['main']['temp']
    description = data['weather'][0]['description']
    print(f"Weather in {city}: {description}, {temp}°C")
