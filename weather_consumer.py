from kafka import KafkaConsumer
import json

TOPIC = 'test-topic'  # Replace with your topic name
KAFKA_SERVER = 'localhost:9092'

# === KAFKA CONSUMER ===
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
