from kafka import KafkaProducer
import requests
import json
import time

# === CONFIGURATION ===
API_KEY = '859e8379a8420db607bb664b1464b15d'  # Replace with your OpenWeatherMap API key
LAT = '28.6139'                # Example: New Delhi
LON = '77.2090'
TOPIC = 'test-topic'
KAFKA_SERVER = 'localhost:9092'

# === KAFKA PRODUCER ===
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather():
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    return response.json()

# === PRODUCE DATA CONTINUOUSLY ===
while True:
    try:
        weather_data = get_weather()
        producer.send(TOPIC, weather_data)
        print(f"Sent weather data: {weather_data['weather'][0]['description']} at {weather_data['main']['temp']}°C")
        time.sleep(10)  # Wait 10 seconds before next fetch
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)
