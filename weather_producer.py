from kafka import KafkaProducer
import requests
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('API_KEY')
LAT = os.getenv('LAT')
LON = os.getenv('LON')
TOPIC = os.getenv('TOPIC')
KAFKA_SERVER = os.getenv('KAFKA_SERVER')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather():
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    return response.json()


while True:
    try:
        weather_data = get_weather()
        producer.send(TOPIC, weather_data)
        print(f"Sent weather data: {weather_data['weather'][0]['description']} at {weather_data['main']['temp']}°C")
        time.sleep(2) 
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)
