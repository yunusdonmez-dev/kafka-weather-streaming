import json
import time
import requests
import os                                   
from dotenv import load_dotenv               
from kafka import KafkaProducer

# 1. Configuration
API_KEY = os.getenv("WEATHER_API_KEY")
CITY = "Amsterdam"
KAFKA_TOPIC = "weather_data"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# 2. Initialize Kafka Producer
# We use a 'value_serializer' to ensure data is sent as JSON bytes
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather_data():
    """Fetches real-time weather data from API"""
    url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}&aqi=no"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Extracting specific fields as per instructions
        return {
            "city": data['location']['name'],
            "temperature_c": data['current']['temp_c'],
            "humidity": data['current']['humidity'],
            "wind_kph": data['current']['wind_kph'],
            "local_time": data['location']['localtime'],
            "last_updated": data['current']['last_updated']
        }
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

# 3. Main Loop
print(f"Starting producer for city: {CITY}...")
try:
    while True:
        weather_report = get_weather_data()
        
        if weather_report:
            # Send data to Kafka
            producer.send(KAFKA_TOPIC, value=weather_report)
            print(f"Sent data: {weather_report}")
        
        # Wait 60 seconds before next fetch
        time.sleep(60)
except KeyboardInterrupt:
    print("Stopping producer...")
    producer.close()