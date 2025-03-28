import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

def get_data():
    import requests

    res = requests.get("https://api.openweathermap.org/data/2.5/weather?lat=39.28&lon=-2.79&appid=4e1e0e5bd9fb4fc23c7f28ce5cd97264")
    res = res.json()
    #res = res['results'][0]

    return res

def format_data(res):
    data = {}
    data['id'] = uuid.uuid4()
    data['timestampUTC'] = res['dt']
    data['timezone'] = res['timezone']
    data['location'] = res['name']
    data['weather'] = res['weather'][0]['main']
    data['weather_description'] = res['weather'][0]['description']
    data['temperature'] = res['main']['temp']
    data['thermal_sensation'] = res['main']['feels_like']
    data['temp_max'] = res['main']['temp_max']
    data['temp_min'] = res['main']['temp_min']
    data['pressure'] = res['main']['grnd_level']
    data['humidity'] = res['main']['humidity']
    data['visibility'] = res['visibility']
    data['wind_speed'] = res['wind']['speed']
    data['wind_deg'] = res['wind']['deg']
    data['wind_gust'] = res['wind']['gust']
    data['clouds'] = res['clouds']['all']
    data['precipitation'] = res['rain']['1h'] if 'rain' in res and '1h' in res['rain'] else 0
    data['sunrise_utc'] = res['sys']['sunrise']
    data['sunset_utc'] = res['sys']['sunset']
    return data


"""
def format_data(res):
    data = {}
    location = res['location']
    data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data
"""


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)
            res = {k: str(v) if isinstance(v, uuid.UUID) else v for k, v in res.items()}

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

"""
with DAG('user_automation',
         schedule_interval= '@daily',
         start_date=datetime(2024,6,29),
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

"""