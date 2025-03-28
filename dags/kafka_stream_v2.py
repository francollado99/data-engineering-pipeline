import uuid
from datetime import datetime, timedelta
import json
import pprint
from airflow import DAG
import requests
from confluent_kafka import Producer
from airflow.operators.python import PythonOperator

def get_data(**kwargs):
    try:
        api_url = "https://api.openweathermap.org/data/2.5/weather?lat=39.28&lon=-2.79&appid=4e1e0e5bd9fb4fc23c7f28ce5cd97264"
        response = requests.get(api_url)
        weather_data = response.json()
        weather_data_formatted = format_data(weather_data)

        kwargs['ti'].xcom_push(key='weather_data', value=weather_data_formatted)

        return weather_data_formatted
    except Exception as e:
        print(f"Fail connecting to API: {str(e)}")
        return None

def print_json(**kwargs):
    ti = kwargs["ti"]
    json_data = ti.xcom_pull(task_ids="fetch_weather_task", key='weather_data')
    if json_data:
        pprint.pprint(json_data)
    else:
        print("Couldn't obtain JSON")

def json_serialization(**kwargs):
    ti = kwargs["ti"]
    json_data = ti.xcom_pull(task_ids="fetch_weather_task", key='weather_data')
    if json_data:
        producer_config = {
            "bootstrap.servers": "broker:29092",
            "client.id": "airflow"
        }

        producer = Producer(producer_config)
        json_message = json.dumps(json_data).encode('utf-8')
        producer.produce('airflow-spark', value=json_message)
        producer.flush()
    else:
        print("Couldn't obtain JSON")

def format_data(res):
    data = {}
    data['id'] = str(uuid.uuid4())
    data['timestamputc'] = res['dt']
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

dag = DAG('mi_dag',
    schedule_interval=timedelta(seconds=5),
    start_date=datetime(2024, 6, 29),
)

task1 = PythonOperator(
    task_id="fetch_weather_task",
    python_callable=get_data,
    dag=dag,
    provide_context=True,
)

task2 = PythonOperator(
    task_id="print_logs",
    python_callable=print_json,
    dag=dag,
    provide_context=True,
)

task3 = PythonOperator(
    task_id="send_task_to_kafka-spark",
    python_callable=json_serialization,
    dag=dag,
    provide_context=True,
)

task1 >> task2 >> task3
