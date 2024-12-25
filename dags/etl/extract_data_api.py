import requests
import psycopg2
from psycopg2 import sql
import json


def create_table():
    conn = psycopg2.connect(
    dbname="weather_api_db",
    user="user",
    password="password",
    host="weather-db",
    port="5432"
    )

    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id INT,
        city_name VARCHAR(255),
        lon FLOAT,
        lat FLOAT,
        weather VARCHAR(255),
        temp FLOAT,
        feels_like FLOAT,
        humidity INT,
        pressure INT,
        sunrise TIMESTAMP,
        sunset TIMESTAMP,
        timestamp TIMESTAMP PRIMARY KEY,
        forecast_temp VARCHAR(255),
        forecast_humid VARCHAR(255)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

def get_data(url):
    response = requests.get(url)
    data = {}
    if response.status_code == 200:
        data = response.json()
        weather_data={
            'id':data["id"],
            'city_name': data["name"],
            'lon':data["coord"]["lon"],
            'lat': data["coord"]["lat"],
            'weather': data["weather"][0]["main"],
            'temp':data["main"]["temp"],
            'feels_like':data["main"]["feels_like"],
            'humidity':data["main"]["humidity"],
            'pressure': data["main"]["pressure"],
            "sunrise" : data["sys"]["sunrise"],
            "sunset" : data["sys"]["sunset"],
        }
    else:
        print(f"Error fetching data: {response.status_code}")
    create_table()
    return weather_data
