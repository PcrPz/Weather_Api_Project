import psycopg2
from datetime import datetime

def load(data):
    # เชื่อมต่อไปยัง PostgreSQL
    conn = psycopg2.connect(
        dbname="weather_api_db",
        user="user",
        password="password",
        host="weather-db",
        port="5432"
    )

    cursor = conn.cursor()

    # คำสั่ง SQL เพื่อ Insert ข้อมูล
    insert_query = """
        INSERT INTO weather_data (
            id, city_name, lon, lat, weather, temp, feels_like, humidity, pressure,
            sunrise, sunset, timestamp, forecast_temp, forecast_humid
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
    """


    data_tuple = (
        data['id'], data['city_name'], data['lon'], data['lat'], data['weather'],
        data['temp'], data['feels_like'], data['humidity'], data['pressure'],
        data['sunrise'], 
        data['sunset'],
        data['timestamp'], 
        data['forecast_temp'], data['forecast_humid']
    )

    cursor.execute(insert_query, data_tuple)
    conn.commit()
    cursor.close()
    conn.close()

    print("ข้อมูลถูกบันทึกลงในฐานข้อมูลแล้ว!")
    return
