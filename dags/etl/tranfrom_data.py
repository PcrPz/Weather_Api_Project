import os
from datetime import datetime

def check_missing(data):
    for key, value in data.items():
        if value is None or value == '':
            if isinstance(value, (int, float)):
                data[key] = 0
            else:
                data[key] = "Unknown" 


def clear_time(data):
    data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data["sunrise"] = datetime.fromtimestamp(data["sunrise"]).strftime('%Y-%m-%d %H:%M:%S')
    data["sunset"] = datetime.fromtimestamp(data["sunset"]).strftime('%Y-%m-%d %H:%M:%S')

def forecast_weather(data):
    temp = data['temp']
    humidity = data['humidity']
    
    if temp > 30:
        forecast_temp = "Hot"
    elif 20 <= temp <= 30:
        forecast_temp = "Warm"
    else:
        forecast_temp = "Cold"

    data['forecast_temp'] = forecast_temp

    if humidity > 80:
        forecast_humid = "Perhap rain"
    elif humidity < 40:
        forecast_humid = "Dry"
    else:
        forecast_humid = "Medium"
        
    data['forecast_humid'] = forecast_humid
    return data


def tranform_data_df(data):
    check_missing(data)
    clear_time(data)
    data["temp"] = data["temp"] - 273.15
    data["feels_like"] = data["feels_like"] - 273.15
    forecast_weather(data)
    return data

