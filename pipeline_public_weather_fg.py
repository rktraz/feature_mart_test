# Imports
import requests
import json
import datetime

import pandas as pd
import numpy as np

import hopsworks
import modal
from geopy.geocoders import Nominatim


LOCAL = False

if LOCAL == False:
    # Create a modal.Stub instance 
    stub = modal.Stub(name="pipeline_public_weather_fg")

    # Create a custom image
    image = modal.Image.debian_slim().pip_install(["pandas", "requests", "hopsworks", "geopy"]) 

    @stub.function(
        schedule=modal.Period(days=1), 
        image=image, 
        secret=modal.Secret.from_name("HOPSWORKS_API_KEY")
    )
    def modal_pipeline():        
        main()


def get_city_coordinates(city_name: str):
    """
    Takes city name and returns its latitude and longitude (rounded to 2 digits after dot).
    """   
    # Initialize Nominatim API (for getting lat and long of the city)
    geolocator = Nominatim(user_agent="MyApp")
    city = geolocator.geocode(city_name)

    latitude = round(city.latitude, 2)
    longitude = round(city.longitude, 2)
    
    return latitude, longitude


def get_weather_data(city_name: str,
                     start_date: str = None,
                     end_date: str = None,
                     forecast: bool = False):
    """
    Takes city name and returns pandas DataFrame with weather data.
    
    'start_date' and 'end_date' are required if you parse historical observations data. (forecast=False)
    
    If forecast=True - returns 7 days forecast data by default (without specifying daterange).
    """
    
    latitude, longitude = get_city_coordinates(city_name=city_name)
    
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'hourly': ['temperature_2m','relativehumidity_2m','precipitation',
                   'weathercode','windspeed_10m','winddirection_10m'],
        'start_date': start_date,
        'end_date': end_date
    }
    
    if forecast:
        # Historical forecast endpoint
        base_url = 'https://api.open-meteo.com/v1/forecast' 
    else:
        # Historical observations endpoint
        base_url = 'https://archive-api.open-meteo.com/v1/archive?' 
        
    response = requests.get(base_url, params=params)
    
    response_json = response.json()
    
    some_metadata = {key: response_json[key] for key in ('latitude', 'longitude',
                                                         'timezone', 'hourly_units')}
    
    res_df = pd.DataFrame(response_json["hourly"])
    
    res_df["forecast_hr"] = 0 # By default fill it with zeroes
    
    if forecast:
        res_df["forecast_hr"] = res_df.index
    
    some_metadata["city_name"] = city_name
    res_df["city_name"] = city_name
    
    # Rename columns
    res_df = res_df.rename(columns={
        "time": "base_time",
        "temperature_2m": "temperature",
        "relativehumidity_2m": "relative_humidity",
        "weathercode": "weather_code",
        "windspeed_10m": "wind_speed",
        "winddirection_10m": "wind_direction"
    })
    
    # Change columns order
    res_df = res_df[["city_name", "base_time", "forecast_hr", "temperature",
                     "relative_humidity", "weather_code", "wind_speed", "wind_direction"]]
    
    # Convert dates
    res_df["base_time"] = pd.to_datetime(res_df["base_time"])
    
    return res_df, some_metadata


def main():
    # Connect to Hopsworks FS
    project = hopsworks.login(project='weather')
    fs = project.get_feature_store() 

    weather_fg = fs.get_or_create_feature_group(
        name='weather_data',
        version=1
    )
        
    city_names = [
        'Kyiv',
        'London',
        'Paris',
        'Stockholm',
        'New_York',
        'Los_Angeles',
        'Singapore',
        'Sidney',
        'Hong_Kong',
        'Rome'
    ]
    # Get date parameters
    today = datetime.date.today() # datetime object

    day7next = str(today + datetime.timedelta(6))# "yyyy-mm-dd"
    day7ago = str(today - datetime.timedelta(7)) # "yyyy-mm-dd"

    # Parse and insert updated data from observations endpoint
    observations_batch = pd.DataFrame()
    for city_name in city_names:
        weather_df_temp, metadata_temp = get_weather_data(city_name, forecast=False,
                                                          start_date=day7ago, end_date=day7ago)
        observations_batch = pd.concat([observations_batch, weather_df_temp])
        
    weather_fg.insert(observations_batch, write_options={"wait_for_job": False})
    
    # Parse and insert new data from forecast endpoint for new day in future
    forecast_batch = pd.DataFrame()

    for city_name in city_names:
        weather_df_temp, metadata_temp = get_weather_data(city_name, forecast=True,
                                                          start_date=day7next, end_date=day7next)
        forecast_batch = pd.concat([forecast_batch, weather_df_temp])
    
    weather_fg.insert(forecast_batch, write_options={"wait_for_job": False})


if __name__ == "__main__":
    if LOCAL == True:
        main()
    else:
        stub.deploy("pipeline_public_weather_fg")
        with stub.run():
            modal_pipeline.call()