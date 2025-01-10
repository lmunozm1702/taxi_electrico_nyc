import functions_framework
import openmeteo_requests
import requests_cache
import pandas as pd
import csv
import calendar

from retry_requests import retry
from io import StringIO
from google.cloud import storage
from datetime import datetime

def get_year():
    """Retorna el año en curso
    """

    if datetime.now().month == 1:
        year = datetime.now().year - 1
    else:
        year = datetime.now().year

    return str(year)

def get_prev_month():
    """Retorna el año en curso
    """

    if datetime.now().month == 1:
        month = 12
    else:
        month = datetime.now().month - 1

    return str(month)

def get_last_day():
    """Retorna el ultimo dia del mes
    """

    return str(calendar.monthrange(int(get_year()), int(get_prev_month()))[1])


@functions_framework.http
def hello_http(request):
   # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    year = get_year()
    year_month = get_year() + '-' + get_prev_month()

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    latitudes = [40.6815, 40.6501, 40.7834, 40.8499, 40.5623]
    longitudes = [-73.8365, -73.9496, -73.9663, -73.8664, -74.1399]
    boroughs = ["Queens", "Brooklyn", "Manhattan", "Bronx", "Staten Island"]
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "start_date": "-".join([year_month,"01"]),
        "end_date": "-".join([year_month,get_last_day()]),
        "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "rain", "snowfall", "weather_code", "pressure_msl", "cloud_cover", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m"],
        "timezone": "America/New_York"
    }
    responses = openmeteo.weather_api(url, params=params)

    # Initialize an empty list to store the data for all boroughs
    data_list = []

    # Process each borough
    for i, response in enumerate(responses):
        hourly = response.Hourly()
        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
            "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
            "dew_point_2m": hourly.Variables(2).ValuesAsNumpy(),
            "apparent_temperature": hourly.Variables(3).ValuesAsNumpy(),
            "rain": hourly.Variables(4).ValuesAsNumpy(),
            "snowfall": hourly.Variables(5).ValuesAsNumpy(),
            "weather_code": hourly.Variables(6).ValuesAsNumpy(),
            "pressure_msl": hourly.Variables(7).ValuesAsNumpy(),
            "cloud_cover": hourly.Variables(8).ValuesAsNumpy(),
            "wind_speed_10m": hourly.Variables(9).ValuesAsNumpy(),
            "wind_direction_10m": hourly.Variables(10).ValuesAsNumpy(),
            "wind_gusts_10m": hourly.Variables(11).ValuesAsNumpy(),
            "borough": boroughs[i]
        }
        data_list.append(pd.DataFrame(hourly_data))

    # Concatenate all the dataframes into one
    df_clima = pd.concat(data_list, ignore_index=True)

    client_gcs = storage.Client()
    bucket = client_gcs.get_bucket('ncy-taxi-bucket')
    csv_name = "_".join([year_month,"weather.csv"])
    blob = bucket.blob('/'.join(['raw_datasets/weather',year,csv_name])) 

    csv_buffer = df_clima.to_csv(index=False)

    blob.upload_from_string(csv_buffer, content_type="text/csv")

    print(f"Archivo Guardado en: gs://{bucket}/{blob}")

    return f'check the results in the logs'
