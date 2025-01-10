import functions_framework
import csv

from datetime import datetime
from sodapy import Socrata
from google.cloud import storage

DATASET_URL = "data.cityofnewyork.us"
DATASET_ID = "rhe8-mgbb"
YEARS = ["2022","2023", "2024"]
COLUMNS = ['license_number', 'name', 'type', 'current_status', 'dmv_license_plate_number', 'vehicle_vin_number', 'vehicle_type', 'model_year', 'medallion_type', 'agent_number', 'agent_name', 'agent_telephone_number', 'agent_address', 'last_updated_date', 'last_updated_time']

def get_current_year():
    """Retorna el año en curso
    """
    return str(datetime.now().year)

def get_current_month():
    """Retorna el año en curso
    """
    return str(datetime.now().month)

@functions_framework.http
def hello_http(request):
    client_soda = Socrata(DATASET_URL, None)

    for year in YEARS:

        if year == get_current_year():
            months = int(get_current_month())
        else:
            months = 13

        for month in range(1,months):
            year_month = year + '-' + str(month)
            where_query = "".join(["last_updated_date = '",year_month,"-01T00:00:00.000'"])
            results = client_soda.get(DATASET_ID, where=where_query, limit=20000, order="last_updated_date DESC")
            field_names = COLUMNS

            client_gcs = storage.Client()
            bucket = client_gcs.get_bucket('ncy-taxi-bucket')
            csv_name = "_".join([year_month,"active_medallion_vehicles.csv"])
            blob = bucket.blob('/'.join(['raw_datasets/active_medallion_vehicles',year,csv_name])) 

            with blob.open(mode="w", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=field_names)
                writer.writeheader()
                writer.writerows(results)
            print(f"El archivo {year_month}-active_medallion_vehicles.csv se guardo")

    return f'check the results in the logs'
