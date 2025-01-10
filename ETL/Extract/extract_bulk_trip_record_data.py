import functions_framework
import requests

from bs4 import BeautifulSoup
from datetime import datetime
from google.cloud import storage

HEADERS = {
        'Accept': '*/*',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0 '
    }
TRIP_DATA_URL = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
YEARS_TO_SCRAP = ['2022']


def get_current_year():
  """Retorna el año en curso
  """
  return datetime.now().year


def make_request(url: str):
  """Llama una HTTP request por medio de una URL & retorna su response

  Args:
      url (str): URL de la pagina web

  Returns:
      object: response de la url indicada
  """
  session = requests.session()
  return session.get(url, headers=HEADERS, stream=True)


def get_dataset_name(url: str):
  """_summary_

  Args:
      url (str): _description_

  Returns:
      str: nombre del dataset con su formato
  """

  url_dataset = url.split("/")

  return url_dataset[-1]


def save_file(response: object, name: str, year: str):
  """Guarda el contenido del response en un archivo parquet

  Args:
      response (object): Response con el dataset
      name (str): Nombre del dataset
  """
  ### Guardar en Local
  client = storage.Client()
  bucket = client.get_bucket('ncy-taxi-bucket')
  blob = bucket.blob('/'.join(['raw_datasets/trip_record_data',year,name])) 
  ## Use bucket.get_blob('path/to/existing-blob-name.txt') to write to existing blobs
  with blob.open(mode='wb') as file:
    for chunk in response.iter_content(chunk_size=8192):
      file.write(chunk)
  print(f'El Dataset {name} se guardo correctamente')




def bulk_extract_year(year: str, soup: object):
  """Extrae todos los dataset del año indicado.

  Args:
      year (str): año a extraer los dataset
      soup (object): objeto de BeautifulSoup del response.text
  """

  div_id = ''.join(['faq',year])

  div_year = soup.find('div', id=div_id)
  a_labels_datasets = div_year('a')

  for a_label in a_labels_datasets:
    url_dataset = a_label['href'].strip()

    name_dataset = get_dataset_name(url_dataset)

    file_response = make_request(url_dataset)

    if file_response.status_code == 200:
        save_file(file_response, name_dataset, year)
    else:
        print(f'ERROR: El Dataset {name_dataset} No esta disponible')
  return print(f'La extraccion de datasets corresponidente al {year} termino')



@functions_framework.http
def hello_http(request):
    
  trip_data_response = make_request(TRIP_DATA_URL)

  if trip_data_response.status_code == 200:
    soup = BeautifulSoup(trip_data_response.text, 'html.parser')

    for year in YEARS_TO_SCRAP:
      bulk_extract_year(year, soup)
  else:
    print(f"No se logro acceder al sitio! Respuesta del Server:{trip_data_response.status_code}")

  return f'check the results in the logs'
