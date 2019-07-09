from chalice import Chalice
import requests as rq
import boto3
import os
import json
from datetime import date, timedelta
NYCOD = os.environ.get('NYCOPENDATA', None)
BUCKET, KEY = 'philipp-packt', '311data'
resource = 'fhrw-4uyv'
time_col = 'Created Date'


def _upload_json(obj, filename, bucket, key):
    S3 = boto3.client('s3', region_name='us-east-1')
    key += ('/' + filename)

    S3.Object(Bucket=bucket, Key=key).put(Body=json.dumps(obj))


def _get_data(resource, time_col, date, offset=0):
    '''collect data from NYC open data
    '''
          
    Q = f"where=created_date between '{date:%Y-%m-%d}' AND '{date:%Y-%m-%d}T23:59:59.000'"
    url = f'https://data.cityofnewyork.us/resource/{resource}.json?$limit=50000&$offset={offset}&${Q}'

    headers = {"X-App-Token": NYCOD} if NYCOD else None
    r = rq.get(url, headers=headers)
    r.raise_for_status()

    data = r.json()
    if len(data) == 50_000:
        offset2 = offset + 50000
        data2 = _get_data(resource, time_col, date, offset=offset2)
        data.extend(data2)

    return data

app = Chalice(app_name='collect-311')


@app.schedule('rate(1 day)')
def get_data(event):
    yesterday = date.today() - timedelta(days=1)
    data = _get_data(resource, time_col, yesterday, offset=0)
    _upload_json(data, f'{yesterday:%Y-%m-%d}.json', bucket=BUCKET, key=KEY)