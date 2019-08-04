from chalice import Chalice
import requests as rq
import boto3
import json
from datetime import date, timedelta, datetime
from statistics import median
BUCKET, FOLDER, MEDIANS_FOLDER = 'philipp-packt', '311/raw_data', '311/'
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
    r = rq.get(url)
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
    _upload_json(data, f'{yesterday:%Y-%m-%d}.json', bucket=BUCKET, key=FOLDER)


def _is_dataset(key):
    '''check if triggered by data we're interested in '''
    return ('311data' in key) and key.endswith('.json')


def _calc_medians(data):

    results = {}
    for record in data:
        ct = record["complaint_type"]
        if ct not in results:
            results[ct] = []
        
        spent = datetime.strptime(record['closed_date'], '%Y-%m-%d %H:%M:%S') - datetime.strptime(record['created_date'], '%Y-%m-%d %H:%M:%S')
        spent = spent.seconds // 3600 # hours
        results[ct].append(spent)
        
    return {k : median(v) for k, v in results.items()}


def _get_raw_data(bucket, key):
    r = rq.get(f'https://{bucket}.s3.amazonaws.com/{key}')
    r.raise_for_status()
    return r.json()


@app.on_s3_event(bucket=BUCKET,
                 events=['s3:ObjectCreated:*'])
def compute_medians(event):
    if _is_dataset(event.key):
        data = _get_raw_data(bucket=event.bucket, key=event.key)
        medians = _calc_medians(data)
        _upload_json(medians, 'medians.json', bucket=BUCKET, key=MEDIANS_FOLDER)



    

