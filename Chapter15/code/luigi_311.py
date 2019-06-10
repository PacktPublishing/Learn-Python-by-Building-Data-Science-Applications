import luigi
from pathlib import Path
import requests as rq
import os
import pandas as pd
NYCOD = os.environ.get('NYCOPENDATA', {'app':None})['app']

folder = Path(__file__).parents[1] / 'data' 

def _get_data(resource, time_col, date, offset=0):
    '''collect data from NYC open data
    '''
          
    Q = f"where=created_date between '{date}' AND '{date}T23:59:59.000'"
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


class CollectRestaurants(luigi.Task):
    time_col = 'Created Date'
    date = luigi.DateParameter()
    resource = 'fhrw-4uyv'

    def output(self):
        path = f'{folder}/311/{self.date:%Y/%m/%d}.csv'
        return luigi.LocalTarget(path)

    def run(self):
        data = _get_data(self.resource, self.time_col, self.date, offset=0)
        df = pd.DataFrame(data)
        
        self.output().makedirs()
        df.to_csv(self.output().path)

    