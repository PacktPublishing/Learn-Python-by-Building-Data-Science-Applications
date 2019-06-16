import luigi
from pathlib import Path
import requests as rq
import os
import pandas as pd
from datetime import timedelta, date, datetime
from sqlalchemy import String
from luigi.contrib import sqla
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


class Collect311(luigi.Task):
    time_col = 'Created Date'
    date = luigi.DateParameter(default=date.today())
    resource = 'fhrw-4uyv'

    def output(self):
        path = f'{folder}/311/{self.date:%Y/%m/%d}.csv'
        return luigi.LocalTarget(path)

    def run(self):
        data = _get_data(self.resource, self.time_col, self.date, offset=0)
        df = pd.DataFrame(data)
        
        self.output().makedirs()
        df.to_csv(self.output().path)
        
    
class Top10(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    start = luigi.DateParameter(default=datetime(2019,1,1))
    N = luigi.NumericalParameter(default=15, min_value=1, max_value=100, var_type=int)
    
    def requires(self):
        # data for the last {window} days
        delta = self.date - self.start
        dates = [self.start + timedelta(days=d) for d in range(delta.days + 1)]
        return { d.strftime('%Y-%m-%d'): Collect311(date=(d)) for d in dates }
    
    def output(self):
        return {'report':luigi.LocalTarget(f'{folder}/311/top{self.N}.csv'),
                'flag': luigi.LocalTarget(f'{folder}/311/_flags/{self.date:%Y/%m/%d}_{self.N}.flag')}
    
    @staticmethod
    def _analize(df, N=20):
        stats = df['complaint_type'].value_counts().head(N).to_dict()
        stats['total_complaints'] = len(df)

        return stats
        
    def run(self):

        data = []
        for k, v in self.input().items():
            try:
                df = pd.read_csv(v.path)
                stats = self._analize(df, N=self.N)
                stats['date'] = k
                data.append(stats)
            except:
                pass
        
        data = pd.DataFrame(data)

        # self.output()['report'].makedirs()
        data.to_csv(self.output()['report'].path)

        with self.output()['flag'].open('w') as f:
            f.write('!')
    
    def complete(self):
        return self.output()['flag'].exists()


        
                

    