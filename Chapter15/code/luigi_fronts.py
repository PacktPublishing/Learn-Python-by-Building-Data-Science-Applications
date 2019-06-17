# luigi_fronts.py
import json
from pathlib import Path

import luigi
from wikiwwii.collect.fronts import collect_fronts

URL = 'https://en.wikipedia.org/wiki/List_of_World_War_II_battles'

folder = Path(__file__).parents[1] / 'data' 


class ParseFronts(luigi.Task):
    url = luigi.Parameter(default=URL)
    
   
    def output(self):
        name = self.url.split('/')[-1]
        path = str(folder / f'{name}.json')
        return luigi.LocalTarget(path)
 
    def run(self):
        data = collect_fronts(self.url)
        with open(self.output().path, 'w') as f:
            json.dump(data, f)
