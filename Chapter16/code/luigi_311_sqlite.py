import luigi
from pathlib import Path
import requests as rq
import os
import pandas as pd
from datetime import timedelta, date, datetime
from luigi.contrib import sqla
import sqlalchemy
from copy import copy
from sqlite_schemas import COLUMNS_RAW, TOP

NYCOD = os.environ.get("NYCOPENDATA", {"app": None})["app"]
folder = Path(__file__).parents[1] / "data"
SQLITE_STRING = "sqlite:///../data/311.db"

def _get_data(resource, time_col, date, offset=0):
    """collect data from NYC open data
    """

    Q = f"where=created_date between '{date}' AND '{date}T23:59:59.000'"
    url = f"https://data.cityofnewyork.us/resource/{resource}.json?$limit=50000&$offset={offset}&${Q}"

    headers = {"X-App-Token": NYCOD} if NYCOD else None
    r = rq.get(url, headers=headers)
    r.raise_for_status()

    data = r.json()
    if len(data) == 50_000:
        data2 = _get_data(resource, time_col, date, offset=(offset + 50_000))
        data.extend(data2)

    return data


class Collect311_SQLITE(sqla.CopyToTable):
    time_col = "Created Date"
    date = luigi.DateParameter(default=date.today())
    resource = "fhrw-4uyv"

    columns = COLUMNS_RAW
    connection_string = SQLITE_STRING  # SQLite database as a file
    table = "raw"  # name of the table to store data


    def rows(self):
        data = _get_data(self.resource, self.time_col, self.date, offset=0)
        
        df =  pd.DataFrame(data).astype(str) #.drop('location', axis=1)
        df['unique_key'] = df['unique_key'].astype(int)

        for row in df.to_dict('split')['data']:
            yield row



class Top10_SQLITE(sqla.CopyToTable):
    date = luigi.DateParameter(default=date.today())
    N = 10 #luigi.NumericalParameter(default=5, min_value=1, max_value=100, var_type=int)
    
    columns = TOP
    connection_string = SQLITE_STRING  # SQLite database as a file
    table = "top"  # name of the table to store data

    def requires(self):
        return Collect311_SQLITE(date=self.date)

    @staticmethod
    def _analize(df, date, N=10):

        dict_ = {"boro": "NYC", "date": date, "metric": "complaints", "value": len(df)}
        stats = [dict_]

        top_N = df["complaint_type"].value_counts().nlargest(N).to_dict()
        for k, v in top_N.items():
            dict_["metric"] = k
            dict_["value"]: v
            stats.append(copy(dict_))

        for boro, group in df.groupby("borough"):
            dict_["boro"] = boro
            dict_["metric"] = "complaints"
            dict_["value"] = len(group)
            stats.append(copy(dict_))

            top_N = group["complaint_type"].value_counts().nlargest(N).to_dict()
            for k, v in top_N.items():
                dict_["metric"] = k
                dict_["value"]: v
                stats.append(copy(dict_))

        return stats

    def rows(self):
        con = sqlalchemy.create_engine(self.connection_string)
        
        
        Q = f"SELECT borough, complaint_type FROM raw WHERE closed_date BETWEEN '{self.date:%Y-%m-%d}' AND '{self.date:%Y-%m-%d} 23:59:59';"
        data = pd.read_sql_query(Q, con)
        for row in self._analize(data, date=date, N=self.N):
            yield row




