import luigi
from pathlib import Path
import requests as rq
import os
import pandas as pd
from datetime import timedelta, date, datetime
from copy import copy

# NYCOD = os.environ.get("NYCOPENDATA", {"app": None})["app"]
# Socrate allows data retrieval without a dedicated token.
# Still, for a more stable performance, it makes sense to get your privat token

folder = Path(__file__).parents[1] / "data"


def _get_data(resource, time_col, date, offset=0):
    """collect data from NYC open data
    """

    Q = f"where=created_date between '{date}' AND '{date}T23:59:59.000'"
    url = f"https://data.cityofnewyork.us/resource/{resource}.json?$limit=50000&$offset={offset}&${Q}"

    # headers = {"X-App-Token": NYCOD} if NYCOD else None
    r = rq.get(url) #, headers=headers)
    r.raise_for_status()

    data = r.json()
    if len(data) == 50_000:
        offset2 = offset + 50000
        data2 = _get_data(resource, time_col, date, offset=offset2)
        data.extend(data2)

    return data


class Collect311(luigi.Task):
    time_col = "Created Date"
    date = luigi.DateParameter(default=date.today())
    resource = "fhrw-4uyv"

    def output(self):
        path = f"{folder}/311/{self.date:%Y/%m/%d}.csv"
        return luigi.LocalTarget(path)

    def run(self):
        data = _get_data(self.resource, self.time_col, self.date, offset=0)
        df = pd.DataFrame(data)

        self.output().makedirs()
        df.to_csv(self.output().path)


class Top10(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    N = luigi.NumericalParameter(default=10, min_value=1, max_value=100, var_type=int)

    def requires(self):
        return Collect311(date=self.date)

    def output(self):
        return luigi.LocalTarget(f"{folder}/311/top{self.N}.csv")

    @staticmethod
    def _analize(df, date, N=10):

        dict_ = {"boro": "NYC", "date": date, "metric": "complaints", "value": len(df)}
        stats = [dict_]

        top_N = df["complaint_type"].value_counts().nlargest(N).to_dict()
        for k, v in top_N.items():
            dict_["metric"] = k
            dict_["balue"]: v
            stats.append(copy(dict_))

        for boro, group in df.groupby("borough"):
            dict_["boro"] = boro
            dict_["metric"] = "complaints"
            dict_["value"] = len(group)
            stats.append(copy(dict_))

            top_N = group["complaint_type"].value_counts().nlargest(N).to_dict()
            for k, v in top_N.items():
                dict_["metric"] = k
                dict_["balue"]: v
                stats.append(copy(dict_))

        return stats

    def run(self):
        df = pd.read_csv(self.input().path)

        data = pd.DataFrame(self._analize(df, date=self.date, N=self.N)).set_index(
            "date"
        )
        data.to_csv(self.output().path)

