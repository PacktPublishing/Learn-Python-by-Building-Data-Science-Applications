import requests as rq
from datetime import date
import json

folder = '.'

time_col = "Created Date"
resource = "fhrw-4uyv"

def _get_data(resource:str, time_col:str, date:date, offset:int=0):
    """collect data from NYC open data

    Args:
        resource: NYC OD resource id
        time_col: column that stores timestampes to query on
        date: date to pull data for
        offset: records offset. You probably want to keep it equal to zero.
    """

    Q = f"where=created_date between '{date}' AND '{date}T23:59:59.000'"
    url = f"https://data.cityofnewyork.us/resource/{resource}.json?$limit=50000&$offset={offset}&${Q}"

    r = rq.get(url)
    r.raise_for_status()

    data = r.json()
    if len(data) == 50_000:
        offset2 = offset + 50000
        data2 = _get_data(resource, time_col, date, offset=offset2)
        data.extend(data2)

    return data


if __name__ == '__main__':
    today = date.today()
    data = _get_data(resource, time_col, date=today)

    with open(f'311_{today:%Y-%m-%d}.json', 'w') as f:
        json.dump(data, f)
