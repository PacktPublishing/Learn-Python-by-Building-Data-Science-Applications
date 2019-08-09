from pathlib import Path
import requests as rq
import pandas as pd
from tqdm import tqdm
from pandas.tseries.offsets import MonthEnd
from datetime import timedelta, date, datetime

# NYCOD = os.environ.get("NYCOPENDATA", {"app": None})["app"]
# Socrate allows data retrieval without a dedicated token.
# Still, for a more stable performance, it makes sense to get your privat token
time_col = "Created Date"
resource = "fhrw-4uyv"
folder = Path(__file__).parent / "data"


def _get_data(resource, time_col, start, end, offset=0):
    """collect data from NYC open data
    """

    Q = f"where=created_date between '{start}' AND '{end}T23:59:59.000'"
    url = f"https://data.cityofnewyork.us/resource/{resource}.json?$limit=50000&$offset={offset}&${Q}"

    # headers = {"X-App-Token": NYCOD} if NYCOD else None
    r = rq.get(url) #, headers=headers)
    r.raise_for_status()

    data = r.json()
    if len(data) == 50_000:
        offset2 = offset + 50000
        data2 = _get_data(resource, time_col, start, end, offset=offset2)
        data.extend(data2)

    return data


if __name__ == '__main__':
    for f in tqdm(range(1, 13)):
        start = f'2018-{f:02}-01'
        end = (pd.to_datetime(start) + MonthEnd(1)).strftime('%Y-%m-%d')
        data = _get_data(resource, time_col, start, end)
        data = pd.DataFrame(data)

        data.to_csv(str(folder / f'311/{start[:-3]}.csv'))