import pandas as pd
from glob import glob
import json


def _read_all_data(root_path="../Chapter16/data/311/2019/06"):
    files = glob(root_path + "/**.csv")
    datas = []
    for file in files:
        try:
            df = pd.read_csv(file, index_col=0)
            if len(df) > 0:
                for col in [
                    "created_date",
                    "closed_date",
                    "resolution_action_updated_date",
                ]:
                    df[col] = pd.to_datetime(df[col])

                datas.append(df)

        except Exception as e:
            raise Exception(e, file)

    return pd.concat(datas, sort=False).reset_index(drop=True)


def _calculate_medians(data):
    data["spent"] = (data["closed_date"] - data["created_date"]) / pd.np.timedelta64(
        1, "h"
    )

    tp = data.groupby("complaint_type")["spent"].median()
    tp.index = tp.index.str.lower()
    tp = tp[pd.notnull(tp)].round(2).to_dict()

    with open("./model.json", "w") as f:
        json.dump(tp, f)


if __name__ == "__main__":
    data = _read_all_data()
    _calculate_medians(data)
