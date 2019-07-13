import json
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import randint as sp_randint
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import RandomizedSearchCV
from sklearn.tree import DecisionTreeClassifier  # , export_graphviz
from sklearn.ensemble import RandomForestClassifier

this_folder = Path(__file__).parent


def prepare_model():
    return DecisionTreeClassifier(random_state=2019)


def prepare_random_forest():
    return RandomForestClassifier(random_state=2019)


def prepare_data():
    path = str(this_folder / "data/EF_battles_corrected.csv")
    data = pd.read_csv(path, parse_dates=["start", "end"])
    data["result_num"] = data["result"].map({"axis": -1, "allies": 1}).fillna(0)
    mask = data[["allies_infantry", "axis_infantry"]].isnull().any(1)
    return data[~mask]


def _impute(data):
    cols_to_fill = [
        "allies_planes",
        "axis_planes",
        "axis_tanks",
        "allies_tanks",
        "axis_guns",
        "allies_guns",
    ]
    mask_null = data[cols_to_fill].isnull()
    data[cols_to_fill] = data[cols_to_fill].fillna(0)
    return data


def _generate_binary_most_common(col, N=10):
    mask = col.notnull()
    lead_list = [
        el.strip() for _, cell in col[mask].iteritems() for el in cell if el != ""
    ]
    c = Counter(lead_list)

    mc = c.most_common(N)
    df = pd.DataFrame(index=col.index, columns=[name[0] for name in mc])

    for name in df.columns:
        df.loc[mask, name] = col[mask].apply(lambda x: name in x).astype(int)
    return df.fillna(0)


def feature_engineering(data):
    data["end_num"] = (data["end"].dt.year - 1938) * 12 + data["end"].dt.month
    data["start_num"] = (data["start"].dt.year - 1938) * 12 + data["start"].dt.month
    data["duration"] = (data["end_num"] - data["start_num"]).clip(lower=1)

    data["infantry_ratio"] = data["allies_infantry"] / data["axis_infantry"]
    for tp in "infantry", "planes", "tanks", "guns":
        data[f"{tp}_diff"] = data[f"allies_{tp}"] - data[f"axis_{tp}"]

    return data[
        [
            "duration",
            "infantry_ratio",
            "infantry_diff",
            "planes_diff",
            "tanks_diff",
            "guns_diff",
        ]
    ]


def _add_leaders(data, N=2):
    axis_pop = _generate_binary_most_common(data["axis_leaders"].str.split(","), N=N)
    allies_pop = _generate_binary_most_common(
        data["allies_leaders"].str.split(","), N=N
    )

    return pd.concat([axis_pop, allies_pop], axis=1)


def _hyperopt(model, X, y, param_dist):
    rs = RandomizedSearchCV(
        model,
        param_distributions=param_dist,
        cv=4,
        iid=False,
        random_state=2019,
        n_iter=50,
    )
    rs.fit(X, y)
    print(f"Accuracy: {rs.best_score_}")
    return rs


def _generate_metrics_v1(rs, X, y):

    y_pred = rs.best_estimator_.predict(X)

    return {
        "accuracy": rs.best_score_,
        "params": rs.best_params_,
        "model": rs.best_estimator_.__repr__(),
        "report": classification_report(y, y_pred),
    }


def _generate_metrics_v2(rs, X, y):
    y_pred = rs.best_estimator_.predict(X)

    return {
        "accuracy": rs.best_score_,
        "params": rs.best_params_,
        "model": rs.best_estimator_.__repr__().replace(
            "\n                       ", " "
        ),
        "report": classification_report(y, y_pred),
    }


def main():
    data = _impute(prepare_data())
    # model = prepare_model()
    model = prepare_random_forest()

    features = feature_engineering(data)

    cols = [
        "allies_infantry",
        "axis_infantry",
        "allies_tanks",
        "axis_tanks",
        "allies_planes",
        "axis_planes",
    ]

    y = data["result_num"]
    X = pd.concat([data[cols], features, _add_leaders(data, N=2)], axis=1)
    # X = data[cols]

    param_dist = {
        "max_depth": sp_randint(5, 25),
        "max_features": sp_randint(1, X.shape[1]),
        "min_samples_split": sp_randint(2, 11),
        "criterion": ["gini", "entropy"],
    }

    # for random_forest, remove othervise
    param_dist["n_estimators"] = sp_randint(50, 2000)

    rs = _hyperopt(model, X, y, param_dist)

    metrics = _generate_metrics_v2(rs, X, y)

    out_path = str(this_folder / "data/metrics.json")
    with open(out_path, "w") as f:
        json.dump(metrics, f)


if __name__ == "__main__":
    main()
