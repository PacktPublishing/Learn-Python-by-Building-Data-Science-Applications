# luigi_fronts.py
import json
import time
from pathlib import Path

import luigi
from luigi.contrib.s3 import S3Client, S3Target
from luigi_fronts import ParseFronts
from misc import _parse_in_depth
from wikiwwii.collect.battles import parse_battle_page

folder = Path(__file__).parents[1] / "data"
bucket = "philipp-packt"


class ParseFrontS3(luigi.Task):
    front = luigi.Parameter()
    client = S3Client()

    def requires(self):
        return ParseFronts()

    def output(self):
        path = f"s3://{bucket}/wikiwii/fronts/{self.front}.json"
        return S3Target(
            path=path, client=self.client
        )  # <<< swapped local target with s3

    def run(self):
        with open(self.input().path, "r") as f:
            fronts = json.load(f)

        front = fronts[self.front]
        result = {}

        for cp_name, campaign in front.items():
            result[cp_name] = _parse_in_depth(campaign, cp_name)

        with self.output().open("w") as f:
            json.dump(result, f)


class ParseAll(luigi.Task):
    fronts = [
        "African Front",
        "Mediterranean Front",
        "Western Front",
        "Atlantic Ocean",
        "Eastern Front",
        "Indian Ocean",
        "Pacific Theatre",
        "China Front",
        "Southeast Asia Front",
    ]

    def requires(self):
        return [ParseFrontS3(front=f) for f in self.fronts]
