from pathlib import Path
from shutil import copyfile
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("-r", action="store_true")
parser.add_argument("-dryrun", action="store_true")
root_folder = Path(__file__).parent
data_formats = ["csv", "geojson", "json", "db", "html", 'pkl', 'joblib']
FOLDER = "_data"


def _pull_datas(dryrun: bool):
    try:
        (root_folder / FOLDER).mkdir(exist_ok=False)
    except:
        raise Exception(f'Folder {FOLDER} already exist!')
    

    files = []
    for format_ in data_formats:
        files.extend(list(root_folder.glob(f"Chapter*/**/*.{format_}")))
        destinations = [(root_folder / FOLDER / file) for file in files]

    for source, destination in zip(files, destinations):
        if dryrun:
            print(f"{str(source)} -> {str(destination)}")
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open(mode="xb") as fid:
                fid.write(source.read_bytes())


def _move_back(dryrun: bool):
    assert (root_folder / FOLDER).exists(), f"No folder {FOLDER} found!"
    files = list((root_folder / FOLDER).glob("**/*"))
    destinations = [(root_folder / str(file)[len(FOLDER) + 1 :]) for file in files]

    for source, destination in zip(files, destinations):
        if dryrun:
            print(f"{str(source)} -> {str(destination)}")
        else:
            with destination.open(mode="xb") as fid:
                fid.write(source.read_bytes())


def main(reverse: bool, dryrun: bool):

    if reverse:
        _move_back(dryrun)
    else:
        _pull_datas(dryrun)


if __name__ == "__main__":
    args = parser.parse_args()
    main(reverse=args.r, dryrun=args.dryrun)
