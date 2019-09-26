import csv
import json
import time
import argparse
import settings
import multiprocessing as mp
from multiprocessing import Pool
from functools import partial 
from pathlib import Path

def json_directory_to_csv(json_path, csv_subdir):
    """
    Converts AIS JSON files from data folder to CSV files in the temporary folder.

    Parameters:
    temp_subdir: OS specific path
        Path to the subdirectory to store the csvs in
    json_subdir: OS specific pth
        Path to a subdirectory with some subset of the data

    Returns:
    None

    """

    # # Obtain all json files within subdirectory
    # json_files = json_subdir.glob('**/*.json')
    #
    # json_counter = 0
    # for json_path in json_files:
        # Finds the two digits just before the underscore
        # These digits represent the day of the month in the filename
        # match: str = re.search('([0-9]){2}(?=_)', json_path.name)
        # day_of_month = int(match.group(0))
        # if day_of_month < start_end_days[0] or day_of_month > start_end_days[1]:
        #     continue

    print(f"Processing {json_path}")
    # print(f"Planning to write to {csv_subdir}")
    try:
        json_path = Path(json_path)
        with open(json_path) as infile:
            data = json.load(infile)

        with open((csv_subdir / json_path.stem).with_suffix('.csv'), 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC)
            #  TODO: remove all '0x00' characters

            for i, segment in enumerate(data):

                # Skip Userinfo row
                if i == 0:
                    continue

                else:
                    for j, observation in enumerate(segment):

                        # Write Header
                        if j == 0:
                            csvwriter.writerow(observation.keys())
                            csvwriter.writerow(observation.values())

                        else:
                            csvwriter.writerow(observation.values())
    
    except AttributeError:
        print('Found a string')

    time.sleep(1)


def run(dirs):
    # Set environment variables
    settings.load()
    # Get root directory from environment
    # BASE_DIR = settings.get_base_dir()
    JSON_DIR = settings.get_json_dir()
    DATA_DIR = settings.get_data_dir()
    CSV_DIR = DATA_DIR.joinpath('ais_csv_files')

    if not CSV_DIR.is_dir():
        CSV_DIR.mkdir(parents=True, exist_ok=False)

    for subdir in dirs:
        # we need to set up subdirectories to read json from
        # and subdirectories to write csvs into
        json_subdir = JSON_DIR.joinpath(subdir)
        csv_subdir = CSV_DIR.joinpath(subdir)

        print("Is csv_subdir a dir?", csv_subdir.is_dir())
        if not csv_subdir.is_dir():
            csv_subdir.mkdir(parents=True, exist_ok=False)

        else:
            # now we actually write the csvs into the temp subdirectory
            print(f"Converting json from {str(json_subdir.resolve())}; saving to {str(csv_subdir.resolve())}.")

            full_paths = []
            for path in json_subdir.iterdir():
                full_paths.append(str(path.resolve()))
            p = Pool()
            p.map(partial(json_directory_to_csv, csv_subdir=csv_subdir), full_paths)
            # print(f"Converted {json_count} files from {json_subdir.name}") 


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Settings for converting json')
    parser.add_argument('-dirs', metavar='-dir',
                        help='Pick the json directories you want to parse',
                        nargs='+', type=str, default=['2019Apr'])

    args = parser.parse_args()
    run(args.dirs)
