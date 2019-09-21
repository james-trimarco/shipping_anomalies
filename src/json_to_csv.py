import csv
import json
import time
import argparse


def json_directory_to_csv(temp_subdir, json_subdir):
    """
    Converts AIS JSON files from data folder to CSV files in the temporary folder.

    Parameters:
    temp_subdir: OS specific path
        Path to the subdirectory to store the csvs in
    json_subdir: OS specific pth
        Path to a subdirectory with some subset of the data

    Returns:
    json_counter: int
        Records count of processed json files

    """

    # Obtain all json files within subdirectory
    json_files = json_subdir.glob('**/*.json')

    json_counter = 0
    for json_path in json_files:
        # Finds the two digits just before the underscore
        # These digits represent the day of the month in the filename
        # match: str = re.search('([0-9]){2}(?=_)', json_path.name)
        # day_of_month = int(match.group(0))
        # if day_of_month < start_end_days[0] or day_of_month > start_end_days[1]:
        #     continue

        print(f"Processing {json_path}")

        json_counter += 1

        with open(json_path) as infile:
            data = json.load(infile)

        with open((temp_subdir / json_path.stem).with_suffix('.csv'), 'w', newline='') as csvfile:
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

                        else:
                            csvwriter.writerow(observation.values())

    time.sleep(1)
    return json_counter


def run(dirs):
    for subdir in dirs:
        #  we need to set up subdirectories to read json from
        #  and subdirectories to write csvs into
        json_subdir = AIS_DIR.joinpath(subdir)
        temp_subdir = TEMP_DIR.joinpath(subdir)
        temp_subdir.mkdir(parents=True, exist_ok=True)

        #  now we actually write the csvs into the temp subdirectory
        print(f"Converting json from {json_subdir.name}; saving to {temp_subdir.name}.")
        json_count = json_directory_to_csv(temp_subdir, json_subdir)
        print(f"Converted {json_count} files from {json_subdir.name}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Settings for converting json')
    parser.add_argument('-dirs', metavar='-dir',
                        help='Pick the json directories you want to parse',
                        nargs='+', type=str, default=['2019Apr'])

    args = parser.parse_args()
    run(args.dirs)
