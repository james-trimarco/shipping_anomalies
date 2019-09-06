import csv
import json
from pathlib import Path
from itertools import chain, repeat
import time


def get_json_directory():
    """
    Takes user input to obtain the path to JSON file directory.

    Returns:
    class: OS specific path

    """

    possible_responses = {'Y', 'y', 'Yes', 'yes', 'N', 'n', 'No', 'no'}
    prompts = chain([f"Are JSON files in the current working directory? {Path.cwd()} (Y/N) "],
                    repeat("Improper Input! Please type Y or N: ", 2))
    replies = map(input, prompts)
    cwd_input = next(filter(possible_responses.__contains__, replies), None)

    if cwd_input is None:
        print("Too many attempts. Exiting...")
        time.sleep(2)
        exit()

    elif cwd_input.strip().lower() in ('y', 'yes'):
        directory = Path.cwd()
        return directory

    else:
        prompts = chain(["Enter path containing json files: "], repeat("Improper path! Please retry: ", 2))
        replies = map(input, prompts)
        paths = map(Path, replies)
        directory = next(filter(Path.exists, paths), None)

        if directory is None:
            print("Too many attempts. Exiting...")
            time.sleep(2)
            exit()

        else:
            return directory


def json_directory_to_csv(directory):
    """
    Converts AIS JSON files into CSV files.

    Parameters:
    arg1 (class): OS specific path

    """

    json_files = directory.glob('*.json')

    print(f'Converting JSON files in {directory}')

    for json_path in json_files:

        print(f"Processing {json_path}")

        with open(json_path) as infile:
            data = json.load(infile)

        with open(json_path.with_suffix('.csv'), 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC)

            for i, pt in enumerate(data):

                # Skip Userinfo row
                if i == 0:
                    continue

                else:
                    for j, observation in enumerate(pt):

                        # Write Header
                        if j == 0:
                            csvwriter.writerow(observation.keys())

                        else:
                            csvwriter.writerow(observation.values())

    print("COMPLETE")
    time.sleep(2)


def main():
    json_directory = get_json_directory()
    json_directory_to_csv(json_directory)


main()
