#################################################i############
# Executes various SQL and Python files to run ETL pipeline #
#############################################################

import os
import settings
from utils import create_connection_from_dict, execute_sql, json_directory_to_csv, remove_dir
from etl.load_raw import load_csv
import argparse


def run(read_json, dirs, date_range):
    """
    Creates raw-cleaned-semantic schemas and populates the raw schema.

    Parameters
    ----------
    read_json: bool
        Whether or not the script should read original json files
    dirs: [str]
        List of names of the directories to import
    date_range: [int]
        List of two ints with the first and last day to collect files from

    Returns
    -------
    None

    """
    # Set environment variables
    settings.load()
    # Get root directory from environment
    BASE_DIR = settings.get_base_dir()
    DATA_DIR = settings.get_data_dir()
    SQL_DIR = BASE_DIR.joinpath('sql')
    TEMP_DIR = settings.get_temp_dir().joinpath('ais_temp')

    # Get PostgreSQL database credentials
    psql_credentials = settings.get_psql()
    print('Running with credentials: ', psql_credentials)

    # Initialize temp dir
    if not TEMP_DIR.is_dir():
        TEMP_DIR.mkdir(parents=True, exist_ok=False)

    # Create SQLAlchemy engine from database credentials
    engine = create_connection_from_dict(psql_credentials, 'postgresql')

    ## ---- CREATE SCHEMAS ----

    print("Creating schemas")
    execute_sql(os.path.join(SQL_DIR, 'create_schemas.sql'), engine, read_file=True)

    ## ---- CREATE TABLES ----

    print("Creating tables")
    execute_sql(os.path.join(SQL_DIR, 'create_tables.sql'), engine, read_file=True)

    ## ---- CONVERT JSON TO TEMP CSV ----

    for subdir in dirs:
        json_subdir = DATA_DIR.joinpath(subdir)
        temp_subdir = TEMP_DIR.joinpath(subdir)

        if read_json:
            #  if we're reading json, we need to clear the temp directory
            if TEMP_DIR.is_dir():
                print(f"{TEMP_DIR.name} already exists. Deleting.")
                remove_dir(TEMP_DIR)
                #  create the temp directory
                TEMP_DIR.mkdir(parents=True, exist_ok=False)

            temp_subdir.mkdir(parents=True, exist_ok=True)
            print(f"Converting json from {json_subdir.name}; saving to {temp_subdir.name}.")
            json_count = json_directory_to_csv(temp_subdir, json_subdir, date_range)
            print(f"Converted {json_count} files from {json_subdir.name}")

        print(f"Uploading csv files to database from {temp_subdir.name}.")
        load_csv(TEMP_DIR, engine, temp_subdir, 'raw.ais')
        print(f"Finished converted json from {json_subdir.name}")

        print(f"Deleting csv files from {temp_subdir.name}")
        remove_dir(temp_subdir)

    return


def str_to_bool(input_str):
    """
    Converts string input to boolean

    Parameters:
    input_str: str
    Returns:
    Boolean or error
    """
    if isinstance(input_str, bool):
        return input_str
    if input_str.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif input_str.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Settings for create_raw')
    parser.add_argument('-rj', metavar='-rj',
                        help='will we import json directories?',
                        type=str_to_bool, default=False)
    parser.add_argument('-dirs', metavar='-dir',
                        help='Pick the json directories you want to parse',
                        nargs='+', type=str, default=['2019Apr'])
    parser.add_argument('-dr', metavar='-daterange',
                        help='Pick the first and last day to collect json from',
                        nargs='+', type=int, default=[1, 7])
    args = parser.parse_args()
    run(args.rj, args.dirs, args.dr)
