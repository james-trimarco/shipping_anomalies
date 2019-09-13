#################################################i############
# Executes various SQL and Python files to run ETL pipeline #
#############################################################

import os
import settings
from utils import create_connection_from_dict, execute_sql, json_directory_to_csv
from etl.load_raw import load_csv
import argparse


def run(read_json=False):
    """
    Creates raw-cleaned-semantic schemas and populates the raw schema.

    Parameters
    ----------
    read_json: bool
        Whether or not the script should read original json files

    Returns
    -------
    None

    """
    print("Read json: ", read_json)
    # Set environment variables
    settings.load()
    # Get root directory from environment
    BASE_DIR = settings.get_base_dir()
    DATA_DIR = settings.get_data_dir()
    SQL_DIR = BASE_DIR.joinpath('sql')
    TEMP_DIR = settings.get_temp_dir().joinpath('ais_temp')
    # create the temp directory
    # TODO: Should exist_ok be false here?
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    print(BASE_DIR.parts)
    print(TEMP_DIR.parts)
    # transform json to csv

    # Get PostgreSQL database credentials
    psql_credentials = settings.get_psql()
    print('Running with credentials: ', psql_credentials)

    # Create SQLAlchemy engine from database credentials
    engine = create_connection_from_dict(psql_credentials, 'postgresql')

    ## ---- CREATE SCHEMAS ----

    print("Creating schemas")
    execute_sql(os.path.join(SQL_DIR, 'create_schemas.sql'), engine, read_file=True)

    ## ---- CREATE TABLES ----

    print("Creating tables")
    execute_sql(os.path.join(SQL_DIR, 'create_tables.sql'), engine, read_file=True)

    ## ---- CONVERT JSON TO TEMP CSV ----

    if read_json:
        print("Converting json; saving to /temp directory")
        json_directory_to_csv(DATA_DIR, TEMP_DIR, ['2019Apr'])

    load_csv(TEMP_DIR, engine, 'raw.ais')

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Settings for create_raw')
    parser.add_argument('read_json', metavar='-j',
                        help='will we import json directories?',
                        type=bool, default=False)
    args = parser.parse_args()
    run(args.read_json)
