#################################################i############
# Executes various SQL and Python files to import raw data   #
##############################################################

import os
import settings
from utils import create_connection_from_dict, execute_sql, remove_dir
from etl.load_raw import load_csv
import argparse


def run():
    """
    Creates raw-cleaned-semantic schemas and populates the raw schema only.

    Parameters
    ----------
    read_json: bool
        Whether or not the script should read original json files
    write_json: bool
        Whether or not the script should write csvs of ais files
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
    base_dir = settings.get_base_dir()
    sql_dir = base_dir.joinpath('sql')
    data_dir = settings.get_data_dir()
    filtered_dir = data_dir.joinpath('ais_filtered')

    # Get PostgreSQL database credentials
    psql_credentials = settings.get_psql()
    #  print('Running with credentials: ', psql_credentials)

    # Create SQLAlchemy engine from database credentials
    engine = create_connection_from_dict(psql_credentials, 'postgresql')

    ## ---- CREATE SCHEMAS ----

    print("Creating schemas")
    execute_sql(os.path.join(sql_dir, 'create_schemas.sql'), engine, read_file=True)

    ## ---- CREATE TABLES ----

    print("Creating tables")
    execute_sql(os.path.join(sql_dir, 'create_tables.sql'), engine, read_file=True)

    ## ---- UPLOAD SHAPEFILES ----

    # print("Uploading shapefiles")
    # TODO: get this fully hooked up and working
    # load_shp(DATA_DIR, dir_dict, credentials_dict):

    ## ---- WRITE filtered CSVs to db ----
    for a in filtered_dir.glob("*/*/*"):
        if a.is_dir():
            filtered_dir = a




        #  this is where we upload csvs from the database
        #  the intention is that we sometimes do this with previously parsed csvs
            # print(f"Uploading csv files to database from {filtered_dir.name}.")
            load_csv(filtered_dir, engine, 'raw.ais')
        # print(f"Finished converted json from {json_subdir.name}")
        # print(f"Deleting csv files from {temp_subdir.name}")
        # remove_dir(temp_subdir)

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Settings for create_raw')

    args = parser.parse_args()
    run()
