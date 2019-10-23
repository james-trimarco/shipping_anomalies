#################################################i############
# Executes various SQL and Python files to import raw data   #
##############################################################

import os
import settings
from utils import create_connection_from_dict, execute_sql, remove_dir, copy_csv_to_db
from etl.load_raw import load_csv, load_iuu_list
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
    filtered_dir = data_dir.joinpath('ais_deduped')

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

    ## ---- UPLOAD TABLES ----

    print("Processing scraped vessels table.")
    copy_csv_to_db(os.path.join(data_dir, 'updated_boats.csv'), 'raw.vessels', engine)
    print("Processing IUU list.")
    load_iuu_list(os.path.join(data_dir, 'IUUList-20190902.txt'), engine)
    

    ## ---- UPLOAD SHAPEFILES ----

    # print("Uploading shapefiles")
    # TODO: get this fully hooked up and working
    # load_shp(DATA_DIR, dir_dict, credentials_dict):

    ## ---- WRITE filtered CSVs to db ----

    for path in filtered_dir.glob("*"):
        if path.is_dir():
            filtered_subdir = path
            #  this is where we upload csvs from the database
            #  the intention is that we sometimes do this with previously parsed csvs
            print(f"Uploading csv files to database from {filtered_subdir.name}.")
            try:
                load_csv(filtered_subdir, engine, 'raw.ais', sep='\t', quote='\b')
            except IsADirectoryError:
                #raise 
                print('Found directory, not file')
        print(f"Finished converted json from {filtered_subdir.name}")

    ## ---- ClEAN DATA ----
    print("Cleaning data")
    execute_sql(os.path.join(sql_dir, 'clean_data.sql'), engine, read_file=True)

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Settings for create_raw')

    args = parser.parse_args()
    run()
