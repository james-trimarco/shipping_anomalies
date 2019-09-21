#################################################i############
# Executes various SQL and Python files to import raw data   #
##############################################################

import os
import settings
from utils import create_connection_from_dict, execute_sql, json_directory_to_csv, remove_dir
from etl.load_raw import load_csv



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
    BASE_DIR = settings.get_base_dir()
    JSON_DIR = settings.get_json_dir()
    SQL_DIR = BASE_DIR.joinpath('sql')
    TEMP_DIR = settings.get_temp_dir().joinpath('ais_temp')

    # Get PostgreSQL database credentials
    psql_credentials = settings.get_psql()
    #  print('Running with credentials: ', psql_credentials)

    # Create SQLAlchemy engine from database credentials
    engine = create_connection_from_dict(psql_credentials, 'postgresql')

    ## ---- CREATE SCHEMAS ----

    print("Creating schemas")
    execute_sql(os.path.join(SQL_DIR, 'create_schemas.sql'), engine, read_file=True)

    ## ---- CREATE TABLES ----

    print("Creating tables")
    execute_sql(os.path.join(SQL_DIR, 'create_tables.sql'), engine, read_file=True)

    ## ---- UPLOAD SHAPEFILES ----

    print("Uploading shapefiles")
    # TODO: get this fully hooked up and working 
    # load_shp(DATA_DIR, dir_dict, credentials_dict):

    ## ---- CONVERT JSON TO TEMP CSV ----



        if write_json:
            #  this is where we upload csvs from the database
            #  the intention is that we sometimes do this with previously parsed csvs
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

    args = parser.parse_args()
    run(args.read_json, args.dirs, args.days)
