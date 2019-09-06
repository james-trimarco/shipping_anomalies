#################################################i############
# Executes various SQL and Python files to run ETL pipeline #
#############################################################

import os
import settings
from utils import create_connection, create_connection_from_dict, json_directory_to_csv, \
                  execute_sql
from etl.load_raw import load_csv


def run():
    """
    Execute Extract-Transform-Load (ETL) process.

    Parameters
    ----------
    ROOT_FOLDER : str
        Directory where the project is stored locally.
    DATA_FOLDER : str
        Directory where the raw data are stored locally.

    Returns
    -------
    None
    """

    # Set environment variables
    settings.load()
    # Get root directory from environment
    BASE_DIR = settings.get_base_dir()
    DATA_DIR = BASE_DIR.joinpath('data')
    SQL_DIR = BASE_DIR.joinpath('sql')
    TEMP_DIR = BASE_DIR.joinpath('temp')
    # create the temp directory
    # TODO: Should exist_ok be false here?
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    print(BASE_DIR.parts)

    # transform json to csv
    #json_directory_to_csv(DATA_FOLDER, TEMP_FOLDER)

    # Get PostgreSQL database credentials
    psql_credentials = settings.get_psql()
    print(psql_credentials)

    # execute_sql(os.path.join(SQL_FOLDER, 'create_schemas.sql'), engine, read_file=True)

    # Create SQLAlchemy engine from database credentials
    engine = create_connection_from_dict(psql_credentials, 'postgresql')

    ## ---- CREATE SCHEMAS ----

    print("Creating schemas")
    execute_sql(os.path.join(SQL_DIR, 'create_schemas.sql'), engine, read_file=True)

    ## ---- CREATE TABLES ----

    print("Creating tables")
    execute_sql(os.path.join(SQL_DIR, 'create_tables.sql'), engine, read_file=True)

    ## ---- CONVERT JSON TO TEMP CSV ----

    print("Converting json; saving to /temp directory")
    # json_directory_to_csv(DATA_DIR, TEMP_DIR)
    load_csv(TEMP_DIR, engine, 'raw.ais')

    ## ---- TESTING ----
    test = engine.execute('select * from raw.ais limit 1;')

    print(test)


if __name__ == '__main__':
    run()
