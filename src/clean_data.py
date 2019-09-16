#################################################i############
# Executes various SQL and Python files to import raw data   #
##############################################################

import os
import settings
from utils import create_connection_from_dict, execute_sql, json_directory_to_csv, remove_dir
from etl.load_raw import load_csv

def run():
    """
    Populates the cleaned schema.

    Parameters:
        None
    Returns:
        None
    """
    # Set environment variables
    settings.load()
    # Get needed directories
    SQL_DIR = BASE_DIR.joinpath('sql')
    # Get PostgreSQL database credentials
    psql_credentials = settings.get_psql()

    # Create SQLAlchemy engine from database credentials
    engine = create_connection_from_dict(psql_credentials, 'postgresql')

    ## ---- clean_data ----

    print("Cleaning data")
    execute_sql(os.path.join(SQL_DIR, 'clean_data.sql'), engine, read_file=True)

if __name__ == '__main__':
    run()