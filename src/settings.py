import os
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

def load():
    """
    Looks at the dotenv file to define environment variables.
    returns:
        None
    """
    load_dotenv(find_dotenv())


def get_psql():
    """
    We're not currently using all of these params for use on CompSci01, but they're defined anyway in case
    this project gets run elsewhere at some point.
    
    returns:
        A dictionary of credentials for Postgres.
    """
    POSTGRES_HOST = os.environ['POSTGRES_HOST']
    POSTGRES_DB = os.environ['POSTGRES_DB']
    POSTGRES_USER = os.environ['POSTGRES_USER']
    POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
    POSTGRES_PORT = os.environ['POSTGRES_PORT']

    psql_credentials = {'host': POSTGRES_HOST,
                        'dbname': POSTGRES_DB,
                        'user': POSTGRES_USER,
                        'password': POSTGRES_PASSWORD,
                        'port': POSTGRES_PORT}

    return psql_credentials


def get_base_dir():
    # TODO: can this be cleaner?
    BASE_DIR = Path(__file__).resolve().parent.parent
    return BASE_DIR

def get_data_dir():
    DATA_DIR = Path(os.environ['DATA_DIR'])
    return DATA_DIR

def get_temp_dir():
    TEMP_DIR = Path(os.environ['TEMP_DIR'])
    return TEMP_DIR
