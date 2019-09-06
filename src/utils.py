import sqlalchemy as db
import yaml
import csv
import json
from pathlib import Path
from itertools import chain, repeat
import time


def create_connection(drivername, username, database, echo=False):
    """
    Creates connection to a database from specified parameters

    Parameters
    ----------
    drivername : string
        The driver of the database to connect to e.g. 'postgresql'
    username : string
    password : string
    host : string
    database : string
    port : string

    echo : True, False, or "debug"
        Passing boolean value True prints SQL query output to stdout.
        Passing "debug" prints SQL query + result set output to stdout.

    Returns
    -------
    engine : SQLAlchemy engine object
    """

    db_url = db.engine.url.URL(drivername=drivername,
                               username=username,
                               # password=password,
                               # host=host,
                               database=database,
                               # port=port,
                               )
    engine = db.create_engine(db_url, echo=echo)
    return engine


def create_connection_from_dict(dictionary, driver):
    """
    Creates connection to a database from parameters given in a dictionary

    Parameters
    ----------
    drivername : string
        The driver of the database to connect to e.g. 'postgresql'
    dictionary : dict
        Dict of parameters (e.g. {'host': host, 'user': user})

    Returns
    -------
    engine : SQLAlchemy engine object
    """

    engine = create_connection(drivername=driver,
                               username=dictionary['user'],
                               # password=dictionary['password'],
                               # host=dictionary['host'],
                               database=dictionary['dbname'],
                               # port=dictionary['port'],
                               )

    return engine


def load_yaml(filename):
    """
     Returns the contents of a yaml file in a dict

     Parameters
     ----------
     filename : string
        The full filepath string '.../.../.yaml' of the yaml file to be loaded

     Returns
     -------
     yaml_contents : dict
        Contents of the yaml file (may be a nested dict)
    """

    with open(filename, 'r') as ymlfile:
        yaml_contents = yaml.safe_load(ymlfile)
    return yaml_contents


def json_directory_to_csv(DATA_FOLDER, TEMP_FOLDER):
    """
    Converts AIS JSON files from data folder to CSV files in the temporary folder.

    Parameters:
    arg1 (class): OS specific path
    arg2 (class): OS specific path

    """

    # Obtain all json files within subdirectories
    json_files = DATA_FOLDER.glob('**/*.json')

    print(f'Converting JSON files in {DATA_FOLDER}')

    for json_path in json_files:

        print(f"Processing {json_path}")

        with open(json_path) as infile:
            data = json.load(infile)

        with open((TEMP_FOLDER / json_path.stem).with_suffix('.csv'), 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC)

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

    print("COMPLETE")
    time.sleep(3)

def execute_sql(string, engine, read_file, print_=False, return_df=False, chunksize=None, params=None):
    """
    Executes a SQL query from a file or a string using SQLAlchemy engine
    Note: Must only be basic SQL (e.g. does not run PSQL \copy and other commands)
    Note: SQL file CANNOT START WITH A COMMENT! There can be comments later on in the file, but for some reason
    doesn't work if you start with one (seems to treat the entire file as commented)

    Parameters:
    string : string
        Either a filename (with full path string '.../.../.sql') or a specific query string to be executed
        Can include "parameters" (in the form of {param_name}) whose values are filled in at the time of execution
    engine : SQLAlchemy engine object
        To connect to DB
    read_file : boolean
        Whether to treat the string as a filename or a query
    print_ : boolean
        Whether to print the 'Executed query' statement
    return_df : boolean
        Whether to return the result table of query as a Pandas dataframe
    chunksize : int
        Rows will be read in batches of this size at a time; all rows will be read at once if not specified
    params : dict
        In the case of parameterized SQL, the dictionary of parameters in the form of {'param_name': param_value}

    Returns:
    ResultProxy : ResultProxy
        see SQLAlchemy documentation; results of query
    """

    if read_file:
        query = Path(string).read_text()
    else:
        query = string

    if params is not None:
        query = query.format(**params)

    if print_:
        print('Query executed')

    if return_df:
        res_df = pd.read_sql_query(query, engine, chunksize=chunksize)
        return res_df
    else:  # Not all result objects return rows.
        engine.execute(query)
