import sqlalchemy as db
import yaml


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
