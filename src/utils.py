import sqlalchemy as db
import psycopg2

def create_connection(drivername, host, database, port, echo=False):
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
                               # username=username,
                               # password=password,
                               host=host,
                               database=database,
                               port=port)
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
                               # username=dictionary['user'],
                               # password=dictionary['password'],
                               host=dictionary['host'],
                               database=dictionary['dbname'],
                               port=dictionary['port'])

    return engine