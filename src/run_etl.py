#################################################i############
# Executes various SQL and Python files to run ETL pipeline #
#############################################################

import os
import settings
from utils import create_connection, create_connection_from_dict


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
    # Get environment folders
    ROOT_FOLDER = settings.get_root_dir()
    DATA_FOLDER = os.path.join(ROOT_FOLDER, 'data/')
    SQL_FOLDER = os.path.join(ROOT_FOLDER, 'sql/')

    # TODO: I think we'll need a function that writes the data_files.yaml
    # file, maybe right here? We have way too many files to write
    # the yaml by hand.

    # Data files to be loaded
    # TODO: For now I'm just taking the first two
    data_config = os.path.join(ROOT_FOLDER, 'config/data_files.yaml')

    # Get PostgreSQL database credentials
    psql_credentials = settings.get_psql()
    print(psql_credentials)

    # execute_sql(os.path.join(SQL_FOLDER, 'create_schemas.sql'), engine, read_file=True)

    # Create SQLAlchemy engine from database credentials
    engine = create_connection_from_dict(psql_credentials, 'postgresql')
    test = engine.execute('select * from raw.ais;')

    print(test)





if __name__ == '__main__':
    run()