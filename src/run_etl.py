#################################################i############
# Executes various SQL and Python files to run ETL pipeline #
#############################################################

import os
import settings


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

    # Data files to be loaded
    #data_config = os.path.join(ROOT_FOLDER, 'config/base/data_files.yaml')

    print(ROOT_FOLDER, DATA_FOLDER, SQL_FOLDER)



if __name__ == '__main__':
    run()