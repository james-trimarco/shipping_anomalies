import settings
import pandas as pd
from pathlib import Path
import shutil
from utils import create_connection_from_dict, execute_sql
from modeling.label_images import fishing_prefilter, nonfishing_dataframe_creator, sampler, trajectory_separator


def run():
    """
    TODO: write docstring
    """
    # Set environment variables
    settings.load()
    # Get PostgreSQL database credentials
    psql_credentials = settings.get_psql()
    # Create SQLAlchemy engine from database credentials
    engine = create_connection_from_dict(psql_credentials, 'postgresql')
    # Get data to process from postgres
    quants_df = execute_sql('select * from features.quants;', engine, read_file=False, return_df=True)

    data_dir = settings.get_data_dir()

    fishy_stuff = fishing_prefilter(quants_df)
    nonfish = nonfishing_dataframe_creator(quants_df, fishy_stuff)
    dataset = sampler(fishy_stuff, nonfish)
    trajectory_seperator(dataset, data_dir)


if __name__ == '__main__':
    run()
