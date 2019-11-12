import settings
import pandas as pd
from pathlib import Path
import shutil
from utils import create_connection_from_dict, execute_sql
from modeling.label_images import fishing_prefilter, nonfishing_dataframe_creator, sampler, trajectory_separator
from modeling.train_test_split import split_data


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
    print(data_dir.parts)
    labeled_fishing_dir = data_dir / 'labeled_data' / 'fishing'
    print(labeled_fishing_dir.parts)
    labeled_nonfishing_dir = data_dir / 'labeled_data' / 'nonfishing'
    cnn_split_dir = data_dir / 'cnn_split'
    cnn_split_dir.mkdir(parents=True, exist_ok=True)


    # Create labeled data
    #fishy_stuff = fishing_prefilter(quants_df)
    #nonfish = nonfishing_dataframe_creator(quants_df, fishy_stuff)
    #dataset = sampler(fishy_stuff, nonfish)
    #trajectory_separator(dataset, data_dir)

    # Create train / test split
    split_data(labeled_fishing_dir, labeled_nonfishing_dir, cnn_split_dir, binary_name='fishing', set_seed=223)

if __name__ == '__main__':
    run()
