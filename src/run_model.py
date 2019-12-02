import settings
import pandas as pd
from pathlib import Path
import shutil
from utils import create_connection_from_dict, execute_sql
from modeling.label_images import fishing_prefilter, nonfishing_dataframe_creator, sampler, trajectory_separator
from modeling.train_test_split import split_data
from modeling.train_cnn import Conv_block, Dense_block, run_cnn


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
    labeled_fishing_dir = data_dir / 'labeled_data' / 'fishing'
    labeled_nonfishing_dir = data_dir / 'labeled_data' / 'nonfishing'
    cnn_split_dir = data_dir / 'cnn_split'
    if cnn_split_dir.exists():
        shutil.rmtree(cnn_split_dir, ignore_errors=False, onerror=None)
    
    cnn_split_dir.mkdir(parents=True, exist_ok=True)


    # Create labeled data
    print('Creating labeled data.')
    fishy_stuff = fishing_prefilter(quants_df)
    nonfish = nonfishing_dataframe_creator(quants_df, fishy_stuff)
    dataset = sampler(fishy_stuff, nonfish)
    trajectory_separator(dataset, data_dir)

    # Create train / test split
    print("Creating train/test split")
    split_data(labeled_fishing_dir, labeled_nonfishing_dir, cnn_split_dir, binary_name='fishing', set_seed=223)
    
    # Train the cnn
    run_cnn(cnn_split_dir, batchsize=256, epochs=50, color_mode='rgb', start_filters=8, depth=2, dense_count = 2, dense_neurons = 256, bnorm = False)

if __name__ == '__main__':
    run()
