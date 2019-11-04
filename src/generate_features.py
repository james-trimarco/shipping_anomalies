import settings
from utils import create_connection_from_dict, execute_sql, remove_dir
import pandas as pd
import numpy as np
import movingpandas as mp
from datetime import timedelta
import sqlalchemy as db
from feature_generation.create_images import df_to_geodf, save_matplotlib_img
from feature_generation.create_samples import create_cnn_sample
from feature_generation.compute_quants import *
import time

def create_cnn_sample(sql_dir, engine, min_pings_init, min_dist):
    params = {}
    # Set all parameters for sql file
    params['min_pings_init'] = int(min_pings_init)
    params['min_dist'] = float(min_dist)
    sql_file = sql_dir / 'create_sample_trajectories.sql'
    execute_sql(sql_file, engine, read_file=True, params=params)
    print('Created table of sample trajectories for CNN.')

def run(min_pings_init=30, min_pings_split=20, min_dist=2.0):
    """
    Runs feature generation that allows modeling stage to take place.
    Feature generation involves 3 main stages:
        - generating a sample to show the model
        - breaking sample up into trajectories
        - computing quantitative features on each trajectory
        - writing an image of each trajectory to folders grouped by 'vessel_type'

    :param min_pings_init: int
        The minimum number of AIS data points that must appear in a trajectory for it to be
        included in the sample.
    :param min_pings_split: int
        Applied after splitting trajectories at the gap. Should be smaller than min_pings_init.
        Ensures that split trajectories also have more than a certain minimum number of pings.

    :returns:
        None
    """
    start = time.time()
    # Set environment variables
    settings.load()
    # Get PostgreSQL database credentials
    psql_credentials = settings.get_psql()
    base_dir = settings.get_base_dir()
    sql_dir = base_dir.joinpath('sql')
    data_dir = settings.get_data_dir()

    # Create SQLAlchemy engine from database credentials
    engine = create_connection_from_dict(psql_credentials, 'postgresql')
    # Create a sql table with complete trajectories
    sample_switch = input("Create new sample for Convolutional Neural Net? (Y/N)")
    if sample_switch in ['Y', 'y', '1', 'Yes']:
        print("Creating CNN sample.")
        create_cnn_sample(sql_dir, engine, min_pings_init=min_pings_init, min_dist=min_dist)
    # Get data to process from postgres
    execute_sql('drop table if exists features.quants;', engine, read_file=False)
    if (data_dir / 'trajectories').is_dir():
        print("Removing old trajectories directory.")
        remove_dir(data_dir / 'trajectories')

    try:
        df = execute_sql("select * from features.cnn_sample", engine, read_file=False, return_df=True)
        print("Grabbing trajectory data")
    except db.exc.ProgrammingError:
        print("The table features.cnn_sample doesn't exist. Please create one.")
        raise SystemExit

    # Set data types of several key columns
    df = df.rename(columns={'time_stamp': 't'})
    df['t'] = pd.to_datetime(df['t'])
    df['longitude'] = pd.to_numeric(df['longitude'])
    df['latitude'] = pd.to_numeric(df['latitude'])
    # Set df index
    df.index = df['t']
    df_geo = df_to_geodf(df)
    # Filter by date and mmsi
    df_group = df_geo.groupby([pd.Grouper(freq='D'), 'mmsi'])
    # Loop through the grouped dataframes
    counter = 0
    for name, group in df_group:
        if len(group) < min_pings:
            continue
        trajectory = mp.Trajectory(name, group)
        print(trajectory.df.columns)
        # Split the trajectory at the gap
        split_trajectories = list(trajectory.split_by_observation_gap(timedelta(minutes=30)))

        ### CREATE TRAJECTORY IDs
        for split_index, trajectory in enumerate(split_trajectories):
            # create a universal trajectory ID
            # format is: mmsi-date-split_index
            trajectory.df['traj_id'] = str(name[1]) + '-' + str(name[0].date()) + '-' + str(split_index)

        ### CREATE QUANT FEATURES AND WRITE IMAGES TO DISK

        for split in split_trajectories:
            if len(split.df) < min_pings:
                continue
            else:
                try:
                    quants = compute_quants(split.df[['longitude', 'latitude']])
                    quants['traj_id'] = str(split.df['traj_id'].iloc[0])
                    quants['vessel_type'] = str(split.df['vessel_type'].iloc[0])
                    quants.to_sql('quants', engine, schema='features',
                                  if_exists='append', index=False)
                    ### WRITE IMAGES TO DISK
                    save_matplotlib_img(split, data_dir)
                    counter += 1
                except:
                    print(f"An error occurred processing trajectory {split.df['traj_id'].iloc[0]}.") 

    end = time.time()
    print(f"Generated features for {str(counter)} images in {str(round(end - start))} seconds.")
    return


if __name__ == '__main__':
    run(min_pings_init=30, min_pings_split=20, min_dist=2.0)
