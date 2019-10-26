import settings
from utils import create_connection_from_dict, execute_sql
import pandas as pd
import numpy as np
import movingpandas as mp
from feature_generation.create_images import df_to_geodf, save_matplotlib_img
from feature_generation.create_samples import create_cnn_sample
from feature_generation.compute_quants import *
import time


def run():
    """
    TODO: write docstring
    """
    start = time.time()
    # Set environment variables
    settings.load()
    # Get PostgreSQL database credentials
    psql_credentials = settings.get_psql()
    base_dir = settings.get_base_dir()
    sql_dir = base_dir.joinpath('sql')

    # Create SQLAlchemy engine from database credentials
    engine = create_connection_from_dict(psql_credentials, 'postgresql')
    # Create a sql table with complete trajectories
    create_cnn_sample(sql_dir, engine, min_pings=50)
    # Get data to process from postgres
    df = execute_sql("""
                     WITH sample as (
SELECT mmsi,
       time_stamp::DATE
    FROM features.cnn_sample 
    GROUP BY mmsi,
             time_stamp::DATE
            HAVING count(*) > 50
            LIMIT 500
    ) SELECT c.* FROM features.cnn_sample c 
INNER JOIN sample s
ON c.mmsi = s.mmsi
AND c.time_stamp::DATE = s.time_stamp::DATE;
                     """,
                     engine, read_file=False,
                     return_df=True)
    # Set data types of several key columns
    df['time_stamp'] = pd.to_datetime(df['time_stamp'])
    df['longitude'] = pd.to_numeric(df['longitude'])
    df['latitude'] = pd.to_numeric(df['latitude'])
    # Set df index
    df.index = df['time_stamp']
    df_geo = df_to_geodf(df)
    print(df_geo.info())
    # Filter by date and mmsi
    df_group = df_geo.groupby([pd.Grouper(freq='D'), 'mmsi'])
    # Split images at the gap
    for name, group in df_group:
        trajectory = mp.Trajectory(name, group)
        print("splitting trajectory...")
        split_trajectory = trajectory.split_by_observation_gap(timedelta(minutes=30))
        print(split_trajectory.head())

    # Create standard window size for images
    traj_lon = df_group['longitude'].agg(np.ptp)
    traj_lat = df_group['latitude'].agg(np.ptp)
    window_lon = traj_lon.mean() + 2 * traj_lon.std()
    window_lat = traj_lat.mean() + 2 * traj_lat.std()
    print("window size : ", round(window_lon, 2), ' ', round(window_lat, 2))

    ### CREATE QUANT FEATURES
    for name, group in df_group:
        try:
            quants = compute_quants(group[['time_stamp', 'longitude', 'latitude']])
            quants['traj_id'] = str(name[1]) + '-' + str(name[0].date())
            quants['day'] = name[0].date()
            quants['mmsi'] = name[1]
            quants.to_sql('quants', engine, schema='features', if_exists='append',
                      index=False)
        except: 
            print("An error occurred computing quants.")

    ### CREATE TABLE OF IMAGES

    width = 64  # TODO: pass this in dynamically
    height = 64
    i = 0
    rows_list = []
    for name, group in df_group:
        row_dict = {'traj_id': str(name[1]) + '-' + str(name[0].date()),
                    'day': name[0].date(),
                    'mmsi': name[1],
                    'img': vessel_img(group, window_lon, window_lat),
                    'width': width,
                    'height': height}
        rows_list.append(row_dict)
        i += 1
    img_df = pd.DataFrame(rows_list, columns=['traj_id', 'mmsi', 'day', 'img'])
    print(f"created {i} trajectories.")
    print(img_df.head(20))
    img_df.to_sql('images', engine, schema='features', if_exists='append',
                  index=False)
    end = time.time()
    print(end - start)


if __name__ == '__main__':
    run()
