import settings
from utils import create_connection_from_dict, execute_sql
# import findspark
#
# findspark.init()
# import pyspark
import pandas as pd
import numpy as np
from feature_generation.create_images import vessel_img, img_reduce
from feature_generation.compute_quants import *
import time

# Configure Spark
# conf = pyspark.SparkConf()
# conf.set('spark.local.dir', '/Akamai/tmp_spark/')
# conf.set('spark.executor.memory', '5g')
# conf.set('spark.driver.memory', '20g')
# conf.set('spark.worker.dir', '/Akamai')
# conf.set("spark.sql.shuffle.partitions", "2500")

# Tell Spark to use all the local clusters
# sc = pyspark.SparkContext('local[*]', 'airports', conf)

# Tell spark to create a session
#from pyspark.sql import SparkSession


# sess = SparkSession.builder.config(conf=conf).getOrCreate()
# sess = SparkSession(sc)


def run():
    """
    TODO: write docstring
    """
    start = time.time()
    # Set environment variables
    settings.load()
    # Get PostgreSQL database credentials
    psql_credentials = settings.get_psql()
    #  print('Running with credentials: ', psql_credentials)

    # Create SQLAlchemy engine from database credentials
    engine = create_connection_from_dict(psql_credentials, 'postgresql')
    # Get data to process from postgres
    df = execute_sql("""
                     WITH sample as (
SELECT mmsi,
       time_stamp::DATE
    FROM eda.CNN_SAMPLE_3 
    GROUP BY mmsi,
             time_stamp::DATE
            HAVING count(*) > 50
            LIMIT 500
    ) SELECT c.* FROM eda.CNN_SAMPLE_3 c 
INNER JOIN sample s
ON c.mmsi = s.mmsi
AND c.time_stamp::DATE = s.time_stamp::DATE;
                     """,
                     engine, read_file=False,
                     return_df=True)
    # Set data type of time_stamp column
    df['time_stamp'] = pd.to_datetime(df['time_stamp'])
    df['longitude'] = pd.to_numeric(df['longitude'])
    df['latitude'] = pd.to_numeric(df['latitude'])
    # Set df index
    df.index = df['time_stamp']
    # print(df.info())
    # Filter by date and mmsi
    df_group = df.groupby([pd.Grouper(freq='D'), 'mmsi'])
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
