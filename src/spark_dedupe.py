import findspark
import datetime
from pathlib import Path
from utils import remove_dir

# Initialize pyspark finder
findspark.init()

# Get pyspark
import pyspark

# Simple timing
start = datetime.datetime.now()

# Configure Spark
conf = pyspark.SparkConf()
conf.set('spark.local.dir', '/Akamai/tmp_spark/')
conf.set('spark.executor.memory', '15g')
conf.set('spark.driver.memory', '15g')

# Tell Spark to use all the local clusters
sc = pyspark.SparkContext('local[*]', 'ais', conf)

# Tell spark to create a session
from pyspark.sql import SparkSession

# sess = SparkSession(sc).builder.config(sc.getConf).config("spark.local.dir", "/Akamai_scratch/").getOrCreate()
sess = SparkSession(sc)

# Hold back on the error messages
sc.setLogLevel("ERROR")

all_csv_files = Path('/Akamai/ais_project_data/ais_csv_files/')


bounding_box =  {'FL': {'W': -90.50,
                        'E': -76.00,
                        'N': 31.00,
                        'S': 22.00},
                 'GU': {'W': -3.00,
                        'E': 10.50,
                        'N': 7.50,
                        'S': -10.00},
                 'SR': {'W': 70.00,
                        'E': 90.00,
                        'N': 23.00,
                        'S': 0.00},
                 'BS': {'W': 172.00,
                        'E': -150.00,
                        'N': 67.00,
                        'S': 50.00},
                 }


def ais_in_box(rdd, box):
    south, north, east, west = box['S'], box['N'], box['E'], box['W']
    if west < east:
        return rdd.filter(lambda x: south <= float(x[3]) <= north) \
                  .filter(lambda x: west <= float(x[2]) <= east)
    else:
        return rdd.filter(lambda x: south <= float(x[3]) <= north) \
                  .filter(lambda x: west <= float(x[2]) <= 180 and -180 <= float(x[2] <= east))


for monthly_dir in all_csv_files.glob('*/'):
    csvs_with_dupes = monthly_dir.glob('*.csv')

    raw_data = sc.textFile(csvs_with_dupes) \
        .map(lambda line: line.split(',')) \
        .map(lambda x: [z.strip('\"') for z in x])

    # Define header as first row
    header = raw_data.first()
    # Remove header
    ais_with_dupes = raw_data.filter(lambda x: x != header)
    print("Name of subdirectory: ", subpath.name)
    print("Rows in original data: ", ais_with_dupes.count())

    ais_fl = ais_in_box(ais_with_dupes, bounding_box['FL'])
    ais_gu = ais_in_box(ais_with_dupes, bounding_box['GU'])
    ais_sr = ais_in_box(ais_with_dupes, bounding_box['SR'])
    ais_bs = ais_in_box(ais_with_dupes, bounding_box['BS'])

    ais_bounded = sc.union([ais_fl, ais_gu, ais_sr, ais_bs])

    print("Rows in bounded data: ", ais_bounded.count())
    # Remove reviews that share mmsi and time
    # Note that the entire row is the value here
    all_dupes = ais_bounded.map(lambda x: ((x[0], x[1]), x))
    # Group by row values, dropping duplicates
    ais_deduped = all_dupes.reduceByKey(lambda x, y: x) \
        .map(lambda x: x[1])

    print("Rows in deduped data: ", ais_deduped.count())


    def toCSVLine(data):
        return ','.join(str(d) for d in data)


    lines = ais_deduped.map(toCSVLine)

    deduped_path = Path('/Akamai/ais_project_data/ais_deduped')
    save_path = deduped_path.joinpath(monthly_dir.name)

    if save_path.is_dir():
        remove_dir(save_path)

    print(save_path.parts)

    lines.saveAsTextFile(str(save_path.resolve()))

    end = datetime.datetime.now()

    print("Runtime: ", end - start)
