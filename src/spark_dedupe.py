import findspark
import datetime
from pathlib import Path
from utils import remove_dir
import csv 

# Initialize pyspark finder
findspark.init()

# Get pyspark
import pyspark

# Simple timing
start = datetime.datetime.now()

# Configure Spark
conf = pyspark.SparkConf()
conf.set('spark.local.dir', '/Akamai/tmp_spark/')
conf.set('spark.executor.memory', '5g')
conf.set('spark.driver.memory', '20g')
conf.set('spark.worker.dir', '/Akamai')
#conf.set("spark.sql.shuffle.partitions", "2500")

# Tell Spark to use all the local clusters
sc = pyspark.SparkContext('local[*]', 'airports', conf)

# Tell spark to create a session
from pyspark.sql import SparkSession

sess = SparkSession.builder.config(conf=conf).getOrCreate()
sess = SparkSession(sc)

print(sc._conf.getAll())
# Hold back on the error messages
sc.setLogLevel("ERROR")

all_csv_files = Path('/Akamai/ais_project_data/ais_csv_files/')
sep_csv_files = Path('/Akamai/ais_project_data/ais_csv_files/2019Sep/*.csv')


bounding_box =  {'FL': {'W': -90.50,
                        'E': -76.00,
                        'N': 31.00,
                        'S': 22.00},
                 'GU': {'W': -3.00,
                        'E': 10.50,
                        'N': 7.50,
                        'S': -10.00},
                 'SR': {'W': 77.00,
                        'E': 90.00,
                        'N': 23.00,
                        'S': 0.00},
                 }


def ais_in_boxes(rdd, boxes):
    fl, gu, sr = boxes['FL'], boxes['GU'], boxes['SR']
    return rdd.filter(lambda x: (fl['S'] <= float(x[3]) <= fl['N'] and fl['W'] <= float(x[2]) <= fl['E'])
                                or (gu['S'] <= float(x[3]) <= gu['N'] and gu['W'] <= float(x[2]) <= gu['E'])
                                or (sr['S'] <= float(x[3]) <= sr['N'] and sr['W'] <= float(x[2]) <= sr['E']))


for monthly_dir in all_csv_files.glob('*/'):
    csvs_with_dupes = monthly_dir.joinpath('*.csv')


#raw_data = sc.textFile(str(sep_csv_files.resolve())) \
#                 .map(lambda line: line.split(',')) \
#                 .map(lambda x: [z.strip('\"') for z in x])
    

    raw_data = sc.textFile(str(csvs_with_dupes.resolve())) \
                 .mapPartitions(lambda x: csv.reader(x))	
    #raw_data = sess.read.csv("header","true").rdd
 
    
    # Define header as first row
    header = raw_data.first()
    # Remove header
    ais_with_dupes = raw_data.filter(lambda x: x != header)
    raw_data.unpersist()

    print('\n' + '#####' + '\n')
    print(ais_with_dupes.take(1))
    print("Name of subdirectory: ", monthly_dir.name)
    print("Rows in original data: ", ais_with_dupes.count())

    ais_bounded = ais_in_boxes(ais_with_dupes, bounding_box)

    # ais_bounded = sc.union([ais_fl, ais_gu, ais_sr, ais_bs])

    print("Rows in bounded data: ", ais_bounded.count())
    # Remove reviews that share mmsi and time
    # Note that the entire row is the value here
    all_dupes = ais_bounded.map(lambda x: ((x[0], x[1]), x))
    # Group by row values, dropping duplicates
    ais_deduped = all_dupes.reduceByKey(lambda x, y: x) \
        .map(lambda x: x[1])
    #ais_deduped = all_dupes.distinct()
    ais_with_dupes.unpersist()
    print("Rows in deduped data: ", ais_deduped.count())


    def toCSVLine(data):
        return '\t'.join(str(d) for d in data)


    lines = ais_deduped.map(toCSVLine)

    deduped_path = Path('/Akamai/ais_project_data/ais_deduped')
    save_path = deduped_path.joinpath(monthly_dir.name)
    #save_path = deduped_path.joinpath('2019Sep')

    if save_path.is_dir():
        remove_dir(save_path)

    print(save_path.parts)

    lines.saveAsTextFile(str(save_path.resolve()))

    end = datetime.datetime.now()

    print("Runtime: ", end - start)