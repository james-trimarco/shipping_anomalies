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
conf = pyspark.SparkConf().setAll([('spark.local.dir', ['/Akamai/tmp_spark', '/Akamai_scratch/tmp_spark'])])
conf.set('spark.local.dir', '/Akamai/tmp_spark/')
conf.set('spark.executor.memory', '200g')
conf.set('spark.driver.memory', '200g')
conf.set('spark.worker.dir', '/Akamai')
conf.set("spark.sql.shuffle.partitions", "2500")

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
sep_csv_files = Path('/Akamai/ais_project_data/ais_csv_files/2019Jun/*.csv')

for monthly_dir in all_csv_files.glob('*/'):
    csvs_with_dupes = monthly_dir.joinpath('*.csv')

    raw_data = sc.textFile(str(sep_csv_files.resolve())) \
        .map(lambda line: tuple(z.strip('\"') for z in line.split(','))) 
    
    
    # Define header as first row
    header = raw_data.first()
    # Remove header
    ais_with_dupes = raw_data.filter(lambda x: x != header)
    raw_data.unpersist()

    print(ais_with_dupes.take(5))
    print("Name of subdirectory: ", monthly_dir.name)
    print("Rows in original data: ", ais_with_dupes.count())

    # Remove reviews that share mmsi and time
    # Note that the entire row is the value here
    #all_dupes = ais_with_dupes.map(lambda x: ((x[0], x[1]), x))
    # Group by row values, dropping duplicates
    #ais_deduped = all_dupes.reduceByKey(lambda x, y: x) \
    #    .map(lambda x: x[1])
    ais_deduped = ais_with_dupes.distinct()
    ais_with_dupes.unpersist()
    print("Rows in deduped data: ", ais_deduped.count())


    def toCSVLine(data):
        return ','.join(str(d) for d in data)


    lines = ais_deduped.map(toCSVLine)

    deduped_path = Path('/Akamai/ais_project_data/ais_deduped')
    save_path = deduped_path.joinpath('2019Jun')

    if save_path.is_dir():
        remove_dir(save_path)

    print(save_path.parts)

    lines.saveAsTextFile(str(save_path.resolve()))

    end = datetime.datetime.now()

    print("Runtime: ", end - start)
