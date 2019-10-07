import settings
from utils import create_connection_from_dict, execute_sql
import findspark
findspark.init()
import pyspark
import pandas as pd
import pytz
from feature_generation.create_images import vessel_img, img_reduce

# Configure Spark
conf = pyspark.SparkConf()
conf.set('spark.local.dir', '/Akamai/tmp_spark/')
conf.set('spark.executor.memory', '5g')
conf.set('spark.driver.memory', '20g')
conf.set('spark.worker.dir', '/Akamai')
# conf.set("spark.sql.shuffle.partitions", "2500")

# Tell Spark to use all the local clusters
sc = pyspark.SparkContext('local[*]', 'airports', conf)

# Tell spark to create a session
from pyspark.sql import SparkSession

#sess = SparkSession.builder.config(conf=conf).getOrCreate()
#sess = SparkSession(sc)


def run():
    """
    TODO: write docstring
    """
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
                                  engine, read_file = False, 
                                  return_df=True)
    # Set data type of time_stamp column
    df['time_stamp']=pd.to_datetime(df['time_stamp'])
    # Set df index
    df.index = df['time_stamp']
    #print(df.info())
    # Filter by date and mmsi
    timezone = pytz.timezone('GMT')
    #print("count: ", df['mmsi'].resample('D').count())
    df_group = df.groupby([pd.Grouper(freq='D'), 'mmsi'])
    #df_group.apply(lambda x: print(x.count()))
    for name, group in df_group:
        #print(name)
        #print(group)
        vessel_img(group)
        break

if __name__ == '__main__':

    run()
