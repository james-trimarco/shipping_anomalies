import settings
from utils import create_connection_from_dict, execute_sql
import findspark
findspark.init()
import pyspark


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

sess = SparkSession.builder.config(conf=conf).getOrCreate()
sess = SparkSession(sc)


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

    df_to_visualize = execute_sql('select mmsi, time_stamp, longitude, latitude from cleaned.ais limit 50000',
                                  engine, return_df=True)

    print(df_to_visualize.head(5))




if __name__ == '__main__':

    run()
