import pandas as pd
# import aequitas
from pathlib import Path
import settings
from utils import create_connection_from_dict, execute_sql, remove_dir
import time

def run():
    start = time.time()
    # Set environment variables
    settings.load()
    # Get PostgreSQL database credentials
    psql_credentials = settings.get_psql()
    base_dir = settings.get_base_dir()

    # Create SQLAlchemy engine from database credentials
    engine = create_connection_from_dict(psql_credentials, 'postgresql')
    vessels = execute_sql("select * from cleaned.vessels;", engine, read_file=False, return_df=True)
    results_path = Path('/Users/james/Documents/NCDS/Semester_3/pds/shipping_anomalies/aux_data/results.csv')
    results = pd.read_csv(results_path)
    results['mmsi'] = results.apply(lambda x: x.Filename[11:20], axis=1)
    print("Joining CNN with vessels table.")
    joined = results.join(vessels, how='left', on='mmsi')
    print(joined.head())


if __name__ == '__main__':
    run()







