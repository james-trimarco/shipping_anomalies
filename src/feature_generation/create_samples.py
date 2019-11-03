from utils import execute_sql

def create_cnn_sample(sql_dir, engine, min_pings, min_dist):
    params = {}
    # Set all parameters for sql file
    params['min_pings'] = int(min_pings)
    params['min_dist'] = float(min_dist)
    sql_file =sql_dir / 'create_sample_trajectories.sql'
    execute_sql(sql_file, engine, read_file=True, params=params)
    print('Created table of sample trajectories for CNN.')
