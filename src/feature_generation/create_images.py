import pathlib
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame, read_file
from shapely.geometry import Point, LineString, Polygon
import movingpandas as mp
from datetime import datetime, timedelta
import matplotlib.pyplot as plt


def df_to_geodf(df):
    """
    Takes in pd dataframe converts to geodf
    """
    geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
    crs = {'init': 'epsg:4326'}  # Coordinate reference system : WGS84
    df = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
    return df


def new_box(bbox_tuple):
    """
    Takes in trajectory boundry box of form (LAT1, LON1, LAT2, LON2),
    outputs cords for bounding trajectory in a square.
    """
    # TUPLE HAS FORM (LAT1, LON1, LAT2, LON2)
    lat_length = bbox_tuple[2] - bbox_tuple[0]
    lon_length = bbox_tuple[3] - bbox_tuple[1]

    difference = lat_length - lon_length
    abs_diff = abs(difference)
    extension = abs_diff / 2

    # Lat > Lon
    if difference > 0:
        return (bbox_tuple[0], bbox_tuple[1] - extension, bbox_tuple[2], bbox_tuple[3] + extension)

    # Lat < Lon
    elif difference < 0:
        return (bbox_tuple[0] - extension, bbox_tuple[1], bbox_tuple[2] + extension, bbox_tuple[3])

    else:
        return bbox_tuple


def out_images(out_dir, traj1):
    """
    Converts trajectory to image and saves to out_dir
    """
    cord = new_box(traj1.get_bbox())
    ax = traj1.plot(column='speed', with_basemap=False, vmin=0, vmax=20, cmap="Greys_r")
    ax.set_axis_off()
    ax.set_ylim([cord[1], cord[3]])
    ax.set_xlim([cord[0], cord[2]])
    # ax.set_facecolor("black")
    fig = ax.get_figure()
    fig.savefig(out_dir, bbox_inches='tight', dpi=42.7, pad_inches=0)
    fig.clf()
    plt.close()
    return


def save_matplotlib_img(traj, class_col, seq_id, date):
    """
    Adds poshness
    Generates trajectories images and saves by class
    """
    # classes     = traj.df[class_col].unique()
    class_value = traj.df[class_col].iloc[0]
    mmsi_value = traj.df['mmsi'].iloc[0]
    out_dir1 = f'trajectories/{class_value}/{mmsi_value}_{date.year}__{date.month}_{date.day}_{seq_id}.png'
    out_images(out_dir1, traj)

    # if 'Fishing' in classes:
    #    out_dir1 = f'trajectories/{class_value}/{mmsi_value}_{date.year}__{date.month}_{date.day}_{seq_id}.png'
    #    out_images(out_dir1,traj)
    #    return
    # else:
    #    out_dir1 = f'trajectories/{class_value}/{mmsi_value}_{day}_{seq_id}.png'
    #   out_images(out_dir1,traj)
    #    return
    return


def get_traj(df, ID, min_length, seq_id, date):
    """
    takes in one month of data and generates list of trajectories on given day
    """
    # Building Trajectories
    t_start = datetime.now()
    trajectories = []
    traj_count = 0
    for key, values in df.groupby([ID]):
        seq_id += 1
        trajectory = mp.Trajectory(key, values)
        # split by gap here if len greater then 1 loop through values for each
        split_trajectory = trajectory.split_by_observation_gap(timedelta(minutes=30))
        if len(split_trajectory) > 1:
            for i in split_trajectory:
                if i.get_length() < MIN_LENGTH:
                    # eliminate trajectories with too few pings
                    continue
                else:
                    seq_id += 1
                    # trajectories.append(i)
                    traj_count += 1
                    trajectory = save_matplotlib_img(i, 'AIS Vessel Type', seq_id, date)
        else:
            # trajectories.append(trajectory)
            traj_count += 1
            trajectory = save_matplotlib_img(trajectory, 'AIS Vessel Type', seq_id, date)

    return print(
        "{} created {} trajectories in {}".format(str(date), traj_count, datetime.now() - t_start))  # trajectories


# Executed Code Starts Here

# load data
# cnn = pd.read_csv('cnn_sample_3.csv')
# boats = pd.read_csv('updated_boats.csv')

#boats['AIS Vessel Type'] = boats['AIS Vessel Type'].fillna("Unspecified")  # nans to unspecified

# create directories
# curr_dir = pathlib.Path.cwd()
# (curr_dir / 'trajectories').mkdir(parents=True, exist_ok=True)
# for i in list(boats['AIS Vessel Type'].unique()):
#     (curr_dir / 'trajectories' / i).mkdir(parents=True, exist_ok=True)
#
# # prepping input
# # cnn =  (cnn, 'time_stamp', 'longitude', 'latitude')
# # cnn_boats = cnn.merge(boats, left_on='mmsi', right_on='MMSI')[['mmsi', 'AIS Vessel Type', 'time_stamp', 'geometry']]
#
# cnn_boats['t'] = pd.to_datetime(cnn_boats['time_stamp'], format='%m/%d/%y %H:%M')
# cnn_boats = cnn_boats.set_index('t')
#
# # generating trajectory images
# for i in range(1, cnn_boats.index.days_in_month[0] + 1):  # loop by days in month
#     seq_id = 0
#     cnn_boats_in = cnn_boats[cnn_boats.index.day == i]
#     date = cnn_boats_in.index.date[0]
#     get_traj(df=cnn_boats_in, ID='mmsi', seq_id=0, date=date)
