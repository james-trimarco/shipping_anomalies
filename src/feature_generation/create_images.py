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
    Takes in pd dataframe converts to geopandas dataframe with projection = 4326.
    """
    if len(df.index) < 2:
        return
    else:
        geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
        crs = {'init': 'epsg:4326'}  # Coordinate reference system : WGS84
        df = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
        return df


def new_box(bbox_tuple):
    """
    Takes in trajectory boundary box of form (LAT1, LON1, LAT2, LON2),
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


def out_images(out_path, traj1):
    """
    Converts trajectory to image and saves to out_dir
    """
    # import pdb; pdb.set_trace()
    # traj1.add_speed()
    coord = new_box(traj1.get_bbox())
    ax = traj1.plot(column = 'speed', 
                    with_basemap=False, vmin=0, vmax=20, cmap="Greys_r")
    ax.set_axis_off()
    ax.set_ylim([coord[1], coord[3]])
    ax.set_xlim([coord[0], coord[2]])
    fig = ax.get_figure()
    fig.savefig(out_path, bbox_inches='tight', dpi=72, pad_inches=0)
    fig.clf()
    plt.close()
    return


def save_matplotlib_img(split, data_dir):
    """
    Finds the correct directory and filename for a split trajectory and requests
    writing of a png.
    :param split:
        A movingpandas trajectory object, with an embedded dataframe attribute
    :return:
        None
    """
    df = split.df
    vessel_type = str(df['vessel_type'].iloc[0])
    traj_id = str(df['traj_id'].iloc[0])
    # TODO: implement different directories for different experiments
    print(f"printing image {traj_id} in directory: {vessel_type}")
    out_dir = data_dir / f'trajectories/{vessel_type}'
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / traj_id
    out_images(out_path, split)
    return

