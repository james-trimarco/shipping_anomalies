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


def new_box(bbox_list):
    """
    Takes in trajectory boundary box of form (LAT1, LON1, LAT2, LON2),
    outputs cords for bounding trajectory in a square.
    """
    # TUPLE HAS FORM (LAT1, LON1, LAT2, LON2)
    # sample: tuple(13.6602, 45.61043, 13.77498, 45.63727)
    lat_length_old = bbox_list[2] - bbox_list[0]
    lon_length_old = bbox_list[3] - bbox_list[1]

    padding = float(.1)
    bbox_list[0] -= (lat_length_old * padding)
    bbox_list[2] += (lat_length_old * padding)
    bbox_list[1] -= (lon_length_old * padding)
    bbox_list[3] += (lon_length_old * padding)

    lat_length = bbox_list[2] - bbox_list[0]
    lon_length = bbox_list[3] - bbox_list[1]

    difference = lat_length - lon_length
    abs_diff = abs(difference)
    extension = abs_diff / 2

    # Lat > Lon
    if difference > 0:
        return (bbox_list[0], bbox_list[1] - extension, bbox_list[2], bbox_list[3] + extension)

    # Lat < Lon
    elif difference < 0:
        return (bbox_list[0] - extension, bbox_list[1], bbox_list[2] + extension, bbox_list[3])

    else:
        return bbox_list


def out_images(out_path, trj_in, base_map):
    """
    Converts trajectory to image and saves to out_dir
    """
    # import pdb; pdb.set_trace()
    # traj1.add_speed()

    trj_trj = trj_in  # trajectory object in
    trj_trj.df = trj_trj.df.to_crs(epsg=4326)  # stame.toner CRS = 3857
    temp_df = trj_in.get_df_with_speed()
    temp_df = temp_df.assign(prev_pt=temp_df.geometry.shift())
    temp_df['line'] = temp_df.apply(trj_trj._connect_prev_pt_and_geometry, axis=1)
    temp_df = temp_df.set_geometry('line')[1:]

    fig, ax = plt.subplots(facecolor='black', edgecolor='none')
    # Adding features to plot
    temp_df.plot(ax=ax, column='speed', vmin=0, vmax=20, cmap='Reds')
    base_map.plot(ax=ax, color='white')

    # Resizing plot bounding box
    bbox = list(trj_trj.get_bbox())  # needs to converted from a tuple to a list to make mutable
    cord = new_box(bbox)
    ax.set_axis_off()
    ax.set_ylim([cord[1], cord[3]])
    ax.set_xlim([cord[0], cord[2]])

    fig = ax.get_figure()
    fig.savefig(out_path, bbox_inches='tight', dpi=42.7, pad_inches=0, facecolor=fig.get_facecolor())
    fig.clf()
    plt.close()

    return


def save_matplotlib_img(split, data_dir, base_map):
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
    out_images(out_path, split, base_map)
    return
