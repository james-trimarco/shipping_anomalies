import numpy as np
import pandas as pd

# the image_dict will contain (initially blank) images
# the keys are (lat,lon) tuples that give the bottom-left edge of the square
# the values are (initially blank) 128x128 numpy arrays
# image_dict[(lat,lon)]=np.zeros((128,128))

def img_reduce(x, y):
    return {s: x[s] + y[s] for s in x.keys()}

def increment_matrix(matrix, index):
    np.add.at(matrix, index, 1)
    return matrix

def vessel_img(df, window_lon, window_lat, pixel_width=64):
    df = df.reset_index(drop=True)
    center_point = (df['latitude'].mean(), df['longitude'].mean())
    lower_left = (center_point[0] - (window_lat/2), center_point[1] - (window_lon/2)) 
    lat_bins = np.histogram([1], bins=pixel_width, range=(lower_left[0], lower_left[0]+window_lat))[1]
    lon_bins = np.histogram([1], bins=pixel_width, range=(lower_left[1], lower_left[1]+window_lon))[1]
    # round off lat/lon to the .1 (for example) to get which image needs to be updated
    # subtract the previous round off from lat/lon, then divide by .00078125 (.1/128)
    # to get which point in that image should be incremented
    # rows of df have lat-long values
    df['px'] = df.apply(lambda row: (np.digitize(row['latitude'], lat_bins), 
                                          np.digitize(row['longitude'], lon_bins)), axis = 1)
    #print(df.head(10))
    #df['img_pnt'] = (int((df['latitude'] - round(df['longitude'], 1)) / (.00078125)),
    #                 int((df['latitude'] - round(df['longitude'], 1)) / (.00078125)))
    df['img'] = df.apply(lambda row: np.zeros((pixel_width, pixel_width)), axis=1)
    try:
        df['img'] = df.apply(lambda row: increment_matrix(row['img'], row['px']), axis=1)
    except:
        print("Point out of range.")
        #print("pixel index: ", df['px'])
    img = df['img'].sum().reshape(-1)
    img = df['img'].sum().reshape(-1)
    #import pdb; pdb.set_trace()    
    #summary_df = pd.DataFrame([
    return img.tolist()
