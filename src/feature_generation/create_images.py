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

def vessel_img(df, window_lon, window_lat, pixel_width=12):
    df = df.reset_index(drop=True)
    center_point = (df['latitude'].mean(), df['longitude'].mean())
    lower_left = (center_point[0] - (window_lat/2), center_point[1] - (window_lon/2)) 
    lat_bins = np.histogram([1], bins=pixel_width, range=(lower_left[0], lower_left[0]+window_lat))[1]
    lon_bins = np.histogram([1], bins=pixel_width, range=(lower_left[1], lower_left[1]+window_lon))[1]
    # round off lat/lon to the .1 (for example) to get which image needs to be updated
    # subtract the previous round off from lat/lon, then divide by .00078125 (.1/128)
    # to get which point in that image should be incremented
    # rows of df have lat-long values
    df['px'] = df.apply(lambda row: (np.digitize(row['latitude'], lat_bins) - 1, 
                                          np.digitize(row['longitude'], lon_bins) - 1), axis = 1)
    #print(df.head(10))
    #df['img_pnt'] = (int((df['latitude'] - round(df['longitude'], 1)) / (.00078125)),
    #                 int((df['latitude'] - round(df['longitude'], 1)) / (.00078125)))
    df['img'] = df.apply(lambda row: np.zeros((pixel_width, pixel_width)), axis=1)
    try:
        df['img'] = df.apply(lambda row: increment_matrix(row['img'], row['px']), axis=1)
    except:
        print("A problem occurred")
    img = df['img'].sum()
    print(pd.DataFrame(img), '\n')
    # we are performing these actions on single pings, so add this ping in the img_dic
    #import pdb; pdb.set_trace()
    #df['img']= df['img'].apply(lambda x: np.add.at(x, (2, 2), 1))
    # pretend df was always a spark dataframe and the assignments above were done by mapping (lol)
    # df structure will be mapped down to MMSI,timechunk,img_dict
    #return df.groupby(['mmsi', 'timechunk']).reduce(lambda x, y: x[:2] + [imgreduce(x[2], y[2])])
