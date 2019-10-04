


# the image_dict will contain (initially blank) images
# the keys are (lat,lon) tuples that give the bottom-left edge of the square
# the values are (initially blank) 128x128 numpy arrays
# image_dict[(lat,lon)]=np.zeros((128,128))

def imgreduce(x, y):
    return {s: x[s] + y[s] for s in x.keys()}


def vessel_img(df):
    # round off lat/lon to the .1 (for example) to get which image needs to be updated
    # subtract the previous round off from lat/lon, then divide by .00078125 (.1/128)
    # to get which point in that image should be incremented
    df['img_key'] = (round(df['latitude'], 1), round(df['longitude'], 1))
    # rows of df have lat-long values
    df['img_pnt'] = (int((df['latitude'] - round(df['longitude'], 1)) / (.00078125)),
                     int((df['latitude'] - round(df['longitude'], 1)) / (.00078125)))
    # df['img_dict']={df['img_key']:np.zeros((128,128))}
    df['img_dict'] = {x: np.zeros((128, 128)) for x in [tenth_rounded_lat_lon_tuples]}
    # we are performing these actions on single pings, so add this ping in the img_dict
    df['img_dict'][df['img_key']][df['img_pnt']] = 1
    # create a custom function for reduce-combining img_dict values

    # pretend df was always a spark dataframe and the assignments above were done by mapping (lol)
    # df structure will be mapped down to MMSI,timechunk,img_dict
    return df.groupby(['mmsi', 'timechunk']).reduce(lambda x, y: x[:2] + [imgreduce(x[2], y[2])])

