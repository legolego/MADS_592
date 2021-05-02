#import dask


import dask.dataframe as dd
#import pandas as pd
import numpy as np
#import geopandas as gpd
#import fastparquet
import os
import datetime

print('hello')
print(datetime.datetime.now())

shape = os.path.join('data/taxi_shape', 'taxi_zones.shp')
folder = os.path.join('data', 'yellow_tripdata_2010-*.csv')
# taxi_geo ="D:\Downloads\geo.json"
# write_to = r"C:\Users\corsi\OneDrive - Umich\milestone_1"

print(folder)

trips = dd.read_csv(folder, parse_dates=['pickup_datetime','dropoff_datetime' ],
                 error_bad_lines=False, dtype={'store_and_fwd_flag':object, 'trip_distance':'float64'})

cols = trips.columns.to_list()
dtypes = trips.dtypes.to_list()
dtypes_dict = dict(zip(cols, dtypes))

del dtypes_dict['pickup_datetime']
del dtypes_dict['dropoff_datetime']

trips = dd.read_csv(folder, parse_dates=['pickup_datetime', 'dropoff_datetime'],
                 error_bad_lines=False, dtype=dtypes_dict)




def assign_taxi_zones(df, lon_var, lat_var, locid_var):
    """Joins DataFrame with Taxi Zones shapefile.
    This function takes longitude values provided by `lon_var`, and latitude
    values provided by `lat_var` in DataFrame `df`, and performs a spatial join
    with the NYC taxi_zones shapefile. 
    The shapefile is hard coded in, as this function makes a hard assumption of
    latitude and longitude coordinates. It also assumes latitude=0 and 
    longitude=0 is not a datapoint that can exist in your dataset. Which is 
    reasonable for a dataset of New York, but bad for a global dataset.
    Only rows where `df.lon_var`, `df.lat_var` are reasonably near New York,
    and `df.locid_var` is set to np.nan are updated. 
    Parameters
    ----------
    df : pandas.DataFrame or dask.DataFrame
        DataFrame containing latitudes, longitudes, and location_id columns.
    lon_var : string
        Name of column in `df` containing longitude values. Invalid values 
        should be np.nan.
    lat_var : string
        Name of column in `df` containing latitude values. Invalid values 
        should be np.nan
    locid_var : string
        Name of series to return. 
    """

    import geopandas
    from shapely.geometry import Point


    # make a copy since we will modify lats and lons
    localdf = df[[lon_var, lat_var]].copy()
    
    # missing lat lon info is indicated by nan. Fill with zero
    # which is outside New York shapefile. 
    localdf[lon_var] = localdf[lon_var].fillna(value=0.)
    localdf[lat_var] = localdf[lat_var].fillna(value=0.)
    

    shape_df = geopandas.read_file(shape)
    shape_df.drop(['OBJECTID', "Shape_Area", "Shape_Leng", "borough", "zone"],
                  axis=1, inplace=True)
    shape_df = shape_df.to_crs({'init': 'epsg:4326'})

    try:
        local_gdf = geopandas.GeoDataFrame(
            localdf, crs={'init': 'epsg:4326'},
            geometry=[Point(xy) for xy in
                      zip(localdf[lon_var], localdf[lat_var])])

        local_gdf = geopandas.sjoin(
            local_gdf, shape_df, how='left', op='within')

        return local_gdf.LocationID.rename(locid_var)
    except ValueError as ve:
        print(ve)
        print(ve.stacktrace())
        series = localdf[lon_var]
        series = np.nan
        return series



trips['pickup_taxizone_id'] = trips.map_partitions(
    assign_taxi_zones, "pickup_longitude", "pickup_latitude",
    "pickup_taxizone_id", meta=('pickup_taxizone_id', np.float64))
trips['dropoff_taxizone_id'] = trips.map_partitions(
    assign_taxi_zones, "dropoff_longitude", "dropoff_latitude",
    "dropoff_taxizone_id", meta=('dropoff_taxizone_id', np.float64))


#trips.head()

trips.to_parquet('./data/trips.parquet', write_index=False)
print(datetime.datetime.now())
