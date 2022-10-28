import os
import datetime
import pandas as pd
import boto3
import botocore
import xarray as xr
import h3


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

def get_data():
    date = datetime.date(2022, 5, 1)
    prefix = date.strftime('%Y/%m/')
    era5_bucket = 'era5-pds'
    metadata_key = prefix + 'data/precipitation_amount_1hour_Accumulation.nc'

    if not os.path.isdir('data'):
        os.makedirs('data')
    output_file_name = os.path.join('data', 'era5_{}-{}_precip.nc'.format(date.strftime('%Y'), date.strftime('%m')))

    if os.path.exists(output_file_name):
        return output_file_name
    else:
        print('Downloading ERA5 data...')
        client = boto3.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))
        client.download_file(era5_bucket, metadata_key, output_file_name)
        print('DONE')
        return output_file_name

def create_parquet(args):
    timestamp_from = args.timestamp_from
    timestamp_to = args.timestamp_to

    if timestamp_from is None:
        timestamp_from = '2022-05-01 00:00:00'
    else:
        timestamp_from = ' '.join(timestamp_from.split('_'))
    if timestamp_to is None:
        timestamp_to = '2022-05-31 23:00:00'
    else:
        timestamp_to = ' '.join(timestamp_to.split('_'))


    h3_cell = args.h3_cell
    output_file_name = get_data()
    print('Querying for \n'
          'timestamp_from = {}, \n'
          'timestamp_to = {}, \n'
          'h3 cell = {}'.format(timestamp_from, timestamp_to, h3_cell))
    cell_coords = h3.cell_to_latlng(h3_cell)

    ds = xr.open_dataset(output_file_name)

    ds_clipped = ds.where((cell_coords[1]-1 < ds.lon) & (ds.lon < cell_coords[1]+1)
                        & (cell_coords[0]-1 < ds.lat) & (ds.lat < cell_coords[0]+1), drop=True)
    parquet_file_name = output_file_name\
                        .split('\\')[-1]\
                        .split('.')[0]+'_'+h3_cell+'.parquet'


    ds_time_filtered = ds_clipped.sel(time1=slice(timestamp_from, timestamp_to))

    df = ds_time_filtered.to_dataframe()
    df['lon'] = df.index.get_level_values(2)
    df['lat'] = df.index.get_level_values(3)
    df['h3'] = df.apply(lambda row: h3.latlng_to_cell(row['lat'], row['lon'], 5), axis=1)

    df_h3_filtered = df[df['h3']==h3_cell]\
        .drop(columns=['time1_bounds', 'lat', 'lon']).reset_index()
    df_h3_filtered = df_h3_filtered.rename(
        columns={'time1': 'Timestamp', 'precipitation_amount_1hour_Accumulation': 'Precipitation'}) \
        .drop(columns=['nv', 'lat', 'lon'])

    if not os.path.isdir('results'):
        os.makedirs('results')
    df_h3_filtered.to_parquet(os.path.join('results', parquet_file_name))
    print('DONE')
    print('Parquet file {} placed in the /results folder.'.format(parquet_file_name))
    print('Results: ')
    print(pd.read_parquet(os.path.join('results', parquet_file_name)))

