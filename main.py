import pandas as pd

from google.cloud import storage


PRIVATE_KEY_FILEPATH = 'credentials/chrome-encoder-375816-0ebf7100ee4d.json'
GCP_BUCKET_NAME = 'dtc_data_lake_324126473010'


def transform_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    raw_df['dispatching_base_num'] = raw_df['dispatching_base_num'].astype('str')
    raw_df['pickup_datetime'] = pd.to_datetime(raw_df['pickup_datetime'])
    raw_df['dropOff_datetime'] = pd.to_datetime(raw_df['dropOff_datetime'])
    raw_df['PUlocationID'] = raw_df['PUlocationID'].fillna(0).astype('int')
    raw_df['DOlocationID'] = raw_df['DOlocationID'].fillna(0).astype('int')
    raw_df['SR_Flag'] = raw_df['SR_Flag'].astype('str')
    raw_df['Affiliated_base_number'] = raw_df['Affiliated_base_number'].astype('str')

    raw_df = raw_df.rename(columns={
        'dropOff_datetime': 'drop_off_datetime',
        'PUlocationID': 'pu_location_id',
        'DOlocationID': 'do_location_id',
        'SR_Flag': 'sr_flag',
        'Affiliated_base_number': 'affiliated_base_number'
    })

    return raw_df


def run():
    gcp_client = storage.Client.from_service_account_json(
        json_credentials_path=PRIVATE_KEY_FILEPATH
    )
    # fhv_ny_taxi_data/
    gcp_bucket = gcp_client.bucket(GCP_BUCKET_NAME)
    
    for year in range(2019, 2020):
        for month in range(1, 13):
            dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz"
            print(f"Downloading data from {dataset_url} file ...")
            raw_df = pd.read_csv(
                filepath_or_buffer=dataset_url
            )
            dataset_name = dataset_url.split('/')[-1].replace('.csv.gz', '.parquet')
            dataset_local_file_path = f"datasets/{dataset_name}"

            df = transform_data(raw_df)
            print(df.head(10))
            df.to_parquet(
                path=dataset_local_file_path,
                engine='fastparquet',
                compression='gzip'
            )
            print(f"Dataset saved to {dataset_local_file_path}")
            print(f"Uploading dataset to GCP ... ")
            gcp_blob = gcp_bucket.blob(
                f"fhv_ny_taxi_data/{year}/{dataset_name}"
            )
            gcp_blob.upload_from_filename(
                dataset_local_file_path
            )
            print("Done!")


if __name__ == '__main__':
    run()
