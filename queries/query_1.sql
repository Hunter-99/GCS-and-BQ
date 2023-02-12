create or replace external table `chrome-encoder-375816.trips_data_all.fhv_tripdata_external`
options (
    format = 'CSV',
    uris = ['gs://dtc_data_lake_324126473010/fhv_ny_taxi_data/2019/fhv_tripdata_2019-*.csv.gz']
);

create or replace table `chrome-encoder-375816.trips_data_all.fhv_tripdata_native` as
    select * from `chrome-encoder-375816.trips_data_all.fhv_tripdata_external`;

select
    count(*)
from `chrome-encoder-375816.trips_data_all.fhv_tripdata_native`;
