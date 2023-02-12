create or replace table `chrome-encoder-375816.trips_data_all.fhv_tripdata_native_partitioned_and_clustered`
    partition by date(pickup_datetime)
    cluster by affiliated_base_number
as select * from `chrome-encoder-375816.trips_data_all.fhv_tripdata_native`;