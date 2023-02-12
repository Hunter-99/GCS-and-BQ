select
    distinct affiliated_base_number
from `chrome-encoder-375816.trips_data_all.fhv_tripdata_native`
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';

select
    distinct affiliated_base_number
from `chrome-encoder-375816.trips_data_all.fhv_tripdata_native_partitioned_and_clustered`
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';
