select
    count(distinct affiliated_base_number)
from `chrome-encoder-375816.trips_data_all.fhv_tripdata_native`;

select
    count(distinct affiliated_base_number)
from `chrome-encoder-375816.trips_data_all.fhv_tripdata_external`;
