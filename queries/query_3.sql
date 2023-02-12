select
    count(*)
from `chrome-encoder-375816.trips_data_all.fhv_tripdata_native`
where PUlocationID is not null
  and DOlocationID is not null;
