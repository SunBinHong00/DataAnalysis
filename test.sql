SELECT 
    flight_id,
    departure_id,
    CAST(CONCAT(SUBSTRING(departure_id, 1, 4), '-', SUBSTRING(departure_id, 5, 2), '-', SUBSTRING(departure_id, 7, 2)) AS DATE) AS DEPARTURE_DATE,
    departure_detail,
    arrival_id,
    CAST(CONCAT(SUBSTRING(arrival_id, 1, 4), '-', SUBSTRING(arrival_id, 5, 2), '-', SUBSTRING(arrival_id, 7, 2)) AS DATE) AS ARRIVAL_DATE,
    arrival_detail,
    total_fare,
    CAST(departure_detail[1].sdt AS VARCHAR) AS departure_time_1,
    CAST(departure_detail[2].sdt AS VARCHAR) AS departure_time_2
    -- CAST(SUBSTRING(CAST(departure_detail[2].sdt AS VARCHAR), 10, 8) AS TIME) AS departure_time_2,
    -- CAST(SUBSTRING(CAST(departure_detail[3].sdt AS VARCHAR), 10, 8) AS TIME) AS departure_time_3
FROM 
    "instantrip"."20240430" 
WHERE 
    partition_1 = 'EWR' 
LIMIT 10;