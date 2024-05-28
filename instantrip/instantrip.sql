-- 데이터 천처리 쿼리
WITH InitialData AS (
    SELECT 
        flight_id,
        -- 출발 도착 날짜
        CAST(CONCAT(SUBSTRING(departure_id, 1, 4), '-', SUBSTRING(departure_id, 5, 2), '-', SUBSTRING(departure_id, 7, 2)) AS DATE) AS DEPARTURE_DATE,
        CAST(CONCAT(SUBSTRING(arrival_id, 1, 4), '-', SUBSTRING(arrival_id, 5, 2), '-', SUBSTRING(arrival_id, 7, 2)) AS DATE) AS ARRIVAL_DATE,
        -- 출발 도착 경유 횟수
        LENGTH(departure_id) - LENGTH(REPLACE(departure_id, '+', '')) AS departure_layover_cnt,
        LENGTH(arrival_id) - LENGTH(REPLACE(arrival_id, '+', '')) AS arrival_layover_cnt,
    -- CAST(departure_detail[1].sdt AS VARCHAR) AS departure_time_1,
    -- CAST(departure_detail[2].sdt AS VARCHAR) AS departure_time_2,
    -- CAST(SUBSTRING(CAST(departure_detail[2].sdt AS VARCHAR), 10, 8) AS TIME) AS departure_time_2,
        departure_detail,
        arrival_detail,
        total_fare,
        partition_0,
        partition_1
    FROM 
        "instantrip"."20240430" 
),
FlightDetails AS (
    SELECT 
        *,
        date_diff('day', DEPARTURE_DATE, ARRIVAL_DATE) AS nights,
        -- 일요일0 토요일6
        day_of_week(DEPARTURE_DATE) AS departure_week,
        day_of_week(ARRIVAL_DATE) AS arrival_week
        
    FROM InitialData
)

SELECT *
FROM FlightDetails
limit 10

-- 공항 별 nights 별 제일 싼 항공권
WITH InitialData AS (
    SELECT 
        flight_id,
        CAST(CONCAT(SUBSTRING(departure_id, 1, 4), '-', SUBSTRING(departure_id, 5, 2), '-', SUBSTRING(departure_id, 7, 2)) AS DATE) AS DEPARTURE_DATE,
        CAST(CONCAT(SUBSTRING(arrival_id, 1, 4), '-', SUBSTRING(arrival_id, 5, 2), '-', SUBSTRING(arrival_id, 7, 2)) AS DATE) AS ARRIVAL_DATE,
        LENGTH(departure_id) - LENGTH(REPLACE(departure_id, '+', '')) AS d_layover_cnt,
        LENGTH(arrival_id) - LENGTH(REPLACE(arrival_id, '+', '')) AS a_layover_cnt,
        total_fare,
        partition_1
    FROM 
        "instantrip"."20240430" 
),
FlightDetails AS (
    SELECT 
        *,
        date_diff('day', DEPARTURE_DATE, ARRIVAL_DATE) AS days_difference
    FROM InitialData
),
FlightRank AS (
    SELECT 
        *,
        RANK() OVER (PARTITION BY partition_1, days_difference ORDER BY total_fare, DEPARTURE_DATE,flight_id ASC) AS fare_rank
    FROM FlightDetails
),
FlightRankFirst AS (
    SELECT *
    FROM FlightRank
    WHERE fare_rank = 1
)

SELECT *
FROM FlightRankFirst
ORDER BY partition_1, days_difference

-- 공항 별 항공권 가격 하위 30퍼 평균
SELECT 
    partition_1,
    round(AVG(total_fare)) AS trimmed_mean
FROM (
    SELECT 
        partition_1,
        total_fare,
        PERCENT_RANK() OVER (PARTITION BY partition_1 ORDER BY total_fare) AS pr
    FROM  "instantrip"."20240430"
) ranked
WHERE pr < 0.3
GROUP BY partition_1
order by trimmed_mean

-- 공항 별 항공권 수
SELECT count(0) as cnt_per_airport, partition_0
FROM  "instantrip"."20240430"
group by partition_0
order by cnt_per_airport desc

-- 공항 별 요일 별 평균 항공권 가격
WITH InitialData AS (
    SELECT 
        CAST(CONCAT(SUBSTRING(departure_id, 1, 4), '-', SUBSTRING(departure_id, 5, 2), '-', SUBSTRING(departure_id, 7, 2)) AS DATE) AS DEPARTURE_DATE,
        partition_1,
        total_fare
    FROM 
        "instantrip"."20240430" 
),
AddWeekData AS (
    SELECT 
        *,
        day_of_week(DEPARTURE_DATE) AS departure_week
    FROM InitialData
)


select count(0) as cnt_per_week, partition_1, departure_week
from AddWeekData
group by partition_1, departure_week
order by partition_1, departure_week

-- 출발 시간대 별 항공권 가격
SELECT COUNT(0) AS CNT, TT
FROM (
SELECT 
    CASE
        WHEN CAST(date_format(departure_date_1, '%i') AS integer) > 30 THEN CAST(date_format(departure_date_1, '%H') AS integer) + 1
        ELSE CAST(date_format(departure_date_1, '%H') AS integer)
    END AS TT
FROM(
    SELECT 
        date_parse(departure_detail[1].sdt , '%Y%m%d%H%i') AS departure_date_1
    FROM 
        "instantrip"."20240430"
))
GROUP BY TT
ORDER BY TT

-- 경유 횟수 별 항공권 가격
select round(avg(total_fare)) as avg_fare, departure_layover_cnt
from(
select
    -- LENGTH(arrival_id) - LENGTH(REPLACE(arrival_id, '+', '')) AS arrival_layover_cnt
    LENGTH(departure_id) - LENGTH(REPLACE(departure_id, '+', '')) AS departure_layover_cnt,
    total_fare
from "instantrip"."20240430"
)
group by departure_layover_cnt
order by departure_layover_cnt

-- 경유 횟수 별 항공권 수
select count(0) as stop_over_cnt, departure_layover_cnt
from(
select
    -- LENGTH(arrival_id) - LENGTH(REPLACE(arrival_id, '+', '')) AS arrival_layover_cnt
    LENGTH(departure_id) - LENGTH(REPLACE(departure_id, '+', '')) AS departure_layover_cnt,
    total_fare
from "instantrip"."20240430"
)
group by departure_layover_cnt
order by departure_layover_cnt

-- 경유 대기 시간 분포도
WITH layover_counts AS (
    SELECT
        flight_id,
        departure_detail,
        LENGTH(departure_id) - LENGTH(REPLACE(departure_id, '+', '')) AS departure_layover_cnt
    FROM "instantrip"."20240430"
),
parsed_data AS (
    SELECT
        flight_id,
        departure_layover_cnt,
        CASE WHEN departure_layover_cnt >= 0 THEN CAST(SUBSTRING(departure_detail[1].CT, 1, 2) AS INTEGER) * 60 
                                                + CAST(SUBSTRING(departure_detail[1].CT, 3, 2) AS INTEGER) ELSE NULL END AS ct_0,
                                                
        CASE WHEN departure_layover_cnt >= 1 THEN CAST(SUBSTRING(departure_detail[2].CT, 1, 2) AS INTEGER) * 60 
                                                + CAST(SUBSTRING(departure_detail[2].CT, 3, 2) AS INTEGER) ELSE NULL END AS ct_1,
                                                
        CASE WHEN departure_layover_cnt >= 2 THEN CAST(SUBSTRING(departure_detail[3].CT, 1, 2) AS INTEGER) * 60 
                                                + CAST(SUBSTRING(departure_detail[3].CT, 3, 2) AS INTEGER) ELSE NULL END AS ct_2,
                                                
        CASE WHEN departure_layover_cnt >= 3 THEN CAST(SUBSTRING(departure_detail[4].CT, 1, 2) AS INTEGER) * 60 
                                                + CAST(SUBSTRING(departure_detail[4].CT, 3, 2) AS INTEGER) ELSE NULL END AS ct_3
    FROM layover_counts
    WHERE departure_layover_cnt > 0
),
union_data AS (
    SELECT
        CAST(ct_0 AS INTEGER) AS ct_minutes
    FROM parsed_data
    WHERE CAST(ct_0 AS INTEGER) > 0
    UNION ALL
    SELECT
        CAST(ct_1 AS INTEGER) AS ct_minutes
    FROM parsed_data
    WHERE CAST(ct_1 AS INTEGER) > 0
    UNION ALL
    SELECT
        CAST(ct_2 AS INTEGER) AS ct_minutes
    FROM parsed_data
    WHERE CAST(ct_2 AS INTEGER) > 0
    UNION ALL
    SELECT
        CAST(ct_3 AS INTEGER) AS ct_minutes
    FROM parsed_data
    WHERE CAST(ct_3 AS INTEGER) > 0
)
SELECT
    ROUND(ct_minutes / 30.0)/2 AS rounded_hours,
    COUNT(*) AS count
FROM
    union_data
GROUP BY
    ROUND(ct_minutes / 30.0)
ORDER BY
    rounded_hours;

-- 공항별 랜덤 10개 추출
WITH RandomRows AS (
    SELECT 
        partition_1, 
        flight_id
    FROM (
        SELECT 
            partition_1, 
            flight_id,
            ROW_NUMBER() OVER (PARTITION BY partition_1 ORDER BY RAND()) as row_num
        FROM "instantrip"."20240430"
    ) AS ordered_rows
    WHERE row_num <= 10
)

SELECT p.*
FROM "instantrip"."20240430" p
JOIN RandomRows r
ON p.partition_1 = r.partition_1 AND p.flight_id = r.flight_id;

-- 출발 도착 경유 횟수 카운트 피벗 테이블
WITH layover_counts AS (
    SELECT
        flight_id,
        LENGTH(departure_id) - LENGTH(REPLACE(departure_id, '+', '')) AS departure_layover_cnt,
        LENGTH(arrival_id) - LENGTH(REPLACE(arrival_id, '+', '')) AS arrival_layover_cnt
    FROM "instantrip"."20240430"
)

SELECT
    departure_layover_cnt,
    COUNT(IF(arrival_layover_cnt = 0, 1, NULL)) AS arrival_layover_0,
    COUNT(IF(arrival_layover_cnt = 1, 1, NULL)) AS arrival_layover_1,
    COUNT(IF(arrival_layover_cnt = 2, 1, NULL)) AS arrival_layover_2,
    COUNT(IF(arrival_layover_cnt = 3, 1, NULL)) AS arrival_layover_3,
    COUNT(IF(arrival_layover_cnt = 4, 1, NULL)) AS arrival_layover_4,
    COUNT(IF(arrival_layover_cnt = 5, 1, NULL)) AS arrival_layover_5
FROM layover_counts
GROUP BY departure_layover_cnt
ORDER BY departure_layover_cnt;



-- 직항 출발 시간 별 항공권 수
WITH InitialData AS (
    SELECT 
        flight_id,
        -- 출발 도착 날짜
        CAST(SUBSTRING(departure_id, 1, 4) || '-' || SUBSTRING(departure_id, 5, 2) || '-' || SUBSTRING(departure_id, 7, 2) AS DATE) AS DEPARTURE_DATE,
        CAST(SUBSTRING(arrival_id, 1, 4) || '-' || SUBSTRING(arrival_id, 5, 2) || '-' || SUBSTRING(arrival_id, 7, 2) AS DATE) AS ARRIVAL_DATE,
        -- 출발 도착 경유 횟수
        LENGTH(departure_id) - LENGTH(REPLACE(departure_id, '+', '')) AS departure_layover_cnt,
        LENGTH(arrival_id) - LENGTH(REPLACE(arrival_id, '+', '')) AS arrival_layover_cnt,
        date_parse(departure_detail[1].sdt , '%Y%m%d%H%i') AS departure_sdt,
        date_parse(arrival_detail[1].sdt , '%Y%m%d%H%i') AS arrival_sdt,
        departure_detail,
        arrival_detail,
        total_fare,
        partition_1
    FROM 
        "instantrip"."20240430" 
),
DirectFlight AS (
SELECT 
    flight_id,
    DEPARTURE_DATE,
    ARRIVAL_DATE,
    departure_sdt,
    arrival_sdt,
    date_diff('day', DEPARTURE_DATE, ARRIVAL_DATE) AS nights,
    total_fare,
    partition_1
FROM InitialData
WHERE departure_layover_cnt = 0 AND arrival_layover_cnt = 0
),
GroupedFlights AS (
SELECT 
    flight_id,
    DEPARTURE_DATE,
    ARRIVAL_DATE,
    departure_sdt,
    arrival_sdt,
    nights,
    total_fare,
    partition_1,
    -- Grouping departure_sdt into 30-minute intervals
    DATE_TRUNC('minute', departure_sdt) + INTERVAL '15' minute * CAST(FLOOR(EXTRACT(MINUTE FROM departure_sdt) / 15) AS INTEGER) AS dep_sdt_30min
FROM DirectFlight
)

SELECT 
    date_format(dep_sdt_30min, '%H%i') AS dep_sdt_30min_formatted,
    COUNT(*) AS flight_count
FROM GroupedFlights
GROUP BY date_format(dep_sdt_30min, '%H%i')
ORDER BY date_format(dep_sdt_30min, '%H%i');


-- 직항 출발 시간대 별 평균 가격
WITH InitialData AS (
    SELECT 
        flight_id,
        -- 출발 도착 날짜
        CAST(SUBSTRING(departure_id, 1, 4) || '-' || SUBSTRING(departure_id, 5, 2) || '-' || SUBSTRING(departure_id, 7, 2) AS DATE) AS DEPARTURE_DATE,
        CAST(SUBSTRING(arrival_id, 1, 4) || '-' || SUBSTRING(arrival_id, 5, 2) || '-' || SUBSTRING(arrival_id, 7, 2) AS DATE) AS ARRIVAL_DATE,
        -- 출발 도착 경유 횟수
        LENGTH(departure_id) - LENGTH(REPLACE(departure_id, '+', '')) AS departure_layover_cnt,
        LENGTH(arrival_id) - LENGTH(REPLACE(arrival_id, '+', '')) AS arrival_layover_cnt,
        date_parse(departure_detail[1].sdt , '%Y%m%d%H%i') AS departure_sdt,
        date_parse(arrival_detail[1].sdt , '%Y%m%d%H%i') AS arrival_sdt,
        departure_detail,
        arrival_detail,
        total_fare,
        partition_0,
        partition_1
    FROM 
        "instantrip"."20240430" 
),
DirectFlight AS (
SELECT 
    flight_id,
    DEPARTURE_DATE,
    ARRIVAL_DATE,
    departure_sdt,
    arrival_sdt,
    date_diff('day', DEPARTURE_DATE, ARRIVAL_DATE) AS nights,
    total_fare,
    partition_0,
    partition_1
FROM InitialData
WHERE departure_layover_cnt = 0 AND arrival_layover_cnt = 0
),
GroupedFlights AS (
SELECT 
    flight_id,
    DEPARTURE_DATE,
    ARRIVAL_DATE,
    departure_sdt,
    arrival_sdt,
    nights,
    total_fare,
    partition_0,
    partition_1,
    -- Grouping departure_sdt into 30-minute intervals
    DATE_TRUNC('hour', departure_sdt) + INTERVAL '30' minute * CAST(FLOOR(EXTRACT(MINUTE FROM departure_sdt) / 30) AS INTEGER) AS dep_sdt_30min
FROM DirectFlight
)

SELECT 
    date_format(dep_sdt_30min, '%H:%i') AS dep_sdt_30min_formatted,
    round(avg(total_fare),-3) AS avg_fare,
    partition_0
FROM GroupedFlights
GROUP BY date_format(dep_sdt_30min, '%H:%i'), partition_0
ORDER BY date_format(dep_sdt_30min, '%H:%i');

-- 직항 가격 쿼리
WITH InitialData AS (
    SELECT 
        flight_id,
        -- 출발 도착 날짜
        CAST(SUBSTRING(departure_id, 1, 4) || '-' || SUBSTRING(departure_id, 5, 2) || '-' || SUBSTRING(departure_id, 7, 2) AS DATE) AS DEPARTURE_DATE,
        CAST(SUBSTRING(arrival_id, 1, 4) || '-' || SUBSTRING(arrival_id, 5, 2) || '-' || SUBSTRING(arrival_id, 7, 2) AS DATE) AS ARRIVAL_DATE,
        -- 출발 도착 경유 횟수
        LENGTH(departure_id) - LENGTH(REPLACE(departure_id, '+', '')) AS departure_layover_cnt,
        LENGTH(arrival_id) - LENGTH(REPLACE(arrival_id, '+', '')) AS arrival_layover_cnt,
        date_parse(departure_detail[1].sdt , '%Y%m%d%H%i') AS departure_sdt,
        date_parse(arrival_detail[1].sdt , '%Y%m%d%H%i') AS arrival_sdt,
        departure_detail,
        arrival_detail,
        total_fare,
        partition_0,
        partition_1
    FROM 
        "instantrip"."20240430" 
),
DirectFlight AS (
SELECT 
    flight_id,
    DEPARTURE_DATE,
    ARRIVAL_DATE,
    departure_sdt,
    arrival_sdt,
    date_diff('day', DEPARTURE_DATE, ARRIVAL_DATE) AS nights,
    total_fare,
    partition_0,
    partition_1
FROM InitialData
WHERE departure_layover_cnt = 0 AND arrival_layover_cnt = 0
)

SELECT total_fare, partition_0
from DirectFlight