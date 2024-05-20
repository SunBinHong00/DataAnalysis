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