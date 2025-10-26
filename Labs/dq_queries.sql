-- Query from cell (potentially): y_JDYUgWu2eI
SELECT
    schema_name,
    location
FROM
    `{project_id}`.INFORMATION_SCHEMA.SCHEMATA
WHERE
    schema_name = 'netflix';;

-- Query from cell (potentially): bo8wpHK2yaPM
SELECT 'users' AS table_name, COUNT(*) AS row_count FROM `{project_id}.netflix.users`
UNION ALL
SELECT 'movies' AS table_name, COUNT(*) AS row_count FROM `{project_id}.netflix.movies`
UNION ALL
SELECT 'watch_history' AS table_name, COUNT(*) AS row_count FROM `{project_id}.netflix.watch_history`
UNION ALL
SELECT 'recommendation_logs' AS table_name, COUNT(*) AS row_count FROM `{project_id}.netflix.recommendation_logs`
UNION ALL
SELECT 'search_logs' AS table_name, COUNT(*) AS row_count FROM `{project_id}.netflix.search_logs`
UNION ALL
SELECT 'reviews' AS table_name, COUNT(*) AS row_count FROM `{project_id}.netflix.reviews`;;

-- Query from cell (potentially): YC0hhtJ1ytSK
SELECT country,
       COUNT(*) AS n,
       ROUND(100*COUNTIF(subscription_plan IS NULL)/COUNT(*),2) AS pct_missing_subscription_plan
FROM `{project_id}.netflix.users`
GROUP BY country
ORDER BY pct_missing_subscription_plan DESC;;

-- Query from cell (potentially): WSdxC0E9u2eK
-- Users: % missing per column
WITH base AS (
  SELECT COUNT(*) n,
         COUNTIF(country IS NULL) miss_country,
         COUNTIF(subscription_plan IS NULL) miss_plan,
         COUNTIF(age IS NULL) miss_age
  FROM `{project_id}.netflix.users`
)
SELECT n,
       ROUND(100*miss_country/n,2) AS pct_missing_country,
       ROUND(100*miss_plan/n,2)   AS pct_missing_subscription_plan,
       ROUND(100*miss_age/n,2)    AS pct_missing_age
FROM base;;

-- Query from cell (potentially): 7Sb_uGyczluJ
-- Verification: Print the three missingness percentages
WITH base AS (
  SELECT COUNT(*) n,
         COUNTIF(country IS NULL) miss_country,
         COUNTIF(subscription_plan IS NULL) miss_plan,
         COUNTIF(age IS NULL) miss_age
  FROM `{project_id}.netflix.users`
)
SELECT ROUND(100*miss_country/n,2) AS pct_missing_country,
       ROUND(100*miss_plan/n,2)   AS pct_missing_subscription_plan,
       ROUND(100*miss_age/n,2)    AS pct_missing_age
FROM base;;

-- Query from cell (potentially): 3qxp1PPj0EMu
-- Report duplicate groups on (user_id, movie_id, watch_date, device_type) with counts (top 20)
SELECT user_id, movie_id, watch_date, device_type, COUNT(*) AS dup_count
FROM `{project_id}.netflix.watch_history`
GROUP BY user_id, movie_id, watch_date, device_type
HAVING dup_count > 1
ORDER BY dup_count DESC
LIMIT 20;;

-- Query from cell (potentially): 13fa69a9
-- Create table watch_history_dedup keeping one row per group
CREATE OR REPLACE TABLE `{project_id}.netflix.watch_history_dedup` AS
SELECT * EXCEPT(rk) FROM (
  SELECT h.*,
         ROW_NUMBER() OVER (
           PARTITION BY user_id, movie_id, watch_date, device_type
           ORDER BY progress_percentage DESC, watch_duration_minutes DESC
         ) AS rk
  FROM `{project_id}.netflix.watch_history` h
)
WHERE rk = 1;;

-- Query from cell (potentially): cc65cef5
-- Verification: Before/after count query comparing raw vs watch_history_dedup
SELECT 'watch_history_raw' AS table_name, COUNT(*) AS row_count FROM `{project_id}.netflix.watch_history`
UNION ALL
SELECT 'watch_history_dedup' AS table_name, COUNT(*) AS row_count FROM `{project_id}.netflix.watch_history_dedup`;;

-- Query from cell (potentially): 5Cg16gz81Bbf
SELECT user_id, movie_id, watch_date, device_type, COUNT(*) AS dup_count
FROM `{project_id}.netflix.watch_history`
GROUP BY user_id, movie_id, watch_date, device_type
HAVING dup_count > 1
ORDER BY dup_count DESC
LIMIT 20;;

-- Query from cell (potentially): 63e8c3e5
-- Create table watch_history_dedup keeping one row per group
CREATE OR REPLACE TABLE `{project_id}.netflix.watch_history_dedup` AS
SELECT * EXCEPT(rk) FROM (
  SELECT h.*,
         ROW_NUMBER() OVER (
           PARTITION BY user_id, movie_id, watch_date, device_type
           ORDER BY progress_percentage DESC, watch_duration_minutes DESC
         ) AS rk
  FROM `{project_id}.netflix.watch_history` h
)
WHERE rk = 1;;

-- Query from cell (potentially): a0263970
-- Verification: Before/after count query comparing raw vs watch_history_dedup
SELECT 'watch_history_raw' AS table_name, COUNT(*) AS row_count FROM `{project_id}.netflix.watch_history`
UNION ALL
SELECT 'watch_history_dedup' AS table_name, COUNT(*) AS row_count FROM `{project_id}.netflix.watch_history_dedup`;;

-- Query from cell (potentially): A_k4F-IC2D4c
-- Compute IQR bounds for watch_duration_minutes on watch_history_dedup and report % outliers
WITH dist AS (
  SELECT
    APPROX_QUANTILES(watch_duration_minutes, 4)[OFFSET(1)] AS q1,
    APPROX_QUANTILES(watch_duration_minutes, 4)[OFFSET(3)] AS q3
  FROM `{project_id}.netflix.watch_history_dedup`
),
bounds AS (
  SELECT q1, q3, (q3-q1) AS iqr,
         q1 - 1.5*(q3-q1) AS lo,
         q3 + 1.5*(q3-q1) AS hi
  FROM dist
)
SELECT
  COUNTIF(h.watch_duration_minutes < b.lo OR h.watch_duration_minutes > b.hi) AS outliers,
  COUNT(*) AS total,
  ROUND(100*COUNTIF(h.watch_duration_minutes < b.lo OR h.watch_duration_minutes > b.hi)/COUNT(*),2) AS pct_outliers
FROM `{project_id}.netflix.watch_history_dedup` h
CROSS JOIN bounds b;;

-- Query from cell (potentially): IDkgxbV52zY0
-- Verification: Min/median/max before vs after capping
WITH before AS (
  SELECT 'before' AS which,
         MIN(watch_duration_minutes) AS min_val,
         APPROX_QUANTILES(watch_duration_minutes, 2)[OFFSET(1)] AS median_val,
         MAX(watch_duration_minutes) AS max_val
  FROM `{project_id}.netflix.watch_history_dedup`
),
after AS (
  SELECT 'after' AS which,
         MIN(watch_duration_minutes_capped) AS min_val,
         APPROX_QUANTILES(watch_duration_minutes_capped, 2)[OFFSET(1)] AS median_val,
         MAX(watch_duration_minutes_capped) AS max_val
  FROM `{project_id}.netflix.watch_history_robust`
)
SELECT * FROM before UNION ALL SELECT * FROM after;;

-- Query from cell (potentially): gjEgpucY3paD
-- In watch_history_robust, compute and summarize flag_binge for sessions > 8 hours
SELECT
  COUNTIF(watch_duration_minutes_capped > 8*60) AS sessions_over_8h,
  COUNT(*) AS total,
  ROUND(100*COUNTIF(watch_duration_minutes_capped > 8*60)/COUNT(*),2) AS pct
FROM `{project_id}.netflix.watch_history_robust`;;

-- Query from cell (potentially): 94e53ea9
-- In users, compute and summarize flag_age_extreme if age is <10 or >100
SELECT
  COUNTIF(age < 10 OR age > 100) AS extreme_age_rows,
  COUNT(*) AS total,
  ROUND(100*COUNTIF(age < 10 OR age > 100)/COUNT(*),2) AS pct
FROM `{project_id}.netflix.users`;;

-- Query from cell (potentially): NnJE3dCd4UvZ
-- In movies, compute and summarize flag_duration_anomaly where duration_minutes < 15 or > 480
SELECT
  COUNTIF(duration_minutes < 15) AS titles_under_15m,
  COUNTIF(duration_minutes > 480) AS titles_over_480m,
  COUNT(*) AS total,
  ROUND(100*COUNTIF(duration_minutes < 15 OR duration_minutes > 480)/COUNT(*),2) AS pct_duration_anomaly
FROM `{project_id}.netflix.movies`;;

-- Query from cell (potentially): JAxF1wrm4qSL
-- Verification: Compact summary query for all flags
WITH
  binge_summary AS (
    SELECT 'flag_binge' AS flag_name, ROUND(100*COUNTIF(watch_duration_minutes_capped > 8*60)/COUNT(*),2) AS pct_of_rows
    FROM `{project_id}.netflix.watch_history_robust`
  ),
  age_summary AS (
    SELECT 'flag_age_extreme' AS flag_name, ROUND(100*COUNTIF(age < 10 OR age > 100)/COUNT(*),2) AS pct_of_rows
    FROM `{project_id}.netflix.users`
  ),
  duration_summary AS (
    SELECT 'flag_duration_anomaly' AS flag_name, ROUND(100*COUNTIF(duration_minutes < 15 OR duration_minutes > 480)/COUNT(*),2) AS pct_of_rows
    FROM `{project_id}.netflix.movies`
  )
SELECT * FROM binge_summary
UNION ALL SELECT * FROM age_summary
UNION ALL SELECT * FROM duration_summary;;

