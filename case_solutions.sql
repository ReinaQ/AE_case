-- Create the BQ dataset

CREATE SCHEMA IF NOT EXISTS dbt_test
OPTIONS (location = 'EU');


-- Create each input table

CREATE TABLE IF NOT EXISTS dbt_test.input_0115 (
  account_id STRING NOT NULL,
  balance_dkk FLOAT64,
  balance_day DATE,
  load_day DATE)
AS (
  SELECT
    'A' AS account_id, CAST('100.0' AS FLOAT64) AS balance_dkk, CAST('2023-01-15' AS DATE FORMAT 'YYYY-MM-DD')AS balance_day, CAST('2023-01-15' AS DATE FORMAT 'YYYY-MM-DD') AS load_day
  UNION ALL
  SELECT
    'B' AS account_id, CAST('1000.0' AS FLOAT64) AS balance_dkk, CAST('2023-01-15' AS DATE FORMAT 'YYYY-MM-DD') AS balance_day, CAST('2023-01-15' AS DATE FORMAT 'YYYY-MM-DD') AS load_day
  UNION ALL
  SELECT
    'C' AS account_id, CAST('10000.0' AS FLOAT64) AS balance_dkk, CAST('2023-01-15' AS DATE FORMAT 'YYYY-MM-DD') AS balance_day, CAST('2023-01-15' AS DATE FORMAT 'YYYY-MM-DD') AS load_day
);


CREATE TABLE IF NOT EXISTS dbt_test.input_0117 (
  account_id STRING NOT NULL,
  balance_dkk FLOAT64,
  balance_day DATE,
  load_day DATE)
AS (
  SELECT
    'A' AS account_id, CAST('100.0' AS FLOAT64) AS balance_dkk, CAST('2023-01-15' AS DATE FORMAT 'YYYY-MM-DD')AS balance_day, CAST('2023-01-15' AS DATE FORMAT 'YYYY-MM-DD') AS load_day
  UNION ALL
  SELECT
    'B' AS account_id, CAST('1000.0' AS FLOAT64) AS balance_dkk, CAST('2023-01-15' AS DATE FORMAT 'YYYY-MM-DD') AS balance_day, CAST('2023-01-15' AS DATE FORMAT 'YYYY-MM-DD') AS load_day
  UNION ALL
  SELECT
    'C' AS account_id, CAST('10000.0' AS FLOAT64) AS balance_dkk, CAST('2023-01-15' AS DATE FORMAT 'YYYY-MM-DD') AS balance_day, CAST('2023-01-15' AS DATE FORMAT 'YYYY-MM-DD') AS load_day
  UNION ALL
  SELECT
    'A' AS account_id, CAST('200.0' AS FLOAT64) AS balance_dkk, CAST('2023-01-17' AS DATE FORMAT 'YYYY-MM-DD')AS balance_day, CAST('2023-01-17' AS DATE FORMAT 'YYYY-MM-DD') AS load_day
  UNION ALL
  SELECT
    'B' AS account_id, CAST('2000.0' AS FLOAT64) AS balance_dkk, CAST('2023-01-13' AS DATE FORMAT 'YYYY-MM-DD') AS balance_day, CAST('2023-01-17' AS DATE FORMAT 'YYYY-MM-DD') AS load_day
  UNION ALL
  SELECT
    'D' AS account_id, CAST('20000.0' AS FLOAT64) AS balance_dkk, CAST('2023-01-17' AS DATE FORMAT 'YYYY-MM-DD') AS balance_day, CAST('2023-01-17' AS DATE FORMAT 'YYYY-MM-DD') AS load_day
  UNION ALL
  SELECT
    'B' AS account_id, CAST('3000.0' AS FLOAT64) AS balance_dkk, CAST('2023-01-16' AS DATE FORMAT 'YYYY-MM-DD') AS balance_day, CAST('2023-01-17' AS DATE FORMAT 'YYYY-MM-DD') AS load_day
);


-- Solution for Q1

CREATE TABLE IF NOT EXISTS dbt_test.output_0115_q1 AS
(
    WITH date_range AS(
        SELECT account_id,
            MIN(balance_day) AS start_date,
            MAX(max_load_day) AS end_date
        FROM(
            SELECT account_id,
            balance_day,
            MAX(load_day) OVER () AS max_load_day
            FROM `dbt_test.input_0115`
        )
        GROUP BY 1
        ORDER BY 1
    ),
    date_list AS(
        SELECT account_id,
            date_array AS balance_day
        FROM date_range
        CROSS JOIN 
            UNNEST(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS date_array
    )
    SELECT date_list.account_id,
        LAST_VALUE(input.balance_dkk IGNORE NULLS) 
        OVER (PARTITION BY date_list.account_id 
                ORDER BY date_list.balance_day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance_dkk,
        date_list.balance_day
    FROM date_list
    LEFT JOIN `dbt_test.input_0115` AS input
    ON date_list.account_id = input.account_id
    AND date_list.balance_day = input.balance_day
    ORDER BY 1,3
)


CREATE TABLE IF NOT EXISTS dbt_test.output_0117_q1 AS
(
    WITH date_range AS(
        SELECT account_id,
            MIN(balance_day) AS start_date,
            MAX(max_load_day) AS end_date
        FROM(
            SELECT account_id,
            balance_day,
            MAX(load_day) OVER () AS max_load_day
            FROM `dbt_test.input_0117`
        )
        GROUP BY 1
        ORDER BY 1
    ),
    date_list AS(
        SELECT account_id,
            date_array AS balance_day
        FROM date_range
        CROSS JOIN 
            UNNEST(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS date_array
    )
    SELECT date_list.account_id,
    LAST_VALUE(input.balance_dkk IGNORE NULLS) 
        OVER (PARTITION BY date_list.account_id 
                ORDER BY date_list.balance_day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance_dkk,
        date_list.balance_day
    FROM date_list
    LEFT JOIN `dbt_test.input_0117` AS input
    ON date_list.account_id = input.account_id
    AND date_list.balance_day = input.balance_day
    ORDER BY 1,3
)


-- Create the account_create_date table for Q2
CREATE TABLE IF NOT EXISTS dbt_test.account_create_date (
  account_id STRING NOT NULL,
  account_created_at TIMESTAMP)
AS (
  SELECT
    'A' AS account_id, TIMESTAMP "2023-01-14 23:59:59" AS account_created_at
  UNION ALL
  SELECT
    'B' AS account_id, TIMESTAMP "2023-01-10 12:10:04" AS account_created_at
  UNION ALL
  SELECT
    'C' AS account_id, TIMESTAMP "2023-01-15 18:23:59" AS account_created_at
  UNION ALL
  SELECT
    'D' AS account_id, TIMESTAMP "2023-01-17 02:41:09" AS account_created_at
);


-- Solution for Q2

CREATE TABLE IF NOT EXISTS dbt_test.output_0115_q2 AS
(
    WITH input_w_creatation_date AS(
        SELECT a.*,
            DATE(b.account_created_at) AS account_creatation_date
        FROM `dbt_test.input_0115` a
        LEFT JOIN
            `dbt_test.account_create_date` b
        ON a.account_id = b.account_id
    ),
    date_range AS(
        SELECT account_id,
            MIN(account_creatation_date) AS start_date,
            MAX(max_load_day) AS end_date
        FROM(
            SELECT account_id,
            account_creatation_date,
            MAX(load_day) OVER () AS max_load_day
            FROM input_w_creatation_date
        )
        GROUP BY 1
        ORDER BY 1
    ),
    date_list AS(
        SELECT account_id,
            date_array AS balance_day
        FROM date_range
        CROSS JOIN 
            UNNEST(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS date_array
    )
    SELECT date_list.account_id,
        IFNULL(LAST_VALUE(input.balance_dkk IGNORE NULLS) 
                OVER (PARTITION BY date_list.account_id 
                ORDER BY date_list.balance_day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                ,0) AS balance_dkk,
        date_list.balance_day
    FROM date_list
    LEFT JOIN `dbt_test.input_0115` AS input
    ON date_list.account_id = input.account_id
    AND date_list.balance_day = input.balance_day
    ORDER BY 1,3
)



CREATE TABLE IF NOT EXISTS dbt_test.output_0117_q2 AS
(
    WITH input_w_creatation_date AS(
        SELECT a.*,
            DATE(b.account_created_at) AS account_creatation_date
        FROM `dbt_test.input_0117` a
        LEFT JOIN
            `dbt_test.account_create_date` b
        ON a.account_id = b.account_id
    ),
    date_range AS(
        SELECT account_id,
            MIN(account_creatation_date) AS start_date,
            MAX(max_load_day) AS end_date
        FROM(
            SELECT account_id,
            account_creatation_date,
            MAX(load_day) OVER () AS max_load_day
            FROM input_w_creatation_date
        )
        GROUP BY 1
        ORDER BY 1
    ),
    date_list AS(
        SELECT account_id,
            date_array AS balance_day
        FROM date_range
        CROSS JOIN 
            UNNEST(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS date_array
    )
    SELECT date_list.account_id,
        IFNULL(LAST_VALUE(input.balance_dkk IGNORE NULLS) 
                OVER (PARTITION BY date_list.account_id 
                ORDER BY date_list.balance_day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                ,0) AS balance_dkk,
        date_list.balance_day
    FROM date_list
    LEFT JOIN `dbt_test.input_0117` AS input
    ON date_list.account_id = input.account_id
    AND date_list.balance_day = input.balance_day
    ORDER BY 1,3
)