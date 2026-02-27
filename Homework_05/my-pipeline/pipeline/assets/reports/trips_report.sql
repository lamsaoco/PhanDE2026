/* @bruin

name: reports.trips_report
type: duckdb.sql

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

depends:
  - staging.trips

columns:
  - name: taxi_type
    type: string
    description: Taxi category (yellow/green)
    primary_key: true
  - name: trip_date
    type: DATE
    description: Date portion of pickup_datetime
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Number of trips in the bucket
    checks:
      - name: non_negative
  - name: total_fare
    type: float
    description: Total fare amount for the bucket
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  taxi_type,
  DATE(pickup_datetime) AS trip_date,
  COUNT(*) AS trip_count,
  SUM(fare_amount) AS total_fare
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY taxi_type, trip_date
