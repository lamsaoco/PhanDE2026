/* @bruin

name: staging.trips
type: duckdb.sql

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

depends:
  - ingestion.trips
  - ingestion.payment_lookup

columns:
  - name: pickup_datetime
    type: timestamp
    description: Trip start timestamp
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Trip end timestamp
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: Fare charged for the trip
    checks:
      - name: non_negative
  - name: passenger_count
    type: integer
    description: Number of passengers
    checks:
      - name: non_negative

custom_checks:
  - name: no_duplicate_trip_times
    description: There should be no duplicate pickup/dropoff datetime pairs
    value: 0
    query: |-
      SELECT COUNT(*) FROM (
        SELECT pickup_datetime, dropoff_datetime, COUNT(*) AS cnt
        FROM {{ this }}
        GROUP BY pickup_datetime, dropoff_datetime
        HAVING COUNT(*) > 1
      )

@bruin */

SELECT
  t.*,
  p.payment_type_name
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup p
  ON t.payment_type = p.payment_type_id
WHERE t.pickup_datetime >= '{{ start_datetime }}'
  AND t.pickup_datetime < '{{ end_datetime }}'
