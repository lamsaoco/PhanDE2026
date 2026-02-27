"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

secrets:
  - key: duckdb-default
    inject_as: duckdb-default

columns:
  - name: pickup_datetime
    type: timestamp
    description: Trip start timestamp
    primary_key: true
  - name: dropoff_datetime
    type: timestamp
    description: Trip end timestamp
    primary_key: true
  - name: taxi_type
    type: string
    description: Yellow/green taxi indicator
  - name: fare_amount
    type: float
    description: Fare charged for the trip
  - name: payment_type
    type: integer
    description: Payment type identifier
  - name: passenger_count
    type: integer
    description: Number of passengers

@bruin"""

import os
import json
from datetime import datetime

import pandas as pd

def materialize():
    """Fetch raw NYC taxi parquet files for the requested date window.

    This implementation uses the built‑in Bruin environment variables to
    determine the start/end dates and any pipeline variables defined by the
    user.  It loops month-by-month over the interval and for each configured
    `taxi_types` downloads the corresponding parquet file via HTTP.  The
    resulting DataFrames are concatenated and a few bookkeeping columns are
    added before returning the combined dataset to Bruin's Python
    materialization layer.
    """

    # parse the window dates provided by Bruin
    start = pd.to_datetime(os.environ.get("BRUIN_START_DATE"))
    end = pd.to_datetime(os.environ.get("BRUIN_END_DATE"))

    # pipeline variables are passed in JSON via BRUIN_VARS
    vars_json = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_json.get("taxi_types", ["yellow", "green"])

    frames = []
    curr = start.replace(day=1)
    while curr < end:
        year_month = curr.strftime("%Y-%m")
        for t in taxi_types:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{t}_tripdata_{year_month}.parquet"
            try:
                df = pd.read_parquet(url)
                
                # Rename columns based on taxi type
                if t == "yellow":
                    df = df.rename(columns={
                        "tpep_pickup_datetime": "pickup_datetime",
                        "tpep_dropoff_datetime": "dropoff_datetime"
                    })
                elif t == "green":
                    df = df.rename(columns={
                        "lpep_pickup_datetime": "pickup_datetime",
                        "lpep_dropoff_datetime": "dropoff_datetime"
                    })
                
                # Select only the columns we need
                df = df[[
                    "pickup_datetime",
                    "dropoff_datetime",
                    "fare_amount",
                    "payment_type",
                    "passenger_count"
                ]]
                
                # Convert payment_type to integer if it's a string
                if df["payment_type"].dtype == "object":
                    df["payment_type"] = pd.to_numeric(df["payment_type"], errors="coerce").astype("int64")
                else:
                    df["payment_type"] = df["payment_type"].astype("int64")
                
                # Add booking columns
                df["taxi_type"] = t
                df["extracted_at"] = pd.Timestamp.utcnow()
                
                frames.append(df)
            except Exception as exc:  # pragma: no cover
                # log and continue so that missing files don't kill a whole run
                print(f"warning: unable to read {url}: {exc}")
        # move to next month
        curr = (curr + pd.DateOffset(months=1)).replace(day=1)

    if frames:
        return pd.concat(frames, ignore_index=True)
    else:
        # return empty DF with no rows but no error
        return pd.DataFrame()
