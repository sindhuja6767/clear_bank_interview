"""
Daily Trip Job - Assumptions and Documentation:
1. Timezone conversion: UTC -> PST (America/Los_Angeles)
2. Trip IDs are unique per trip per date.
3. Distance calculation:
   - Haversine formula used if GPS coverage > 50%
   - Velocity-based approximation otherwise
4. Trip duration = max(local_time) - min(local_time)
5.Trips with no valid data for the date return an empty DataFrame rather than None
6. Output schema:
   trip_id, date_pst, make, model, trip_duration_minutes, distance_travelled
7. The output files are written to the specified location as parquet files.
8. Need to pass the arguments like input drive path , input vehicle path, output locations and date.
9.Logs are written to 'logs/daily_trip_job.log'. 
"""

import argparse
import logging
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession
from daily_trip_job import read_inputs, assemble_daily_trips


logging.basicConfig(
    filename="logs/daily_trip_job.log",   # path to log file
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

handler = RotatingFileHandler("logs/daily_trip_job.log", maxBytes=5_000_000, backupCount=5)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

#Main
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input-drive", required=True)
    p.add_argument("--input-vehicle", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--date", required=True)
    args = p.parse_args()

    spark = SparkSession.builder.appName("ACRTA_daily_trip_job").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    drive, vehicle = read_inputs(spark, args.input_drive, args.input_vehicle)
    result = assemble_daily_trips(spark, drive, vehicle, args.date)

    if result.rdd.isEmpty():
        print("No data for date", args.date)
        logger.warning(f"No trips found for date {args.date}")

    else:
        result.write.mode("overwrite").partitionBy("date_pst").parquet(args.output)
        print("Wrote results to", args.output)
        logger.info(f"Wrote results to {args.output}")


    spark.stop()

if __name__ == "__main__":
    main()
