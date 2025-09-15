"""
Trip SQL Job - Assumptions and Documentation

1. Only trips for which vehicle specifications exist are included.
2. Fuel in litres is calculated as: (fuel_level / 255) * fuel_tank_capacity
   - If fuel increases (refuel), fuel used is capped at 0 (GREATEST method)
3. Engine load percentage = 100 * (eng_load / 255)
4. Trip IDs are assumed unique.
5. The job reads 'Drive' data (parquet) and 'Vehicle' data (CSV).
6. Output can be written to parquet, or results inspected in Spark DataFrame.
7. Logs are written to 'logs/trip_sql.log'.
"""


from pyspark.sql import SparkSession
import logging
from logging.handlers import RotatingFileHandler
logging.basicConfig(
    filename="logs/trip_sql.log",   # path to log file
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

handler = RotatingFileHandler("logs/daily_trip_job.log", maxBytes=5_000_000, backupCount=5)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def main():
    logger.info("Starting Trip SQL Job")

    try:

        # Initialize Spark
        spark = SparkSession.builder.appName("TripSQLJob").getOrCreate()

        # Load inputs
        drive_path = "supporting-data/drive"
        vehicle_path = "supporting-data/vehicle.csv"
        sql_file = "program/trip_sql.sql"
        logger.info(f"Reading input data: Drive={drive_path}, Vehicle={vehicle_path}")

        # Register Drive and Vehicle as temporary views
        spark.read.parquet(drive_path).createOrReplaceTempView("Drive")
        spark.read.csv(vehicle_path, header=True, inferSchema=True).createOrReplaceTempView("Vehicle")
        logger.info("Temporary views created for Drive and Vehicle")

        # Load SQL text from file
        with open(sql_file, "r") as f:
            sql_text = f.read()
        logger.info(f"SQL file '{sql_file}' loaded")


        # Run SQL
        trip_sql_result = spark.sql(sql_text)
        logger.info("SQL executed successfully")

        # Show filtered example trips
        example_trips = [
            "00922df3be5a4589ab385d0c2da2dd81",
            "03b8ac1525474754b4d89d2d647aa8e4",
            "1e20465533c545f98332ff14d5a0af22",
            "02c51e56cc484711b218d3d01196687a"
        ]
        trip_sql_result.filter(trip_sql_result.trip_id.isin(example_trips)).show(truncate=False)

        logger.info(f"Filtered example trips displayed: {example_trips}")


        # Uncomment to inspect top rows
        #trip_sql_result.show(10, truncate=False)
        # logger.info("Top 10 rows displayed")


        # Uncomment to write results to output
        # trip_sql_result.write.mode("overwrite").parquet("output/trip_sql_results")
        # logger.info(f"Results written to {output_path}")


    except Exception as e:
        logger.error(f"Error in Trip SQL Job: {str(e)}", exc_info=True)

    finally:
        # Stop Spark
        spark.stop()
        logger.info("SparkSession stopped, Trip SQL Job finished")

if __name__ == "__main__":
    main()
