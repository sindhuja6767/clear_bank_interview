import logging
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from pyspark.sql.functions import (
    to_timestamp, col, from_utc_timestamp, to_date, lit,min as spark_min, 
    max as spark_max, unix_timestamp, lag, radians, sin, cos, sqrt, asin, sum as spark_sum, expr
    )
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, DecimalType

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


#Read inputs & normalize types
def read_inputs(spark: SparkSession, drive_path: str, vehicle_path: str):
    """
    Read drive and vehicle inputs.
    - Drive is parquet, ensure datetime is timestamp
    - Vehicle is CSV, infer schema and cast vehicle_spec_id to long

    Returns: drive_df, vehicle_df
    """
    drive = spark.read.parquet(drive_path)
    # ensure 'datetime' is timestamp
    drive = drive.withColumn("datetime", to_timestamp(col("datetime")))

    vehicle = spark.read.csv(vehicle_path, header=True, inferSchema=True)
    vehicle = vehicle.withColumn("vehicle_spec_id", col("vehicle_spec_id").cast("long"))

    return drive, vehicle

#Timezone conversion 
def add_local_datetime(drive_df, tz="America/Los_Angeles"):
    """
    Convert UTC datetime to local timezone and add date column
    """
    # adds datetime_pst (timestamp) and date_pst (date)
    df = drive_df.withColumn("datetime_local", from_utc_timestamp(col("datetime"), tz))
    df = df.withColumn("date_local", to_date(col("datetime_local")))
    return df
# filter by target date
def filter_for_date(drive_df, target_date): 
    """
    Filter drive data for the target date (in local timezone)
    target_date string 'YYYY-MM-DD'
    """
    return drive_df.filter(col("date_local") == lit(target_date))

#Calculating trip duration
def compute_trip_duration(filtered_df):
    """
    Compute duration of each trip in minutes
    Trip duration for the date = max(datetime_local) - min(datetime_local) within that trip on that date.

    """
    agg = filtered_df.groupBy("trip_id").agg(
        spark_min("datetime_local").alias("first_ts"),
        spark_max("datetime_local").alias("last_ts")
    )
    # duration minutes
    agg = agg.withColumn("trip_duration_minutes",
                         (unix_timestamp(col("last_ts")) - unix_timestamp(col("first_ts"))) / 60.0)
    return agg

#Calculating distance travelled
#Using Haversine formula as we have latitude and longitude info for best results
def add_prev_coords(filtered_df):
    """
    Add previous lat/long for haversine calculation
    """
    from pyspark.sql.window import Window
    w = Window.partitionBy("trip_id").orderBy("datetime_local")
    df = filtered_df.withColumn("prev_lat", lag("lat").over(w)) \
                    .withColumn("prev_long", lag("long").over(w))
    return df

def compute_haversine_segment(df_with_prev):
    """
    Compute segment distance in km using haversine formula using SQL functions (vectorized)
    """
    la1 = radians(col("prev_lat"))
    lo1 = radians(col("prev_long"))
    la2 = radians(col("lat"))
    lo2 = radians(col("long"))
    dlat = la2 - la1
    dlon = lo2 - lo1
    a = sin(dlat/2) * sin(dlat/2) + cos(la1) * cos(la2) * sin(dlon/2) * sin(dlon/2)
    c = 2 * asin(sqrt(a))
    R = 6371.0
    # if any coord is null, we'll produce null -> coalesce to 0
    seg_distance = (c * R)
    df = df_with_prev.withColumn("seg_distance_km", seg_distance)
    # replace nulls with 0
    df = df.fillna({"seg_distance_km": 0.0})
    return df

#calculating distance of each segment
def sum_distance_per_trip(seg_df):
    """
    Aggregate segment distances to trip level
    """
    return seg_df.groupBy("trip_id").agg(spark_sum("seg_distance_km").alias("distance_travelled_km"))

#Calculating distance using velocity
def compute_velocity_distance(filtered_df):
    """
    Approximate trip distance using velocity if GPS is sparse
    """
    w = Window.partitionBy("trip_id").orderBy("datetime_local")
    df = filtered_df.withColumn("prev_velocity", lag("velocity").over(w)) \
                    .withColumn("prev_ts", lag("datetime_local").over(w))
    # delta seconds
    df = df.withColumn("delta_sec", unix_timestamp(col("datetime_local")) - unix_timestamp(col("prev_ts")))
    # approximate distance in km: avg velocity (km/h) * delta_hours
    df = df.withColumn("seg_distance_km", ((col("prev_velocity") + col("velocity")) / 2.0) * (col("delta_sec")/3600.0))
    df = df.fillna({"seg_distance_km": 0.0})
    return df.groupBy("trip_id").agg(spark_sum("seg_distance_km").alias("distance_travelled_km"))

#Combine duration + distance + vehicle info
def assemble_daily_trips(spark, drive_df, vehicle_df, target_date, tz="America/Los_Angeles"):
    """
    Main task pipeline:
    - Add local datetime
    - Filter for date
    - Compute trip duration
    - Compute distance (Haversine or velocity)
    - Join vehicle info
    - Return final DataFrame
    """
    
    logger.info(f"Processing trips for date {target_date}")
    # add local datetime
    drive_local = add_local_datetime(drive_df, tz)
    #checking for particular dtae
    daily = filter_for_date(drive_local, target_date)

    # daily is the data frame for that particular given date 
    if daily.limit(1).count() == 0:
        logger.warning(f"No trips found for date {target_date}")
        # Return empty DF with expected schema
        schema = StructType([
            StructField("trip_id", LongType(), True),
            StructField("date_pst", DateType(), True),
            StructField("make", StringType(), True),
            StructField("model", StringType(), True),
            StructField("trip_duration_minutes", DecimalType(10, 2), True),
            StructField("distance_travelled", DecimalType(10, 2), True)
        ])
        return spark.createDataFrame([], schema)
       

    # duration
    duration_df = compute_trip_duration(daily)

    coords_count = daily.select("lat", "long").na.drop().count()
    total_rows = daily.count()

    if coords_count / max(1, total_rows) > 0.5:
        # enough GPS coverage => use haversine
        segs = add_prev_coords(daily)
        segs = compute_haversine_segment(segs)
        distance_df = sum_distance_per_trip(segs)
    else:
        distance_df = compute_velocity_distance(daily)

    # combine
    combined = duration_df.join(distance_df, on="trip_id", how="left")

    # vehicle_spec_id per trip
    veh_for_trip = daily.select("trip_id", "vehicle_spec_id").groupBy("trip_id") \
                        .agg(spark_min("vehicle_spec_id").alias("vehicle_spec_id"))

    combined = combined.join(veh_for_trip, on="trip_id", how="left")
    combined = combined.join(vehicle_df, on="vehicle_spec_id", how="left")

    # final columns & types
    result = combined.select(
        expr("try_cast(trip_id as bigint) as trip_id"),
        lit(target_date).cast("date").alias("date_pst"),
        col("make"),
        col("model"),
        col("trip_duration_minutes").cast("double"),
        col("distance_travelled_km").alias("distance_travelled")
    )
    return result
