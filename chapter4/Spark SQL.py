from pyspark.sql import SparkSession

schema = "`date` STRING," \
         "`delay` INT," \
         "`distance` INT," \
         "`origin` STRING," \
         "`destination` STRING"

# Create a SparkSession
spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .getOrCreate())

# Path to data set
csv_file = "data/departuredelays.csv"

# Read and create a temporary view.
"""
DO NOT USE INFER SCHEMA!
Line alpha SUX. The column date will be treated as INT, then 01240293 -> 1240293.
"""
df = (spark.read.format("csv")
      # .option("inferSchema", "true")  # alpha
      .schema(schema)
      .option("header", "true")
      .load(csv_file))

# Create temporary table view.
df.createOrReplaceTempView("us_delay_flights_tbl")

# SQL Operations

# long distance flights
# spark.sql(
#     """
#     SELECT distance,
#        origin,
#        destination
#       FROM us_delay_flights_tbl
#      WHERE distance > 1000
#      ORDER BY distance DESC
#     """
# ).show(10)


# long delay flights
# spark.sql(
#     """
#     SELECT date,
#        delay,
#        origin,
#        destination
#       FROM us_delay_flights_tbl
#      WHERE origin = 'SFO'
#        AND destination = 'ORD'
#        AND delay > 120
#      ORDER BY delay DESC
#     """
# ).show(10)


# frequent long delay dates
# spark.sql(
#     """
#     SELECT COUNT(*) AS num_long_delay,
#        SUBSTRING(date, 1, 2) AS month,
#        SUBSTRING(date, 3, 2) AS day
#       FROM us_delay_flights_tbl
#      WHERE delay > 120
#      GROUP BY month,
#               day
#      ORDER BY num_long_delay DESC
#      LIMIT 10
#     """
# ).show()


# diff delay types
spark.sql(
    """
    SELECT delay,
       CASE WHEN delay > 360                 THEN 'Very Long Delays'
            WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
            WHEN delay > 60 AND delay < 120  THEN 'Short Delays'
            WHEN delay > 0 and delay < 60    THEN 'Tolerable Delays'
            WHEN delay = 0                   THEN 'No Delays'
            ELSE 'Early'
             END AS delay_type,
       origin,
       destination
  FROM us_delay_flights_tbl
    """
).show(10)
