import sys

from pyspark.sql import SparkSession

sys.argv = [
    "Spark Job.py",
    # "data/mnm_dataset.csv",
    "data/test_agg.csv",
]

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    # Build a SparkSession using the SparkSession APIs.
    # If one does not exist, then create an instance.
    # There can only be one SparkSession per JVM.
    spark = (SparkSession
             .builder
             .appName("PythonMnMCount")
             .getOrCreate())

    # Get the M&M data set filename from the command-line arguments
    mnm_file = sys.argv[1]

    # Read the file into a Spark DataFrame using the CSV format by inferring the schema
    # and specifying that the file contains a header,
    # which provides column names for comma-separated fields.
    mnm_df = (spark.read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(mnm_file)
              )
    mnm_df.show(n=5, truncate=False)

    # We use the DataFrame high-level APIs.
    # Note that we don't use RDDs at all.
    # Because some of Spark's functions return the same object, we can chain function calls.
    # 1. Select from the DataFrame the fields "State", "Color", and "Count"
    # 2. Since we want to group each state and its M&M color count, we use groupBy()
    # 3. Aggregate counts of all colors and groupBy() State and Color
    # 4 orderBy() in descending order
    count_mnm_df = (mnm_df.select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # While the above code aggregated and counted for all the states,
    # what if we just want to see the data for a single state, e.g., CA?
    # 1. Select from all rows in the DataFrame
    # 2. Filter only CA state
    # 3. groupBy() State and Color as we did above
    # 4. Aggregate the counts for each color
    # 5. orderBy() in descending order
    # Find the aggregate count for California by filtering
    ca_count_mnm_df = (mnm_df.select("*")
                       .where(mnm_df.State == 'CA')
                       .groupBy("State", "Color")
                       .sum("Count")
                       .orderBy("sum(Count)", ascending=False))
    ca_count_mnm_df.show(n=10, truncate=False)

    spark.stop()
