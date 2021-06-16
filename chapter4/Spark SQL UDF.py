from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

spark = (SparkSession
         .builder
         .appName("Spark_SQL_UDF")
         .getOrCreate())


# Create cubed function
def cubed(s):
    return s * s * s


# Register UDF
spark.udf.register("cubed", cubed, LongType())

# Generate temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

# Query
spark.sql(
    """
    SELECT *
      FROM udf_test
    """
).show()

spark.sql(
    """
    SELECT id,
           cubed(id) AS id_cubed
      FROM udf_test
    """
).show()
