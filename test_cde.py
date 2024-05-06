from pyspark.sql import SparkSession
import pyarrow as pa
import pyarrow.spark as pas

# Initialize Spark session
spark = SparkSession.builder \
    .appName("demo") \
    .getOrCreate()

# Create Spark DataFrame
df = spark.createDataFrame(
    [
        ("sue", 32),
        ("li", 3),
        ("bob", 75),
        ("heo", 13),
    ],
    ["first_name", "age"],
)

# Show Spark DataFrame
df.show()

# Convert Spark DataFrame to Pandas DataFrame using Arrow
pandas_df = pas.DataFrame(df).to_pandas()

# Show Pandas DataFrame
print(pandas_df)

# Stop Spark session
spark.stop()
