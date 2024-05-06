from pyspark.sql import SparkSession
from string_py import Str

# Custom function to execute on each partition
def custom_function(iterator):
    for item in iterator:
        # Perform custom processing on each item
        yield Str(item).first(5)  # Example: Convert each item to uppercase

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Custom Function Execution") \
    .getOrCreate()

# Create RDD or DataFrame
data = ["sue", "li", "bob", "heo"]
rdd = spark.sparkContext.parallelize(data)

# Apply custom function on each partition
result_rdd = rdd.mapPartitions(custom_function)

# Collect and show results
result_list = result_rdd.collect()
print(result_list)

# Stop Spark session
spark.stop()
