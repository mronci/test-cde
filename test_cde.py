# import pandas as pd
# from pyspark.sql import SparkSession

# if __name__ == "__main__":
#     # Create a SparkSession
#     spark = SparkSession.builder \
#         .appName("HelloWorld") \
#         .getOrCreate()

#     # Create a DataFrame with a single column named "message"
#     data = ["Hello", "World"]
#     df = spark.createDataFrame(pd.DataFrame(data, columns=["message"]))

#     # Print the DataFrame
#     df.show()

#     # Stop the SparkSession
#     spark.stop()

from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("HelloWorld") \
        .getOrCreate()

    # Create a DataFrame with a single column named "message"
    data = ["Hello", "World"]
    df = spark.createDataFrame(data, ["message"])

    # Print the DataFrame
    df.show()

    # Stop the SparkSession
    spark.stop()
