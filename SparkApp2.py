from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

# Load a csv file into a DATAFrame
df = spark.read.csv("input_data.csv", header=True, inferSchema = True)

# Perform transformation

df2 = df[(df['Day'] == 'Sat') | (df['Day'] == 'Sun')]

# Save the result parquet
df2.write.parquet("output_data.parquet")

# Stop the sparksession
spark.stop()