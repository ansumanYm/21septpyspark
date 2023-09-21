from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

# Load a csv file into a DATAFrame
df = spark.read.csv("temp.csv", header=True, inferSchema = True)

# Perform transformation

df2 = df[(df['Day'] == 'Sat') | (df['Day'] == 'Sun')]

# Save the result csv
df2.write.parquet("output_data", header=True)

# Stop the sparksession
spark.stop()