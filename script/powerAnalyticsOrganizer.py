from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import col, max as max_, min as min_
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import stddev, mean


# Initialize a Spark Session
spark = SparkSession.builder.appName("PowerAnalyticsOrganizer").getOrCreate()

# Read the data from the CSV file
column_names = ["Date", "Location", "Equipment", "AnotherDate", "ElectricalBoard", "Type", "Time", "Boolean", "RIC2-DC2", "TimeZone", "Value"]
schema = StructType([StructField(field_name, StringType(), True) for field_name in column_names])
df = spark.read.csv('inputData/power_analytics_raw_amps_reading_avg_val3_equipment_kw_20240507.csv', sep='|', header=False, schema=schema)

# Handle missing values
df = df.dropna() # Remove rows with missing values
df = df.fillna("WasNull") # Fill missing values in specified collumn with "value"
df = df.filter(df["Boolean"] != "True")

# Convert values
df = df.withColumn("Value", df["Value"].cast("float"))

# Find out data type of a collumn
value_data_type = dict(df.dtypes)["Value"]
print(f"Data type of 'Value' collumn: {value_data_type}")

# Normalize the "Value" collumn
df = df.withColumn("Value", (col("Value") - df.agg(min_("Value")).first()[0]) / (df.agg(max_("Value")).first()[0] - df.agg(min_("Value")).first()[0]))

# Z-score normalization (standardization)
mean_value = df.select(mean(df['Value'])).collect()[0][0]
stddev_value = df.select(stddev(df['Value'])).collect()[0][0]

df = df.withColumn("Value", (col("Value") - mean_value) / stddev_value)

# Show the data
df.show(truncate=False)