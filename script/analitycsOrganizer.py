from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, sum as sum_, avg, max as max_
from pyspark.sql.types import FloatType

# Initialize a Spark Session
spark = SparkSession.builder.appName("PowerAnalyticsOrganizer").getOrCreate()

# Read the data from the CSV file
column_names = ["date", "site", "tag_name", "date_value", "equipment_name", "equipment_type", "hour_value", "joined", "building", "timezone", "value"]
schema = StructType([StructField(field_name, StringType(), True) for field_name in column_names])
df = spark.read.csv('inputData/power_analytics_raw_amps_reading_avg_val3_equipment_kw_20240507.csv', sep='|', header=False, schema=schema)

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###    TASKS    ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

# 1. Find the sum of Value collumn by Equipment name
## 2. Find the average of Value collumn based on date_value
### 3. Find the maximum value per equipment_name only where joined is true and add datevalue to the data frame

# Convert the value collumn to float
df = df.withColumn("value", col("value").cast(FloatType()))

# Task # 1. Find the sum of Value collumn by Equipment name
equipmentUsage = df.groupBy("equipment_name").agg(sum_("value").alias("total_usage"))
mostUsedEquipment = equipmentUsage.orderBy("total_usage", ascending=False)


# Task #2: Find the average of Value column based on date_value
averageValueByDate = df.groupBy("date_value").agg(avg("value").alias("average_date_value"))

# Task # 3. Find the maximum value per equipment_name only where joined is true and add datevalue to the data frame
df_joined = df.filter(df.joined == True)
maxValuePerEquipment = df_joined.groupBy("equipment_name").agg(max_("value").alias("max_value"), max_("date_value").alias("date_value"))

# Show the data
mostUsedEquipment.show(truncate=False)
averageValueByDate.show(truncate=False)
maxValuePerEquipment.show(truncate=False)
