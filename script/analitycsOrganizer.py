from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, TimestampType
from pyspark.sql.functions import col, sum as sum_, avg, max as max_, first, when, isnull
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

# Initialize a Spark Session
spark = SparkSession.builder.appName("PowerAnalyticsOrganizer").getOrCreate()

# Set column title for every entry
column_names = ["date", "site", "tag_name", "date_value", "equipment_name", "equipment_type", "hour_value", "joined", "building", "timezone", "value"]

# Predefine the type of data of each column
schema = StructType([
    StructField("date", DateType(), True),
    StructField("site", StringType(), True),
    StructField("tag_name", StringType(), True),
    StructField("date_value", TimestampType(), True),
    StructField("equipment_name", StringType(), True),
    StructField("equipment_type", StringType(), True),
    StructField("hour_value", TimestampType(), True),
    StructField("joined", BooleanType(), True),
    StructField("building", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("value", FloatType(), True)
])
# Read the CSV file, separated by |, without headers because we set them and according to the schema created above
df = spark.read.csv('inputData/power_analytics_raw_amps_reading_avg_val3_equipment_kw_20240507.csv', sep='|', header=False, schema=schema)

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###    TASKS    ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

    # 1. Find the sum of Value collumn by Equipment name
    ## 2. Find the average of Value collumn based on date_value
    ### 3. Find the maximum value per equipment_name only where joined is true and add datevalue to the data frame

    # Task # 1. Find the sum of Value collumn by Equipment name
equipmentUsage = df.groupBy("equipment_name").agg(sum_("value").alias("total_usage"))
mostUsedEquipment = equipmentUsage.orderBy("total_usage", ascending=False)

    # Task #2: Find the average of Value column based on date_value
averageValueByDate = df.groupBy("date_value").agg(avg("value").alias("average_date_value"))

    # Task # 3. Find the maximum value per equipment_name only where joined is true and add datevalue to the data frame
    # Define a window partitioned by "equipment_name", ordered by "value" in descending order
window = Window.partitionBy("equipment_name")

maxValue = df.withColumn("max_date_value", first("date_value").over(window)).groupBy(
    "equipment_name").agg(
        max_("value").alias("max_value"),
        first("max_date_value").alias("max_date_value")
    )

# df = df.withColumn("max_value", max_("value").over(window))

    # filter Equipment B11R3 by joined=true and max_value over 
# filteredByEquipment_df = df[(df['equipment_name'] == 'PDU-ASH1-DC1-A12R3-01') & (df['joined'] == True) & (df['max_value'] > 10)]

    # Show the data
# mostUsedEquipment.show(truncate=False) 
# averageValueByDate.show(truncate=False)
# maxValuePerEquipment.show(truncate=False)
# filteredByEquipment_df.show(truncate=False)
df.show(truncate=False)
maxValue.show(truncate=False)















## Transformation and action syntax de pe medium.com
## Orchestration tools (airflow)
## databases indexes to learn
## add to aws info and do a lambda function





# 1. Data Cleaning and Transformation:
# Often, raw data is not in a format that's suitable for analysis. 
# You might be asked to clean the data (e.g., handle missing values, remove duplicates, deal with 
# errors) and transform it into a more suitable format. For example, you could be asked to ensure that 
# all 'date_value' entries are in a standard date format.

    # Check if there are any missing values in the DataFrame
missing_values = df.select([when(isnull(c), c).alias(c) for c in df.columns]).dropna(how='any')
if missing_values.count() == 0:
    print("No missing vals")
else:
    print("Missing vals")

# 2. Feature Engineering: 
# This involves creating new features from existing data that might be more useful 
# for analysis. For example, you could be asked to create a new feature that represents the hour of the 
# day from the 'date_value' column, which could be useful for analyzing power usage patterns throughout the day.

# 3. Data Aggregation: 
# This involves summarizing and grouping data in various ways. For example,
# you could be asked to find the total power usage for each site, or the average power usage 
# for each type of equipment.

# 4. Data Integration: 
# This involves combining data from different sources. If you had another 
# CSV file with additional information about each piece of equipment (e.g., its age, its
# manufacturer), you could be asked to join this data with your existing DataFrame.

# 5. Data Partitioning:
# For large datasets, it's often useful to partition the data in a way that 
# makes analysis more efficient. For example, you could be asked to partition the data by 'site'
# or 'equipment_type', so that all the data for each site or equipment type is stored together.

# 6. Building a Data Pipeline:
# This involves creating a series of steps that process and 
# analyze the data, often on a regular schedule. For example, you could be asked to build a 
# pipeline that runs every day, reads the latest power usage data, cleans and transforms it, 
# aggregates it in various ways, and saves the results to a new CSV file.

# 7. ptimizing Query Performance: 
# This involves making your data processing and analysis code 
# run as efficiently as possible. For example, you could be asked to optimize the code you've 
# written to find the maximum value per equipment_name, to make it run faster or use less memory.

# 8. Data Validation:
# This involves checking that the data is correct and consistent. For example, 
# you could be asked to check that the 'value' column always contains positive numbers, and to report 
# any rows where this is not the case.