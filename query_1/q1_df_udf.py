from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, lower, udf
from pyspark.sql.types import StringType
from time import time

spark = SparkSession.builder \
    .appName("Query 1 DataFrame With UDF") \
    .getOrCreate()

USERNAME = "alexiosfilippakopoulos"

# load data
df1 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2010_2019")
df2 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2020_2025")

# udf for age group classification
def age_group(age):
    try:
        age = int(age)
        if age < 18:
            return "Kids"
        elif age <= 24:
            return "Young Adults"
        elif age <= 64:
            return "Adults"
        else:
            return "Elders"
    except:
        return "Unknown"

age_group_udf = udf(age_group, StringType())

# aggregation function with udf
def aggregate_age_groups(df):
    df_filtered = df.filter(lower(col("Crm Cd Desc")).contains("aggravated assault"))
    print(df_filtered.count())
    df_grouped = df_filtered.withColumn("Age Group", age_group_udf(col("Vict Age"))) \
                            .groupBy("Age Group").count()
    return df_grouped

start = time()

agg1 = aggregate_age_groups(df1)
agg2 = aggregate_age_groups(df2)

# join partial aggregates on age group and sum counts (filling nulls with 0)
final_counts = agg1.alias("a1").join(agg2.alias("a2"), "Age Group", "outer") \
    .select(
        col("Age Group"),
        (col("a1.count").cast("long") + col("a2.count").cast("long")).alias("count")
    ).orderBy(desc("count"))

end = time()
final_counts.show(truncate=False)

print("DataFrame API With UDF", end - start, "seconds\n")
