from pyspark.sql.functions import when, col, desc, lower
from pyspark.sql import SparkSession
from time import time

USERNAME = "alexiosfilippakopoulos"

spark = SparkSession.builder \
    .appName("Query 1 DataFrame No UDF") \
    .getOrCreate()

# load data
df1 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2010_2019")
df2 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2020_2025")

# create a df with aggregated result
def aggregate_age_groups(df):
    df_filtered = df.filter(lower(col("Crm Cd Desc")).contains("aggravated assault"))
    print(df_filtered.count())
    df_grouped = df_filtered.withColumn(
        "Age Group",
        when(col("Vict Age") < 18, "Kids")
        .when((col("Vict Age") <= 24), "Young Adults")
        .when((col("Vict Age") <= 64), "Adults")
        .otherwise("Elders")
    ).groupBy("Age Group").count()
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

print("DataFrame API no UDF:", end - start, "seconds\n")