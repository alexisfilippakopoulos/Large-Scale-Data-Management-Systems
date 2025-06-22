from pyspark.sql import SparkSession
from time import time

spark = SparkSession.builder \
    .appName("Query 1 RDD No UDF") \
    .getOrCreate()

sc = spark.sparkContext

USERNAME = "alexiosfilippakopoulos"

# load data
df1 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2010_2019")
df2 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2020_2025")

# cast Vict Age column to int in DataFrame to avoid string-to-int issues
df1 = df1.withColumn("Vict Age", df1["Vict Age"].cast("int"))
df2 = df2.withColumn("Vict Age", df2["Vict Age"].cast("int"))

rdd1 = df1.rdd
rdd2 = df2.rdd

def aggregate_age_groups_rdd(rdd):
    filtered = rdd.filter(lambda row: row["Crm Cd Desc"] and "aggravated assault" in row["Crm Cd Desc"].lower())

    print("Filtered count:", filtered.count())

    # inline lambda without UDF
    age_group_pairs = (
        filtered
        .map(lambda row: (
            "Kids" if row["Vict Age"] is not None and row["Vict Age"] < 18 else
            "Young Adults" if row["Vict Age"] is not None and row["Vict Age"] <= 24 else
            "Adults" if row["Vict Age"] is not None and row["Vict Age"] <= 64 else
            "Elders" if row["Vict Age"] is not None else None,
            1
        ))
        .filter(lambda x: x[0] is not None)
    )

    # aggregate counts
    return age_group_pairs.reduceByKey(lambda x, y: x + y)

start = time()

agg1 = aggregate_age_groups_rdd(rdd1)
agg2 = aggregate_age_groups_rdd(rdd2)

# join and sum counts
combined = agg1.fullOuterJoin(agg2).mapValues(
    lambda x: (x[0] if x[0] is not None else 0) + (x[1] if x[1] is not None else 0)
)

# sort
final_counts = combined.sortBy(lambda x: -x[1])

end = time()

for group, count in final_counts.collect():
    print(f"{group}: {count}")

print("RDD No UDF:", end - start, "seconds")
