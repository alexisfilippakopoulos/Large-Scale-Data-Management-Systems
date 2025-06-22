from pyspark.sql import SparkSession
from time import time

spark = SparkSession.builder \
    .appName("Query 1 RDD With UDF") \
    .getOrCreate()

sc = spark.sparkContext

USERNAME = "alexiosfilippakopoulos"

# load data 
df1 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2010_2019")
df2 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2020_2025")
rdd1 = df1.rdd
rdd2 = df2.rdd

# udf to aggregate by age group
def aggregate_age_groups_rdd(rdd):
    filtered = rdd.filter(lambda row: row["Crm Cd Desc"] and "aggravated assault" in row["Crm Cd Desc"].lower())
    
    print("Filtered count:", filtered.count())

    def get_age_group(age):
        age = int(age)
        if age is None:
            return None
        if age < 18:
            return "Kids"
        elif age <= 24:
            return "Young Adults"
        elif age <= 64:
            return "Adults"
        else:
            return "Elders"

    age_group_pairs = filtered.map(lambda row: (get_age_group(row["Vict Age"]), 1)) \
                              .filter(lambda x: x[0] is not None)

    # aggregate counts
    grouped_counts = age_group_pairs.reduceByKey(lambda x, y: x + y)
    return grouped_counts

start = time()

agg1 = aggregate_age_groups_rdd(rdd1)
agg2 = aggregate_age_groups_rdd(rdd2)

# join both filtered RDDs
combined = agg1.fullOuterJoin(agg2).mapValues(
    lambda x: (x[0] if x[0] is not None else 0) + (x[1] if x[1] is not None else 0)
)

# sort
final_counts = combined.sortBy(lambda x: -x[1])

end = time()

for group, count in final_counts.collect():
    print(f"{group}: {count}")

print("RDD With UDF:", end - start, "seconds")