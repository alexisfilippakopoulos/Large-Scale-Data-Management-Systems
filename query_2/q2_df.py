from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, when, sum as spark_sum, count, rank
from pyspark.sql.window import Window
from time import time

USERNAME = "alexiosfilippakopoulos"

spark = SparkSession.builder \
    .appName("Query 2 DataFrame No UDF") \
    .getOrCreate()

# load data
df1 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2010_2019")
df2 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2020_2025")

def preprocess(df):
    # convert 'DATE OCC' to timestamp and extract year
    df = df.withColumn("date_occ_ts", to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a")) \
           .withColumn("year", year(col("date_occ_ts")))
    
    # define closed case flag: 1 if closed, else 0
    df = df.withColumn(
        "closed_flag",
        when(
            (col("Status Desc") != "UNK") & (col("Status Desc") != "Invest Cont"),
            1
        ).otherwise(0)
    )
    
    # aggregate by year and precinct (AREA NAME)
    grouped = df.groupBy("year", "AREA NAME") \
                .agg(
                    count("*").alias("total_cases"),
                    spark_sum("closed_flag").alias("closed_cases")
                )
    return grouped

# preprocess + combine datasets
start = time()
agg_19 = preprocess(df1)
agg_25 = preprocess(df2)
combined = agg_19.union(agg_25)

# aggregate again to merge overlapping year-precinct groups
final_agg = combined.groupBy("year", "AREA NAME") \
                    .agg(
                        spark_sum("total_cases").alias("total_cases"),
                        spark_sum("closed_cases").alias("closed_cases")
                    )

# calculate closed case rate
final_agg = final_agg.withColumn(
    "closed_case_rate",
    (col("closed_cases") / col("total_cases")) * 100
)

# define window spec for ranking per year
window_spec = Window.partitionBy("year").orderBy(col("closed_case_rate").desc())

# add rank
final_agg = final_agg.withColumn("rank", rank().over(window_spec))

# filter top 3 ranks per year
top3_final = final_agg.filter(col("rank") <= 3)

# select and order final columns
result_final = top3_final.select(
    col("year"),
    col("AREA NAME").alias("precinct"),
    col("closed_case_rate"),
    col("rank")
).orderBy("year", "rank")

end = time()

result_final.show(truncate=False)

print("DataFrame API:", end - start, "seconds")
