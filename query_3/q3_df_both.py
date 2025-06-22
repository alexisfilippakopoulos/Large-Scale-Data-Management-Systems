from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from time import time

spark = SparkSession.builder \
    .appName("Query 3 DataFrame API Parquet/CSV") \
    .getOrCreate()

sc = spark.sparkContext

USERNAME = "alexiosfilippakopoulos"

###-------------------------------------------------------------------------------------------
###                                USING PARQUET AS SOURCE
###-------------------------------------------------------------------------------------------

start_parquet_query = time()

income_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/income_2015")
pops_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/census_2010")

end_parquet_loading = time()

# clean income column by removing $ and casting to float
income_df = income_df.withColumn(
    "Estimated Median Income",
    regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double")
)

# join on zip code
merged_df = pops_df.join(income_df, on="Zip Code", how="inner")
print("==== Parquet Source Explain Plan ====")
print(merged_df.explain())

# compute average income per person
result_df = merged_df.withColumn(
    "Avg Income Per Person",
    col("Estimated Median Income") / col("Average Household Size")
)
end_parquet_query = time()

print(result_df.select("Zip Code", "Avg Income Per Person").show(truncate=False))


###-------------------------------------------------------------------------------------------
###                                 USING CSV AS SOURCE
###-------------------------------------------------------------------------------------------
start_csv_query = time()

pops_df = sc.textFile("hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv")
income_df = sc.textFile("hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv")

end_csv_loading = time()

# clean and cast estimated median income
income_df = income_df.withColumn(
    "Estimated Median Income",
    regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double")
)


merged_df = pops_df.join(income_df, on="Zip Code", how="inner")
print("==== CSV Source Explain Plan ====")
print(merged_df.explain())

# average income per person
result_df = merged_df.withColumn(
    "Avg Income Per Person",
    col("Estimated Median Income") / col("Average Household Size")
)
end_csv_query = time()

print(result_df.select("Zip Code", "Avg Income Per Person").show(truncate=False))

print("Using CSV as source")
print(f"Total Time Elapsed: {end_csv_query - start_csv_query} seconds")
print(f"Data Loading Time: {end_csv_query - start_csv_query:.2f} seconds "
      f"({100 * (end_csv_loading - start_csv_query) / (end_csv_query - start_csv_query):.2f}% of total time.)")

print("Using Parquet as source")
print(f"Total Time Elapsed: {end_parquet_query - start_parquet_query} seconds")
print(f"Data Loading Time: {end_parquet_query - start_parquet_query:.2f} seconds "
      f"({100 * (end_parquet_loading - start_parquet_query) / (end_parquet_query - start_parquet_query):.2f}% of total time.)")
