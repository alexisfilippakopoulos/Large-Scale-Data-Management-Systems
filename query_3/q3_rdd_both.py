from pyspark.sql import SparkSession
from time import time

spark = SparkSession.builder \
    .appName("Query 3 RDD Parquet/CSV") \
    .getOrCreate()

sc = spark.sparkContext

USERNAME = "alexiosfilippakopoulos"

# function to clean income strings
def clean_income(record):
    try:
        income_str = record[1].replace("$", "").replace(",", "")
        income_val = float(income_str)
        return (record[0], income_val)
    except:
        return None

# function to convert household size to float
def parse_household(record):
    try:
        size = float(record[1])
        return (record[0], size)
    except:
        return None

###-------------------------------------------------------------------------------------------
###                                USING PARQUET AS SOURCE
###-------------------------------------------------------------------------------------------
start_parquet_query = time()

income_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/income_2015")
pops_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/census_2010")

end_parquet_loading = time()

# convert to RDDs and process
income_rdd = income_df.select("Zip Code", "Estimated Median Income").rdd \
    .map(lambda row: (row["Zip Code"], row["Estimated Median Income"])) \
    .map(clean_income) \
    .filter(lambda x: x is not None)

pops_rdd = pops_df.select("Zip Code", "Average Household Size").rdd \
    .map(lambda row: (row["Zip Code"], row["Average Household Size"])) \
    .map(parse_household) \
    .filter(lambda x: x is not None)

# join and compute average income per person
joined_rdd = pops_rdd.join(income_rdd) 
result_rdd = joined_rdd.mapValues(lambda vals: vals[1] / vals[0])

end_parquet_query = time()

print("Results from PARQUET source:")
for row in result_rdd.take(10):
    print(f"Zip Code: {row[0]}, Avg Income Per Person: {row[1]:.2f}")

###-------------------------------------------------------------------------------------------
###                                 USING CSV AS SOURCE
###-------------------------------------------------------------------------------------------
start_csv_query = time()

pops_rdd = sc.textFile("hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv")
income_rdd = sc.textFile("hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv")

end_csv_loading = time()

header_pops = pops_rdd.first()
header_income = income_rdd.first()

pops_rdd = pops_rdd.filter(lambda x: x != header_pops) \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (fields[0], fields[4])) \
    .map(parse_household) \
    .filter(lambda x: x is not None)

income_rdd = income_rdd.filter(lambda x: x != header_income) \
    .map(lambda line: line.split(",")) \
    .map(lambda fields: (fields[0], fields[1])) \
    .map(clean_income) \
    .filter(lambda x: x is not None)

# join and compute average income per person
joined_rdd = pops_rdd.join(income_rdd)
result_rdd = joined_rdd.mapValues(lambda vals: vals[1] / vals[0])

end_csv_query = time()

print("Results from CSV source:")
for row in result_rdd.take(10):
    print(f"Zip Code: {row[0]}, Avg Income Per Person: {row[1]:.2f}")

###-------------------------------------------------------------------------------------------
###                              TIME MEASUREMENT OUTPUT
###-------------------------------------------------------------------------------------------
print("Using PARQUET as source")
print(f"Total Time Elapsed: {end_parquet_query - start_parquet_query:.2f} seconds")
print(f"Data Loading Time: {end_parquet_loading - start_parquet_query:.2f} seconds "
      f"({100 * (end_parquet_loading - start_parquet_query) / (end_parquet_query - start_parquet_query):.2f}% of total time.)")

print("Using CSV as source")
print(f"Total Time Elapsed: {end_csv_query - start_csv_query:.2f} seconds")
print(f"Data Loading Time: {end_csv_query - start_csv_query:.2f} seconds "
      f"({100 * (end_csv_loading - start_csv_query) / (end_csv_query - start_csv_query):.2f}% of total time.)")
