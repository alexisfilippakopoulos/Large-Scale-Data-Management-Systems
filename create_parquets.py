from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, trim

USERNAME = "alexiosfilippakopoulos"

spark = SparkSession.builder \
    .appName("Convert Input Data to Parquets") \
    .getOrCreate()

input_paths = {
    "crime_2010_2019": "hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2010_2019.csv",
    "crime_2020_2025": "hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2020_2025.csv",
    "police_stations": "hdfs://hdfs-namenode:9000/user/root/data/LA_Police_Stations.csv",
    "income_2015": "hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv",
    "census_2010": "hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv",
    "mo_codes": "hdfs://hdfs-namenode:9000/user/root/data/MO_codes.txt"
}
output_base_path = f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet"

# csv to parquet conversion function
def convert_csv_to_parquet(name, path, output_path, sep=",", header=True):
    df = spark.read.option("header", header).option("inferSchema", True).option("sep", sep).csv(path)
    for c in df.columns:
        df = df.withColumn(c, trim(col(c)))
    df.write.mode("overwrite").parquet(f"{output_path}/{name}")
    print(f"Saved: {name}")


convert_csv_to_parquet("crime_2010_2019", input_paths["crime_2010_2019"], output_base_path)
convert_csv_to_parquet("crime_2020_2025", input_paths["crime_2020_2025"], output_base_path)
convert_csv_to_parquet("police_stations", input_paths["police_stations"], output_base_path)
convert_csv_to_parquet("income_2015", input_paths["income_2015"], output_base_path)
convert_csv_to_parquet("census_2010", input_paths["census_2010"], output_base_path)

# handling for MO codes
mo_raw_df = spark.read.text(input_paths["mo_codes"])

mo_df = mo_raw_df.withColumn("MO_Code", trim(split(col("value"), " ", 2)[0])) \
                 .withColumn("Description", trim(split(col("value"), " ", 2)[1])) \
                 .drop("value")

mo_df.write.mode("overwrite").parquet(f"{output_base_path}/mo_codes")
print("Saved: mo_codes")

spark.stop()
