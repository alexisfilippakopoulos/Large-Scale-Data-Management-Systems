from pyspark.sql import SparkSession, functions as F, Window
import re
from time import time

spark = (
    SparkSession.builder
        .appName("Query 4 DataFrame")
        .getOrCreate()
)

USERNAME = "alexiosfilippakopoulos"

def clean_column_names(df):
    for old_name in df.columns:
        new_name = old_name.strip()
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
    return df


start = time()
crime19  = clean_column_names(spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2010_2019"))
crime25  = clean_column_names(spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2020_2025"))
stations = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/police_stations")
mo_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/mo_codes")

# keep codes where description contains 'gun' or 'weapon'
weapon_mo_df = (
    mo_df.filter(
        F.lower(F.col("Description")).rlike(r"\b(gun|weapon)\b")
    )
    .select("MO_Code")
    .distinct()
)

# collect only the valid codes as a broadcast set
weapon_codes = [row.MO_Code for row in weapon_mo_df.collect()]
pattern = r'(?:^|\s)(' + '|'.join(map(re.escape, weapon_codes)) + r')(?:\s|$)'

crimes = (
    crime19
    .unionByName(crime25)
    .filter(~((F.col("LAT") == 0) & (F.col("LON") == 0)))         
    .filter(F.col("Mocodes").rlike(pattern))                      
    .select("LAT", "LON")                                     
    .withColumn("cid", F.monotonically_increasing_id())           
)

st = (
    F.broadcast(
        stations.select(
            F.col("DIVISION"),
            F.col("Y").alias("st_lat"),
            F.col("X").alias("st_lon")
        )
    )
)

cross = crimes.crossJoin(st)


# vectorised Haversine distance

R = 6371.0                        

lat1  = F.radians(F.col("LAT"))
lon1  = F.radians(F.col("LON"))
lat2  = F.radians(F.col("st_lat"))
lon2  = F.radians(F.col("st_lon"))

dlat  = lat2 - lat1
dlon  = lon2 - lon1

a = (
    F.sin(dlat / 2) ** 2
    + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2) ** 2
)
dist = 2 * R * F.asin(F.sqrt(a))

cross = cross.withColumn("distance_km", dist)

win = Window.partitionBy("cid").orderBy("distance_km")
nearest = (
    cross
    .withColumn("rn", F.row_number().over(win))
    .filter(F.col("rn") == 1)
    .select("DIVISION", "distance_km")
)

result = (
    nearest.groupBy("DIVISION")
           .agg(
               F.count("*").alias("#"),
               F.mean("distance_km").alias("average_distance")
           )
           .orderBy(F.desc("#"))
)
end = time()
print(f"Time Elapsed: {end - start} seconds.")
result.show(truncate=False)

print("==== Cross Join Explain Plan ====")
cross.explain(True)
