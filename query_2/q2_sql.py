from pyspark.sql import SparkSession
from time import time

spark = SparkSession.builder \
    .appName("Query 2 SQL API ") \
    .getOrCreate()


USERNAME = "alexiosfilippakopoulos"

# load data
df1 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2010_2019")
df2 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2020_2025")

# temp views for SQL queries
df1.createOrReplaceTempView("crime_2010_2019")
df2.createOrReplaceTempView("crime_2020_2025")

start = time()

# combine datasets and apply all preprocessing and aggregation in a single query
query = """
WITH combined AS (
    SELECT `DATE OCC`, `Status Desc`, `AREA NAME`
    FROM crime_2010_2019
    UNION ALL
    SELECT `DATE OCC`, `Status Desc`, `AREA NAME`
    FROM crime_2020_2025
),

preprocessed AS (
    SELECT
        year(to_timestamp(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) AS year,
        `AREA NAME` AS precinct,
        CASE 
            WHEN `Status Desc` != 'UNK' AND `Status Desc` != 'Invest Cont' THEN 1
            ELSE 0
        END AS closed_flag
    FROM combined
),

aggregated AS (
    SELECT
        year,
        precinct,
        COUNT(*) AS total_cases,
        SUM(closed_flag) AS closed_cases,
        (SUM(closed_flag) / COUNT(*)) * 100 AS closed_case_rate
    FROM preprocessed
    GROUP BY year, precinct
),

ranked AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY year ORDER BY closed_case_rate DESC) AS rank
    FROM aggregated
)

SELECT
    year,
    precinct,
    closed_case_rate,
    rank
FROM ranked
WHERE rank <= 3
ORDER BY year, rank
"""

result_final = spark.sql(query)
result_count = result_final.count()
end = time()

result_final.show(truncate=False)

print("SQL API:", end - start, "seconds")

