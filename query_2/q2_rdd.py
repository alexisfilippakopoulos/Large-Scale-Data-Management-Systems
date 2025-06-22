from pyspark.sql import SparkSession
from datetime import datetime
from time import time

spark = SparkSession.builder \
    .appName("Query 2 RDD Implementation") \
    .getOrCreate()

sc = spark.sparkContext

USERNAME = "alexiosfilippakopoulos"

# load data as RDDs
rdd1 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2010_2019").rdd
rdd2 = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{USERNAME}/data/parquet/crime_2020_2025").rdd

def parse_date_and_extract_year(date_str):
    """ parse date string and extract year """
    try:
        # parse MM/dd/yyyy hh:mm:ss a format
        dt = datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")
        return dt.year
    except:
        try:
            # try alternative format without time
            dt = datetime.strptime(date_str.split()[0], "%m/%d/%Y")
            return dt.year
        except:
            return None

def is_case_closed(status_desc):
    """determine if case is closed based on status description"""
    if status_desc and status_desc != "UNK" and status_desc != "Invest Cont":
        return 1
    return 0

def preprocess_rdd(rdd):
    """preprocess RDD: extract year, determine closed status, group by year and area"""
    
    # map each row to extract relevant fields and compute derived values
    def process_row(row):
        date_occ = row["DATE OCC"] if "DATE OCC" in row else None
        area_name = row["AREA NAME"] if "AREA NAME" in row else None
        status_desc = row["Status Desc"] if "Status Desc" in row else None
        
        # extract year
        year = parse_date_and_extract_year(date_occ) if date_occ else None
        
        # determine if case is closed
        closed_flag = is_case_closed(status_desc)
        # ((year, precinct), (total_cases, closed_cases))
        return ((year, area_name), (1, closed_flag))
    
    # process rows and filter out invalid years
    processed = rdd.map(process_row).filter(lambda x: x[0][0] is not None and x[0][1] is not None)
    
    # aggregate by (year, area_name)
    aggregated = processed.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    
    return aggregated

def calculate_top3_by_year(combined_rdd):
    """calculate top 3 precincts by closed case rate for each year"""
    
    # calculate closed case rate
    def add_rate(item):
        (year, area_name), (total_cases, closed_cases) = item
        rate = (closed_cases / total_cases * 100) if total_cases > 0 else 0
        return (year, (area_name, total_cases, closed_cases, rate))
    
    with_rates = combined_rdd.map(add_rate)
    
    # group by year
    by_year = with_rates.groupByKey()
    
    # for each year, sort by rate and take top 3
    def get_top3_for_year(year_data):
        year, precincts = year_data
        
        # sort precincts by closed_case_rate descending
        sorted_precincts = sorted(precincts, key=lambda x: x[3], reverse=True)
        
        # take top 3 and add rank
        top3 = []
        for rank, (area_name, total_cases, closed_cases, rate) in enumerate(sorted_precincts[:3], 1):
            top3.append((year, area_name, rate, rank))
        
        return top3
    
    # get top 3 for each year and flatten
    top3_by_year = by_year.map(get_top3_for_year)
    flattened = top3_by_year.flatMap(lambda x: x)
    
    return flattened

start = time()

# preprocess both RDDs
agg_rdd1 = preprocess_rdd(rdd1)
agg_rdd2 = preprocess_rdd(rdd2)

# union the RDDs
combined_rdd = agg_rdd1.union(agg_rdd2)

# merge overlapping year-precinct groups (in case same year-precinct appears in both datasets)
final_agg_rdd = combined_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# calculate top 3 precincts by closed case rate for each year
result_rdd = calculate_top3_by_year(final_agg_rdd)

# sort by year and rank
sorted_result = result_rdd.sortBy(lambda x: (x[0], x[3]))  # Sort by (year, rank)

# collect results
results = sorted_result.collect()

end = time()

print("year|precinct|closed_case_rate|rank")
print("-" * 60)
for year, precinct, rate, rank in results:
    print(f"{year}|{precinct}|{rate:.10f}|{rank}")

print(f"\nRDD API: {end - start} seconds")
