import sys
import csv
import datetime
import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, MapType, StringType, DoubleType



#if __name__ == "__main__":
sc = pyspark.SparkContext()
    #main(sc)


def expandVisits(date_range_start, visits_by_day):
    '''
    This function needs to return a Python's dict{datetime:int} where:
      key   : datetime type, e.g. datetime(2020,3,17), etc.
      value : the number of visits for that day
    '''
    date = date_range_start.split('T')[0].split('-')
    year = int(date[0])
    month = int(date[1])
    day = int(date[2])
    vbd = visits_by_day[1:-1].split(',')
    mydict = dict()

    week_day_count = 0
    for i in vbd:
        mydict[datetime.date(year, month, day) + datetime.timedelta(days=week_day_count)] = int(i)
        week_day_count += 1

    return mydict

udfExpand = F.udf(expandVisits, MapType(DateType(), IntegerType()))
getLow = lambda median, stddev: 0.0 if median - stddev < 0 else median - stddev
udfLow = F.udf(getLow, DoubleType())
udfChangeYear = F.udf(lambda x: x.replace(year=2020), DateType())


#def main(sc):
spark = SparkSession(sc)
args = sys.argv[1]

catagories = {"Big Box Grocers": [452210, 452311],
              "Convenience Stores": [445120],
              "Drinking Places": [722410],
              "Full-Service Restaurants": [722511],
              "Limited-Service Restaurants": [722513],
              "Pharmacies and Drug Stores": [446110, 446191],
              "Snack and Bakeries": [311811, 722515],
              "Specialty Food Stores": [445210, 445220, 445230, 445291, 445292, 445299],
              "Supermarkets (except Convenience Stores)": [445110]}

# read core place
df_core_place = spark.read.csv('hdfs:///data/share/bdm/core-places-nyc.csv', header=True, escape='"')
df_core_place = df_core_place.select("placekey", "naics_code").cache()

# read weekly patterns
df_weekly = spark.read.csv('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*', header=False, escape='"').select("_c0", "_c12", "_c16")
df_weekly = df_weekly.withColumnRenamed('_c0', 'placekey').withColumnRenamed('_c12', 'date_range_start').withColumnRenamed('_c16', 'visits_by_day').cache()


for catagory_name, naics_codes in catagories.items():
    df_core_place2 = df_core_place.filter(F.col('naics_code').isin(naics_codes))
    df_main = df_core_place2.join(df_weekly.alias('weekly'), df_core_place2.placekey == df_weekly.placekey, 'inner').select("weekly.placekey", "date_range_start", "visits_by_day", "naics_code")
    df_main = df_main.select('placekey', F.explode(udfExpand('date_range_start', 'visits_by_day')).alias('date', 'visits'), 'naics_code')
    df_main = df_main.filter((df_main.date >= datetime.date(2019, 1, 1)) & (df_main.date <= datetime.date(2020, 12, 31)))
    df_main = df_main.groupBy('date').agg(F.expr('percentile(visits, array(0.5))')[0].alias('median'), F.stddev('visits').alias('stddev'))

    df_main = df_main.withColumn('low', udfLow('median', 'stddev')).withColumn('high', df_main.median + df_main.stddev).withColumn('year', F.year(df_main.date)).drop(df_main.stddev).orderBy('date')

    df_main = df_main.withColumn("date", udfChangeYear('date'))
    
    
    
    catagory_name = catagory_name.replace(" ", "_").lower()
    outfile = args+ "/" + catagory_name
    header_data = [("date", "meidan", "low", "high", "year")]
    df_header = spark.createDataFrame(data=header_data)
    df_main = df_header.union(df_main)
    df_main.write.format("com.databricks.spark.csv").option("header", "false").save(outfile)


