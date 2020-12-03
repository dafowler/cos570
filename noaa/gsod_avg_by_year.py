from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import hdfs

START_YEAR = 1929
END_YEAR = 1930

INPUT_FILE_PATH = 'hdfs:///data/gsod_by_year/YEAR='

USER = 'jh'
APP_NAME = 'gsod_avg_by_year'
OUTPUT_FILE = 'hdfs:///user/' + USER + '/gsod_avg_by_year.csv'

conf = SparkConf().setAppName(APP_NAME).setMaster('yarn')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

input_file = INPUT_FILE_PATH+str(START_YEAR)+'/data.parquet'
df = sqlContext.read.parquet(input_file)
out_df = df.groupBy('YEAR').avg('TEMP').alias('avg_per_year')
for year in range(START_YEAR+1, END_YEAR+1):
    input_file = INPUT_FILE_PATH+str(year)+'/data.parquet'
    df = sqlContext.read.parquet(input_file)
    df1 = df.groupBy('YEAR').avg('TEMP').alias('avg_per_year')
    out_df = out_df.union(df1)
out_df.coalesce(1).write.csv(OUTPUT_FILE, header=True, mode='overwrite')
