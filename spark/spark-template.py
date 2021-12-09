from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

USER = 'david-fowler'
APP_NAME = 'template'
INPUT_FILE = 'hdfs:///data/template.parquet'
OUTPUT_FILE = 'count.csv'
OUTPUT_PATH = 'hdfs:///user/'+USER+'/'+OUTPUT_FILE

conf = SparkConf().setAppName(APP_NAME).setMaster('yarn')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.csv(INPUT_FILE, header=True)

df1 = df.coalesce(1)
df1.write.csv(OUTPUT_PATH, mode='overwrite', header=True)
