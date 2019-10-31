from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

USER = 'jh'
APP_NAME = 'template.py'
INPUT_FILE = 'hdfs:///data/template.csv'
OUTPUT_FILE = 'result.csv'
OUTPUT_PATH = 'hdfs:///user/'+USER+'/'+OUTPUT_FILE

conf = SparkConf().setAppName(APP_NAME).setMaster('yarn')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.csv(INPUT_FILE, header=True)

df.write.csv(OUTPUT_PATH, mode='overwrite')
