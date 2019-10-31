from pyspark.sql import SparkSession
from random import random

NUM_SAMPLES = 1000000

def inside(p):
    x, y = random(), random()
    return x**2+y**2 < 1

spark = SparkSession.builder.appName('pi-filter').getOrCreate()
sc = spark.sparkContext

count = sc.parallelize(list(range(NUM_SAMPLES))).filter(inside).count()
print('Pi is roughly', 4.0*count/NUM_SAMPLES)

spark.stop()
