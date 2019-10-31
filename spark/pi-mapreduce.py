import sys

from random import random
from operator import add
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('pi-mapreduce').getOrCreate()
sc = spark.sparkContext

SAMPLES_PER_PARTITION = 100000

partitions = 2
if len(sys.argv) > 1:
    partitions = int(sys.argv[1])

NUM_SAMPLES = SAMPLES_PER_PARTITION*partitions

def inside(_):
    x = random()
    y = random()
    if x**2 + y**2 <=1:
        return 1
    else:
        return 0

count = sc.parallelize(range(NUM_SAMPLES), partitions).map(inside).reduce(add)
print('Pi is roughly', 4.0*count/NUM_SAMPLES)

spark.stop()
