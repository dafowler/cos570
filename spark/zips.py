from pyspark import SparkContext, SparkConf

APP_NAME = 'zips-jh'
INPUT_FILE = 'hdfs:///user/spark/producers.csv'
OUTPUT_FILE = 'hdfs:///user/spark/jh/counts'

conf = SparkConf().setAppName(APP_NAME).setMaster('yarn')
sc = SparkContext(conf=conf)

# load data 
producers = sc.textFile(INPUT_FILE)

# Pick CSV apart
def zip(line):
    # column 2 contains the address
    address = line.split('"')[1]
    # last column of address contains zip
    # strip whitespace and use first 5 digits only (no speed sort)
    zip = address.split(',')[-1].strip()[:5]
    # Make sure data is valid before returning it
    if zip.isdigit():
        return zip
    else:
        return None

# use the zip mapping function to map producers to zip codes
zips = producers.map(zip)
validZips = zips.filter(lambda z: z is not None)

# count all unique zips
count = validZips.map(lambda z: (z, 1)).reduceByKey(lambda a, b: a+b)

count.saveAsTextFile(OUTPUT_FILE)

