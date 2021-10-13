from pyspark.sql import SparkSession

OUTPUT_FILE = 'hdfs:///user/david-fowler/biblecounts.txt'
INPUT_FILE = 'hdfs:///data/king-james-bible.txt'

spark = SparkSession.builder.appName('biblecount').getOrCreate()
sc = spark.sparkContext

text_file = sc.textFile(INPUT_FILE)
rdd1 = text_file.flatMap(lambda line: line.split(' '))
rdd2 = rdd1.map(lambda word: (word, 1))
rdd3 = rdd2.reduceByKey(lambda a, b: a + b)
rdd3.saveAsTextFile(OUTPUT_FILE)

spark.stop()
