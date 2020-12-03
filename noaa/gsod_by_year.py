from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import hdfs

START_YEAR = 1929
END_YEAR = 2020

HDFS_URL = 'http://had03.hadoop.test:50070'
HDFS_USER = 'jh'
DATALAKE_DIR = '/data/gsod'

USER = 'jh'
APP_NAME = 'gsod_by_year'
OUTPUT_PATH = 'hdfs:///user/' + USER + '/gsod_by_year/'

def main():
    conf = SparkConf().setAppName(APP_NAME).setMaster('yarn')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    hdfs_client = hdfs.InsecureClient(HDFS_URL, user=HDFS_USER)
    for year in range(START_YEAR, END_YEAR+1):
        year_directory = DATALAKE_DIR + '/' + str(year)
        output_filename = OUTPUT_PATH + 'YEAR=' + str(year) + '/data.parquet'
        files = hdfs_client.list(year_directory)
        for fname in files:
            rdd1 = sc.textFile(year_directory +'/' + fname)
            rdd2 = rdd1.filter(lambda row: row[0] is not 'S')
            rdd3 = rdd2.map(extract_columns)
            df1 = sqlContext.createDataFrame(rdd3, schema=schema)
            df1.write.mode('append').parquet(output_filename)
    sc.stop()

schema = StructType([
    StructField('STN', IntegerType(), True),
    StructField('WBAN', IntegerType(), True),
    StructField('YEAR', IntegerType(), False),
    StructField('MO', IntegerType(), False),
    StructField('DA', IntegerType(), False),
    StructField('TEMP', DoubleType(), True),
    StructField('TEMPCNT', IntegerType(), True),
    StructField('DEWP', DoubleType(), True),
    StructField('DEWPCNT', IntegerType(), True),
    StructField('SLP', DoubleType(), True),
    StructField('SLPCNT', IntegerType(), True),
    StructField('STP', DoubleType(), True),
    StructField('STPCNT', IntegerType(), True),
    StructField('VISIB', DoubleType(), True),
    StructField('VISIBCNT', IntegerType(), True),
    StructField('WDSP', DoubleType(), True),
    StructField('WDSPCNT', IntegerType(), True),
    StructField('MXSPD', DoubleType(), True),
    StructField('GUST', DoubleType(), True),
    StructField('MAX', DoubleType(), True),
    StructField('MAXFLAG', BooleanType(), True),
    StructField('MIN', DoubleType(), True),
    StructField('MINFLAG', BooleanType(), True),
    StructField('PRCP', DoubleType(), True),
    StructField('PRCPFLAG', StringType(), True),
    StructField('SNDP', DoubleType(), True),
    StructField('FRSHTT', StringType(), False)
    ])

def extract_columns(row):
    STN = int(row[0:6])
    if STN == 999999: STN = None
    WBAN = int(row[7:12])
    if WBAN == 99999: WBAN = None
    YEAR = int(row[14:18])
    MO = int(row[18:20])
    DA = int(row[20:22])
    TEMP = row[24:30]
    if TEMP == '9999.9':
        TEMP = None
    else:
        TEMP = float(TEMP)
    TEMPCNT = None
    if TEMP is not None: TEMPCNT = int(row[31:33])
    DEWP = row[35:41]
    if DEWP == '9999.9':
        DEWP = None
    else:
        DEWP = float(DEWP)
    DEWPCNT = None
    if DEWP is not None: DEWPCNT = int(row[42:44])
    SLP = row[46:52]
    if SLP== '9999.9':
        SLP = None
    else:
        SLP = float(SLP)
    SLPCNT = None
    if SLP is not None: SLPCNT = int(row[53:55])
    STP = row[57:63]
    if STP== '9999.9':
        STP = None
    else:
        STP = float(STP)
    STPCNT = None
    if STP is not None: STPCNT = int(row[53:55])
    VISIB = row[68:73]
    if VISIB == '999.9':
        VISIB = None
    else:
        VISIB= float(VISIB)
    VISIBCNT = None
    if VISIB is not None: VISIBCNT = int(row[74:76])
    WDSP = row[78:83]
    if WDSP == '999.9':
        WDSP = None
    else:
        WDSP = float(WDSP)
    WDSPCNT = None
    if WDSP is not None: WDSPCNT = int(row[84:86])
    MXSPD= row[88:93]
    if MXSPD == '999.9':
        MXSPD = None
    else:
        MXSPD = float(MXSPD)
    if WDSP is not None: WDSPCNT = int(row[84:86])
    GUST = row[95:100]
    if GUST == '999.9':
        GUST = None
    else:
        GUST = float(GUST)
    MAX = row[102:108]
    if MAX == '9999.9':
        MAX = None
    else:
        MAX = float(MAX)
    MAXFLAG = row[108]
    if MAXFLAG == '*':
        MAXFLAG = True
    else:
        MAXFLAG = False
    MIN = row[110:116]
    if MIN == '9999.9':
        MIN = None
    else:
        MIN = float(MIN)
    MINFLAG = row[117]
    if MINFLAG == '*':
        MINFLAG = True
    else:
        MINFLAG = False
    PRCP = row[118:123]
    if PRCP == '99.99':
        PRCP = None
    else:
        PRCP = float(PRCP)
    PRCPFLAG = row[123]
    SNDP = row[125:130]
    if SNDP == '999.9':
        SNDP = None
    else:
        SNDP = float(SNDP)
    FRSHTT = row[133:]

    return (STN, WBAN, YEAR, MO, DA, TEMP, TEMPCNT, DEWP, DEWPCNT,
            SLP, SLPCNT, STP, STPCNT, VISIB, VISIBCNT, WDSP, WDSPCNT,
            MXSPD, GUST, MAX, MAXFLAG, MIN, MINFLAG, PRCP, PRCPFLAG,
            SNDP, FRSHTT)

if __name__ == '__main__':
    main()
