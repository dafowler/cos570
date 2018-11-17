Spark
=====

Here are some notes on how to run and submit Spark jobs.
Since there currently is no user management in place,
some Spark-related commands must be run as the spark user.

## Running Spark Interactively

```
sudo -u spark PYSPARK_PYTHON=python3 pyspark
```

## Submitting a Spark Job to Yarn

The Python script is named `zips.py`.
```
sudo -u spark spark-submit zips.py --master yarn --deploy-mode cluster
```

## Listing an HDFS Directory

The directory name is `/user/spark`.
```
hdfs dfs -ls /user/spark
```

## Removing an HDFS Directory

This may be necessary when running the same script twice.
HDFS does not easily allow for overwriting of existing files,
so any output files generated need to be removed manually.
The files residing in the HDFS directory `/user/spark/counts-jh` can be removed with the following command:
```
sudo su spark hdfs dfs -rm -r /user/spark/counts-jh
```
Since this is a write operation,
HDFS requires the correct user permissions,
which is why you have to run this as user spark.

## Retrieving an HDFS File

Files are distributed across the HDFS storage nodes.
The following command will retrieve and merge all files into a single one
on a regular, local file system:
```
hdfs dfs -getmerge /user/spark/jh/counts counts
```

