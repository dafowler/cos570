Spark
=====

Here are some notes on how to run and submit Spark jobs.

## Running Spark Interactively

```
pyspark
```

## Submitting a Spark Job to Yarn

The Python script is named `zips.py`.
```
spark-submit zips.py --master yarn --deploy-mode cluster
```

## Listing All Running Yarn Jobs

```
yarn app -list -appStates RUNNING
```

## Listing All Finished Yarn Jobs

```
yarn app -list -appStates FINISHED
```

## List the Last 3 Finished Jobs by User `<user>`

```
yarn app -list -appStates FINISHED | grep <user> | sort -r | head -3
```

## Inspecting Yarn Application Logs

```
yarn logs -applicationId <applicationID>
```
ApplicationID is the one reported by `yarn app -list`.

## Killing a Yarn Application
```
yarn app -kill <applicationID>
```
ApplicationID is the one reported by `yarn app -list`.

## Listing an HDFS Directory

The directory name is `/data`.
```
hdfs dfs -ls /data
```

## Removing an HDFS Directory

The files residing in the HDFS directory `/user/jh/counts.csv` can be removed with the following command:
```
hdfs dfs -rm -r /user/spark/counts.csv
```

## Retrieving an HDFS File

Files are distributed across the HDFS storage nodes.
The following command will retrieve and merge all files into a single one
on a regular, local file system:
```
hdfs dfs -getmerge /user/spark/jh/counts.csv counts.csv
```

