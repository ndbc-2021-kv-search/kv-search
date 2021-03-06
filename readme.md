# KV-Search

> This repository is for "Key-value Storage based Distributed Time Series Similarity Search" with NDBC 2021 and Journal of Software 2021.

KV-Search is a novel distributed time series similarity search algorithm based on key-value database `HBase`. All code is implemented in `Scala 2.11.7`. To run this code, you need a big data cluster with `Hbase 1.4.9`, `Hadoop 2.7.3` and `Spark 2.3.3`.

## Command Line Tool

You can run these commands to carry out the experiments mentioned in this paper.  All tasks are submitted in spark-submit mode.

+ Check Data: do some check of raw data

```shell
spark-submit \
--class com.ndbc.Main check hdfsPath
```

+ Fake Data: generate fake data

```shell
spark-submit \
--class com.ndbc.Main fake hdfsPath sizeMultiple dimMultiple
```

+ Index: store data into HBase with kv-search index

```shell
spark-submit \
--class com.ndbc.Main hdfsPath timeBlockLen valueBlockLen
```

+ Index Id: store data into HBase with id index

```shell
spark-submit \
--class com.ndbc.Main hdfsPath
```

+ Sample Block: Sample blocks to calculate delta

```shell
spark-submit \
--class com.ndbc.Main hdfsPath timeBlockLen valueBlockLen
```

```shell
spark-submit \
--class com.ndbc.Main sample-block hdfsPath sampleNum
```

+ Query One Sequence: query one sequence like "0,10#1,11#2,12#3,13"

```shell
spark-submit \
--class com.ndbc.Main sample-block [brute-force|our] querySeq k otherParam
```

+ Auto Exp: automatic do experiments

```shell
spark-submit \
--class com.ndbc.Main exp hdfsPath hbaseTableName k sampleNum expTimes
```

+ Different K: experiment of different k

```shell
spark-submit \
--class com.ndbc.Main diff-k hdfsPath hbaseTableName sampleNum expTimes kList
```

+ Different Dim: experiment of different dim of query sequences

```shell
spark-submit \
--class com.ndbc.Main diff-dim hdfsPath hbaseTableName k sampleNum expTimes dimList
```

+ Different Sample Rate: experiment of different sampling rate alpha

```shell
spark-submit \
--class com.ndbc.Main diff-sample hdfsPath hbaseTableName k expTimes sampleNums
```

+ Different Tp: experiment of different cycle time

```shell
spark-submit \
--class com.ndbc.Main diff-tp hdfsPath hbaseTableNamePrefix sampleNums expTimes tps
```

+ Different T'p: experiment of different cycle time of time block

```shell
spark-submit \
--class com.ndbc.Main diff-tp hdfsPath hbaseTableNamePrefix sampleNums expTimes tp2s
```

+ Spark Block Filter Or Not: experiment on whether to turn on block filtering

```shell
spark-submit \
--class com.ndbc.Main diff-sample hdfsPath hbaseTableName k expTimes sampleNums
```

```shell
spark-submit \
--class com.ndbc.Main spark-block-filter hdfsPath sampleBlockHdfsPath k sampleNum expTimes
```