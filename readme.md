# KV-Search

> This repository is for "Key-value Storage based Distributed Time Series Similarity Search - NDBC 2021".

KV-Search is a novel distributed time series similarity search algorithm based on key-value database `HBase`. All code is implemented in `Scala`. To run this code, you need a big data cluster with `Spark`, `Hdfs` and `Hadoop`.

## Command Line Tool

You can run these commands to carry out the experiments mentioned in this paper.  All tasks are submitted in spark-submit mode.

+ Check Data

```shell
spark-submit \
--class com.ndbc.Main check hdfsPath
```

+ Fake Data

```shell
spark-submit \
--class com.ndbc.Main fake hdfsPath sizeMultiple dimMultiple
```

+ Index

```shell
spark-submit \
--class com.ndbc.Main hdfsPath timeBlockLen valueBlockLen
```

+ Sample Block

```shell
spark-submit \
--class com.ndbc.Main sample-block hdfsPath sampleNum
```

+ Query One Sequence

```shell
spark-submit \
--class com.ndbc.Main sample-block [brute-force|our] querySeq k otherParam
```

+ Auto Exp

```shell
spark-submit \
--class com.ndbc.Main exp hdfsPath hbaseTableName k sampleNum expTimes
```

+ Different K

```shell
spark-submit \
--class com.ndbc.Main diff-k hdfsPath hbaseTableName sampleNum expTimes kList
```

+ Different Dim

```shell
spark-submit \
--class com.ndbc.Main diff-dim hdfsPath hbaseTableName k sampleNum expTimes dimList
```

+ Different Sample Rate

```shell
spark-submit \
--class com.ndbc.Main diff-sample hdfsPath hbaseTableName k expTimes sampleNums
```

+ Spark Block Filter Or Not

```shell
spark-submit \
--class com.ndbc.Main spark-block-filter hdfsPath sampleBlockHdfsPath k sampleNum expTimes
```

