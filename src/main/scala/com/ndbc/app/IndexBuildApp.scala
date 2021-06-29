package com.ndbc.app

import com.ndbc.util.HBaseUtils._
import com.ndbc.util.RowKeyUtils._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

object IndexBuildApp {
  /**
   * store into hbase: id -> seq
   *
   * @param rdd            [id, seq]
   * @param hbaseTableName hbase table name
   */
  def buildId2SeqIndex(rdd: RDD[String], hbaseTableName: String): Long = {
    val family = Bytes.toBytes("default")
    val qualifier = Bytes.toBytes("t1")
    val putRdd = rdd.map(record => {
      val idAndSeq = record.split("\t")
      val (id, seq) = (idAndSeq.head.toInt, idAndSeq.last)
      val rowKey = Bytes.toBytes(id)
      val put = new Put(rowKey)
      put.addColumn(family, qualifier, Bytes.toBytes(id + "#" + seq))
    })

    createTableIfNotExists(hbaseTableName, Seq("default"))
    val jobConf = jobConfig(hbaseTableName)
    putRdd.map(put => (new ImmutableBytesWritable, put)).saveAsHadoopDataset(jobConf)

    putRdd.count()
  }

  /**
   * store into hbase: time@max@min@id -> (block, seq)
   *
   * @param rdd            rdd of string
   * @param timeBlockLen   length of time block
   * @param valueBlockLen  length of value block in time block
   * @param hbaseTableName hbase table name
   * @return number of rows in hbase
   */
  def buildTimeMaxMinId2BlockSeqIndex(rdd: RDD[String], timeBlockLen: Int, valueBlockLen: Int, hbaseTableName: String): Long = {
    val family = Bytes.toBytes("default")
    val qualifier1 = Bytes.toBytes("t1")
    val qualifier2 = Bytes.toBytes("t2")

    val convertRdd = rdd.flatMap(line => {
      val idAndSeq = line.split("\t", 2)
      val id = idAndSeq.head.toInt
      val seq = idAndSeq.last.split(",").map(_.toDouble).toSeq
      for (i <- seq.indices by timeBlockLen)
        yield {
          val timeBlockSeq = seq.slice(i, i + timeBlockLen)
          val timeValueBlockMaxMin = (for (j <- timeBlockSeq.indices by valueBlockLen)
            yield {
              val timeValueBlock = timeBlockSeq.slice(j, j + valueBlockLen)
              (timeValueBlock.max, timeValueBlock.min)
            }).toArray.toSeq
          // local max-min in time block or global max-min in seq
          // decision: local info can cope with increasing dims of seq
          // [time, time block seq max, time block seq min, id, time value block, time block seq]
          (i / timeBlockLen, timeBlockSeq.max, timeBlockSeq.min, id, timeValueBlockMaxMin, timeBlockSeq)
        }
    })

    val putRdd = convertRdd.map(record => {
      val (time, timeBlockSeqMax, timeBlockSeqMin, id, block, seq) = record
      val rowKey = timeMaxMinIdIndex(time, timeBlockSeqMax, timeBlockSeqMin, id)
      val put = new Put(rowKey)
      // max1 min1,max2 min2,max3 min3
      put.addColumn(family, qualifier1, Bytes.toBytes(block.map(t => t._1 + " " + t._2).mkString(",")))
      put.addColumn(family, qualifier2, Bytes.toBytes(seq.mkString(",")))
    })

    createTableIfNotExists(hbaseTableName, Seq("default"))
    val jobConf = jobConfig(hbaseTableName)
    putRdd.map(put => (new ImmutableBytesWritable, put)).saveAsHadoopDataset(jobConf)
    putRdd.count()
  }
}
