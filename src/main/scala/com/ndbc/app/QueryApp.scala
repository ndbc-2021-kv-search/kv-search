package com.ndbc.app

import com.ndbc.util.DistanceUtils._
import com.ndbc.util.HBaseUtils
import com.ndbc.util.OtherUtils._
import com.ndbc.util.QueryUtils._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object QueryApp {
  /**
   * spark-scan to brute force
   *
   * @return ([(id, distance)], query time, scan times always equals to one)
   */
  def sparkScan(spark: SparkContext, path: String,
                qsWithIndex: Seq[(Int, Double)], k: Int): (Seq[(Int, Double)], Double, Int) = {
    val tic = System.currentTimeMillis()
    val data = spark.textFile(path)
    val (startTime, endTime) = (qsWithIndex.head._1, qsWithIndex.last._1)
    val qs = qsWithIndex.map(_._2)
    val res = data
      .map(line => {
        val (id, seq) = splitLine(line)
        (id, seq.slice(startTime, endTime + 1))
      })
      .map(idAndSeq => (idAndSeq._1, chebyshevDistance(idAndSeq._2, qs)))
      .takeOrdered(k)(Ordering[(Double, Int)].on(t => (t._2, t._1)))

    val tok = System.currentTimeMillis()
    (res, (tok - tic) / 1000.0, 1)
  }

  /**
   * kv-scan to brute force
   *
   * @param hbaseTableName hbase table name
   * @param querySeq       query sequence
   * @param k              k
   * @return ([(id, distance)], query time, scan times always equals to one)
   */
  def kvScan(hbaseTableName: String, querySeq: Seq[Double], k: Int): (Seq[(Int, Double)], Double, Int) = {
    val tic = System.currentTimeMillis()
    val connect = HBaseUtils.getConnection
    val hTable = connect.getTable(TableName.valueOf(hbaseTableName))
    val (family, qualifier1) = (Bytes.toBytes("default"), Bytes.toBytes("t1"))

    val scan = new Scan()
    val topKRes = hTable.getScanner(scan).asScala
      .map(r => {
        val idAndSeq = Bytes.toString(r.getValue(family, qualifier1)).split("#")
        val (id, seq) = (idAndSeq.head.toInt, idAndSeq.last.split(",").map(_.toDouble))
        (id, chebyshevDistance(querySeq, seq))
      }).toSeq
      .sortBy(_._2).take(k)
    val tok = System.currentTimeMillis()
    (topKRes, (tok - tic) / 1000.0, 1)
  }

  /**
   * knn query by our kv-search
   *
   * @return ([(id, distance)], query time, scan times)
   */
  def kvSearch(hbaseTableName: String, qsWithIndex: Seq[(Int, Double)], k: Int, isBlockFilter: Boolean,
               timeBlockLen: Int, valueBlockLen: Int, sampleBlockRdd: RDD[(Double, Double)],
               sampleNum: Int, totalNum: Int): (Seq[(Int, Double)], Double, Int) = {
    val tic = System.currentTimeMillis()
    val tuple3 = genTuple3(qsWithIndex, timeBlockLen, valueBlockLen)
    val estimatedDelta = estimateDelta(sampleBlockRdd,
      (qsWithIndex.maxBy(_._2)._2, qsWithIndex.minBy(_._2)._2), k, sampleNum, totalNum)
    val res1 = multiRowRangeQuery(hbaseTableName, tuple3, k, estimatedDelta, isBlockFilter)
    val realDelta = res1.map(_._2).max
    println(
      s"""
         |+++++++
         |estimate delta and res1: [$estimatedDelta, ${res1.length}]
         |real delta in res1:      [$realDelta]
         |+++++++
         |""".stripMargin)

    if (realDelta <= estimatedDelta) {
      (res1, (System.currentTimeMillis() - tic) / 1000.0, 1)
    } else {
      val res2 = multiRowRangeQuery(hbaseTableName, tuple3, k, realDelta, isBlockFilter)
      (res2, (System.currentTimeMillis() - tic) / 1000.0, 2)
    }
  }
}

