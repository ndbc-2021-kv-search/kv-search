package com.ndbc.util

import com.ndbc.util.DistanceUtils._
import com.ndbc.util.RowKeyUtils._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.FilterList.Operator
import org.apache.hadoop.hbase.filter.{FilterList, MultiRowRangeFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.math._

object QueryUtils {
  /**
   * multi range query based on time-max-min-id index
   *
   * @param hbaseTableName hbase table name
   * @param tuple3         [time, block, seq]
   * @param k              k
   * @param delta          max chebyshev distance in first k candidates
   * @param isBlockFilter  use block to filter or not
   * @return [(id, chebyshev)]
   */
  def multiRowRangeQuery(hbaseTableName: String, tuple3: Seq[(Int, Seq[(Int, Double, Double)], Seq[(Int, Double)])],
                         k: Int, delta: Double, isBlockFilter: Boolean = true): Seq[(Int, Double)] = {
    val connect = HBaseUtils.getConnection
    val hTable = connect.getTable(TableName.valueOf(hbaseTableName))
    val (family, qualifier1, qualifier2) =
      (Bytes.toBytes("default"), Bytes.toBytes("t1"), Bytes.toBytes("t2"))

    val queryTimeBlockLen = tuple3.length
    val qs = tuple3.flatMap(_._3.map(_._2))
    val (globalMax, globalMin) = (qs.max, qs.min)
    println(
      s"""
         |+++++++
         |row key range in max column:
         |delta, qs max and min: [$delta, $globalMax, $globalMin];
         |range key range:       [${globalMin - delta}, ${globalMax + delta}]
         |query time block num:  [$queryTimeBlockLen]
         |+++++++
         |""".stripMargin)

    val ranges = ListBuffer[MultiRowRangeFilter.RowRange]()
    for (timeBlockIndex <- tuple3.map(_._1)) {
      val startRowKey = RowKeyUtils.timeMaxIndex(timeBlockIndex, max(globalMin - delta, 0.0))
      val stopRowKey = RowKeyUtils.timeMaxIndex(timeBlockIndex, globalMax + delta)
      ranges += new MultiRowRangeFilter.RowRange(startRowKey, true, stopRowKey, true)
    }
    val multiRowRangeFilter = new MultiRowRangeFilter(MultiRowRangeFilter.sortAndMerge(ranges.asJava))

    val filterList = new FilterList(Operator.MUST_PASS_ALL)
    filterList.addFilter(multiRowRangeFilter)

    val scan = new Scan()
    scan.setFilter(filterList)

    val res = hTable.getScanner(scan).asScala
    // id-chebyshev distance
    val qsMap = tuple3.map(r => r._1 -> (r._2, r._3)).toMap
    val finalRes = res.par.map(r => {
      val (timeBlockIndex, id) = parseRowKeyTimeAndId(r.getRow)
      if (isBlockFilter) {
        val queryBlock = qsMap(timeBlockIndex)._1.map(t => (t._2, t._3))
        val valueBlock = Bytes.toString(r.getValue(family, qualifier1))
          .split(",").map(_.split(" ").map(_.toDouble)).map(t => (t.head, t.last))
        val targetBlock = qsMap(timeBlockIndex)._1.map(t => valueBlock(t._1))
        if (multiBlockDistance(targetBlock, queryBlock) > delta) {
          // if one block is false, this seq will be discarded
          (false, id, timeBlockIndex, Double.NaN)
        } else {
          val valueBlockSeq = Bytes.toString(r.getValue(family, qualifier2)).split(",").map(_.toDouble)
          val querySeq = qsMap(timeBlockIndex)._2.map(_._2)
          val targetSeq = qsMap(timeBlockIndex)._2.map(t => valueBlockSeq(t._1))
          (true, id, timeBlockIndex, chebyshevDistance(targetSeq, querySeq))
        }
      } else {
        val valueBlockSeq = Bytes.toString(r.getValue(family, qualifier2)).split(",").map(_.toDouble)
        val querySeq = qsMap(timeBlockIndex)._2.map(_._2)
        val targetSeq = qsMap(timeBlockIndex)._2.map(t => valueBlockSeq(t._1))
        (true, id, timeBlockIndex, chebyshevDistance(targetSeq, querySeq))
      }
    })
      .groupBy(_._2)
      .filter(t => t._2.size == queryTimeBlockLen && t._2.forall(_._1))
      .mapValues(_.maxBy(_._4)._4).toList
      .sortBy(t => (t._2, t._1)).take(k)

    connect.close()
    finalRes
  }

  /**
   * use par to implement multi row range query
   */
  def multiRowRangeQuery2(hbaseTableName: String, tuple3: Seq[(Int, Seq[(Int, Double, Double)], Seq[(Int, Double)])],
                          k: Int, delta: Double, isBlockFilter: Boolean = true): Seq[(Int, Double)] = {
    val connect = HBaseUtils.getConnection
    val hTable = connect.getTable(TableName.valueOf(hbaseTableName))
    val (family, qualifier1, qualifier2) =
      (Bytes.toBytes("default"), Bytes.toBytes("t1"), Bytes.toBytes("t2"))

    val queryTimeBlockLen = tuple3.length
    val qs = tuple3.flatMap(_._3).map(_._2)
    val (globalMax, globalMin) = (qs.max, qs.min)
    println(
      s"""
         |+++++++
         |row key range in max column:
         |delta, qs max and min: [$delta, $globalMax, $globalMin];
         |range key range:       [${globalMin - delta}, ${globalMax + delta}]
         |query time block num:  [$queryTimeBlockLen]
         |+++++++
         |""".stripMargin)

    val res = tuple3.par.flatMap(r => {
      val timeBlockIndex = r._1
      // val (blockSeqMax, blockSeqMin) = (r._3.maxBy(_._2)._2, r._3.minBy(_._2)._2)
      // val startRowKey = RowKeyUtils.timeMaxIndex(timeBlockIndex, blockSeqMin - delta)
      // val stopRowKey = RowKeyUtils.timeMaxIndex(timeBlockIndex, blockSeqMax + delta)
      val startRowKey = RowKeyUtils.timeMaxIndex(timeBlockIndex, max(globalMin - delta, 0.0))
      val stopRowKey = RowKeyUtils.timeMaxIndex(timeBlockIndex, globalMax + delta)
      val scan = new Scan()
        .withStartRow(startRowKey, true)
        .withStopRow(stopRowKey, true)
      hTable.getScanner(scan).asScala
    })

    // id-chebyshev distance
    val qsMap = tuple3.map(r => r._1 -> (r._2, r._3)).toMap
    val finalRes = res.par.map(r => {
      val (timeBlockIndex, id) = parseRowKeyTimeAndId(r.getRow)
      if (isBlockFilter) {
        val queryBlock = qsMap(timeBlockIndex)._1.map(t => (t._2, t._3))
        val valueBlock = Bytes.toString(r.getValue(family, qualifier1))
          .split(",").map(_.split(" ").map(_.toDouble)).map(t => (t.head, t.last))
        val targetBlock = qsMap(timeBlockIndex)._1.map(t => valueBlock(t._1))
        if (multiBlockDistance(targetBlock, queryBlock) > delta) {
          // if one block is false, this seq will be discarded
          (false, id, timeBlockIndex, Double.NaN)
        } else {
          val valueBlockSeq = Bytes.toString(r.getValue(family, qualifier2)).split(",").map(_.toDouble)
          val querySeq = qsMap(timeBlockIndex)._2.map(_._2)
          val targetSeq = qsMap(timeBlockIndex)._2.map(t => valueBlockSeq(t._1))
          (true, id, timeBlockIndex, chebyshevDistance(targetSeq, querySeq))
        }
      } else {
        val valueBlockSeq = Bytes.toString(r.getValue(family, qualifier2)).split(",").map(_.toDouble)
        val querySeq = qsMap(timeBlockIndex)._2.map(_._2)
        val targetSeq = qsMap(timeBlockIndex)._2.map(t => valueBlockSeq(t._1))
        (true, id, timeBlockIndex, chebyshevDistance(targetSeq, querySeq))
      }
    })
      .groupBy(_._2)
      .filter(t => t._2.length == queryTimeBlockLen && t._2.forall(_._1))
      .mapValues(_.maxBy(_._4)._4).toList
      .sortBy(t => (t._2, t._1)).take(k)

    connect.close()
    finalRes
  }

  /**
   * estimate delta by sampling block data
   */
  def estimateDelta(sampleBlockRdd: RDD[(Double, Double)], qsBlock: (Double, Double), k: Int,
                    sampleNum: Int, totalNum: Int): Double = {
    val sampleK = (k * (sampleNum.toDouble / totalNum)).toInt + 1
    val delta = sampleBlockRdd
      .map(b => max(abs(b._1 - qsBlock._2), abs(b._2 - qsBlock._1)))
      .takeOrdered(sampleK)
      .max
    delta
  }

  /**
   * spark use block filter or not
   */
  def sparkBlockFilter(data: RDD[(Int, Double, Double, Seq[Double])], k: Int, delta: Double,
                       qs: Seq[Double], isBlockFilter: Boolean): (Seq[(Int, Double)], Double) = {
    val tic = System.currentTimeMillis()
    val (qsMax, qsMin) = (qs.max, qs.min)
    val res =
      if (isBlockFilter) {
        data
          .map(r => {
            if (blockDistance((r._2, r._3), (qsMax, qsMin)) > delta) {
              (false, -1, Double.NaN)
            } else {
              (true, r._1, chebyshevDistance(r._4, qs))
            }
          })
          .filter(_._1)
          .map(t => (t._2, t._3))
          .takeOrdered(k)(Ordering[(Double, Int)].on(t => (t._2, t._1)))
      } else {
        data
          .map(r => (r._1, chebyshevDistance(r._4, qs)))
          .takeOrdered(k)(Ordering[(Double, Int)].on(t => (t._2, t._1)))
      }
    val tok = System.currentTimeMillis()
    (res, (tok - tic) / 1000.0)
  }
}
