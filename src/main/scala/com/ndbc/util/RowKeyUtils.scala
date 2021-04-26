package com.ndbc.util

import org.apache.hadoop.hbase.util.Bytes

object RowKeyUtils {
  // row key: [time index, max, min, id], [Int, Double, Double, Int], where bytes are [4, 8, 8, 4]
  // todo invert the sign bite of IEEE 754 to keep the relative order of double unchanged

  /**
   * join time block index and max value to form row key prefix
   */
  def timeMaxIndex(time: Int, maxValue: Double): Array[Byte] = {
    Bytes.toBytes(time) ++ Bytes.toBytes(maxValue)
  }

  /**
   * join time block index, max value and min value to form row key prefix
   */
  def timeMaxMinIndex(time: Int, maxValue: Double, minValue: Double): Array[Byte] = {
    Bytes.toBytes(time) ++ Bytes.toBytes(maxValue) ++ Bytes.toBytes(minValue)
  }

  /**
   * join time block index, max value, min value and id to form row key
   */
  def timeMaxMinIdIndex(time: Int, maxValue: Double, minValue: Double, id: Int): Array[Byte] = {
    Bytes.toBytes(time) ++ Bytes.toBytes(maxValue) ++ Bytes.toBytes(minValue) ++ Bytes.toBytes(id)
  }

  /**
   * parse row key
   *
   * @return (time index, max, min, id)
   */
  def parseRowKey(rowKey: Array[Byte]): (Int, Double, Double, Int) = {
    (Bytes.toInt(rowKey.slice(0, 4)), Bytes.toDouble(rowKey.slice(4, 12)),
      Bytes.toDouble(rowKey.slice(12, 20)), Bytes.toInt(rowKey.slice(20, 24)))
  }

  /**
   * parse row key
   *
   * @return (time index, id)
   */
  def parseRowKeyTimeAndId(rowKey: Array[Byte]): (Int, Int) = {
    (Bytes.toInt(rowKey.slice(0, 4)), Bytes.toInt(rowKey.slice(20, 24)))
  }
}
