package com.ndbc.util

import scala.math.max
import scala.util.Random

object OtherUtils {
  /**
   * print result in short
   */
  def printRes(res: Seq[(Int, Double)]): String = {
    val resNum = res.length
    resNum match {
      case 0 => "null"
      case 1 => res.mkString(", ")
      case 2 => res.mkString(", ")
      case _ => res.head + ", " + res(1) + ", ... ," + res.last
    }
  }

  /**
   * split line to id and seq
   *
   * @param line id \t v1,v2,v3,...
   * @return (id, seq)
   */
  def splitLine(line: String): (Int, Seq[Double]) = {
    val idAndSeq = line.split("\t", 2)
    val id = idAndSeq.head.toInt
    val seq = idAndSeq.last.split(",").map(_.toDouble)
    (id, seq)
  }

  /**
   * split input query seq
   */
  def splitQs(qs: String, sep1: String = "#", sep2: String = ","): Seq[(Int, Double)] = {
    qs.split(sep1)
      .map(t => (t.split(sep2).head.toInt, t.split(",").last.toDouble))
  }

  /**
   * generate gaussian between 0 and 1
   */
  def genGaussianZeroToOne(): Double = {
    var res: Double = Double.MaxValue
    while (res < 0.0 || res > 1.0) {
      res = Random.nextGaussian()
    }
    res
  }

  /**
   * generate one fake seq
   */
  def genOneFakeSeq(seq: Seq[Double], fluctuateRate: Double = 0.1): Seq[Double] = {
    val (seqMax, seqMin) = (seq.max, seq.min)
    seq.map(v => {
      val fakeValue = max(v + (OtherUtils.genGaussianZeroToOne() - 0.5) * ((seqMax - seqMin) / 2) * fluctuateRate, 0.0)
      fakeValue.formatted("%.5f").toDouble
    })
  }

  /**
   * generate tuple3
   *
   * @return [(time block index, seq of max-min of time value block, seq of value)]
   */
  def genTuple3(seq: Seq[(Int, Double)], timeBlockLen: Int,
                valueBlockLen: Int): Seq[(Int, Seq[(Int, Double, Double)], Seq[(Int, Double)])] = {
    seq
      .map(t => (t._1 / timeBlockLen, t._1 % timeBlockLen, t._2))
      .groupBy(_._1)
      .mapValues(_.map(t => (t._2, t._3))).toList
      .sortBy(_._1)
      .map(r => {
        val tvBlock = r._2
          .map(t => (t._1 / valueBlockLen, t._2))
          .groupBy(_._1)
          .mapValues(_.map(_._2))
          .map(r => (r._1, r._2.max, r._2.min)).toList
          .sortBy(_._1)
        (r._1, tvBlock, r._2)
      })
  }
}
