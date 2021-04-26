package com.ndbc.app

import com.ndbc.util.OtherUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DataProcessApp {
  /**
   * check whether the feature dimensions of ts are equal
   */
  def checkData(rdd: RDD[String]): Unit = {
    val columnNum = rdd.first().split("\t").last.split(",").length
    val ts = rdd.map(line => {
      val seq = line.split("\t").last.split(",").map(_.toDouble)
      if (seq.length != columnNum) {
        throw new IllegalArgumentException(s"the dimension of each row are not all the same: " +
          s"$line -> [${seq.length} != $columnNum]")
      }
      (seq.max, seq.min)
    })
    val (globalMax, globalMin) = (ts.map(_._1).max(), ts.map(_._2).min())
    println(
      s"""
         |+++++++
         |check data:
         |shape:       [${rdd.count()}, $columnNum];
         |max and min: [$globalMax, $globalMin]
         |+++++++
         |""".stripMargin)
  }

  /**
   * linear interpolation to fix missing values
   *
   * @param seq          seq
   * @param missingValue missing value
   * @return (repairable, fixed sequence)
   */
  def linearInterp(seq: Seq[Double], missingValue: Double = 0): (Boolean, Seq[Double]) = {
    val missingNum = seq.count(_ == missingValue)
    missingNum match {
      case 0 => (true, seq)
      case _ if missingNum == seq.length - 1 =>
        val value = seq.filter(_ != 0).head
        (true, for (_ <- seq.indices) yield value)
      case _ if missingNum == seq.length =>
        (false, Seq.empty)
      case _ =>
        val filledSeq = for ((i, value) <- seq.indices.zip(seq))
          yield {
            if (value == missingValue) {
              // linear interpolation
              val f = seq.lastIndexWhere(_ != missingValue, i - 1)
              val b = seq.indexWhere(_ != missingValue, i + 1)
              if (f != -1 && b != -1) {
                ((seq(b) - seq(f)) / (b - f)) * (i - f) + seq(f)
              } else if (f == -1 && b != -1) {
                val b2 = seq.indexWhere(_ != missingValue, b + 1)
                ((seq(b2) - seq(b)) / (b2 - b)) * (i - b) + seq(b)
              } else {
                val f2 = seq.lastIndexWhere(_ != missingValue, f - 1)
                ((seq(f) - seq(f2)) / (f - f2)) * (i - f2) + seq(f2)
              }
            } else value
          }
        (true, filledSeq)
    }
  }

  /**
   * clean data
   */
  def clean(rdd: RDD[String], missingValue: Double = 0): Unit = {
    val cleanRdd = rdd.map(line => {
      val (id, seq) = splitLine(line)
      val rs = linearInterp(seq, missingValue)
      (rs._1, id, rs._2)
    }).filter(_._1).map(t => t._2 + "\t" + t._3.mkString(","))
    cleanRdd.repartition(1).saveAsTextFile("./clean")
  }

  /**
   * generate fake data
   *
   * 0: raw data, 1: double raw data, ...
   */
  def generateFakeTs(rdd: RDD[String], sizeMultiple: Int, dimMultiple: Int): RDD[String] = {
    rdd
      .flatMap(line => {
        val seq = line.split("\t").last.split(",").map(_.toDouble).toSeq
        val fakeSeqs = (1 to sizeMultiple).par.map(_ => genOneFakeSeq(seq, genGaussianZeroToOne()))
        fakeSeqs.toList :+ seq
      })
      .map(seq => {
        val fakeSeqs = (1 to dimMultiple).par.map(_ => genOneFakeSeq(seq, genGaussianZeroToOne()))
        seq ++ fakeSeqs.flatten
      })
      .zipWithIndex()
      .map(t => (t._2 + 1) + "\t" + t._1.mkString(","))
  }

  /**
   * sample data to cal and save block
   */
  def sampleBlock(data: RDD[String], fraction: Double = 0.01, deltaHdfsPath: String): Unit = {
    data
      .sample(false, fraction)
      .map(_.split("\t").last.split(",").map(_.toDouble))
      .map(seq => (seq.max, seq.min))
      .map(block => block._1 + " " + block._2)
      .saveAsTextFile(deltaHdfsPath)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataProcessApp")
      .setMaster("local[*]")
    val spark = new SparkContext(conf)

    val inputPath = DataProcessApp.getClass.getClassLoader.getResource("TS_8820_60.txt").getPath
    val data = spark.textFile(inputPath)
    checkData(data)
  }
}
