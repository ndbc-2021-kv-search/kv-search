package com.ndbc

import com.ndbc.app.DataProcessApp._
import com.ndbc.app.IndexBuildApp._
import com.ndbc.app.QueryApp._
import com.ndbc.util.OtherUtils._
import com.ndbc.util.QueryUtils._
import org.apache.spark.{SparkConf, SparkContext}

import scala.math._

object Main {
  private val conf = new SparkConf().setAppName("KV-SEARCH")
  private val spark = new SparkContext(conf)

  /**
   * sample data to test
   */
  private def sample(hdfsPath: String, expTimes: Int): Seq[Seq[(Int, Double)]] = {
    val data = spark.textFile(hdfsPath)
    data
      .takeSample(false, expTimes)
      .map(line => line.split("\t").last.split(",").map(_.toDouble).zipWithIndex.map(t => (t._2, t._1)).toSeq)
  }

  /**
   * sample data of different dimension to test
   */
  private def sampleDiffDim(hdfsPath: String, expTimes: Int,
                            dims: Seq[Int]): Seq[(Int, Seq[Seq[(Int, Double)]])] = {
    val data = spark.textFile(hdfsPath)
    val sampleQs = data
      .takeSample(false, expTimes)
      .map(line => line.split("\t").last.split(",").map(_.toDouble).zipWithIndex.map(t => (t._2, t._1)).toSeq)
    dims.map(dim => (dim, sampleQs.map(_.slice(0, dim)).toSeq))
  }

  /**
   * generate fake data operation
   */
  def fakeOp(dataPath: String, sizeMultiple: Int, dimMultiple: Int): Unit = {
    val tic = System.currentTimeMillis()
    val data = spark.textFile(dataPath)
    val fakeData = generateFakeTs(data, sizeMultiple, dimMultiple)
    val fakeDataSize = fakeData.count()
    val fakeDataDim = fakeData.first().split("\t").last.split(",").length

    val fakeDataPath = dataPath.split("_").dropRight(2).mkString("_") + "_" + fakeDataSize + "_" + fakeDataDim
    fakeData.saveAsTextFile(fakeDataPath)
    val tok = System.currentTimeMillis()
    println(
      s"""
         |+++++++
         |generate fake data successfully:
         |params:        [$dataPath to $fakeDataPath, $sizeMultiple, $dimMultiple];
         |generate time: [${(tok - tic) / 1000.0}s]
         |+++++++""".stripMargin)
  }

  /**
   * sample data to construct block operation
   */
  def sampleBlockOp(hdfsPath: String, sampleNum: Int): Unit = {
    val tic = System.currentTimeMillis()
    val data = spark.textFile(hdfsPath)
    val fraction = min(1.0, sampleNum.toDouble / data.count())
    sampleBlock(data, fraction, hdfsPath + s"_${sampleNum}_SAMPLE_BLOCK")
    val tok = System.currentTimeMillis()

    println(
      s"""
         |sample block:
         |param:            [hdfsPath=$hdfsPath, sampleNum=$sampleNum, fraction=$fraction];
         |sample hdfs path: [${hdfsPath + s"_${sampleNum}_SAMPLE_BLOCK"}]
         |sample time:      [${(tok - tic) / 1000.0}s]
         |""".stripMargin)
  }

  /**
   * store data into hbase operation with kv-search index
   */
  def indexOp(hdfsPath: String, timeBlockLen: Int, valueBlockLen: Int): Unit = {
    val tic = System.currentTimeMillis()
    val data = spark.textFile(hdfsPath)
    val hbaseTableName = hdfsPath.split("/").last + s"_${timeBlockLen}_$valueBlockLen"
    val hbaseRows = buildTimeMaxMinId2BlockSeqIndex(data, timeBlockLen, valueBlockLen, hbaseTableName)
    val tok = System.currentTimeMillis()

    println(
      s"""
         |+++++++
         |index successfully:
         |params:           [$hdfsPath to $hbaseTableName, $timeBlockLen, $valueBlockLen];
         |total data count: [${data.count()} to $hbaseRows];
         |index time:       [${(tok - tic) / 1000.0}s]
         |+++++++""".stripMargin)
  }

  /**
   * store data into hbase operation with id index
   */
  def indexIdOp(hdfsPath: String): Unit = {
    val tic = System.currentTimeMillis()
    val data = spark.textFile(hdfsPath)
    val hbaseTableName = hdfsPath.split("/").last + "_id"
    val hbaseRows = buildId2SeqIndex(data, hbaseTableName)
    val tok = System.currentTimeMillis()

    println(
      s"""
         |+++++++
         |index-id successfully:
         |params:           [$hdfsPath to $hbaseTableName];
         |total data count: [${data.count()} to $hbaseRows];
         |index time:       [${(tok - tic) / 1000.0}s]
         |+++++++""".stripMargin)
  }

  /**
   * query one seq operation
   */
  def queryOp(method: String, qs: Seq[(Int, Double)], k: Int, otherParam: String): (Seq[(Int, Double)], Double, Int) = {
    method match {
      case "spark-scan" =>
        val hdfsPath = otherParam
        sparkScan(spark, hdfsPath, qs, k)

      case "kv-scan" =>
        val otherParams = otherParam.split("#")
        assert(otherParams.length == 1)
        val hbaseTableName = otherParams.head.split("_").dropRight(2).mkString("_") + "_id"
        kvScan(hbaseTableName, qs.map(_._2), k)

      case "our" =>
        val otherParams = otherParam.split("#")
        assert(otherParams.length == 3)
        val (hbaseTableName, isBlockFilter, sampleBlockHdfsPath) =
          otherParams.toList match {
            case List(a, b, c) => (a, b.toBoolean, c)
          }
        val sampleBlockRdd = spark.textFile(sampleBlockHdfsPath).map(line => {
          val temp = line.split(" ")
          (temp.head.toDouble, temp.last.toDouble)
        })
        val (totalNum, timeBlockLen, valueBlockLen) =
          hbaseTableName.split("_").toList match {
            case List(_, a, _, b, c, _*) => (a.toInt, b.toInt, c.toInt)
          }
        kvSearch(hbaseTableName, qs, k, isBlockFilter,
          timeBlockLen, valueBlockLen, sampleBlockRdd, 1000, totalNum)
      case _ =>
        throw new IllegalArgumentException(s"$method is not supported")
    }
  }

  /**
   * auto exp operation
   */
  def expOp(hdfsPath: String, hbaseTableName: String, k: Int, sampleNum: Int, expTimes: Int): Unit = {
    val tic = System.currentTimeMillis()
    val sampleQs = sample(hdfsPath, expTimes)

    val res = sampleQs.par.map(qs =>
      (queryOp("spark-scan", qs, k, hdfsPath),
        queryOp("our", qs, k, hbaseTableName + s"#true#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK"),
        queryOp("our", qs, k, hbaseTableName + s"#false#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK"))
    )
    val tok = System.currentTimeMillis()

    for ((r, i) <- res.zipWithIndex) {
      val (bf, our1, our2) = r
      val (bfRes, bfTime, bfScan) = bf
      val (our1Res, our1Time, our1Scan) = our1
      val (our2Res, our2Time, our2Scan) = our2

      println(
        s"""
           |+++++++
           |sample ${i + 1}:
           |params:                         [hdfsPath=$hdfsPath, hbaseTableName=$hbaseTableName, k=$k];
           |spark-scan top-k:               [${bfRes.length}, $bfScan, [${printRes(bfRes)}], ${bfTime}s];
           |our with block filter top-k:    [${our1Res.length}, $our1Scan, [${printRes(our1Res)}], ${our1Time}s];
           |our without block filter top-k: [${our2Res.length}, $our2Scan, [${printRes(our2Res)}], ${our2Time}s]
           |+++++++""".stripMargin)
    }
    println(
      s"""
         |+++++++
         |auto exp:
         |sample number: [$expTimes];
         |running time:  [${(tok - tic) / 1000.0}s]
         |+++++++
         |""".stripMargin)
  }

  /**
   * different k experiment operation
   */
  def diffKOp(hdfsPath: String, hbaseTableName: String,
              sampleNum: Int, expTimes: Int, ks: Seq[Int]): Unit = {
    val tic = System.currentTimeMillis()
    val sampleQs = sample(hdfsPath, expTimes)

    val diffKRes = ks.par.map(k => {
      val sampleRes = sampleQs.par.map(qs =>
        (
          queryOp("spark-scan", qs, k, hdfsPath)._2,
          queryOp("our", qs, k, hbaseTableName + s"#true#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK")._2,
          queryOp("our", qs, k, hbaseTableName + s"#false#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK")._2
        )
      )
      val bfAvgQueryTime = sampleRes.map(_._1).sum / sampleRes.length
      val our1AvgQueryTime = sampleRes.map(_._2).sum / sampleRes.length
      val our2AvgQueryTime = sampleRes.map(_._3).sum / sampleRes.length
      (k, bfAvgQueryTime, our1AvgQueryTime, our2AvgQueryTime)
    }).toList.sortBy(_._1)

    for (r <- diffKRes) {
      val (k, bfAvgQueryTime, our1AvgQueryTime, our2AvgQueryTime) = r
      println(
        s"""
           |+++++++
           |params:                                  [hdfsPath=$hdfsPath, hbaseTableName=$hbaseTableName, k=$k, exp times=$expTimes];
           |k:                                       [$k];
           |spark-scan avg query time:              [${bfAvgQueryTime}s];
           |our with block filter avg query time:    [${our1AvgQueryTime}s];
           |our without block filter avg query time: [${our2AvgQueryTime}s]
           |+++++++""".stripMargin)
    }

    val tok = System.currentTimeMillis()
    println(
      s"""
         |+++++++
         |diff k:
         |k:             [${ks.mkString(", ")}];
         |running time:  [${(tok - tic) / 1000.0}s]
         |+++++++
         |""".stripMargin)
  }

  /**
   * different k experiment operation: just execute kv-search with block filter
   */
  def diffKOur1Op(hdfsPath: String, hbaseTableName: String, sampleNum: Int, expTimes: Int, ks: Seq[Int]): Unit = {
    val tic = System.currentTimeMillis()
    val sampleQs = sample(hdfsPath, expTimes)

    val diffKRes = ks.par.map(k => {
      val sampleRes = sampleQs.par.map(qs =>
        queryOp("our", qs, k, hbaseTableName + s"#true#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK")._2
      )
      val our1AvgQueryTime = sampleRes.sum / sampleRes.length
      (k, our1AvgQueryTime)
    }).toList.sortBy(_._1)

    for (r <- diffKRes) {
      val (k, our1AvgQueryTime) = r
      println(
        s"""
           |+++++++
           |params:                                  [hdfsPath=$hdfsPath, hbaseTableName=$hbaseTableName, exp times=$expTimes];
           |k:                                       [$k];
           |our with block filter avg query time:    [${our1AvgQueryTime}s]
           |+++++++""".stripMargin)
    }

    val tok = System.currentTimeMillis()
    println(
      s"""
         |+++++++
         |diff k:
         |k:             [${ks.mkString(", ")}];
         |running time:  [${(tok - tic) / 1000.0}s]
         |+++++++
         |""".stripMargin)
  }

  /**
   * different k experiment operation: just execute kv-search without block filter
   */
  def diffKOur2Op(hdfsPath: String, hbaseTableName: String, sampleNum: Int, expTimes: Int, ks: Seq[Int]): Unit = {
    val tic = System.currentTimeMillis()
    val sampleQs = sample(hdfsPath, expTimes)

    val diffKRes = ks.par.map(k => {
      val sampleRes = sampleQs.par.map(qs =>
        queryOp("our", qs, k, hbaseTableName + s"#false#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK")._2
      )
      val our2AvgQueryTime = sampleRes.sum / sampleRes.length
      (k, our2AvgQueryTime)
    }).toList.sortBy(_._1)

    for (r <- diffKRes) {
      val (k, our2AvgQueryTime) = r
      println(
        s"""
           |+++++++
           |params:                                  [hdfsPath=$hdfsPath, hbaseTableName=$hbaseTableName, exp times=$expTimes];
           |k:                                       [$k];
           |our without block filter avg query time: [${our2AvgQueryTime}s]
           |+++++++""".stripMargin)
    }

    val tok = System.currentTimeMillis()
    println(
      s"""
         |+++++++
         |diff k:
         |k:             [${ks.mkString(", ")}];
         |running time:  [${(tok - tic) / 1000.0}s]
         |+++++++
         |""".stripMargin)
  }

  /**
   * different dim experiment operation
   */
  def diffDimOp(hdfsPath: String, hbaseTableName: String,
                k: Int, sampleNum: Int, expTimes: Int, dims: Seq[Int]): Unit = {
    val tic = System.currentTimeMillis()
    val sampleDiffDimQs = sampleDiffDim(hdfsPath, expTimes, dims)

    val diffDimRes = sampleDiffDimQs.par.map(
      dimAndQs => {
        val dim = dimAndQs._1
        val sampleQs = dimAndQs._2
        val sampleResInFixedK = sampleQs.par.map(qs =>
          (
            queryOp("spark-scan", qs, k, hdfsPath)._2,
            queryOp("our", qs, k, hbaseTableName + s"#true#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK")._2,
            queryOp("our", qs, k, hbaseTableName + s"#false#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK")._2
          ))
        val bfAvgQueryTime = sampleResInFixedK.map(_._1).sum / sampleResInFixedK.length
        val our1AvgQueryTime = sampleResInFixedK.map(_._2).sum / sampleResInFixedK.length
        val our2AvgQueryTime = sampleResInFixedK.map(_._3).sum / sampleResInFixedK.length
        (dim, bfAvgQueryTime, our1AvgQueryTime, our2AvgQueryTime)
      }).toList.sortBy(_._1)

    for (r <- diffDimRes) {
      val (dim, bfAvgQueryTime, our1AvgQueryTime, our2AvgQueryTime) = r
      println(
        s"""
           |+++++++
           |params:                                  [hdfsPath=$hdfsPath, hbaseTableName=$hbaseTableName, k=$k, exp times=$expTimes];
           |dim:                                     [$dim];
           |spark-scan avg query time:               [${bfAvgQueryTime}s];
           |our with block filter avg query time:    [${our1AvgQueryTime}s];
           |our without block filter avg query time: [${our2AvgQueryTime}s]
           |+++++++""".stripMargin)
    }

    val tok = System.currentTimeMillis()
    println(
      s"""
         |+++++++
         |diff dim:
         |dim:           [${dims.mkString(", ")}];
         |running time:  [${(tok - tic) / 1000.0}s]
         |+++++++
         |""".stripMargin)
  }

  /**
   * different sampling rate experiment operation
   */
  def diffSampleNum(hdfsPath: String, hbaseTableName: String,
                    k: Int, expTimes: Int, sampleNums: Seq[Int]): Unit = {
    val sampleQs = spark.textFile(hdfsPath)
      .takeSample(false, expTimes)
      .map(line => line.split("\t").last.split(",").map(_.toDouble).zipWithIndex.map(t => (t._2, t._1)).toSeq)

    val diffSampleNumRes = sampleNums.par.map(sampleNum => {
      val diffSampleRes = sampleQs.par.map(qs => {
        (
          queryOp("our", qs, k, hbaseTableName + s"#true#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK")._2,
          queryOp("our", qs, k, hbaseTableName + s"#false#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK")._2
        )
      })
      val our1AvgQueryTime = diffSampleRes.map(_._1).sum / diffSampleRes.length
      val our2AvgQueryTime = diffSampleRes.map(_._2).sum / diffSampleRes.length
      (sampleNum, our1AvgQueryTime, our2AvgQueryTime)
    }).toList.sortBy(_._1)

    for (r <- diffSampleNumRes) {
      val (sampleNum, our1AvgQueryTime, our2AvgQueryTime) = r
      println(
        s"""
           |+++++++
           |params:                                  [hdfsPath=$hdfsPath, hbaseTableName=$hbaseTableName, k=$k, exp times=$expTimes];
           |sample num:                              [$sampleNum];
           |our with block filter avg query time:    [${our1AvgQueryTime}s];
           |our without block filter avg query time: [${our2AvgQueryTime}s]
           |+++++++""".stripMargin)
    }
  }

  /**
   * spark full scan with block filter operation
   */
  def sparkBlockFilterOp(hdfsPath: String, sampleBlockHdfsPath: String,
                         k: Int, sampleNum: Int, expTimes: Int): Unit = {
    val data = spark.textFile(hdfsPath)
      .map(line => {
        val idAndSeq = splitLine(line)
        (idAndSeq._1, idAndSeq._2.max, idAndSeq._2.min, idAndSeq._2)
      })
    data.persist()
    val totalNum = data.count().toInt

    val sampleQs = data.takeSample(false, expTimes).map(_._4)
    val sampleBlockRdd = spark.textFile(sampleBlockHdfsPath).map(line => {
      val temp = line.split(" ")
      (temp.head.toDouble, temp.last.toDouble)
    })
    sampleBlockRdd.persist()

    val res = sampleQs.par.map(
      qs => {
        val delta = estimateDelta(sampleBlockRdd, (qs.max, qs.min), k, sampleNum, totalNum)
        (sparkBlockFilter(data, k, delta, qs, true)._2,
          sparkBlockFilter(data, k, delta, qs, false)._2)
      }
    )
    val withBlockFilterAvgQueryTime = res.map(_._1).sum / res.length
    val withoutBlockFilterAvgQueryTime = res.map(_._2).sum / res.length

    println(
      s"""
         |+++++++
         |spark with block filter:
         |params:                              [hdfsPath=$hdfsPath, sampleBlockHdfsPath=$sampleBlockHdfsPath, k=$k, sampleNum=$sampleNum, expTimes=$expTimes]
         |with block filter avg query time:    [$withBlockFilterAvgQueryTime];
         |without block filter avg query time: [$withoutBlockFilterAvgQueryTime];
         |+++++++
         |""".stripMargin)
  }

  /**
   * different tp experiment operation
   */
  def diffTpOp(hdfsPath: String, hbaseTableNamePrefix: String, sampleNum: Int, expTimes: Int, tps: Seq[Int]): Unit = {
    val tic = System.currentTimeMillis()
    val sampleQs = sample(hdfsPath, expTimes)

    val diffTpRes = tps.par.map(tp => {
      val hbaseTableName = hbaseTableNamePrefix + "_" + tp + "_100"
      val sampleRes = sampleQs.par.map(qs =>
        (
          queryOp("our", qs, 91, hbaseTableName + s"#true#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK")._2,
          queryOp("our", qs, 91, hbaseTableName + s"#false#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK")._2
        )
      )
      val our1AvgQueryTime = sampleRes.map(_._1).sum / sampleRes.length
      val our2AvgQueryTime = sampleRes.map(_._2).sum / sampleRes.length
      (tp, our1AvgQueryTime, our2AvgQueryTime)
    }).toList.sortBy(_._1)

    for (r <- diffTpRes) {
      val (tp, our1AvgQueryTime, our2AvgQueryTime) = r
      println(
        s"""
           |+++++++
           |params:                                  [hdfsPath=$hdfsPath, hbaseTableNamePrefix=$hbaseTableNamePrefix, k=91, exp times=$expTimes];
           |tp:                                      [$tp];
           |our with block filter avg query time:    [${our1AvgQueryTime}s];
           |our without block filter avg query time: [${our2AvgQueryTime}s]
           |+++++++""".stripMargin)
    }

    val tok = System.currentTimeMillis()
    println(
      s"""
         |+++++++
         |diff tp:
         |tp:            [${tps.mkString(", ")}];
         |running time:  [${(tok - tic) / 1000.0}s]
         |+++++++
         |""".stripMargin)
  }

  /**
   * different tp2 to experiment operation
   */
  def diffTp2Op(hdfsPath: String, hbaseTableNamePrefix: String, sampleNum: Int, expTimes: Int, tp2s: Seq[Int]): Unit = {
    val tic = System.currentTimeMillis()
    val sampleQs = sample(hdfsPath, expTimes)

    val diffTpRes = tp2s.par.map(tp2 => {
      val hbaseTableName = hbaseTableNamePrefix + "_1000" + "_" + tp2
      val sampleRes = sampleQs.par.map(qs =>
        (
          queryOp("our", qs, 91, hbaseTableName + s"#true#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK")._2,
          queryOp("our", qs, 91, hbaseTableName + s"#false#${hdfsPath}_${sampleNum}_SAMPLE_BLOCK")._2
        )
      )
      val our1AvgQueryTime = sampleRes.map(_._1).sum / sampleRes.length
      val our2AvgQueryTime = sampleRes.map(_._2).sum / sampleRes.length
      (tp2, our1AvgQueryTime, our2AvgQueryTime)
    }).toList.sortBy(_._1)

    for (r <- diffTpRes) {
      val (tp2, our1AvgQueryTime, our2AvgQueryTime) = r
      println(
        s"""
           |+++++++
           |params:                                  [hdfsPath=$hdfsPath, hbaseTableNamePrefix=$hbaseTableNamePrefix, k=91, exp times=$expTimes];
           |tp2:                                     [$tp2];
           |our with block filter avg query time:    [${our1AvgQueryTime}s];
           |our without block filter avg query time: [${our2AvgQueryTime}s]
           |+++++++""".stripMargin)
    }

    val tok = System.currentTimeMillis()
    println(
      s"""
         |+++++++
         |diff tp:
         |tp:            [${tp2s.mkString(", ")}];
         |running time:  [${(tok - tic) / 1000.0}s]
         |+++++++
         |""".stripMargin)
  }

  def main(args: Array[String]): Unit = {
    val op = args(0).toLowerCase
    println(
      s"""
         |+++++++
         |op: [$op]
         |+++++++
         |""".stripMargin)

    op match {
      case "check" =>
        assert(args.length == 2)
        val hdfsPath = args.last
        checkData(spark.textFile(hdfsPath))

      case "fake" =>
        assert(args.length == 4)
        val (hdfsPath, sizeMultiple, dimMultiple) =
          args.toList match {
            case List(_, a, b, c) => (a, b.toInt, c.toInt)
          }
        fakeOp(hdfsPath, sizeMultiple, dimMultiple)

      case "index" =>
        assert(args.length == 4)
        val (hdfsPath, timeBlockLen, valueBlockLen) =
          args.toList match {
            case List(_, a, b, c) => (a, b.toInt, c.toInt)
          }
        indexOp(hdfsPath, timeBlockLen, valueBlockLen)

      case "index-id" =>
        assert(args.length == 2)
        val hdfsPath = args(1)
        indexIdOp(hdfsPath)

      case "sample-block" =>
        assert(args.length == 3)
        val (hdfsPath, sampleNum) =
          args.toList match {
            case List(_, a, b) => (a, b.toInt)
          }
        sampleBlockOp(hdfsPath, sampleNum)

      case "query" =>
        assert(args.length == 5)
        val (method, queryTs, k, otherParam) =
          args.toList match {
            case List(_, a, b, c, d) => (a.toLowerCase, splitQs(b), c.toInt, d)
          }
        val res = queryOp(method, queryTs, k, otherParam)
        println(
          s"""
             |+++++++
             |query:
             |params:     [method=$method, k=$k, otherParam=$otherParam];
             |top-k:      [${printRes(res._1)}];
             |query time: [${res._2}s]
             |+++++++""".stripMargin)

      case "exp" =>
        assert(args.length == 6)
        val (hdfsPath, hbaseTableName, k, sampleNum, expTimes) =
          args.toList match {
            case List(_, a, b, c, d, e) => (a, b, c.toInt, d.toInt, e.toInt)
          }
        expOp(hdfsPath, hbaseTableName, k, sampleNum, expTimes)

      case "diff-k" =>
        assert(args.length == 6)
        val (hdfsPath, hbaseTableName, sampleNum, expTimes, ks) =
          args.toList match {
            case List(_, a, b, c, d, e) => (a, b, c.toInt, d.toInt, e.split(",").map(_.toInt))
          }
        diffKOp(hdfsPath, hbaseTableName, sampleNum, expTimes, ks)

      case "diff-k-our1" =>
        assert(args.length == 6)
        val (hdfsPath, hbaseTableName, sampleNum, expTimes, ks) =
          args.toList match {
            case List(_, a, b, c, d, e) => (a, b, c.toInt, d.toInt, e.split(",").map(_.toInt))
          }
        diffKOur1Op(hdfsPath, hbaseTableName, sampleNum, expTimes, ks)

      case "diff-k-our2" =>
        assert(args.length == 6)
        val (hdfsPath, hbaseTableName, sampleNum, expTimes, ks) =
          args.toList match {
            case List(_, a, b, c, d, e) => (a, b, c.toInt, d.toInt, e.split(",").map(_.toInt))
          }
        diffKOur2Op(hdfsPath, hbaseTableName, sampleNum, expTimes, ks)

      case "diff-dim" =>
        assert(args.length == 7)
        val (hdfsPath, hbaseTableName, k, sampleNum, expTimes, dims) =
          args.toList match {
            case List(_, a, b, c, d, e, f) => (a, b, c.toInt, d.toInt, e.toInt, f.split(",").map(_.toInt))
          }
        diffDimOp(hdfsPath, hbaseTableName, k, sampleNum, expTimes, dims)

      case "diff-sample" =>
        val (hdfsPath, hbaseTableName, k, expTimes, sampleNums) =
          args.toList match {
            case List(_, a, b, c, d, e) => (a, b, c.toInt, d.toInt, e.split(",").map(_.toInt))
          }
        diffSampleNum(hdfsPath, hbaseTableName, k, expTimes, sampleNums)

      case "diff-tp" =>
        assert(args.length == 6)
        val (hdfsPath, hbaseTableNamePrefix, sampleNum, expTimes, tps) =
          args.toList match {
            case List(_, a, b, c, d, e) => (a, b, c.toInt, d.toInt, e.split(",").map(_.toInt))
          }
        diffTpOp(hdfsPath, hbaseTableNamePrefix, sampleNum, expTimes, tps)

      case "diff-tp2" =>
        assert(args.length == 6)
        val (hdfsPath, hbaseTableNamePrefix, sampleNum, expTimes, tp2s) =
          args.toList match {
            case List(_, a, b, c, d, e) => (a, b, c.toInt, d.toInt, e.split(",").map(_.toInt))
          }
        diffTp2Op(hdfsPath, hbaseTableNamePrefix, sampleNum, expTimes, tp2s)

      case "spark-block-filter" =>
        val (hdfsPath, sampleBlockHdfsPath, k, sampleNum, expTimes) =
          args.toList match {
            case List(_, a, b, c, d, e) => (a, b, c.toInt, d.toInt, e.toInt)
          }
        sparkBlockFilterOp(hdfsPath, sampleBlockHdfsPath, k, sampleNum, expTimes)

      case _ => throw new IllegalArgumentException(s"[$op] is not supported")
    }
  }
}
