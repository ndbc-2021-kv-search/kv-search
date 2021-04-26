package com.ndbc.util

import scala.math._

object DistanceUtils {
  /**
   * chebyshev distance
   */
  def chebyshevDistance(s1: Seq[Double], s2: Seq[Double]): Double = {
    assert(s1.length == s2.length)
    s1.zip(s2).map(p => abs(p._1 - p._2)).max
  }

  /**
   * block distance: lower bound
   */
  def blockDistance(a: (Double, Double), b: (Double, Double)): Double = {
    val (aMax, aMin) = a
    val (bMax, bMin) = b
    Seq(
      min(abs(aMax - bMax), abs(aMax - bMin)),
      min(abs(aMin - bMax), abs(aMin - bMin)),
      min(abs(bMax - aMax), abs(bMax - aMin)),
      min(abs(bMin - aMax), abs(bMin - aMin))
    ).max
  }

  /**
   * multi block distance: lower bound
   */
  def multiBlockDistance(m1: Seq[(Double, Double)], m2: Seq[(Double, Double)]): Double = {
    assert(m1.length == m2.length)
    m1.zip(m2).map(b => blockDistance(b._1, b._2)).max
  }
}
