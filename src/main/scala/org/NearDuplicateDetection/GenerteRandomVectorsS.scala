package org.NearDuplicateDetection

import scala.math._
import scala.util.Random

class GenerteRandomVectorsS(vecLen: Int, sigLen: Int, seeds: Array[Long]) extends Serializable {
  var randomVectors = Array.ofDim[Double](sigLen, vecLen)

  for (i <- 0 to (sigLen - 1) ) {
    val v = generateUnitRandomVector(seeds(i))
    randomVectors(i) = v
  }
   
  def getRandomVectors () : Array[Array[Double]] = {
    return randomVectors
  }

  def generateUnitRandomVector(seed: Long) : Array[Double] = {
    var x = 0.0
    var y = 0.0
    var z = 0.0
    val r = new Random(seed)
    var vector = new Array[Double](vecLen)
    var normalizationFactor = 0.0

    for (i <- 0 to (vecLen - 1) ) {
      do {
        x = 2.0 * r.nextDouble - 1.0
        y = 2.0 * r.nextDouble - 1.0          
        z = x * x + y * y
      } while (z > 1 || z == 0)
      val f = (x *sqrt(-2.0 * log(z) / z))
      normalizationFactor += pow(f, 2.0)
      vector(i) = f
    }

    /* normalize vector */
    normalizationFactor = sqrt(normalizationFactor)
    for (i <- 0 to (vecLen - 1) ) {
      val v = vector(i)
      val newf = v / normalizationFactor
      vector(i) = newf
    }

    return vector
  }
}