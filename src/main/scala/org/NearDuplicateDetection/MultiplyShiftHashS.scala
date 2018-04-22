package org.NearDuplicateDetection

import scala.math._

class MultiplyShiftHashS(numHashBits: Int, seeds: Array[Long]) extends Serializable {
  val MAXLENGTH = 10000;
  val LONGSIZE = 64
  var acoeffmatrix = Array.ofDim[Long](seeds.length, MAXLENGTH); 
  var bcoeffmatrix = Array.ofDim[Long](seeds.length, MAXLENGTH);

  for ( i <- 0 to (seeds.length - 1) ) {
    val r = new scala.util.Random(seeds(i))
    for ( j <- 0 to (MAXLENGTH - 1) ) {
      acoeffmatrix(i)(j) = r.nextLong
      bcoeffmatrix(i)(j) = r.nextLong
    }
  }

  def hashStr (shingle: String) : Array[Long] = {
    val b = shingle.getBytes()
    val longbytes = LONGSIZE / 8
    var v = new Array[Long](b.length/longbytes + 1)

    for ( i <- 0 to (b.length - 1) ) {
      v(i/longbytes) |= (b(i) & 0xff) << (longbytes * (i % longbytes))
    }

    return hashLong(v)
  }

  def hashLong (v: Array[Long]) : Array[Long] = {
    var hashVec = new Array[Long](seeds.length)

    for( s <- 0 to (seeds.length - 1) ) {
      var sum = 0L
      for ( i <- 0 to (min(v.length, MAXLENGTH) - 1) ) {
        var a = acoeffmatrix(s)(i)
        var b = bcoeffmatrix(s)(i)
        if ( a % 2 == 0) {
          a += 1L
        }
        sum += v(i) * a + b
      }
      hashVec(s) = sum >>> (LONGSIZE - numHashBits)
    }

    return hashVec
  }
}