package org.NearDuplicateDetection

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import scala.util.matching.Regex
import scala.util.Random 

import org.NearDuplicateDetection._

class ConfRandProj(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, veclen, siglen, permutate, threshold, window, rseed)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val veclen = opt[Int](descr = "length of vector", required = false, default = Some(400))
  val siglen = opt[Int](descr = "length of signature", required = false, default = Some(100))
  val permutate = opt[Int](descr = "permutation times", required = false, default = Some(10))
  val threshold = opt[Int](descr = "distance threshold", required = false, default = Some(15))
  val window = opt[Int](descr = "window size", required = false, default = Some(10))
  val rseed = opt[Int](descr = "random seed", required = false, default = Some(1123456))
  verify()
}

class PartitionerRandProj(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions
  def getPartition(key: Any) : Int = {
    val k = key.asInstanceOf[(String, String)]
    ((k._1.hashCode() & Integer.MAX_VALUE) % numPartitions)
  }
}

object RandomProjectionS extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfRandProj(argv)

    val vecLen = args.veclen()
    val sigLen = args.siglen()
    val permNo = args.permutate()
    val threshold = args.threshold()
    val winSize = args.window()
    val randSeed = args.rseed()

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Length of vector: " + vecLen)
    log.info("Length of signature: " + sigLen)
    log.info("Permutation times: " + permNo)
    log.info("Distance threshold: " + threshold)
    log.info("Window size: " + winSize)
    log.info("Random seed: " + randSeed)
    
    val conf = new SparkConf().setAppName("RandomProjection")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val r = new scala.util.Random(randSeed)
    val seeds = new Array[Long](numHashes)
    for ( i <- 0 to (numHashes - 1) ) {
      seeds(i) = r.nextLong
    }
    val sigSeed = r.nextLong

    textFile
    .flatMap(line => {
      val tokens = line.split(",")
      val docid = tokens(0)
      val stncid = tokens(1)
      val page = line.substring(docid.length + 2)

      val matches = pattern.findAllIn(page).toList
      var sentenceCount = 0
      matches.flatMap(sentence => {
        for ( i <- 0 to (minhash.length - 1) ) {
          minhash(i) = Long.MaxValue
        }
        val shingleCount = sentence.length() - shingleLen + 1
        if (shingleCount > minLen && shingleCount < maxLen) {
          // val value = sentence + " " + docid + ":" + sentenceCount
          val value = docid + ":" + sentenceCount + ", " + sentence

          var hashValue = new Array[String](numHashes)
          for ( i <- 0 to (shingleCount - 1) ) {
            val shingle = sentence.substring(i, i + shingleLen)
            val hash = hashFamily.hashStr(shingle) // to implement hashfamily

            for ( j <- 0 to (numHashes - 1) ) {
              if (hash(j) < minhash(j)) {
                minhash(j) = hash(j)
                hashValue(j) = shingle
              }
            }
          }

          val r = new scala.util.Random(sigSeed)
          var key = List[(String, String)]()
          for ( j <- 0 to (draw -1) ) {
            var signature = new Array[Long](sigLen)
            for (i <- 0 to (sigLen - 1) ) {
              val x = r.nextInt(numHashes)
              signature(i) = minhash(x)
            }
            key = key ++ List((signature.mkString("[", ",", "]"), value))
          }
          sentenceCount += 1
          key
        } else List()
      })
    })
    .groupByKey()
    .mapValues(_.toList)
    .filter(p => (p._2.size > 1))
    .flatMap(p => (p._2.map(s => (p._1, s))))
    // .map(p => (p._1, p._2.mkString("[", ",", "]")))
    .saveAsTextFile(args.output())
  }
}
