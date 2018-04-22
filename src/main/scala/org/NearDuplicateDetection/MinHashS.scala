package org.NearDuplicateDetection.MinHashS

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import scala.util.matching.Regex
import scala.util.Random 

import org.NearDuplicateDetection.MultiplyShiftHashS._

class ConfMinHash(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, hashfuncs, hashbits, siglen, draw, shingle, rseed, min, max)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val hashfuncs = opt[Int](descr = "number of hash functions", required = false, default = Some(20))
  val hashbits = opt[Int](descr = "number of hash bits", required = false, default = Some(30))
  val siglen = opt[Int](descr = "length of signature", required = false, default = Some(8))
  val draw = opt[Int](descr = "draw times", required = false, default = Some(5))
  val shingle = opt[Int](descr = "length of shingle", required = false, default = Some(15))
  val rseed = opt[Int](descr = "random seed", required = false, default = Some(1123456))
  val min = opt[Int](descr = "min length of sentence", required = false, default = Some(20))
  val max = opt[Int](descr = "max length of sentence", required = false, default = Some(600))
  verify()
}

class PartitionerPairsMinHash(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions
  def getPartition(key: Any) : Int = {
    val k = key.asInstanceOf[(String, String)]
    ((k._1.hashCode() & Integer.MAX_VALUE) % numPartitions)
  }
}

object MinHashS extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfMinHash(argv)

    val numHashes = args.hashfuncs()
    val numHashBits = args.hashbits()
    val sigLen = args.siglen()
    val draw = args.draw()
    val shingleLen = args.shingle()
    val randSeed = args.rseed()
    val minLen = args.min()
    val maxLen = args.max()

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Number of hash functions: " + numHashes)
    log.info("Number of hash bits: " + numHashBits)
    log.info("Length of signature: " + sigLen)
    log.info("Draw times: " + draw)
    log.info("Length of shingle: " + shingleLen)
    log.info("Random seed: " + randSeed)
    log.info("Min length of sentence: " + minLen)
    log.info("Max length of sentence: " + maxLen)

    val conf = new SparkConf().setAppName("MinHash")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val pattern = new Regex("[\\s]*([A-Z\"][^.!?]*(?:[.!?](?!['\"]?\\s|$)[^.!?]*)*[.!?]?['\"]?)\\s*$?")
    var minhash = new Array[Long](numHashes)

    val r = new scala.util.Random(randSeed)
    val seeds = new Array[Long](numHashes)
    for ( i <- 0 to (numHashes - 1) ) {
      seeds(i) = r.nextLong
    }
    val sigSeed = r.nextLong
    val hashFamily = new MultiplyShiftHashS(numHashBits, seeds)

    textFile
    .flatMap(line => {
      val tokens = line.split(",")
      val docid = tokens(0)
      val page = line.substring(docid.length + 2)

      val matches = pattern.findAllIn(page).toList
      var sentenceCount = 0
      matches.flatMap(sentence => {
        for ( i <- 0 to (minhash.length - 1) ) {
          minhash(i) = Long.MaxValue
        }
        val shingleCount = sentence.length() - shingleLen + 1
        if (shingleCount > minLen && shingleCount < maxLen) {
          val value = sentence + " " + docid + ":" + sentenceCount

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
          var key = List[(Array[Long], String)]()
          for ( j <- 0 to (draw -1) ) {
            var signature = new Array[Long](sigLen)
            for (i <- 0 to (sigLen - 1) ) {
              val x = r.nextInt(numHashes)
              signature(i) = minhash(x)
            }
            key = key ++ List((signature, value))
          }
          sentenceCount += 1
          key
        } else List()
      })
    })
    .groupByKey()
    .saveAsTextFile(args.output())
  }
}