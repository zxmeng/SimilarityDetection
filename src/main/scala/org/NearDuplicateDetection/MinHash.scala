package org.NearDuplicateDetection

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner

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

object MinHash extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfMinHash(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Number of hash functions: " + args.hashfuncs())
    log.info("Number of hash bits: " + args.hashbits())
    log.info("Length of signature: " + args.siglen())
    log.info("Draw times: " + args.draw())
    log.info("Length of shingle: " + args.shingle())
    log.info("Random seed: " + args.rseed())
    log.info("Min length of sentence: " + args.min())
    log.info("Max length of sentence: " + args.max())

    val conf = new SparkConf().setAppName("MinHash")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val threshold = args.threshold()
    val total = sc.broadcast(textFile.count())
    var marginal = 0.0f

    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val uniqueTokens = tokens.take(40).distinct
          uniqueTokens.flatMap(tokenX => 
            uniqueTokens.map(tokenY => (tokenX, tokenY))
            ++ List((tokenX, "*"))
          )
          .filter(tokenXY => tokenXY._1 != tokenXY._2)
        } else List()
      })
      .map(p => (p, 1))
      .reduceByKey(_ + _)
      .filter(p => (p._2 >= threshold))
      .repartitionAndSortWithinPartitions(new PartitionerPairsPMI(args.reducers()))
      .map(p => if (p._1._2 == "*") {
        marginal = p._2
        ((p._1._1, p._1._2), (0.0f, p._2))
      } else {
        ((p._1._2, p._1._1), (p._2 / marginal, p._2))
      })
      .repartitionAndSortWithinPartitions(new PartitionerPairsPMI(args.reducers()))
      .map(p => if (p._1._2 == "*") {
        marginal = p._2._2
        (p._1, p._2)
      } else {
        (p._1, (scala.math.log10((total.value * p._2._1 / marginal).toDouble), p._2._2))
      })
      .filter(p => (p._1._2 != "*"))
      .saveAsTextFile(args.output())
  }
}
