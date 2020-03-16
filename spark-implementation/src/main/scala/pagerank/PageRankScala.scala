package pagerank

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object PageRankScala {
  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 1) {
      logger.error("Usage:\nwc.PageRankMain <k>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val k = Integer.parseInt(args(0))

    var graph = scala.collection.mutable.ArrayBuffer.empty[(Int, Int)]

    // Form the synthetic graph
    for (i <- 0 until k) {
      val vertexIdStart = (k * i) + 1
      val vertexIdEnd = vertexIdStart + (k - 1)

      for (j <- vertexIdStart until  vertexIdEnd) {
        graph = graph :+ (j, j+1)
      }

      graph = graph :+ (vertexIdEnd , 0)
    }

    val graphRDD = sc.parallelize(graph.toSeq)

    val kSquare = k * k
    var ranks = scala.collection.mutable.ArrayBuffer.empty[(Int, Double)]
    ranks = ranks :+ (0, 0.0)
    val initialPageRankValue: Double = (1.0 / kSquare)

    // Initialize vertices with initial page rank values
    for (i <- 1 to kSquare) {
      ranks = ranks :+ (i, initialPageRankValue)
    }

    val ranksRDD = sc.parallelize(ranks.toSeq)
  }
}
