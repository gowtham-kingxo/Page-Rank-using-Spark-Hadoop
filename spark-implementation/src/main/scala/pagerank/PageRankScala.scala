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
    var noIncomingEdgeVertices = scala.collection.mutable.ArrayBuffer.empty[(Int, Double)]

    // Form the synthetic graph
    for (i <- 0 until k) {
      val vertexIdStart = (k * i) + 1
      val vertexIdEnd = vertexIdStart + (k - 1)

      // Since vertices with no incoming edges would not be included in the joined RDD
      noIncomingEdgeVertices = noIncomingEdgeVertices :+ (vertexIdStart, 0.0)

      for (j <- vertexIdStart until vertexIdEnd) {
        graph = graph :+ (j, j + 1)
      }

      graph = graph :+ (vertexIdEnd, 0)
    }

    val graphRDD = sc.parallelize(graph.toSeq)

    val noIncomingEdgeVerticesRDD = sc.parallelize(noIncomingEdgeVertices.toSeq)

    val totalVertices = k * k
    var ranks = scala.collection.mutable.ArrayBuffer.empty[(Int, Double)]
    val initialPageRankValue: Double = (1.0 / totalVertices)
    val alpha: Double = 0.2
    val randomSurferProbabilityPerVertex: Double = alpha / totalVertices
    val oneMinusAlpha: Double = 1 - alpha

    // Initialize vertices with initial page rank values
    ranks = ranks :+ (0, 0.0)
    for (i <- 1 to totalVertices) {
      ranks = ranks :+ (i, initialPageRankValue)
    }

    var ranksRDD = sc.parallelize(ranks.toSeq)

    var ranksRDD1 = ranksRDD
    for (i <- 1 to 10) {
      val joinedData = graphRDD.join(ranksRDD)
      val joinedRowValues = joinedData.map(row => row._2)

      ranksRDD = joinedRowValues.reduceByKey(_ + _)
      ranksRDD = ranksRDD.union(noIncomingEdgeVerticesRDD)

      val probabilityFromDanglingNodes: Double = ranksRDD.lookup(0).head
      val probabilityFromDanglingNodesPerVertex: Double = probabilityFromDanglingNodes / totalVertices

      ranksRDD = ranksRDD.map(vertex => {
        val vertexId = vertex._1
        val pageRankValue = vertex._2

        vertexId match {
          case 0 => (vertexId, 0.0)

          case _ => {
            // Page rank formula: p' = α * 1/|G| + (1 − α) * (m /|G| + p)
//            val randomSurferProbability: Double = alpha * (1 / totalVertices)
//            val pageRankProbability: Double = (1 - alpha) * (pageRankValue + probabilityFromDanglingNodesPerVertex)
//
//            val newPageRankValue: Double = randomSurferProbability + pageRankProbability

            // Calculating page rank without alpha
             val newPageRankValue = pageRankValue + probabilityFromDanglingNodesPerVertex

            (vertexId, newPageRankValue)
          }
        }
      })

      ranksRDD.collect().foreach(x => println(x))
      val sumProbability = ranksRDD.map(_._2).sum()
      println("Sum: " + sumProbability)
    }
  }
}
