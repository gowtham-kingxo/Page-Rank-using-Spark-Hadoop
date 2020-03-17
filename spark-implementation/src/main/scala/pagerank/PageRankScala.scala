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

      for (j <- vertexIdStart until  vertexIdEnd) {
        graph = graph :+ (j, j+1)
      }

      graph = graph :+ (vertexIdEnd , 0)
    }

    val graphRDD = sc.parallelize(graph.toSeq)

    val noIncomingEdgeVerticesRDD = sc.parallelize(noIncomingEdgeVertices.toSeq)

    val kSquare = k * k
    var ranks = scala.collection.mutable.ArrayBuffer.empty[(Int, Double)]
    val initialPageRankValue: Double = (1.0 / kSquare)

    // Initialize vertices with initial page rank values
    ranks = ranks :+ (0, 0.0)
    for (i <- 1 to kSquare) {
      ranks = ranks :+ (i, initialPageRankValue)
    }

    var ranksRDD = sc.parallelize(ranks.toSeq)


      val joinedData = graphRDD.join(ranksRDD)
      val joinedRowValues = joinedData.map(row => row._2)

      ranksRDD = joinedRowValues.reduceByKey(_ + _)
      ranksRDD = ranksRDD.union(noIncomingEdgeVerticesRDD)

//      val sumProbability = ranksRDD.map(_._2).sum()
      val probabilityFromDanglingNodes: Double = ranksRDD.lookup(0).head
      val probabilityFromDanglingNodesPerVertex: Double = probabilityFromDanglingNodes / kSquare

      ranksRDD = ranksRDD.map(vertex => {
        val vertexId = vertex._1
        val pageRankValue = vertex._2

        vertexId match  {
          case 0 => (vertexId, 0.0)

          case _ => {
            val newPageRankValue = pageRankValue + probabilityFromDanglingNodesPerVertex
            (vertexId, newPageRankValue)
          }
        }
      })

//    ranksRDD.collect().foreach(x => println(x))
//    val sumProbability = ranksRDD.map(_._2).sum()
//    println("Sum: " + sumProbability)

  }
}
