package com.pagerank

import org.apache.spark.sql.SparkSession

object PageRankWithoutTaxation {
  def main(args: Array[String]): Unit = {

    val arguments = new ArgumentParser(args)

    val spark = SparkSession.builder()
        .master(arguments.masterNode)
        .getOrCreate()
        .sparkContext

    // generate the graph/matrix from the given input files
    val linkGraph = new LinkGraph(spark, arguments.linksFilePath, arguments.titlesFilePath)
    // calculate the page rank using the graph/matrix
    var ranks = linkGraph.pageRankWithoutTaxation()

    ranks.join(linkGraph.titlesRDD)
         .map({case (id, (pageRank, title)) => (pageRank, title)})
         .sortByKey(false)
         .take(10)
         .foreach({case (pageRank, title) => println(s"$title $pageRank")})

    spark.stop()
  }
}