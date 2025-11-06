package com.pagerank

import org.apache.spark.sql.SparkSession

object PageRankWithTaxation {
  def main(args: Array[String]): Unit = {

    val arguments = new ArgumentParser(args)

    val spark = SparkSession.builder()
        .master(arguments.masterNode)
        .getOrCreate()
        .sparkContext

    // generate the graph/matrix from the given input files
    val linkGraph = new LinkGraph(spark, arguments.linksFilePath, arguments.titlesFilePath)
    // the linksRDD doesn't change but is used in every page rank iteration, so we can cache it
    linkGraph.linksRDD.cache()
    // calculate the page rank using the graph/matrix
    var ranks = linkGraph.pageRankWithTaxation()

    val topPages = ranks.join(linkGraph.titlesRDD)
                        .map({case (id, (pageRank, title)) => (pageRank, title)})
                        .sortByKey(false)
                        .take(10)
                        .map({case (pageRank, title) => f"($title, $pageRank%.19f)"})

    spark.parallelize(topPages).coalesce(1).saveAsTextFile(arguments.outputPath)
  }
}