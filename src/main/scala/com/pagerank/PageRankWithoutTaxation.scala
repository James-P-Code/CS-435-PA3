package com.pagerank

import org.apache.spark.sql.SparkSession

object PageRankWithoutTaxation {
  def main(args: Array[String]): Unit = {
    val arguments = new ArgumentParser(args)

    val spark = SparkSession.builder()
        .master(arguments.masterNode)
        .getOrCreate()
        .sparkContext

    val titles = spark.textFile(arguments.titlesFilePath)
        .zipWithIndex()
        .mapValues(x => x + 1)
        .map({case (title, id) => (id.toString, title)})

    val partitions = 40 // based on 4x number of cores (1 core per worker, 10 worker nodes)
    val links = spark.textFile(arguments.linksFilePath)
        .repartition(partitions)
        .map(line => (line.split(": ")(0), line.split(":")(1)))
        .cache()
    val totalPages = links.count.toDouble
    var ranks = links.map { case (rootPage, outLinks) => (rootPage, 1.0 / totalPages) }

    val iterations = 25

    for (i <- 1 to iterations) {
        val iterationRank = links.join(ranks).flatMap {
            case (rootPage, (urls, rank)) =>
                val outLinks = urls.split(" ")
                outLinks.map(url => (url, rank / outLinks.length))
        }
        ranks = iterationRank.reduceByKey(_+_)
    }

    ranks.join(titles)
        .map({case (id, (pageRank, title)) => (pageRank, title)})
        .sortByKey(false)
        .take(10)
        .foreach({case (pageRank, title) => println(s"$title $pageRank")})

    spark.stop()
  }
}