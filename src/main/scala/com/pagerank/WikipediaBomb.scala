package com.pagerank

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

object WikipediaBomb {
    def main(args: Array[String]): Unit = {
        val arguments = new ArgumentParser(args)

        val spark = SparkSession.builder()
            .master(arguments.masterNode)
            .getOrCreate()
            .sparkContext

        // find the link ID for "Rocky_Mountain_National_Park"
        val rmnpId = spark.textFile(arguments.titlesFilePath)
            .zipWithIndex()
            .mapValues(x => x + 1)
            .filter({case (title, id) => title.equals("Rocky_Mountain_National_Park")})
            .map({case (title, id) => id})
            .first()

        // find all the pages with "surfing" in the title
        val surfingIndices = spark.textFile(arguments.titlesFilePath)
            .zipWithIndex()
            .mapValues(x => x + 1)
            .filter({case (title, id) => title.toLowerCase.contains("surfing")})
            .map({case (title, id) => id.toString})
            .collect()
            .toSet
        val surfingIndicesSet = spark.broadcast(surfingIndices)

        val linkGraph = new LinkGraph(spark, arguments.linksFilePath, arguments.titlesFilePath)
        val links = linkGraph.linksRDD

        // add the RMNP link id to all pages with "surfing" in the title
        val bombedLinks = links.map({case (rootPage, outLinks) =>
            val updatedOutLinks =
                if (surfingIndicesSet.value.contains(rootPage)) outLinks + " " + rmnpId
                else outLinks
                (rootPage, updatedOutLinks)
            })

        displayBombedPageRank(bombedLinks, linkGraph.titlesRDD, surfingIndicesSet)
    }

    // calculates the page rank (w/o taxation) for the sub-graph of pages with "surfing" in the title
    def displayBombedPageRank(bombedLinks: RDD[(String, String)],
                              titles: RDD[(String, String)],
                              surfingIndices: Broadcast[Set[String]]
    ): Unit = {

        val subGraph = bombedLinks.filter({case (rootPage, outLinks) => surfingIndices.value.contains(rootPage)})
        val totalPages = subGraph.count.toDouble
        var ranks = subGraph.map {case (rootPage, outLinks) => (rootPage, 1.0 / totalPages)}

        ranks = LinkGraph.pageRankWithoutTaxation(subGraph, ranks)

        ranks.join(titles)
            .map({case (id, (pageRank, title)) => (pageRank, title)})
            .sortByKey(false)
            .take(10)
            .foreach({case (pageRank, title) => println(s"$title $pageRank")})
    }
}
