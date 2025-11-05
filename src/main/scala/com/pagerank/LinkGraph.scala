package com.pagerank

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class LinkGraph (val spark: SparkContext,
                 val linksFilePath: String, 
                 val titlesFilePath: String) {
    
    // partitions is based on 4x the number of cores per worker (1 core per worker, 10 workers)
    private val PARTITIONS = 40
  
    val titlesRDD: RDD[(String, String)] =
        spark.textFile(titlesFilePath)
             .zipWithIndex()
             .mapValues(x => x + 1)
             .map({case (title, id) => (id.toString, title)})

    val linksRDD: RDD[(String, String)] =
        spark.textFile(linksFilePath)
             .map(line => (line.split(": ")(0), line.split(":")(1)))
             .repartition(PARTITIONS)

    def pageRanks: RDD[(String, Double)] = {
        val totalPages = linksRDD.count.toDouble
        linksRDD.map { case (rootPage, outLinks) => (rootPage, 1.0 / totalPages) }
    }

    def pageRankWithoutTaxation(iterations: Int = LinkGraph.DEFAULT_ITERATIONS): RDD[(String, Double)] = {
        LinkGraph.pageRankWithoutTaxation(linksRDD, pageRanks, iterations)
    }

    def pageRankWithTaxation(iterations: Int = LinkGraph.DEFAULT_ITERATIONS): RDD[(String, Double)] = {
        LinkGraph.pageRankWithTaxation(linksRDD, pageRanks, iterations)
    }
}

/* this allows the pageRankWithoutTaxation() to be called like a static method which 
  can use the RDDs defined above, or take other RDDs as arguments */
object LinkGraph {
    private val DEFAULT_ITERATIONS = 25
    private val BETA = 0.85

    def pageRankWithoutTaxation(links: RDD[(String, String)], 
                            initialRanks: RDD[(String, Double)],
                            iterations: Int = DEFAULT_ITERATIONS)
    : RDD[(String, Double)] = {
        
        var ranks = initialRanks

        for (i <- 1 to iterations) {
            val iterationRank = links.join(ranks).flatMap {
                case (rootPage, (urls, rank)) =>
                    val outLinks = urls.split(" ")
                    outLinks.map(url => (url, rank / outLinks.length))
            }
            ranks = iterationRank.reduceByKey(_+_)
        }
        ranks
    }

    def pageRankWithTaxation(links: RDD[(String, String)], 
                            initialRanks: RDD[(String, Double)],
                            iterations: Int = DEFAULT_ITERATIONS)
    : RDD[(String, Double)] = {
        
        var ranks = initialRanks
        var size = initialRanks.count()

        for (i <- 1 to iterations) {
            val iterationRank = links.join(ranks).flatMap {
                case (rootPage, (urls, rank)) =>
                    val outLinks = urls.split(" ")
                    outLinks.map(url => (url, (rank / outLinks.length) * BETA + (1.0 - BETA) / size))
            }
            ranks = iterationRank.reduceByKey(_+_)
        }
        ranks
    }
}
