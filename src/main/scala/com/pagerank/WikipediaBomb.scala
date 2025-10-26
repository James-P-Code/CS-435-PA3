package com.pagerank

import org.apache.spark.sql.SparkSession

object WikipediaBomb {
    def main(args: Array[String]): Unit = {
        val arguments = new ArgumentParser(args)

        val spark = SparkSession.builder()
            .master(arguments.masterNode)
            .getOrCreate()
            .sparkContext

        val rmnpIndex = spark.textFile(arguments.titlesFilePath)
            .zipWithIndex()
            .mapValues(x => x + 1)
            .filter({case (title, id) => title.equals("Rocky_Mountain_National_Park")})
            .first()

        val rmnpId = rmnpIndex._2.toString

        val surfingIndices = spark.textFile(arguments.titlesFilePath)
            .zipWithIndex()
            .mapValues(x => x + 1)
            .filter({case (title, id) => title.toLowerCase.contains("surfing")})
            .map({case (title, id) => id.toString})
            .collect()
            .toSet

        val surfingIndicesSet = spark.broadcast(surfingIndices)

        val partitions = 40
        val links = spark.textFile(arguments.linksFilePath)
            .repartition(partitions)
            .map(line => (line.split(": ")(0), line.split(":")(1)))

        val bombedLinks = links.map({case (rootPage, outLinks) =>
            val updatedOutLinks =
                if (surfingIndicesSet.value.contains(rootPage)) outLinks + " " + rmnpId
                else outLinks
            s"$rootPage: $updatedOutLinks"
            })
            .coalesce(1)
            
        bombedLinks.saveAsTextFile(arguments.outputPath)
    }
}
