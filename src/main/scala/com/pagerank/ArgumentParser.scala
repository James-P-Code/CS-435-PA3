package com.pagerank

class ArgumentParser (val args: Array[String]) {

    private val TITLES_INDEX = 0
    private val LINKS_INDEX = 1
    private val NODE_INDEX = 2
    private val OUTPUT_INDEX = 3
    private val SEARCH_WORD_INDEX = 4

    def titlesFilePath : String = args(TITLES_INDEX)

    def linksFilePath : String = args(LINKS_INDEX)

    def masterNode : String =
        if (args.length > NODE_INDEX) "spark://" + args(NODE_INDEX)
        else "local"

    def outputPath : String = args(OUTPUT_INDEX)

    def searchWord : String =
        if (args.length > SEARCH_WORD_INDEX) args(SEARCH_WORD_INDEX)
        else ""
}
