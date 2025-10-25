package com.pagerank

class ArgumentParser (val args: Array[String]) {

    private val TITLES_INDEX = 0
    private val LINKS_INDEX = 1
    private val NODE_INDEX = 2

    def titlesFilePath : String = args(TITLES_INDEX)

    def linksFilePath : String = args(LINKS_INDEX)

    def masterNode : String =
        if (args.length > NODE_INDEX) "spark://" + args(NODE_INDEX)
        else "local"

}
