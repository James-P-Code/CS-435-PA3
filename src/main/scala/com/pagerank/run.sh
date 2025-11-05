#!/bin/bash
# current variables are from my repo

JAR_LOC="/s/bach/n/under/tgabel25/Fall2025/cs435/PA3/CS-435-PA3/target/scala-2.12/pagerank_2.12-1.0.jar"
TITLES="/s/bach/n/under/tgabel25/Fall2025/cs435/PA3/CS-435-PA3/input/titles-sorted.txt"
LINKS="/s/bach/n/under/tgabel25/Fall2025/cs435/PA3/CS-435-PA3/input/links-simple-sorted.txt"
MASTER_NODE="leon"

$SPARK_HOME/bin/spark-submit --class com.pagerank.PageRankWithoutTaxation --master local[*] $JAR_LOC $TITLES $LINKS


