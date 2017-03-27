#!/usr/bin/env bash
## args[0]  the dataset   e.g."E:/benchmarks/LUBM/datasets/nt1"
##                               or "hdfs://10.2.28.65:9000/jianglili/lubm/universities/1"
##  args[1]  the executor memory  e.g."3g"
## args[2]  the local model or else (cluster mode)  e.g. 1(true) or 0(false)
## args[3] the schema file  e.g. "E://benchmarks//LUBM//univ-benchQL-ub.owl"
##                                 or "/home/cluster/jianglili/lubm/univ-benchQL.owl"
## args[4] the server port  e.g. 9998
spark-submit --driver-java-options "-Xms2g -Xmx4g"  --class org.buaa.nlsde.jianglili.query.spark.S2XTServer --master spark://10.2.28.65:7077
sparkForSparql.jar hdfs://10.2.28.65:9000/jianglili/lubm/universities/1 3g  1 /home/cluster/jianglili/lubm/univ-benchQL.owl

