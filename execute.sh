#!/usr/bin/env bash
-server
-Xms218m
-Xmx1536m
-XX:MaxPermSize=500m
-XX:ReservedCodeCacheSize=480m
-XX:+UseConcMarkSweepGC
-XX:SoftRefLRUPolicyMSPerMB=50
-ea
-Dsun.io.useCanonCaches=false
-Djava.net.preferIPv4Stack=true
-XX:+HeapDumpOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow


-Dsbt.global.base=G:/sbtcache/boot/
-Dsbt.ivy.home=G:/sbtcache/.ivy2
-XX:MaxPermSize=512M


-Xms256m  -Xmx1024m
-i E:/benchmarks/generator/dataset/yagoTaxonomy.ttl -mem 2g -q E:/benchmarks/generator/dataset/query/YAGO_Query1.rq -o E:/benchmarks/generator/dataset/result/result.nt -jn sqX


-i E:/benchmarks/generator/dataset/yagoTaxonomy.ttl -mem 2g -q E:/benchmarks/generator/dataset/query/YAGO_Query1.rq -o E:/benchmarks/generator/dataset/result/result.nt -jn sqX



-i E:/benchmarks/generator/dataset/yagoTaxonomy.ttl -mem 2g -q E:/benchmarks/generator/dataset/query/YAGO_Query1.rq -o E:/benchmarks/generator/dataset/result/result.nt -l -jn sqX

-i E:/benchmarks/generator/dataset/yagoTaxonomy.ttl
-mem 2g -q E:/benchmarks/generator/dataset/query/YAGO_Query1.rq
-o E:/benchmarks/generator/dataset/result/result.nt -l -jn sqX
本地
spark-submit --driver-java-options "-Xms1g -Xmx4g -agentlib:jdwp=transport=dt_socket,address=9904,server=y,suspend=y" --class de.tf.uni.freiburg.sparkrdf.run.QueryExecutor --master local[2] sparkForSparql.jar  -i hdfs://10.2.1.141:9000/user/root/yago/input  -mem 6g  -countBased  -so  -q  /home/jianglili/yago2/query/YAGO_Query1.rq  -o hdfs://10.2.1.141:9000/user/root/yago/result/rs
spark-submit --driver-java-options -agentlib:jdwp=transport=dt_socket,address=9904,server=y,suspend=y --class de.tf.uni.freiburg.sparkrdf.run.QueryExecutor --master local[2] sparkForSparql.jar  -i hdfs://10.2.1.141:9000/user/root/yago/input  -mem 4g  -countBased  -so  -q  hdfs://10.2.1.141:9000/user/root/yago/query/YAGO_Query1.rq  -o hdfs://10.2.1.141:9000/user/root/yago/result
spark-submit --driver-java-options "-Xms1g -Xmx2g -agentlib:jdwp=transport=dt_socket,address=9904,server=y,suspend=y" --class de.tf.uni.freiburg.sparkrdf.run.QueryExecutor --master local[2] sparkForSparql.jar  -i hdfs://10.2.1.141:9000/user/root/yago/input  -mem 4g  -countBased  -so  -q  /home/jianglili/yago2/query/YAGO_Query1.rq  -o hdfs://10.2.1.141:9000/user/root/yago/result/rs
spark-submit --driver-java-options "-Xms1g -Xmx2" --class de.tf.uni.freiburg.sparkrdf.run.QueryExecutor --master local[2] sparkForSparql.jar  -i hdfs://10.2.1.141:9000/user/root/yago/input  -mem 4g  -countBased  -so  -q  /home/jianglili/yago/query/YAGO_Query1.rq  -o hdfs://10.2.1.141:9000/user/root/yago/result/rs
spark-submit --driver-java-options -Xmx4g --class de.tf.uni.freiburg.sparkrdf.run.QueryExecutor --master local[2] sparkForSparql.jar  -i hdfs://10.2.28.65:9000/jianglili/yago/input   -countBased  -so  -q  /home/cluster/jianglili/yago/query/YAGO_Query1.rq  -o hdfs://10.2.28.65:9000/jianglili/yago/result/qr2
spark-submit --class de.tf.uni.freiburg.sparkrdf.run.QueryExecutor --master local[2] sparkForSparql.jar  -i hdfs://10.2.28.65:9000/jianglili/yago/input   -countBased  -so  -q  /home/cluster/jianglili/yago/query/YAGO_Query1.rq  -o hdfs://10.2.28.65:9000/jianglili/yago/result/qr
spark-submit --driver-java-options -Xmx4g --class de.tf.uni.freiburg.sparkrdf.run.QueryExecutor --master local[2] sparkForSparql.jar  -i hdfs://10.2.28.65:9000/jianglili/yago/input   -countBased  -so  -q  /home/cluster/jianglili/yago/query/YAGO_Query1.rq  -o hdfs://10.2.28.65:9000/jianglili/yago/result/qr2
spark-submit --driver-java-options -Xmx4g --class de.tf.uni.freiburg.sparkrdf.run.QueryExecutor --master local[2] sparkForSparql.jar  -i hdfs://10.2.28.65:9000/jianglili/yago/input   -countBased  -so  -q  /home/cluster/jianglili/yago/query/YAGO_Query1.rq  -o hdfs://10.2.28.65:9000/jianglili/yago/result/qr2

集群
spark-submit --driver-java-options "-Xms2g -Xmx4g" --class de.tf.uni.freiburg.sparkrdf.run.QueryExecutor --master spark://10.2.28.65:7077 sparkForSparql.jar  -i hdfs://10.2.28.65:9000/jianglili/yago/input   -countBased  -so  -q  /home/cluster/jianglili/yago/query/YAGO_Query1.rq -mem 6g -dp 5 -o hdfs://10.2.28.65:9000/jianglili/yago/result/qr13
集群lubm
spark-submit --driver-java-options "-Xms2g -Xmx4g" --class test.query.queryMain  --master spark://10.2.28.65:7077 sparkForSparql.jar  -i hdfs://10.2.28.65:9000/jianglili/lubm/data/200  -countBased  -so  -q  /home/cluster/jianglili/lubm/query28/query1.rq -mem 6g -dp 5 -o hdfs://10.2.28.65:9000/jianglili/lubm/result/res1 -s /home/cluster/jianglili/lubm/univ-benchQL.owl
28
spark-submit --driver-java-options "-Xms2g -Xmx4g" --class test.query.queryMain  --master spark://10.2.28.65:7077 sparkForSparql.jar  -i hdfs://10.2.28.65:9000/jianglili/lubm/data/200  -countBased  -so  -q  /home/cluster/jianglili/lubm/query28/query1.rq,/home/cluster/jianglili/lubm/query28/query2.rq,/home/cluster/jianglili/lubm/query28/query3.rq,/home/cluster/jianglili/lubm/query28/query4.rq, /home/cluster/jianglili/lubm/query28/query5.rq,/home/cluster/jianglili/lubm/query28/query6.rq, /home/cluster/jianglili/lubm/query28/query7.rq,/home/cluster/jianglili/lubm/query28/query8.rq, /home/cluster/jianglili/lubm/query28/query9.rq,/home/cluster/jianglili/lubm/query28/query10.rq, /home/cluster/jianglili/lubm/query28/query11.rq,/home/cluster/jianglili/lubm/query28/query12.rq, /home/cluster/jianglili/lubm/query28/query13.rq,/home/cluster/jianglili/lubm/query28/query14.rq, /home/cluster/jianglili/lubm/query28/query15.rq,/home/cluster/jianglili/lubm/query28/query16.rq, /home/cluster/jianglili/lubm/query28/query17.rq,/home/cluster/jianglili/lubm/query28/query18.rq, /home/cluster/jianglili/lubm/query28/query19.rq,/home/cluster/jianglili/lubm/query28/query20.rq, /home/cluster/jianglili/lubm/query28/query21.rq,/home/cluster/jianglili/lubm/query28/query22.rq, /home/cluster/jianglili/lubm/query28/query23.rq,/home/cluster/jianglili/lubm/query28/query24.rq, /home/cluster/jianglili/lubm/query28/query25.rq,/home/cluster/jianglili/lubm/query28/query26.rq, /home/cluster/jianglili/lubm/query28/query27.rq,/home/cluster/jianglili/lubm/query28/query28.rq -mem 6g -dp 5  -s /home/cluster/jianglili/lubm/univ-benchQL.owl -fr 1 -o hdfs://10.2.28.65:9000/jianglili/lubm/result/res200


--driver-java-options -Xmx2g



spark.default.confs=list(spark.cores.max="24",
                         spark.executor.memory="50g",
                         spark.driver.memory="30g",
                         spark.driver.extraJavaOptions="-Xms5g -Xmx5g -XX:MaxPermSize=1024M")
sc <- sparkR.init(master="local[24]",sparkEnvir = spark.default.confs)


run the query
