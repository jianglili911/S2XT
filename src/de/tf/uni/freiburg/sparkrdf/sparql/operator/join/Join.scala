package de.tf.uni.freiburg.sparkrdf.sparql.operator.join

import de.tf.uni.freiburg.sparkrdf.constants.Const
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
/**
  * Created by jianglili on 2016/6/3.
  */
trait Join {
  /**
    * Execute an left join
    */
  protected def join(joinVars: Broadcast[java.util.List[String]], left: RDD[SolutionMapping], right: RDD[SolutionMapping]): RDD[SolutionMapping] = {
    if (left == null || right == null) {
      return null
    }

    var result: RDD[SolutionMapping] = null

    if (joinVars.value == null || joinVars.value.isEmpty()) {
      return null;
//      val cartesian = left.cartesian(right)
//      left.unpersist(true)
//      right.unpersist(true)
//
//      result = cartesian.map(result => {
//        result._1.addAllMappings(result._2.getAllMappings());
//        result._1
//      })
    }else {

      val leftPair: RDD[(String, SolutionMapping)] = left.map(solution => {
        var joinKeys: String = ""  //joinKeys is the values of join vars
        joinVars.value.foreach(joinVariable => {
          joinKeys = joinKeys + solution.getValueToField(joinVariable)
        })
        (joinKeys, solution)
      })
      left.unpersist(true)

      val rightPair: RDD[(String, SolutionMapping)] = right.map(solution => {
        var joinKeys: String = ""  //joinKeys is the values of join vars
        joinVars.value.foreach(joinVariable => {
          joinKeys = joinKeys + solution.getValueToField(joinVariable)
        })
        (joinKeys, solution)
      })
      right.unpersist(true)
      val  Joined: RDD[(String, (SolutionMapping, SolutionMapping))] = leftPair.join(rightPair);

      result = Joined.map(result => {
        val leftMapping = result._2._1
          leftMapping.addAllMappings(result._2._2.getAllMappings())
        leftMapping
      })
    }

    if (result.partitions.size > 108) {
      result = result.coalesce(108, false)
    }
    result.persist(Const.STORAGE_LEVEL)
    result
  }
}
