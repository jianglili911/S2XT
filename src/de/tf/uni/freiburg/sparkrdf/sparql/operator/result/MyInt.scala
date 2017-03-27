package de.tf.uni.freiburg.sparkrdf.sparql.operator.result

import org.apache.spark.AccumulatorParam

/**
  * Created by jianglili on 2017/1/11.
  */
object MyInt extends AccumulatorParam[Int]{

  override def addInPlace(r1: Int, r2: Int): Int = r1 | r2

  override def zero(initialValue: Int): Int = 0
}
