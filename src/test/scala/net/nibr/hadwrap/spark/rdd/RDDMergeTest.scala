package net.nibr.hadwrap.spark.rdd

/**
 * Created by Jason Calvert on 5/30/14.
 */

import org.scalatest.{Matchers, FlatSpec}

import org.apache.spark.rdd.RDD

class RDDMergeTest extends FlatSpec with Matchers with SharedSparkContext {

  "RDDMerge.MergeRDDs" should "merge two RDDs" in  {
    val testArray : Array[String] = Array[String]("1","2","3","4","5","6")
    val rDD1 :RDD[String]= sc.parallelize(testArray.splitAt(3)._1.toSeq)
    val rDD2 :RDD[String]= sc.parallelize(testArray.splitAt(3)._2.toSeq)
    val rDDArray = Array[RDD[String]](rDD1,rDD2)
    val mergedRdds = RDDMerge.mergeRDDs(sc,rDDArray)
    mergedRdds.collect() should be (testArray)
  }

}
