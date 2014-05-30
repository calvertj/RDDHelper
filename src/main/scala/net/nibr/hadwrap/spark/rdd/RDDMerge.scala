/**
 * Created by Jason Calvert on 5/30/14.
 *
 */

package net.nibr.hadwrap.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.annotation.tailrec
import scala.reflect.ClassTag

object RDDMerge {
  //for the initial call, we create the accumulator
  def mergeRDDs[T:ClassTag](sc: SparkContext, rddArray: Array[RDD[T]]) :RDD[T] = {
    val emptyRDD = sc.makeRDD(Array[T]())
    mergeRDDsHelper(rddArray,emptyRDD)
  }
  //we are doing tail recursion, this is the helper with an accumulator
  @tailrec private def mergeRDDsHelper[T:ClassTag](rddArray: Array[RDD[T]],rddAccumulator: RDD[T]) : RDD[T]= {
    if (rddArray.size == 1){
      rddAccumulator.union(rddArray(0))
    }else {
      mergeRDDsHelper(rddArray.tail,rddAccumulator.union(rddArray.head))
    }
  }
}