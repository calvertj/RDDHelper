package net.nibr.hadwrap.spark.rdd

/**
 * Created by Jason Calvert on 5/30/14.
 */

import org.scalatest.{Matchers, FlatSpec}

import org.apache.spark.rdd.RDD

class FileNamesToRDDTest extends FlatSpec with Matchers with SharedSparkContext {

  "FileNames.RDDFromStingList" should "create a union-ed RDD from all the FileNames in the Seq[String] input" in  {
    val testArray : Array[String] = Array[String]("src/test/resources/fileOne.txt","src/test/resources/fileTwo.txt")
    val testArrayResult : Array[String] = Array[String]("1","2","3","4","5","6")
    val newRDD = FileNamesToRDD.RDDFromStringList(sc, testArray)
    newRDD.collect() should be (testArrayResult)
  }

}
