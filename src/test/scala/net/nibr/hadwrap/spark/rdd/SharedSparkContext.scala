package net.nibr.hadwrap.spark.rdd

/**
 * Created by Jason Calvert on 5/30/14.
 */
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.apache.spark.{SparkConf, SparkContext}

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkContext extends BeforeAndAfterAll {
  self: Suite =>

  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  var conf = new SparkConf(false)

  override def beforeAll() {
    _sc = new SparkContext("local", "test", conf)
    super.beforeAll()
  }

  override def afterAll() {
    LocalSparkContext.stop(_sc)
    _sc = null
    super.afterAll()
  }
}
