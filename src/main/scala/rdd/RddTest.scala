package rdd

import org.apache.spark.sql.SparkSession

object RddTest extends App {

  val spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
  val sc = spark.sparkContext

  val data = Array(1,2,3,4,5,6,7)

  val dataRdd = sc.parallelize(data, 12).foreach(println(_))

}
