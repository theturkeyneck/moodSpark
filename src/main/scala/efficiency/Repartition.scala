package efficiency

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object Repartition extends App {

  //Start Session + Spark context
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Test")
    // .config("spark.sql.autoBroadcastJoinThreshold", 11485760)
    // .config("spark.sql.shuffle.partitions", 30)
    .getOrCreate()

  //Read DF
  // val names = spark.read.option("header", true).option("inferSchema", true)
    //.csv("src/main/resources/baby-names.csv")

 // val groupBybabies = names.groupBy(col("name")).count.foreach(_ => ())

  //names.repartition(50).groupBy(col("name")).count.foreach(_ => ())

  // names.repartition(50, col("name")).groupBy(col("name")).count.foreach(_ => ())

  // names.repartition(50, col("year")).groupBy("name").count.foreach(_ => ())

  val df = spark.range(0, 20)
   println(df.rdd.partitions.length)
  df.write.mode(SaveMode.Overwrite)csv("partition.csv")

  val df2 = df.repartition(6)
  //df2.show()
  println(df2.rdd.partitions.length)
  df2.write.mode(SaveMode.Overwrite)csv("repartition.csv")

  val df3 = df.coalesce(6)
  //df3.show()
  //println(df3.rdd.partitions.length)
  df3.write.mode(SaveMode.Overwrite) csv ("coalesce.csv")

  //Thread.sleep(100000000)

}
