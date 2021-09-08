package efficiency

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object Repartition extends App {

  //Start Session + Spark context
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Test")
    // .config("spark.sql.autoBroadcastJoinThreshold", 11485760)
    //.config("spark.sql.shuffle.partitions", 30)
    .getOrCreate()

  //Read DF
  val names = spark.read.option("header", true).option("inferSchema", true)
    .csv("src/main/resources/baby-names.csv")

  // Some transformations are very direct such as select, filtering, etc but some other are a little bit heavy
  // like groupBy and joins
  // val groupByBabies = names.groupBy(col("name")).count.foreach(_ => ())


  //names.repartition(50).groupBy("name").count.foreach(_ => ())
  // 250 partitions: 50 from repartition because it generates shuffle and 200 from groupBy

  //names.repartition(50, col("name")).groupBy("name").count.foreach(_ => ())
  // now we are making 50 partitions because we are repartitioning by key

  // internally, Spark uses a different algorithm if we partition by key or by number of partitions.
  // if it's the latter, spark uses a round robin algorithm, one by one, meanwhile if we partition by key it uses
  // hash function and makes it easier.

  // We must be careful when we repartition, if we use very few partitions, we will exceed the executor memory and
  // it will break

  // if we can avoid doing repartition, better. If the 200 partitions have data, great. Don't be afraid of having
  // a high number of partitions if all of them are working fine

  // what will happen if...
  // names.repartition(50, col("year")).groupBy("name").count.foreach(_ => ())

  //coalesce

  val df = spark.range(0, 20)
  // println(df.rdd.partitions.length)
  // df.write.mode(SaveMode.Overwrite)csv("partition.csv")

  //val df2 = df.repartition(6)
  //df2.show()
  // println(df2.rdd.partitions.length)
  // df2.write.mode(SaveMode.Overwrite)csv("repartition.csv")

  val df3 = df.coalesce(6)
  df3.show()
  //println(df3.rdd.partitions.length)
  //df3.write.mode(SaveMode.Overwrite) csv ("coalesce.csv")

  Thread.sleep(100000000)

}
