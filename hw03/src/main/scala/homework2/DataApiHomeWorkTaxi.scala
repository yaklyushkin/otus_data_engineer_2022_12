package homework2

import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{col, count, max, mean, min, round, stddev, udf}
import org.joda.time.format.DateTimeFormat

import java.util.Properties

object DataApiHomeWorkTaxi extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"


  val taxiFactsDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiFactsDF.printSchema()
  println(taxiFactsDF.count())


  // Задание 1
  val taxiZonesDF = spark.read
    .option("delimiter", ",")
    .option("header", "true")
    .csv("src/main/resources/data/taxi_zones.csv")
  taxiZonesDF.printSchema()
  println(taxiZonesDF.count())

  val task1 = taxiFactsDF
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"), "inner")
    .select("Zone", "PULocationID")
    .groupBy("Zone", "PULocationID")
    .agg(
      functions.count("*").as("cnt")
    )
    .orderBy(col("cnt").desc, col("Zone"))
  task1.show(10)
  task1.write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .save("src/main/resources/data/task1")


  // Задание 2
  // Не совсем понятно, что значит 'В какое время происходит больше всего вызовов', поэтому расчёт идёт
  // с разбивкой по часам, то есть сколько заказов было в каждый из 24 часов за всё время наблюдений
  val fmt = DateTimeFormat forPattern "yyyy-MM-dd HH:mm:ss.S"

  def checkAndParseDate(input: String): String = {
    if (input == null) {
      "-1"
    } else {
      try {
        val datetime = fmt parseDateTime input
        input.substring(11, 13)
      } catch {
        case e: IllegalArgumentException => "-2"
      }
    }
  }

  val task2RDD = taxiFactsDF.rdd
  val task2 = task2RDD
    .map(row => (checkAndParseDate(row(1).toString), 1))
    .reduceByKey((x:Int, y:Int) => x+y)
    .sortBy(row => row._2.toInt, ascending=false)

  task2.cache()

  task2.take(10).foreach(line => {
    println(line._1 + " - " + line._2)
  })
  task2
    .repartition(1)
    .map(row => row._1.toString + " - " + row._2.toString)
    .saveAsTextFile("src/main/resources/data/task2")


  // Задание 3
  /* init.sql
CREATE TABLE task3(
  "Zone" varchar NULL,
  cnt integer NULL,
  mean_distance double precision NULL
);
*/
  case class task3FactsSchema(DOLocationID: String, trip_distance: String)
  val encoderFacts = org.apache.spark.sql.Encoders.product[task3FactsSchema]
  val task3FactsDS = taxiFactsDF
    .select("DOLocationID", "trip_distance")
    .as(encoderFacts)

  case class task3ZonesSchema(LocationID: String, Zone: String)

  val encoderZones = org.apache.spark.sql.Encoders.product[task3ZonesSchema]
  val task3ZonesDS = taxiZonesDF
    .select("LocationID", "Zone")
    .as(encoderZones)

  val task3 = task3FactsDS
    .filter(row => row.trip_distance != null && row.trip_distance.toFloat > 0)
    .groupBy(col("DOLocationID"))
    .agg(
      count(col("DOLocationID")) as "cnt",
      round(mean(col("trip_distance")), 2) as "mean_distance"
    )
    .join(task3ZonesDS, col("DOLocationID") === col("LocationID"), "inner")
    .select("Zone", "cnt", "mean_distance")
    .sort(col("cnt").desc, col("mean_distance").desc, col("Zone").desc)
  task3.cache()

  task3.take(10).foreach(println)

  val connectionProperties = new Properties()
  connectionProperties.put("user", user)
  connectionProperties.put("password", password)
  connectionProperties.put("driver", driver)
  task3
    .write
    .mode(SaveMode.Overwrite)
    .jdbc(url, "task3", connectionProperties)
  println(s"Postgres done")
}

