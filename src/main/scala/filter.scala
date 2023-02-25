//package filter_pack
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.expressions.UserDefinedFunction
import sys.process._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
//import org.apache.spark.sql.functions.col
//import java.net.{URL, URLDecoder}
//import scala.util.Try
import org.apache.spark.sql.types._
import java.math._
import org.json4s.JsonDSL._


object filter {
  //Внутри класса
  def main(args: Array[String]): Unit = {
    // поднимаем Spark сессию
    val spark: SparkSession = SparkSession.builder()
        //spark.conf.set("spark.sql.session.timeZone", "UTC")
      .appName("filter-rinatkaa")
      .getOrCreate()
 //   val sc = spark.sparkContext
    spark.conf.set("spark.sql.session.timeZone", "UTC")
// Читаем в DF из кафки все сообщения с нулевым офсетом
    val df = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", "lab04_input_data")
      .option("includeHeaders", "true")
      //оффсет
      .option("startingOffsets", "earliest")
      //   .option("kafka.group.id", s"consumer-group-for-this-job")
      .load()

      // Схема для чтения/маппинга сообщений
      val schema = new StructType()
        .add("event_type", StringType, true)
        .add("category", StringType, true)
        .add("item_id", StringType, true)
        .add("item_price", StringType, true)
        .add("uid", StringType, true)
        .add("timestamp", LongType, true)

    //UDF для конвертации из последовательности байт в текст
    val toStringDF = udf((x: Array[Byte]) => new String(x))
    // читаем в NewDF + колонку в с
    val newDF = df.withColumn("value", toStringDF(df("value"))).select("value")
    //.printSchema()
    //применяем схему
    val value_df = newDF.withColumn("value", from_json(col("value"), schema))
    value_df.printSchema()
    //уплощаем, добывая из структуры все вложенные поля
    var explodedDf2 = value_df.select("value.*", "*")
    //конвертим unixtimestamp в Date
    val ts1 = explodedDf2.withColumn("date", from_unixtime(col("timestamp") / 1000, "yyyyMMdd")).as("date")
      .withColumn("p_date", col("date"))
    //.select("category", "date")
    //.show(5)
   //убираем ненужное поле
    val df_finish = ts1.drop("value") //.show(5)
    //val df_filtered = df_finish.filter($"event_type" === "buy" && $"uid" === "40b29579-e845-45c0-a34d-03630d296a81")
    //.show(5, false)
    //.count()
    //готовим два DF по условиям фильтрации
    val df_filtered_view = df_finish.filter(col("event_type") === "view")
    val df_filtered_buy = df_finish.filter(col("event_type") === "buy")
    //df_filtered_buy.count()
    //пишем в файлы на HDFS
    df_filtered_view.write
      .mode("overwrite")
      .partitionBy("p_date")
      .json("visits/view")
    df_filtered_buy.write
      .mode("overwrite")
      .partitionBy("p_date")
      .json("visits/buy")
  }

  println("Hello")
  //println("""hadoop fs -ls /user/rinat.gareev/visits/view".!!""")



}