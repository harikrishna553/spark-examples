package org.example

/**
 * Hello world!
 *
 */
import org.apache.spark.sql.SparkSession;

object App extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("FirstSparkApp")
    .getOrCreate();

  println("First SparkContext:")
  println("APP Name :"+spark.sparkContext.appName);
  println("Deploy Mode :"+spark.sparkContext.deployMode);
  println("Master :"+spark.sparkContext.master);
}
