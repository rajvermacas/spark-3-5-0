package com.example

import org.apache.spark.sql.SparkSession

object SparkApp {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Scala Spark Example")
      .config("spark.master", "local")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    // Sample data processing
    val data = Seq(("Alice", 25), ("Bob", 30), ("Charlie", 35))
    val df = spark.createDataFrame(data).toDF("name", "age")
    
    // Show the DataFrame
    df.show()

    // Stop Spark Session
    spark.stop()
  }
}
