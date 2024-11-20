package com.example

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkAppTest extends AnyFlatSpec with Matchers {
  "SparkApp" should "create a DataFrame" in {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Spark Test")
      .master("local[*]")
      .getOrCreate()

    // Sample data
    val data = Seq(("Alice", 25), ("Bob", 30), ("Charlie", 35))
    val df = spark.createDataFrame(data).toDF("name", "age")

    // Assertions
    df.count() shouldBe 3
    df.columns shouldBe Array("name", "age")

    // Stop Spark Session
    spark.stop()
  }
}
