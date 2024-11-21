package com.example

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkApp {
  // Define the schema for phases
  private val phaseSchema = StructType(Seq(
    StructField("id", LongType, nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("description", StringType, nullable = false)
  ))

  // Define the main schema
  private val schema = StructType(Seq(
    StructField("id", LongType, nullable = false),
    StructField("phases", ArrayType(phaseSchema), nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("description", StringType, nullable = false),
    StructField("start_date", StringType, nullable = false)
  ))

  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Scala Spark Example")
      .config("spark.master", "local")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    import spark.implicits._

    // Read JSON file with custom schema
    val rawDF = spark.read
      .option("multiline", "true")
      .schema(schema)  // Apply the custom schema
      .json("src/main/resources/dummy.json")
    
    // Example: If you need to filter before converting to DataRecord
    // val filteredDF = rawDF.filter($"id" <= 2)  // Any filtering operations
    val filteredDF = rawDF  // Any filtering operations
    
    // Show the first row
    println("First row of the JSON file:")
    // filteredDF.show(1, false)
    filteredDF.show(false)

    // Example of accessing phases with explicit schema
    println("Extracting phases from first record:")
    filteredDF.select($"phases").take(1).foreach { row =>
      val phases = row.getSeq[Row](0)
      phases.foreach { phase =>
        println(s"Phase: ${phase.getAs[String]("name")}, Description: ${phase.getAs[String]("description")}")
      }
    }

    // Create a new row using the schema
    val newPhase = Row(3L, "Phase 3", "Description of Phase 3")
    val newRow = Row(
      2L,                     // id
      Seq(newPhase),         // phases
      "john",                // name
      "new row",            // description
      "2023-02-01"          // start_date
    )
    
    // Convert new row to DataFrame using the schema
    val newRowDF = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(newRow)),
      schema
    )
    
    persistToJson(spark, filteredDF, newRowDF)

    spark.stop()
  }

  def persistToJson(spark: SparkSession, existingDF: DataFrame, newRowDF: DataFrame): Unit = {
    // First, read the existing JSON file to get current data
    val currentDF = spark.read
      .option("multiline", "true")
      .schema(schema)
      .json("src/main/resources/dummy.json")
    
    // Union all DataFrames: current data and new row, then remove duplicates
    val updatedDF = currentDF.union(newRowDF).distinct()
    
    // Create a temporary directory for the new JSON
    val tempDir = "temp_json_output"
    
    // Write to temporary location
    updatedDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("multiline", true)
      .json(tempDir)
      
    // Move the generated JSON file to the target location
    import org.apache.hadoop.fs.{FileSystem, Path}
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    
    // Find the generated JSON file in temp directory
    val tempJsonFile = fs.globStatus(new Path(s"$tempDir/part-*.json"))(0).getPath()
    val targetPath = new Path("src/main/resources/dummy.json")
    
    // Delete the existing file if it exists
    val file = new java.io.File(targetPath.toString)
    if (file.exists()) {
      file.delete()
    }
    
    // Copy the file and delete temp directory
    fs.rename(tempJsonFile, targetPath) // Move new file
    fs.delete(new Path(tempDir), true) // Clean up temp directory
      
    println("New row has been appended to the JSON file")
  }

  // Helper method to create a phase row
  def createPhase(id: Long, name: String, description: String): Row = {
    Row(id, name, description)
  }

  // Helper method to create a data record row
  def createDataRecord(id: Long, phases: Seq[Row], name: String, description: String, startDate: String): Row = {
    Row(id, phases, name, description, startDate)
  }
}
