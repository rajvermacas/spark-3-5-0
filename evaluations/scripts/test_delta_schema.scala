import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Create a Delta table with initial schema
println("Creating initial Delta table...")

// Create a DataFrame with initial schema
val initialData = Seq(
  (1, "Record 1", "Description 1"),
  (2, "Record 2", "Description 2")
)

val initialDF = spark.createDataFrame(initialData)
  .toDF("id", "name", "description")

// Define Delta table path
val deltaTablePath = "src/main/resources/delta_test_table"

// Write initial data to Delta table
println("Writing initial data to Delta table...")
initialDF.write
  .format("delta")
  .mode("overwrite")
  .save(deltaTablePath)

// Read the Delta table to verify
println("Reading Delta table after initial write:")
val readDF = spark.read.format("delta").load(deltaTablePath)
readDF.show(false)
println(s"Schema after initial write: ${readDF.schema.treeString}")

// Create a DataFrame with an additional column
println("\nCreating DataFrame with additional column...")
val newData = Seq(
  (3, "Record 3", "Description 3", "2023-01-01"),
  (4, "Record 4", "Description 4", "2023-01-02")
)

val newDF = spark.createDataFrame(newData)
  .toDF("id", "name", "description", "start_date")

println("New DataFrame schema:")
println(newDF.schema.treeString)

// Try to append without mergeSchema
println("\nTrying to append without mergeSchema...")
try {
  newDF.write
    .format("delta")
    .mode("append")
    .save(deltaTablePath)
  
  println("Write succeeded without mergeSchema (unexpected!)")
} catch {
  case e: Exception => 
    println(s"Write failed as expected: ${e.getMessage.split("\n").head}")
}

// Read the Delta table to verify it hasn't changed
println("\nReading Delta table after failed append:")
val readDF2 = spark.read.format("delta").load(deltaTablePath)
readDF2.show(false)

// Now try with mergeSchema=true
println("\nTrying to append with mergeSchema=true...")
try {
  newDF.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(deltaTablePath)
  
  println("Write succeeded with mergeSchema as expected")
} catch {
  case e: Exception => 
    println(s"Write failed unexpectedly: ${e.getMessage.split("\n").head}")
}

// Read the Delta table to verify schema evolution
println("\nReading Delta table after successful append with schema evolution:")
val readDF3 = spark.read.format("delta").load(deltaTablePath)
readDF3.show(false)
println(s"Final schema after schema evolution: ${readDF3.schema.treeString}")

println("\nTest completed successfully")
