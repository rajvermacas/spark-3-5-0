import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Create a test directory for our schema evolution test
val testDir = "src/main/resources/schema_test"

// Create a DataFrame with initial schema
println("Creating initial DataFrame...")
val initialData = Seq(
  (1, "Record 1", "Description 1"),
  (2, "Record 2", "Description 2")
)

val initialDF = spark.createDataFrame(initialData)
  .toDF("id", "name", "description")

// Show the initial schema
println("Initial DataFrame schema:")
println(initialDF.schema.treeString)

// Write initial data to Parquet
println("\nWriting initial data to Parquet...")
initialDF.write
  .mode("overwrite")
  .parquet(testDir)

// Read the Parquet file to verify
println("Reading Parquet file after initial write:")
val readDF = spark.read.parquet(testDir)
readDF.show(false)

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
    .mode("append")
    .parquet(testDir)
  
  println("Write succeeded without mergeSchema (this will cause schema mismatch when reading)")
} catch {
  case e: Exception => 
    println(s"Write failed: ${e.getMessage.split("\n").head}")
}

// Read the Parquet file after append without mergeSchema
println("\nReading Parquet file after append without mergeSchema:")
try {
  val readDF2 = spark.read.parquet(testDir)
  readDF2.show(false)
  println("Schema after append without mergeSchema:")
  println(readDF2.schema.treeString)
} catch {
  case e: Exception => 
    println(s"Read failed: ${e.getMessage.split("\n").head}")
}

// Now try with mergeSchema=true for reading
println("\nReading with mergeSchema=true:")
try {
  val readDF3 = spark.read
    .option("mergeSchema", "true")
    .parquet(testDir)
  readDF3.show(false)
  println("Schema after reading with mergeSchema=true:")
  println(readDF3.schema.treeString)
} catch {
  case e: Exception => 
    println(s"Read failed: ${e.getMessage.split("\n").head}")
}

// Clean up and start fresh for a proper test with mergeSchema during write
println("\n\n===== Testing mergeSchema during write =====")

// Create a fresh test directory
val testDir2 = "src/main/resources/schema_test2"

// Write initial data
println("Writing initial data...")
initialDF.write
  .mode("overwrite")
  .parquet(testDir2)

// Read to verify
val verifyDF = spark.read.parquet(testDir2)
println("Initial data:")
verifyDF.show(false)

// Now append with mergeSchema
println("\nAppending with mergeSchema=true...")
try {
  newDF.write
    .mode("append")
    .option("mergeSchema", "true")
    .parquet(testDir2)
  
  println("Write succeeded with mergeSchema")
} catch {
  case e: Exception => 
    println(s"Write failed: ${e.getMessage.split("\n").head}")
}

// Read to verify schema evolution
println("\nReading after append with mergeSchema:")
try {
  val finalDF = spark.read.parquet(testDir2)
  finalDF.show(false)
  println("Final schema after append with mergeSchema:")
  println(finalDF.schema.treeString)
} catch {
  case e: Exception => 
    println(s"Read failed: ${e.getMessage.split("\n").head}")
}

println("\nTest completed successfully")
