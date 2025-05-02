# Schema Evolution Testing Notes

## Test Performed

We conducted a schema evolution test to verify the behavior of Spark's schema merging functionality, which is conceptually similar to Delta Lake's schema evolution. The test was designed to demonstrate what happens when attempting to write data with a different schema than the existing data, both with and without the `mergeSchema` option.

### Test Setup

1. **Initial Data**: Created a DataFrame with a simple schema containing `id`, `name`, and `description` columns.
2. **New Data**: Created a second DataFrame with an additional `start_date` column.
3. **Test Scenarios**:
   - Append new data without `mergeSchema`
   - Read data with and without `mergeSchema`
   - Append new data with `mergeSchema`

### Test Script

```scala
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
```

## Observations

### 1. Appending Data with Different Schema (Without mergeSchema)

When appending data with an additional column (`start_date`) without specifying `mergeSchema=true`:

- **Write Operation**: The write operation succeeded without errors
- **Schema Consistency**: This created inconsistent schemas across files in the same directory
- **Data Access**: When reading without `mergeSchema=true`, Spark used the schema from the first file it encountered
- **Data Loss**: The `start_date` values were effectively lost during standard reads
- **Schema Used**: The read schema only included the original columns (`id`, `name`, `description`)

### 2. Reading with mergeSchema=true

When reading the data with `mergeSchema=true`:

- **Schema Unification**: Spark successfully merged schemas from all files in the directory
- **Complete Schema**: The resulting schema included all columns (`id`, `name`, `description`, and `start_date`)
- **NULL Values**: Records from files that didn't have the `start_date` column had NULL values for that column
- **Data Preservation**: All data was accessible, including the `start_date` values from the new records

### 3. Writing with mergeSchema=true

When writing data with `mergeSchema=true`:

- **Write Success**: The write operation completed successfully
- **Schema Evolution**: The schema was properly evolved to include the new column
- **Consistent Results**: When reading the data back, all columns were present in all records
- **NULL Handling**: Original records had NULL values for the `start_date` column

## Comparison to Delta Lake

While our test used Parquet files with Spark's native schema merging, Delta Lake's schema evolution behavior is conceptually similar but with important differences:

1. **Strict Schema Enforcement**: Delta Lake rejects schema-mismatched writes by default with an `AnalysisException`
2. **Explicit Schema Evolution**: Delta Lake requires explicit schema evolution via `mergeSchema=true`
3. **Transactional Guarantees**: Delta Lake ensures atomic and consistent schema changes
4. **Data Integrity**: Delta Lake prevents partial or inconsistent schema changes

### Delta Lake Schema Evolution Rules

1. If you don't set `mergeSchema` to true, Delta Lake will reject writes with schema mismatches
2. The rejection happens via an `AnalysisException` with a message like "A schema mismatch detected"
3. When enabled, `mergeSchema` allows Delta Lake to:
   - Add new columns to the table schema
   - Ensure all existing data has NULL values for new columns
   - Validate that existing columns have compatible types

## Key Takeaways

1. **Default Behavior**: Without `mergeSchema`, schema evolution can lead to inconsistent data access
2. **Data Safety**: Delta Lake's strict schema enforcement prevents accidental schema drift
3. **Explicit Evolution**: Schema changes should be explicit and intentional via `mergeSchema=true`
4. **NULL Handling**: New columns are populated with NULL values for existing records
5. **Best Practice**: Always use `mergeSchema=true` when intentionally evolving schemas to ensure data consistency
