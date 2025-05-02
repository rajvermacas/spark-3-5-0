import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Define the schema for phases
val phaseSchema = StructType(Seq(
  StructField("id", LongType, nullable = false),
  StructField("name", StringType, nullable = false),
  StructField("description", StringType, nullable = false)
))

// Define the main schema
val schema = StructType(Seq(
  StructField("id", LongType, nullable = false),
  StructField("phases", ArrayType(phaseSchema), nullable = false),
  StructField("name", StringType, nullable = false),
  StructField("description", StringType, nullable = false),
  StructField("start_date", StringType, nullable = false)
))

// Read JSON file with custom schema
val rawDF = spark.read
  .option("multiline", "true")
  .schema(schema)  // Apply the custom schema
  .json("src/main/resources/dummy_data.json")

// Select specific columns
val filteredDF = rawDF.select(
  "id",
  "phases",
  "name",
  "description",
  "start_date"
)

// Show the first row
println("First row of the JSON file:")
filteredDF.show(false)

// Access phases with explicit schema
println("Extracting phases from first record:")
filteredDF.select($"phases").take(1).foreach { row =>
  val phases = row.getSeq[Row](0)
  phases.foreach { phase =>
    println(s"Phase: ${phase.getAs[String]("name")}, Description: ${phase.getAs[String]("description")}")
  }
}

println("Spark application completed successfully")
