/**
 * Exception Ladder Test in Scala
 * 
 * This script demonstrates how to use initCause in Scala to create
 * an exception ladder (chain of exceptions) and properly display it.
 */

object ExceptionLadderTest {
  
  // Custom exception classes to demonstrate the ladder
  class DatabaseException(message: String) extends Exception(message)
  class ConnectionException(message: String) extends Exception(message)
  class ConfigurationException(message: String) extends Exception(message)
  
  /**
   * Simulates a database operation that might fail
   */
  def performDatabaseOperation(): Unit = {
    try {
      // Simulate connecting to database
      connectToDatabase()
      println("Database operation completed successfully")
    } catch {
      case e: Exception =>
        // Create a new exception that wraps the original one
        val dbException = new DatabaseException("Failed to perform database operation")
        dbException.initCause(e)
        throw dbException
    }
  }
  
  /**
   * Simulates connecting to a database
   */
  def connectToDatabase(): Unit = {
    try {
      // Simulate loading configuration
      loadConfiguration()
      println("Connected to database successfully")
    } catch {
      case e: Exception =>
        // Create a new exception that wraps the original one
        val connException = new ConnectionException("Failed to connect to database")
        connException.initCause(e)
        throw connException
    }
  }
  
  /**
   * Simulates loading configuration
   */
  def loadConfiguration(): Unit = {
    // Simulate a configuration error
    val configException = new ConfigurationException("Configuration file not found or invalid")
    throw configException
  }
  
  /**
   * Prints the full exception ladder
   */
  def printExceptionLadder(exception: Throwable): Unit = {
    println("\n=== EXCEPTION LADDER ===")
    
    var currentException: Throwable = exception
    var level = 1
    
    while (currentException != null) {
      println(s"Level $level: ${currentException.getClass.getSimpleName} - ${currentException.getMessage}")
      
      // Print stack trace for this level
      println("Stack trace for this level:")
      currentException.getStackTrace.take(3).foreach(element => 
        println(s"    at ${element.getClassName}.${element.getMethodName}(${element.getFileName}:${element.getLineNumber})")
      )
      
      // Move to the cause
      currentException = currentException.getCause
      level += 1
      
      if (currentException != null) {
        println("\nCaused by:")
      }
    }
    
    println("=== END OF EXCEPTION LADDER ===")
  }
  
  /**
   * Alternative method to print exception ladder using built-in printStackTrace
   */
  def printFullStackTrace(exception: Throwable): Unit = {
    println("\n=== FULL STACK TRACE (BUILT-IN) ===")
    exception.printStackTrace()
    println("=== END OF FULL STACK TRACE ===")
  }
  
  /**
   * Main method to run the test
   */
  def main(args: Array[String]): Unit = {
    println("Starting Exception Ladder Test...")
    
    try {
      // This will trigger our exception ladder
      performDatabaseOperation()
    } catch {
      case e: Exception =>
        println("Caught exception at the top level")
        
        // Method 1: Custom exception ladder printing
        printExceptionLadder(e)
        
        // Method 2: Using built-in printStackTrace
        printFullStackTrace(e)
        
        // Method 3: Getting the root cause directly
        println("\n=== ROOT CAUSE ===")
        val rootCause = getRootCause(e)
        println(s"Root cause: ${rootCause.getClass.getSimpleName} - ${rootCause.getMessage}")
        println("=== END OF ROOT CAUSE ===")
    }
    
    println("\nTest completed.")
  }
  
  /**
   * Utility method to get the root cause of an exception
   */
  def getRootCause(throwable: Throwable): Throwable = {
    var rootCause = throwable
    while (rootCause.getCause != null) {
      rootCause = rootCause.getCause
    }
    rootCause
  }
}

// Run the main method
ExceptionLadderTest.main(Array())
