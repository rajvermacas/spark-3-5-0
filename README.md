# Scala Spark Maven Project

## Project Overview
This is a sample Scala Spark project configured with Maven.

## Prerequisites
- Java 11+
- Maven 3.6+
- Scala 2.13

## Project Configuration
- Scala Version: 2.13
- Spark Version: 3.5.0
- Build Tool: Maven

## Building the Project
```bash
mvn clean package
```

## Running the Application
```bash
mvn exec:java -Dexec.mainClass="com.example.SparkApp"
```

## Project Structure
- `src/main/scala`: Main Scala source code
- `src/test/scala`: Test cases
- `pom.xml`: Maven project configuration
