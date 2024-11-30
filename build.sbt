ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.13"

lazy val root = (project in file("."))
  .settings(
    name := "NLP4"
  )
// Spark NLP Dependency
libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.5.1"

// Apache Spark Dependencies (Ensure compatibility with Spark NLP version)
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3" // Use 3.4.1 if 3.5.3 is not supported
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" // Align version
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.1" // Optional, only if needed

// Add resolver for John Snow Labs
resolvers += "John Snow Labs" at "https://repo.johnsnowlabs.com/public/"
