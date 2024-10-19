ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "AssignmentOneBigData"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.0",
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
   "org.mongodb.scala" %% "mongo-scala-driver" % "4.6.0"
)

