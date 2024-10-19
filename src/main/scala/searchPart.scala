import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object searchPart {
  def main(args: Array[String]): Unit = {
    val mongoLink: String = "mongodb://localhost:27017/test12.dictionary"

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MongoDBSearchApp")
      .config("spark.mongodb.read.connection.uri", mongoLink)
      .config("spark.mongodb.write.connection.uri", mongoLink)
      .getOrCreate()

    val dataFrame = spark.read
      .format("mongodb")
      .option("database", "test12")
      .option("collection", "dictionary")
      .load()

    println("Please enter your query: ")
    val wordInput = scala.io.StdIn.readLine().trim.toLowerCase

    val searchResults = dataFrame
      .filter(lower(col("value")).contains(wordInput))
      .select("value")

    var fileName = Set[String]()

    if (searchResults.count() > 0) {
      println(s"Documents matching query '$wordInput':")
      searchResults.collect().foreach { row =>
        val valueField = row.getAs[String]("value")
        val fileNames = valueField.split(",").filter(_.trim.matches(".*\\.txt")).map(_.trim)
        fileName ++= fileNames
      }
      println(s"The file that contain the word ${wordInput} is :  ${fileName.mkString(", ")}")

    } else {
      println(s"The word '${wordInput}' is not found")
    }

    spark.stop()

  }
}
