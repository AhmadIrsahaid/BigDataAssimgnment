import org.apache.spark.sql.{SaveMode, SparkSession}

object sparkMongo {
  def main(args: Array[String]): Unit = {
    val mongoLink: String = "mongodb://localhost:27017/test2.likes"
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("readFromFileUsingMongo")
      .config("spark.mongodb.read.connection.uri", mongoLink)
      .config("spark.mongodb.write.connection.uri", mongoLink)
      .getOrCreate()
    val dataFrameFromFile = spark.read.text("src/wholeInvertedIndex.txt")
    try {
      val df = spark.read.format("mongodb").option("collection", "likes").load()
      dataFrameFromFile.write.format("mongodb")
        .mode(SaveMode.Append)
        .option("database", "test12")
        .option("collection", "dictionary")
        .save()
      dataFrameFromFile.show()
      println("Data successfully written to MongoDB.")
    } catch {
      case e: Exception =>
        println(s"Error reading from MongoDB: ${e.getMessage}")
        sys.exit(1)
    }
    spark.stop()


  }

}
