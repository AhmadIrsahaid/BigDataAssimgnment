import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{File, PrintWriter, BufferedReader, FileReader}
import org.apache.spark.sql.SparkSession


object Main {
  def main(args: Array[String]): Unit = {
    // Spark Configuration
    val conf = new SparkConf().setMaster("local[*]").setAppName("orders")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // MongoDB Configuration
    val mongoLink: String = "mongodb://localhost:27017/config.posts"
    val spark = SparkSession.builder()
      .master("local")
      .appName("readFromFileUsingMongo")
      .config("spark.mongodb.connection.uri", mongoLink)
      .getOrCreate()

    // Load DataFrame from MongoDB
    try {
      val df = spark.read.format("mongodb").option("collection", "posts").load()
      df.show()
    }catch {
      case e: Exception =>
        println(s"Error reading from MongoDB: ${e.getMessage}")
        sc.stop()
        sys.exit(1)
    }

    val fileOne = sc.textFile("src/doc_1.txt").map(line => ("doc_1.txt", line))
    val fileTwo = sc.textFile("src/doc_2.txt").map(line => ("doc_2.txt", line))
    val fileThree = sc.textFile("src/doc_3.txt").map(line => ("doc_3.txt", line))

    val FilesRDD = fileOne.union(fileTwo).union(fileThree)

    println("Please enter a word: ")
    val wordInput = scala.io.StdIn.readLine().trim

    val occurrencesPerFile = FilesRDD
      .flatMap(tuple => {
        val fileName = tuple._1
        val line = tuple._2
        line.split("\\s+").map(word => (fileName, word))
      })
      .filter(tuple => tuple._2.equalsIgnoreCase(wordInput))
      .map(tuple => (tuple._1, 1))
      .reduceByKey(_ + _)

    val results = occurrencesPerFile.collect()

    val outputLines = results.map(tuple => {
      val fileName = tuple._1
      val count = tuple._2
      s"$wordInput, $count, $fileName"
    })

    if (outputLines.isEmpty) {
      println(s"Sorry, the word '$wordInput' was not found in any file.")
      sys.exit()
    }

    val homeDir = System.getProperty("user.home")
    val outputFilePath = s"$homeDir/wholeInvertedIndex.txt"

    val writer = new PrintWriter(new File(outputFilePath))
    try {
      outputLines.foreach(line => writer.println(line))
    } finally {
      writer.close()
    }
    println("Content of the output file:")
    println("the word , count(word) , file")
    val fileReader = new BufferedReader(new FileReader(outputFilePath))
    var line: String = null
    while ({ line = fileReader.readLine(); line != null }) {
      println(line)
    }
    fileReader.close()


    sc.stop()
  }
}
