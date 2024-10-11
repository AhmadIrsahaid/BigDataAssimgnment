import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{File, PrintWriter, BufferedReader, FileReader}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("orders")
    val sc = new SparkContext(conf)
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)
    sc.setLogLevel("ERROR")

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
        line.split(" ").map(word => (fileName, word))
      })
      .filter(tuple => tuple._2.equalsIgnoreCase(wordInput))
      .map(tuple => (tuple._1, 1))
      .reduceByKey((x, y) => x + y)

    val results = occurrencesPerFile.collect()

    val outputLines = results.map(tuple => {
      val fileName = tuple._1
      val count = tuple._2
      s"$wordInput, $count, $fileName" // Format: word, wordCount, fileName
    })


    val homeDir = System.getProperty("user.home")
    val outputFilePath = s"$homeDir/wholeInvertedIndex.txt"

    val writer = new PrintWriter(new File(outputFilePath))

    try {
      outputLines.foreach(line => writer.println(line))
    } finally {
      writer.close()
    }

    println("Content of the output file:")
    val fileReader = new BufferedReader(new FileReader(outputFilePath))
    var line: String = null
    while ({ line = fileReader.readLine(); line != null }) {
      println(line)
    }
    fileReader.close() 

    // Stop the Spark context
    sc.stop()
  }
}
