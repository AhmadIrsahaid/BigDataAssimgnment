import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{File, PrintWriter}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("InvertedIndex")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val fileOne = sc.textFile("src/doc_1.txt").flatMap(line => line.split(" ")).map(word => (word, "doc_1.txt"))
    val fileTwo = sc.textFile("src/doc_2.txt").flatMap(line => line.split(" ")).map(word => (word, "doc_2.txt"))
    val fileThree = sc.textFile("src/doc_3.txt").flatMap(line => line.split(" ")).map(word => (word, "doc_3.txt"))

    val filesRDD = fileOne.union(fileTwo).union(fileThree)


    val wordFileCount = filesRDD.map(tuple => ((tuple._1, tuple._2), 1)) // tuple._1 = word ,, tuple._2 = filename
      .reduceByKey(_ + _)

    val wordToFileMap = wordFileCount.map(tuple => (tuple._1._1, (tuple._1._2, tuple._2)))
      .groupByKey()

    val wordCounter = wordToFileMap.map(tuple => {
      val totalWordCount = tuple._2.map(_._2).sum
      val fileName = tuple._2.map(x => s"${x._1} ").mkString(", ")
      s"${tuple._1}, ${totalWordCount},  ${fileName}"
    })

    val result = wordCounter.collect()

    val outputFile = new PrintWriter(new File("src/wholeInvertedIndex.txt"))

    result.foreach { line =>
      outputFile.println(line)
    }

    outputFile.close()

    result.foreach(println)
  }
}
