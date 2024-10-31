import breeze.util.BloomFilter
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Bloom Filter Check")
      .master("local[*]")
      .getOrCreate()

    val FileOne = spark.sparkContext.textFile("src/doc_1.txt")
    val FileTwo = spark.sparkContext.textFile("src/doc_2.txt")
    val FileThree = spark.sparkContext.textFile("src/doc_3.txt")

    val wordsRDD1 = FileOne.flatMap(line => line.split("\\W+"))
    val wordsRDD2 = FileTwo.flatMap(line => line.split("\\W+"))
    val wordsRDD3 = FileThree.flatMap(line => line.split("\\W+"))

    val numOfExpectedItems = 10000
    val falsePositiveRate = 0.01

    val bloomFilter1 = BloomFilter.optimallySized[String](numOfExpectedItems, falsePositiveRate)
    val bloomFilter2 = BloomFilter.optimallySized[String](numOfExpectedItems, falsePositiveRate)
    val bloomFilter3 = BloomFilter.optimallySized[String](numOfExpectedItems, falsePositiveRate)

    wordsRDD1.foreach(word => bloomFilter1 += word)
    wordsRDD2.foreach(word => bloomFilter2 += word)
    wordsRDD3.foreach(word => bloomFilter3 += word)

    def checkWord(word: String, bloomFilter: BloomFilter[String], wordsRDD: org.apache.spark.rdd.RDD[String]): Unit = {
      val inBloomFilter = bloomFilter.contains(word)
      val inRDD = wordsRDD.collect().contains(word)
      println(s"Word '$word': In Bloom Filter = $inBloomFilter, In Actual RDD = $inRDD")
    }


    def calculateErrorRate(bloomFilter: BloomFilter[String], wordsRDD: org.apache.spark.rdd.RDD[String]): Double = {
      val allWords = wordsRDD.distinct.collect()
      val falsePositives = allWords.count(word => bloomFilter.contains(word) && !wordsRDD.collect().contains(word))
      val totalWords = allWords.length
      falsePositives.toDouble / totalWords
    }


    val wordToCheck = "adventurous"
    checkWord(wordToCheck, bloomFilter1, wordsRDD1)
    checkWord(wordToCheck, bloomFilter2, wordsRDD2)
    checkWord(wordToCheck, bloomFilter3, wordsRDD3)

    val errorRate1 = calculateErrorRate(bloomFilter1, wordsRDD1)
    val errorRate2 = calculateErrorRate(bloomFilter2, wordsRDD2)
    val errorRate3 = calculateErrorRate(bloomFilter3, wordsRDD3)

    println(s"Error Rate for Document 1: $errorRate1")
    println(s"Error Rate for Document 2: $errorRate2")
    println(s"Error Rate for Document 3: $errorRate3")

    spark.stop()
  }
}
