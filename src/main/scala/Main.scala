import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.embeddings._
import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkNLP.start()

    val df = spark.read.orc("data2.crc").toDF()

    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")
      .setCleanupMode("shrink")

    val tokenizer = new Tokenizer()
      .setInputCols(Array("document"))
      .setOutputCol("tokens")

    val embedding =  WordEmbeddingsModel.pretrained()
      .setInputCols(Array("document", "tokens"))
      .setOutputCol("word_embeddings")

    val posTagger = PerceptronModel.pretrained()
      .setInputCols(Array("document", "tokens"))
      .setOutputCol("pos")

    val nerTagger = NerCrfModel.pretrained()
      .setInputCols(Array("document", "tokens", "word_embeddings","pos"))
      .setOutputCol("ner")

    val pipeline = new Pipeline().setStages(Array(documentAssembler, tokenizer, embedding, posTagger, nerTagger))
    val model = pipeline.fit(df)

    val result = model.transform(df)

    val analysis = result.select(
      expr("transform(pos, x -> x.result) as pos_result"),
      expr("transform(ner, x -> x.result) as ner_result")
    )

    analysis.show(truncate = false)


    val flattened = result.select(
      expr("explode(arrays_zip(tokens.result, pos.result, ner.result)) as Tokens")
    ).select(
      col("Tokens.0").as("token"),
      col("Tokens.1").as("pos"),
      col("Tokens.2").as("ner")
    )

    flattened.show(false)

    flattened.groupBy("pos", "ner").count().orderBy(desc(count)).show(truncate = false)


  }
}
