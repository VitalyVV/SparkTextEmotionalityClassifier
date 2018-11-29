import org.apache.spark.ml.Pipeline
import org.apache.spark._
import org.apache.spark.ml.classification.{LinearSVC, LogisticRegression}
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType


object Model {
  def main(args: Array[String]) {
    val config = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Test app")

    val session = SparkSession.builder()
      .config(config)
      .appName("test")
      .master("local")
      .getOrCreate()

    val training = session.read-----
      .format("csv")
      .option("header", "true")
      .load("hdfs://Sentiment/twitter/train.csv")

    val df = training.withColumn("Sentiment", training.col("Sentiment").cast(IntegerType))

    val Array(train, test) = df.randomSplit(Array[Double](0.7, 0.3))

    val dfs = train.toDF("ItemID", "label", "SentimentText")

    val lsvc = new LinearSVC()
      .setMaxIter(10)
      .setRegParam(0.1)

    val (stage1, stage2, stage3) = buildFeatureSelection("SentimentText")

    val pipeline = new Pipeline().setStages(Array(stage1, stage2, stage3, lsvc))

    val model = pipeline.fit(dfs)
    println("###" * 10)
    println(model)
    println("###" * 10)

    model.save("model")
    println("Model had been saved")
    session.stop()
  }

  def buildFeatureSelection(inputColumn: String): (RegexTokenizer, StopWordsRemover, HashingTF) = {
    val regexTokenizer = new RegexTokenizer()
      .setInputCol(inputColumn)
      .setOutputCol("words")
      .setPattern("(\\@\\w+)|\\W+")

    val stopWords = new StopWordsRemover().
      setInputCol(regexTokenizer.getOutputCol).setOutputCol("cleanWords")

    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(regexTokenizer.getOutputCol)
      .setOutputCol("features")

    (regexTokenizer, stopWords, hashingTF)
  }
}
