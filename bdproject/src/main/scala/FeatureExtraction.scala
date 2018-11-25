// $example on$
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, Tokenizer}
// $example off$
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object FeatureExtraction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FeatureExtraction")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //
    val hdfs_folder = "hdfs://<namenode>/home/user/text_data"

//    val inputDataFrame = sqlContext.createDataFrame(Seq(
//      (0, "Hi I heard about Spark"),
//      (1, "I wish Java could use case classes"),
//      (2, "Logistic,regression,models,are,neat")
//    )).toDF("label", "tweet")

    val inputDataFrame = loadFileModel(hdfs_folder+"some_file",sc)

    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val model = new LogisticRegression() //another model should be used
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, model))


    //TODO model
// maybe its worth adding idf too - can see after getting training
    sc.stop()
  }

  def loadFileModel(path:String, sc:SparkContext) = {

    //returns DataFrame from txt file in format label;text;class
  }
}
