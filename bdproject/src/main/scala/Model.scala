import org.apache.spark.ml.Pipeline
import org.apache.spark._
import org.apache.spark.ml.classification.{LinearSVC, LogisticRegression}
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.monotonically_increasing_id


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

    val training = session.read
      .format("csv")
      .option("header", "true")
      .load("hdfs://Sentiment/twitter/train.csv")

    val df = training.withColumn("Sentiment", training.col("Sentiment").cast(IntegerType))

    val Array(train, test) = df.randomSplit(Array[Double](0.7, 0.3))

    val extractor = new FeatureExtraction()
//     val dfs = normalize(train.toDF("ItemID", "label", "SentimentText"), "SentimentText", 
//       "ItemID", "label")

    val trainData = train.toDF("ItemID", "label", "SentimentText")
    var dfs = extractor.normalize(trainData, "SentimentText",  "ItemID")
    
    dfs = trainData.as("df1").join(dfs.as("df2"), trainData("ItemID") === dfs ("id"), "inner")
      .select("df1."+"ItemID", "df1."+"label", "df2.normalized")
    dfs = dfs.withColumnRenamed("normalized", "SentimentText")
    
    val lsvc = new LinearSVC()
      .setMaxIter(10)
      .setRegParam(0.1)

    val (stage1, stage2, stage3) = extractor.buildFeatureSelection("SentimentText")

    val pipeline = new Pipeline().setStages(Array(stage1, stage2, stage3, lsvc))

    val model = pipeline.fit(dfs)
    println("###" * 10)
    println(model)
    println("###" * 10)

    model.save("model")
    println("Model had been saved")
    session.stop()
  }
  
//   def clean()(col:Column): Column = {

//     var reg1 = regexp_replace(col,"\\' ", " ")
//     reg1 = regexp_replace(reg1,"\\'\\w+", "")
//     reg1 = regexp_replace(reg1, "e+", "e")
//     reg1 = regexp_replace(reg1, "o+", "o")
//     reg1 = regexp_replace(reg1, "a+", "a")
//     reg1 = regexp_replace(reg1, "i+", "i")
//     reg1 = regexp_replace(reg1, "y+", "y")
//     reg1 = regexp_replace(reg1, "(?<!\\s)in", "ing")
//     reg1
//   }

//   def buildFeatureSelection(inputColumn: String): (RegexTokenizer, StopWordsRemover, HashingTF) = {
//     val regexTokenizer = new RegexTokenizer()
//       .setInputCol(inputColumn)
//       .setOutputCol("words")
//       .setPattern("(\\@\\w+)|\\W+|(http\\:\\/\\/\\w+)|(https\\:\\/\\/\\w+)")

//     val stopWords = new StopWordsRemover().
//       setInputCol(regexTokenizer.getOutputCol).setOutputCol("cleanWords")

//     val hashingTF = new HashingTF()
//       .setNumFeatures(1000)
//       .setInputCol(regexTokenizer.getOutputCol)
//       .setOutputCol("features")


//     (regexTokenizer, stopWords, hashingTF)
//   }
  
//   def normalize(datafr: DataFrame, column: String, idCol: String, labelCol:String): DataFrame = {
//     var norFrame = datafr.select(clean()(col(column)).as("normalized"))
//     norFrame.show()

//     norFrame = norFrame.withColumn("id", monotonically_increasing_id())

//     val mydf = datafr.as("df1").join(norFrame.as("df2"), datafr(idCol) === norFrame("id"), "inner")
//       .select("df1."+idCol, "df1."+labelCol, "df2.normalized")

//     mydf.withColumnRenamed("normalized", column)

//   }
}
