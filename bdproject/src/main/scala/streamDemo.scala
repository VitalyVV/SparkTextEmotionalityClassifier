import com.github.catalystcode.fortis.spark.streaming.rss.RSSInputDStream
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark._
import org.apache.spark.ml.classification.{LinearSVC, LogisticRegression}
import org.apache.spark.sql.functions.{monotonically_increasing_id, regexp_replace, col}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Column
import org.jsoup.Jsoup

object streamDemo {
  def main(args: Array[String]) {
    val durationSeconds = 90
    val conf = new SparkConf()
      .setAppName("RSS Spark Application")
      .setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))
    sc.setLogLevel("ERROR")
	
    val extractor = new FeatureExtraction()

    val urls = Seq("https://queryfeed.net/tw?token=5bfec0d2-4657-4d2a-98d0-69f3584dc3b3&q=%23weather")
    val stream = new RSSInputDStream(urls, Map[String, String](
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds)
    val model = PipelineModel.read.load("model")
    stream.foreachRDD(rdd=>{
      if (!rdd.isEmpty()){
        val spark = SparkSession.builder()
          .appName(sc.appName)
          .getOrCreate()
        import spark.sqlContext.implicits._
        var df = rdd.toDF()
        df.filter(df("description")("value").contains("lang=\"en\""))
        val noHTML = udf { s: String => Jsoup.parse(s).body().text() }
        df = df.withColumn("tweet", noHTML(df("description")("value")))
        df.select("tweet")
          .show()

        df = df.withColumn("ItemID", monotonically_increasing_id())
        val normalizeddf = extractor.normalize(df, "tweet", "ItemID")
        df = df.as("df1")
          .join(normalizeddf.as("df2"), df("ItemID") === normalizeddf("id"), "inner")
          .select("df1."+"ItemID", "df1.uri", "df1.title","df1.authors", "df2.normalized")
        df = df.withColumnRenamed("normalized", "SentimentText")

        df.show()
        val res = model.transform(df)
        res.write.mode("append").csv("hdfs://output.csv")
      }


    })

    // run forever
    ssc.start()
    ssc.awaitTermination()
  }
}
