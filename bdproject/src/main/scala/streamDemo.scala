import com.github.catalystcode.fortis.spark.streaming.rss.RSSInputDStream
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
    val durationSeconds = 10
    //val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))
    sc.setLogLevel("ERROR")

    val urls = Seq("https://queryfeed.net/tw?q=%23fantasticbeasts2")
    val stream = new RSSInputDStream(urls, Map[String, String](
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds)
    stream.foreachRDD(rdd=>{
      val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
      import spark.sqlContext.implicits._
      var df = rdd.toDF()
      df.show()
      val descrCol = df("description")
      val noHTML = udf { s: String => Jsoup.parse(s) }

      df.withColumn("tweet", noHTML(df("description")))
    })

    // run forever
    ssc.start()
    ssc.awaitTermination()
  }
}
