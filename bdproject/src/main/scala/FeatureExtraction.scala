import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{monotonically_increasing_id, regexp_replace, col}


class FeatureExtraction {

  def clean()(col: Column): Column = {

    var reg1 = regexp_replace(col, "\\' ", " ")
    reg1 = regexp_replace(reg1, "\\'\\w+", "")
    reg1 = regexp_replace(reg1, "e+", "e")
    reg1 = regexp_replace(reg1, "o+", "o")
    reg1 = regexp_replace(reg1, "a+", "a")
    reg1 = regexp_replace(reg1, "i+", "i")
    reg1 = regexp_replace(reg1, "y+", "y")
    reg1 = regexp_replace(reg1, "(?<!\\s)in", "ing")
    reg1
  }

  def normalize(datafr: DataFrame, column: String, idCol: String): DataFrame = {
    var norFrame = datafr.select(clean()(col(column)).as("normalized"))
    norFrame.show()

    norFrame = norFrame.withColumn("id", monotonically_increasing_id())

    norFrame

    //      val mydf = datafr.as("df1").join(norFrame.as("df2"), datafr(idCol) === norFrame("id"), "inner")
    //        .select("df1."+idCol,  "df2.normalized")
    //      mydf.withColumnRenamed("normalized", column)

  }


  def buildFeatureSelection(inputColumn: String): (RegexTokenizer, StopWordsRemover, HashingTF) = {
    val regexTokenizer = new RegexTokenizer()
      .setInputCol(inputColumn)
      .setOutputCol("words")
      .setPattern("(\\@\\w+)|[^\\W+']|(http\\:\\/\\/\\w+)|(https\\:\\/\\/\\w+)")

    val stopWords = new StopWordsRemover().
      setInputCol(regexTokenizer.getOutputCol).setOutputCol("cleanWords")

    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(regexTokenizer.getOutputCol)
      .setOutputCol("features")

    (regexTokenizer, stopWords, hashingTF)
  }
}
