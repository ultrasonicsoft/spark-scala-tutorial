import org.apache.spark.sql.SparkSession

object WordCountApp{
  def main(args: Array[String]): Unit ={
    val readmeFileName = "/Users/balramchavan/Desktop/softwares/spark-2.4.4-bin-hadoop2.7/README.md"
    val sparkSession = SparkSession.builder().master("local[4]").appName("Word Count").getOrCreate()
    val readmeFile = sparkSession.sparkContext.textFile(readmeFileName)
    val lineCount = readmeFile.count()
    println(s"Line count: ${lineCount}");

    val wordCount = readmeFile.flatMap(line=>line.split(" ")).count()

//    wordCount.saveAsTextFile("/Users/balramchavan/Desktop/softwares/spark-2.4.4-bin-hadoop2.7/word-count.txt")
    println(s"Word count: ${wordCount}")
    sparkSession.stop()
//    val spark = SparkSession.builder.master("local[4]").appName("Simple Application").getOrCreate()

  }
}
