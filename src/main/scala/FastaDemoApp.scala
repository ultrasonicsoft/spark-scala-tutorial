import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.rdd.ADAMContext

object FastaDemoApp {
  def main(args: Array[String]): Unit ={
    val sparkSession = SparkSession.builder().master("local[4]").getOrCreate()

    val ac = new ADAMContext(sparkSession.sparkContext)
    val sequences = ac.loadFastaDna("/Users/balramchavan/Desktop/softwares/spark-2.4.4-bin-hadoop2.7/test-data.fasta")
    sequences.toDF().show(10)
//    sequences.toDF().filter(row=>row.)
//    val fastaFile = sparkSession.sparkContext.textFile("/Users/balramchavan/Desktop/softwares/spark-2.4.4-bin-hadoop2.7/test-data.fasta")
//    fastaFile.filter(line=>line.startsWith(">")==false).take(5).sortBy(line=>line.length).zipWithIndex.foreach { case (element, index)=>{
//      println(s"Read[${index}]: ${element.length}")
//    }}

//    val lineCount = fastaFile.count()
//    println(s"lineCount: ${lineCount}")

//    val readsCount = fastaFile.filter(line=>line.startsWith(">")).count()
//    println(s"read count: ${readsCount}")

//    val firstRead = fastaFile.take(2).filter(line=>line.startsWith(">") == false).map(line=>( line.length))
//    val firstRead = fastaFile.take(2).filter(line=>line.startsWith(">") == false)
//    firstRead.foreach(println)
//    println(s"Length: ${firstRead.length}")
//    val readLength = firstRead.flatMap(line=>line.split("").map(char=>(char,1)))

//    val readLenghts = fastaFile.flatMap(line=>line.split("")).map(word => (word, 1)).reduceByKey(_ + _)

//    println(s"read length: ${readLenghts.collect().for}")
    //    fastaFile.take(1).foreach(println)
    sparkSession.stop()
  }
}