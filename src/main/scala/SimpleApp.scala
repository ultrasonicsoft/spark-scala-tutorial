import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/Users/balramchavan/Desktop/softwares/spark-2.4.4-bin-hadoop2.7/README.md" // Should be some file on your system
    printf("jai ganesh!!")
    val spark = SparkSession.builder.master("local[4]").appName("Simple Application").getOrCreate()
    val textFile = spark.read.textFile(logFile)
    val lineCount = textFile.count()
    println(s"Total lines in file are: $lineCount")
    println(s"${textFile.first()}")
    val logData = spark.read.textFile(logFile)
    val emptyLines = textFile.filter(line=> line.startsWith("a")).count()
    println(s"Empty lines: ${emptyLines}")
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

//    var accumCount = spark.sparkContext.longAccumulator("count")
//    spark.sparkContext.parallelize(Array(1,2,3,4,5)).foreach(x=> accumCount.add(x))
//    println(s"Accumulator value: ${accumCount.value}")

    val myVectorAcc = new VectorAccumulatorV2
    spark.sparkContext.register(myVectorAcc, "MyVectorAcc1")

    val newVector = new MyVector()
    newVector.setAge(30)

    myVectorAcc.add(newVector)

    println(s"new age: ${myVectorAcc.value.value}")
    Thread.sleep(1000000)
    spark.stop()
  }
}

class MyVector{
  private var age: Long = 0

  def reset(): Unit = {
    this.age = 0
  }

  def add(value: MyVector): Unit = {
    this.age = this.age + value.age
  }

  def setAge(value:Long): Unit = {
    this.age = value
  }

  def value : Long = {
    return this.age
  }

}
class VectorAccumulatorV2 extends AccumulatorV2[MyVector, MyVector]{
  private val myVector: MyVector = new MyVector()

  def reset(): Unit = {
    myVector.reset()
  }

  override def add(v:MyVector): Unit = {
    myVector.add(v)
  }

  override def copy(): AccumulatorV2[MyVector, MyVector] = {
    return new VectorAccumulatorV2 ()
  }

  override def isZero: Boolean = {
    return false
  }

  override def clone(): AnyRef = super.clone()

  override def merge(other: AccumulatorV2[MyVector, MyVector]): Unit = {
  }

  override def value: MyVector = {
    return this.myVector
  }
}