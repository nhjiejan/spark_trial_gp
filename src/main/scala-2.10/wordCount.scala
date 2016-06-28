import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object wordCount{
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")



    val logFile = "C:\\Program Files\\spark\\spark-1.6.2-bin-hadoop2.6\\README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}