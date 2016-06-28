/**
  * Created by NHJIEJAN on 28/06/2016.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object gpDataLoad {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")

    val conf = new SparkConf().setAppName("load gp data").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlctx = new SQLContext(sc)

    val opts = Map(
      "url" -> "jdbc:postgresql://127.0.0.1:5432/vagrant?user=vagrant&password=vagrant",
      "dbtable" -> "public.a")


    val df = sqlctx
      .read
      .format("jdbc")
      .options(opts)
      .load

    df.show()

  }
}