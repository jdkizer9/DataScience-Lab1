/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.hadoop.mapreduce.LzoTextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

object SimpleApp {
 def main(args: Array[String]) {
   val s3BucketURI = "s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data/"
   val conf = new SparkConf().setAppName("Correlation Application")
   val sc = new SparkContext(conf)
   sc.newAPIHadoopFile(s3BucketURI,
    classOf[com.hadoop.mapreduce.LzoTextInputFormat],
    classOf[org.apache.hadoop.io.LongWritable],
    classOf[org.apache.hadoop.io.Text])
   .map(kv => kv._2)
   .take(100)
  .foreach(println)
 }
}
