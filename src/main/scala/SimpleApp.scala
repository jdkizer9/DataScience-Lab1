/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.hadoop.mapreduce.LzoTextInputFormat
import com.hadoop.mapreduce.LzoSplitRecordReader
import com.hadoop.mapreduce.LzoLineRecordReader
import com.hadoop.mapreduce.LzoSplitInputFormat
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

object SimpleApp {
 def main(args: Array[String]) {
   val s3BucketURI = "s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data/"
   val conf = new SparkConf().setAppName("Correlation Application")



   val sc = new SparkContext(conf)
   val hadoopConf = sc.hadoopConfiguration
   hadoopConf.set("textinputformat.record.delimiter", "\n")

	sc.newAPIHadoopFile(s3BucketURI,
    classOf[LzoTextInputFormat],
    //classOf[com.hadoop.mapreduce.LzoTextInputFormat],
    classOf[LongWritable],
    classOf[Text]).map(_._2.toString).take(10).foreach(println)



 }
}
