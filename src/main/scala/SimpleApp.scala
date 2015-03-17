// /* SimpleApp.scala */
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
// import org.apache.spark.SparkConf
// import com.hadoop.mapreduce.LzoTextInputFormat
// import com.hadoop.mapreduce.LzoSplitRecordReader
// import com.hadoop.mapreduce.LzoLineRecordReader
// import com.hadoop.mapreduce.LzoSplitInputFormat
// import org.apache.hadoop.mapred.TextInputFormat
// import org.apache.hadoop.io.LongWritable
// import org.apache.hadoop.io.Text

// object SimpleApp {
//  def main(args: Array[String]) {
//    val s3BucketURI = "s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data/"
//    val conf = new SparkConf().setAppName("Correlation Application")



//    val sc = new SparkContext(conf)
//    val hadoopConf = sc.hadoopConfiguration
//    hadoopConf.set("textinputformat.record.delimiter", "\n")

// 	sc.newAPIHadoopFile(s3BucketURI,
//     classOf[LzoTextInputFormat],
//     //classOf[com.hadoop.mapreduce.LzoTextInputFormat],
//     classOf[LongWritable],
//     classOf[Text]).map(_._2.toString).take(10).foreach(println)



//  }
// }



import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.feature._
 
// class Regex(str: String) extends Serializable {
//   val regex = str.r.unanchored
 
//   def matches(str: String) = str match {
//     case regex(_*) => true
//     case _ => false
//   }
// }
 
class NgramRecord(line: String) extends Serializable {
  val field = line.split('\t')
  val ngram = field(0)
  val year = field(1).toInt
  val volumes = field(2).toInt
  val matches = field(3).toInt
 
  // def matches(r: Regex) = r matches ngram
 
  override def toString = s"$ngram,$year,$volumes,$matches"
}
 
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
 
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
 
class Registrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[LongWritable])
    kryo.register(classOf[Text])
  }
}
 
object SimpleApp {
  /* find ngrams that match a regex; args are regex output input [input ..] */
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ngrams")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "Registrator")
    val sc = new SparkContext(conf)
    // val regex = new Regex(args(0))
    // val output = args(1)
    /* if things were simple */
    /* val input = sc.union(args.drop(2).map(sc.textFile(_))) */
    /* alas they are not */

    val s3BucketURI = "s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data/"
    val records = sc.hadoopFile(s3BucketURI, classOf[SequenceFileInputFormat[LongWritable, Text]], classOf[LongWritable], classOf[Text], 16)
					.map(r => new NgramRecord(r._2.toString))

	val yearSet = (1908 to 2008).toSet
	//ngramMap RDD[ngram:String -> Array of Match Counts)
	val ngramMap = records
		.map(r => (r.ngram, (r.year, r.matches.toDouble)))
		.groupByKey()
		.map { case (ngram:String, iter: Iterable[(Int,Double)]) => {
				(ngram, iter.filter(pair => yearSet.contains(pair._1)))
			}
		}
		.filter { case (ngram:String, iter: Iterable[(Int,Double)]) => {
				((iter.size == yearSet.size) && (iter.foldLeft(0.0)(_ + _._2 ) > yearSet.size*20))
			}
		}
		.map { case (ngram:String, iter: Iterable[(Int,Double)]) => {
				val yearMatchPairs: List[(Int, Double)] = iter.toList.sortBy(pair => pair._1)
				val matchVector = Vectors.dense(yearMatchPairs.unzip._2.toArray)
				(ngram, matchVector)
			}
		}
		.cache

	val normalizer = new Normalizer()
	val normalizedNgramMap: RDD[(String, Vector)] = ngramMap
		.mapValues(vec => normalizer.transform(vec))

	val ngramSubset = sc.parallelize(normalizedNgramMap.take(1000), 4).cache

	println(ngramSubset.first._1)
	println(ngramSubset.first._2)
	println("There are " + ngramSubset.count + " 1grams to analyze")

  }
}