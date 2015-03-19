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

import scala.math.sqrt
import scala.math.pow
import java.io._

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation

 
class NgramRecord(line: String) extends Serializable {
  val field = line.split('\t')
  val ngram = field(0)
  val year = field(1).toInt
  val volumes = field(2).toInt
  val matches = field(3).toInt

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

  def correlation(xArray:Array[Double], yArray:Array[Double]): Double = {
  	val meanX = xArray.sum / xArray.length
  	val meanY = yArray.sum / yArray.length

  	val numerator = xArray.zip(yArray).foldLeft(0.0){ case (acc: Double, (x: Double, y:Double)) => {
			acc + (x-meanX)*(y-meanY)
		}
	}

	val denomX = sqrt(xArray.foldLeft(0.0) {case (acc: Double, x:Double) => acc + pow((x-meanX),2.0)})
	val denomY = sqrt(yArray.foldLeft(0.0) {case (acc: Double, y:Double) => acc + pow((y-meanY),2.0)})

	numerator / (denomX*denomY)
  }

  def correlationSeries(xArray:Array[Double], yArray:Array[Double], windowSize: Int = 10): (Double, Array[Double]) = {

  	assert(xArray.size == yArray.size)

  	val windowLeftEdgeRange = ((2-windowSize) to xArray.size-2)
  	val windowList:List[List[Int]] = windowLeftEdgeRange
  		.map(leftEdge => {
  			(leftEdge to leftEdge+windowSize-1)
  			.toList
  			.filter(x => x>=0 && x<xArray.size)
  		})
  		.filter(l => l.length >=5)
  		.toList

  	val correlationArray:Array[Double] = windowList
  		.map( (window: List[Int]) => {

  			val xWindow = window.map(i => xArray(i)).toArray
  			val yWindow = window.map(i => yArray(i)).toArray
  			correlation(xWindow, yWindow)

  		}).toArray
  	val stddev = new StandardDeviation()
  	(stddev.evaluate(correlationArray), correlationArray)
  }

  def main(args: Array[String]) {

  	val writer = new PrintWriter(new File(args(0)))
  	def writeln(str: String): Unit = writer.write(str + '\n')

    val conf = new SparkConf()
      .setAppName("ngrams")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "Registrator")
    val sc = new SparkContext(conf)

    val s3BucketURI = "s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data/"
    val records = sc.hadoopFile(s3BucketURI, classOf[SequenceFileInputFormat[LongWritable, Text]], classOf[LongWritable], classOf[Text], 64)
					.map(r => new NgramRecord(r._2.toString))

	val yearSet = (1908 to 2008).toSet
	//ngramMap RDD[ngram:String -> Vector of Match Counts)
	val normalizer = new Normalizer()
	val alpha = """[a-zA-Z]+""".r
	val ngramMap = records
		.map(r => (r.ngram.toLowerCase, (r.year, r.matches.toDouble)))
		.groupByKey()
		.filter { case (ngram:String, iter: Iterable[(Int,Double)]) => {
				((alpha findFirstIn ngram) != None)
			}
		}
		.map { case (ngram:String, iter: Iterable[(Int,Double)]) => {
				
				val combineYears:Iterable[(Int,Double)] = iter
					.groupBy( (pair:(Int,Double)) => pair._1)
					.mapValues((i:Iterable[(Int,Double)]) => i.foldLeft(0.0)(_ + _._2))
					.toIterable

				(ngram, combineYears.filter(pair => yearSet.contains(pair._1)))
			}
		}
		.filter { case (ngram:String, iter: Iterable[(Int,Double)]) => {
				((iter.size == yearSet.size) && (iter.foldLeft(0.0)(_ + _._2 ) > yearSet.size*20))
			}
		}
		.sample(false, 0.01)
		.map { case (ngram:String, iter: Iterable[(Int,Double)]) => {
				val yearMatchPairs: List[(Int, Double)] = iter.toList.sortBy(pair => pair._1)
				val matchVector = Vectors.dense(yearMatchPairs.unzip._2.toArray)
				(ngram, normalizer.transform(matchVector).toArray)
			}
		}
		.cache

	//val ngramSubset = sc.parallelize(ngramMap.take(1000), 4).cache
	
	//val cartesianWords = ngramSubset.cartesian(ngramSubset)
	//val ngramSamples = ngramMap.sample(false, 0.05)

	writeln("Randomly sampled " + ngramMap.count + " 1grams to analyze")

	val cartesianWords = ngramMap.cartesian(ngramMap)
		.filter { case ( (ngram1:String, array1:Array[Double]), (ngram2:String, array2:Array[Double])) => {
				(ngram1 < ngram2) 
			}
		}.cache

	val pairwiseCorrelations = cartesianWords
		.map { case ( (ngram1:String, array1:Array[Double]), (ngram2:String, array2:Array[Double])) => {
				((ngram1, ngram2), correlation(array1, array2))
			}
		}.cache

	
	//writeln("Randomly sampled " + cartesianWords.count + " 1grams to analyze")
	writeln("There are " + pairwiseCorrelations.count + " pairwise correlations")
	writeln("\n*****************************************************")
	writeln("The 10 most positively correlated are: ")

	pairwiseCorrelations.sortBy(pair => pair._2, false).take(10).foreach(pair => writeln(pair.toString))

	writeln("\n*****************************************************")
	writeln("The 10 most negativly correlated words are: ")
	pairwiseCorrelations.sortBy(pair => pair._2, true).take(10).foreach(pair => writeln(pair.toString))

	writeln("\n*****************************************************")
	writeln("The 10 least correlated words are: ")
	pairwiseCorrelations.map(pair => if(pair._2 >= 0) pair else (pair._1, -pair._2)).sortBy(pair => pair._2, true).take(10).foreach(pair => writeln(pair.toString))

	val corrSeries = cartesianWords
		.map { case ( (ngram1:String, array1:Array[Double]), (ngram2:String, array2:Array[Double])) => {
				((ngram1, ngram2), correlationSeries(array1, array2))
			}
		}.cache

	writeln("\n*****************************************************")
	writeln("The 10 correlation time series with highest std dev: ")
	corrSeries
		//.filter(pair => pair._2._1 < 10)
		.sortBy(pair => pair._2._1, false)
		.take(10)
		.foreach(pair => {
			writeln("\n*****************************************************")
			writeln(pair._1 + ": " + pair._2._1)
			writer.write("[")
			pair._2._2.foreach(x => writer.write(x + ","))
			writer.write("]\n")
			writeln(pair._1._1)
			writer.write("[")
			ngramMap.lookup(pair._1._1).head.foreach(x => writer.write(x + ","))
			writer.write("]\n")
			writeln(pair._1._2)
			writer.write("[")
			ngramMap.lookup(pair._1._2).head.foreach(x => writer.write(x + ","))
			writer.write("]\n")
		})

	writeln("\n*****************************************************")
	writeln("The 10 correlation time series with highest std dev: ")
	corrSeries
		//.filter(pair => pair._2._1 < 10)
		.sortBy(pair => pair._2._1)
		.take(10)
		.foreach(pair => {
			writeln("\n*****************************************************")
			writeln(pair._1 + ": " + pair._2._1)
			writer.write("[")
			pair._2._2.foreach(x => writer.write(x + ","))
			writer.write("]\n")
			writeln(pair._1._1)
			writer.write("[")
			ngramMap.lookup(pair._1._1).head.foreach(x => writer.write(x + ","))
			writer.write("]\n")
			writeln(pair._1._2)
			writer.write("[")
			ngramMap.lookup(pair._1._2).head.foreach(x => writer.write(x + ","))
			writer.write("]\n")
		})

	writeln("The End!!!")
	
	writer.close()

	// println(ngramSubset.first._1)
	// println(ngramSubset.first._2)
	// println("There are " + ngramSubset.count + " 1grams to analyze")



  }
}