/**
 * @authors Beck Gaël & Chelly Dagdia Zaineb
 */


package qfs

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import scala.util.Try
import scala.io.Source 
import scala.util.Sorting.quickSort
import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object Main {
  def main(args: Array[String]): Unit = {

	@transient val sc = new SparkContext(new SparkConf)

	/* Requiered Paramaters
	 * Datasetes should be as follow :
	 * 	Only real
	 * 	Only Categorial
	 * 	Both with real as first columns and categorial next columns
	 */
	val rawdata = sc.textFile(args(0)).cache // args(0) = path to file of any type
	val sep = Try(args(1)).getOrElse(",") // separator type
	val nbFeatures = rawdata.first.split(sep).size - 1 // Number of features supposing that the last column is the label
	val savingPath = args(2)
	val nbColumn = Try(args(3).toInt).getOrElse(1) // number of column for one heuristic for RST & QR, 0 if not used, k>1 otherwise
	val nbIterIfPerFeat = Try(args(4).toInt).getOrElse(10) // number of iteration for the column heuristic for RST , should be != 0
	val nbBucketDiscretization = Try(args(5).toInt).getOrElse(10) // Defined in how many bucket we discretize real data
	val limitReal = Try(args(6).toInt).getOrElse(nbFeatures) // Number from where we start categorial data

	val featuresReduction =  new DimReduction(sc)

	val fw = new FileWriter(savingPath + "Info", true)
	fw.write("Welcome to QFS result file\nParameters are\n\nPer column buckets heuristic\n\t" + nbColumn + " buckets\n\tNumber of iteration : " + nbIterIfPerFeat + "\n\tNumber Bucket Discretization : " + nbBucketDiscretization)

	val realCatLabel = rawdata.map(_.split(sep)).map( rawVector => (rawVector.take(limitReal).map(_.toDouble), rawVector.slice(limitReal, nbFeatures + 1), rawVector.last)).cache
	val realData = realCatLabel.map(_._1).cache
	val categData = realCatLabel.map(_._2)
	val labelData = realCatLabel.map(_._3).zipWithIndex.cache

	val nbRealFeatures = realData.first.size
	val (normalizedCategRDD, occurPerFeat) = Fcts.replaceCatFeatByDble(sc, categData)
	val discretizedRealRDD = Fcts.discretize(sc, realData, nbBucketDiscretization)
	val labelAsDouble = Fcts.labelToDouble(sc, labelData)

	val readyForDimReducRDD = discretizedRealRDD.zip(normalizedCategRDD).zip(labelAsDouble).map{ case((realValues, categValues), (label, id)) => (id, realValues ++ categValues, label) }
	
	val seqFeats = (0 until nbFeatures).toArray
	rawdata.unpersist(true)
	realCatLabel.unpersist(true)

	val t0 = System.nanoTime
	val allReduce = for( i <- 0 until nbIterIfPerFeat) yield(
	{
		val (dividByFeatsRDD, columnsOfFeats) = Fcts.divideByFeatures(readyForDimReducRDD, nbColumn, nbFeatures)
		dividByFeatsRDD.cache
		featuresReduction.roughSetPerFeatsD(dividByFeatsRDD, nbColumn, columnsOfFeats)
	})

	val reduceFeats = allReduce.reduce(_.intersect(_))
	val t1 = System.nanoTime
	val duration = (t1 - t0) / 1000000000D

	val sortedByFeatReadyToSave = reduceFeats.sorted.mkString(",")

	fw.write("\nDuration : " + duration + "\n\nReduce feats (start to index 0) :\n" + sortedByFeatReadyToSave)
	fw.close

	sc.stop
	}
}
