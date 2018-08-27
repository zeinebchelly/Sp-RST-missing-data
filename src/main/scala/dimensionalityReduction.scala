/**
 * @authors Beck Gaël & Chelly Dagdia Zaineb
 */

package qfs

import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random
import org.apache.spark.broadcast.Broadcast


class DimReduction(@transient val sc:SparkContext) extends Serializable {

  // Define KeyValueExtract function
  val keyValueExtract = (f:Array[Int], vector: Array[Double]) =>
  {  
    val key = ListBuffer.empty[Double]
    for(i <- 0 until f.size) key += vector(f(i))
    key
  }

  // Define a function to calculate the IND of each element in the list indDecisionClasses
  val getIND = (f :Array[Int], data:RDD[(Long, Array[Double], Double)]) =>
  {
     val indFeatures = data.map{ case (id, vector, label) => (keyValueExtract(f, vector), ArrayBuffer(id)) }.foldByKey(ArrayBuffer.empty[Long])(_ ++= _)
     indFeatures.map{ case (listValues, objectsId) => objectsId }.collect
  }

  // Compute dependency
  def dependency(indecability: Array[ArrayBuffer[Long]], indDecisionClasses:Broadcast[Array[ArrayBuffer[Long]]]) = (for( indCl <- indDecisionClasses.value ) yield( (for( i <- indecability if( i.intersect(indCl).size != i.size ) ) yield( i.size )).reduce(_ + _) )).reduce(_ + _)

  val generateIndecidabilityDecisionClasses = (data:RDD[(Long, Array[Double], Double)]) =>
  {
    val indDecisionClass = data.map{ case (id, vector, label) => (label, ArrayBuffer(id)) }.foldByKey(ArrayBuffer.empty[Long])(_ ++= _)
    sc.broadcast( indDecisionClass.map{ case (label, objects) => objects }.collect )
  }
  
  /********************************************************************************************/
  /*                                        Rought Set                                        */
  /********************************************************************************************/

  /**
   * Rough Set core function
   * First is the distributed version
   * Second is pure local one
   */
  def roughSetCore(data:RDD[(Long, Array[Double], Double)], allCombinations:Broadcast[Array[Array[Int]]]) =
  {
    val indDecisionClasses = generateIndecidabilityDecisionClasses(data)
    // 1. Calculate the IND for all the combination of features
    val indAllCombinations = sc.parallelize( allCombinations.value.map(f => (f, f.size, getIND(f, data))) )
    val dependencyAll = indAllCombinations.map{ case(comboFeatures, sizeFeatures, indecability) => (comboFeatures, sizeFeatures, dependency(indecability, indDecisionClasses)) }.cache
    var dependencyMax = dependencyAll.max()(Ordering[Int].on(_._3))._3
    val allDependencyMax = dependencyAll.filter(l => l._3 == dependencyMax)
    val maxDependencyMinFeatureNb = allDependencyMax.min()(Ordering[Int].on(_._2))._2
    val allReductSet = allDependencyMax.filter{ case(_, sizeFeatures, _) => sizeFeatures == maxDependencyMinFeatureNb }.map{ case(features, _, _) => features }.collect
    data.unpersist(true)
    dependencyAll.unpersist(true)
    allReductSet    
  }

  def roughSetCore(data:Array[(Long, Array[Double], Double)], allCombinations:Array[Array[Int]]) =
  {
    val indDecisionClasses = generateIndecidabilityDecisionClasses_local(data)
    // 1. Calculate the IND for all the combination of features
    val indAllCombinations = allCombinations.map(f => (f, f.size, getIND_local(f, data)))
    val dependencyAll = indAllCombinations.map{ case(comboFeatures, sizeFeatures, indecability) => (comboFeatures, sizeFeatures, dependency_local(indecability, indDecisionClasses)) }
    var dependencyMax = dependencyAll.maxBy(_._3)._3
    val allDependencyMax = dependencyAll.filter(l => l._3 == dependencyMax)
    val maxDependencyMinFeatureNb = allDependencyMax.minBy(_._2)._2
    val allReductSet = allDependencyMax.filter{ case(_, sizeFeatures, _) => sizeFeatures == maxDependencyMinFeatureNb }.map{ case(features, _, _) => features }
    allReductSet    
  }


  /*
   *  RoughSet classic version
   */
  def roughSet(data:RDD[(Long, Array[Double], Double)], allCombinations:Broadcast[Array[Array[Int]]]) =
  {
    data.cache
    roughSetCore(data, allCombinations)
  }
   
  /*
   *  RoughSet working by range of features
   */
  def roughSetPerFeats(data:RDD[(Long, Array[Array[Double]], Double)], nbColumns:Int, columnsOfFeats:Array[Array[Int]]) =
  {
    data.cache
    val allreductSetPerFeats = for( column <- 0 until nbColumns ) yield({
      val dataPerFeat = data.map{ case(idx, vectorPerFeats, label) => (idx, vectorPerFeats(column), label) }
      dataPerFeat.cache
      val originalFeatures = columnsOfFeats(column)
      val mapOriginalFeatures = originalFeatures.zipWithIndex.map(_.swap).toMap
      val features = (0 until originalFeatures.size).toArray
      val allCombinations = features.flatMap(features.combinations).drop(1)
      val allCombinationsBC = sc.broadcast(allCombinations)
      val allReductSet = roughSetCore(dataPerFeat, allCombinationsBC)
      allReductSet.map(_.map(mapOriginalFeatures))
      })
    allreductSetPerFeats.map( allReduct => allReduct(Random.nextInt(allReduct.size)) ).fold(Array.empty[Int])(_ ++ _)
  }
   
  /*
   *  RoughSet working by range of features
   */
  def roughSetPerFeatsD(data:RDD[(Long, Array[Array[Double]], Double)], nbColumns:Int, columnsOfFeats:Array[Array[Int]]) =
  {
    val dataBC = sc.broadcast(data.collect) 
    val reduct = sc.parallelize(0 until nbColumns).map( numColumn => {
      val dataPerFeat = dataBC.value.map{ case(idx, vectorPerFeats, label) => (idx, vectorPerFeats(numColumn), label) }
      val originalFeatures = columnsOfFeats(numColumn)
      val mapOriginalFeatures = originalFeatures.zipWithIndex.map(_.swap).toMap
      val features = (0 until originalFeatures.size).toArray
      val allCombinations = features.flatMap(features.combinations).drop(1)
      val allReductSet = roughSetCore(dataPerFeat, allCombinations)
      allReductSet.map(_.map(mapOriginalFeatures))
      }).map( allReduct => allReduct(Random.nextInt(allReduct.size)) ).fold(Array.empty[Int])(_ ++ _)
    reduct
  }

  val combGeneration = (features:Array[Int], reductSet:Array[Int]) => 
  {
    val comboList = ListBuffer.empty[Array[Int]]
    for( i <- 0 until features.size ) if( ! reductSet.contains(i) ) comboList += (reductSet :+ features(i))
    comboList 
  }

  /********************************************************************************************/
  /*                  Pure scala functions to apply on each node locally                      */
  /********************************************************************************************/

  // Define a function to calculate the IND of each element in the list indDecisionClasses
  val getIND_local = (f :Array[Int], data:Array[(Long, Array[Double], Double)]) =>
  {
     val indFeatures = data.map{ case(idx, vector, label) => (keyValueExtract(f, vector), idx) }.groupBy(_._1)
     indFeatures.map{ case(listValues, objectsIdx) => objectsIdx.map(_._2).toArray }.toArray
  }

  val dependency_local = (indecability: Array[Array[Long]], indDecisionClasses:Array[List[Long]]) => (for( indCl <- indDecisionClasses) yield( (for( i <- indecability ) yield( if( i.intersect(indCl).size != i.size ) 0 else i.size )).reduce(_ + _) )).reduce(_ + _)

  val generateIndecidabilityDecisionClasses_local = (data:Array[(Long, Array[Double], Double)]) =>
  {
    val indDecisionClass = data.map{ case(id, vector, label) => (label, id) }.groupBy(_._1)
    indDecisionClass.map{ case(label, objects) => objects.map(_._2).toList }.toArray
  }

}
