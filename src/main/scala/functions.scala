/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @author Beck Gaël & Chelly Dagdia Zaineb
 */

package qfs

import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import spire.implicits._
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.mllib.util._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import org.apache.spark
import java.io._
import scala.math.{min, max}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Theses functions are used to preprocess raw data                    
 **/
object Fcts extends Serializable
{
  val replaceCatFeatByDble = (sc:SparkContext, rdd:RDD[Array[String]]) => 
  {
    rdd.cache
    val occurPerFeat = rdd.map(_.map(HashSet(_))).reduce(_.zip(_).map(x => x._1 ++ x._2))
    val fromValToIdx = sc.broadcast(HashMap(occurPerFeat.map(dim => HashMap(dim.toArray.zipWithIndex.map{case(occ, idx) => (occ, idx.toDouble) }:_*)).zipWithIndex.map(_.swap):_*))
    val optimalDiscretRDD = rdd.map(_.zipWithIndex.map{ case(value, idxF) => fromValToIdx.value(idxF)(value) })
    rdd.unpersist(false)
    (optimalDiscretRDD, occurPerFeat)
  }

  val labelToDouble = (sc:SparkContext, rddLabel:RDD[(String, Long)]) =>
  {
    val labels = sc.broadcast(rddLabel.map{ case(label, id) => HashSet(label) }.reduce(_ ++ _).zipWithIndex.map{ case(label, idx) => (label, idx.toDouble) }.toMap)
    rddLabel.map{ case(label, id) => (labels.value(label), id) }
  }

  val discretize = (sc:SparkContext, rdd:RDD[Array[Double]], nbDiscretBucket:Int) =>
  {
    rdd.cache
    val minMaxArray = rdd.map( _.map(v => (v, v)) ).reduce( (v1, v2) => v1.zip(v2).map{ case(((min1,max1),(min2,max2))) => (min(min1, min2), max(max1, max2))})

    val bucketSize = minMaxArray.map{ case(min, max) => ((max - min) / nbDiscretBucket, min) }
    
    val ranges = for( (size, min) <- bucketSize ) yield( for( limit <- 0 until nbDiscretBucket ) yield( min + size * limit ) )
    
    val discretizeRDDstr = rdd.map( vector => vector.zipWithIndex.map{ case(value, idx) => (whichInterval(value, ranges(idx)).toString, idx) }) 

    val fromStrToIdx = sc.broadcast( 
      ranges.map( range => (range :+ Double.PositiveInfinity).map( v => whichInterval(v, range).toString ).zipWithIndex.map{ case(str, idx) => (str, idx.toDouble) }.toMap )
        .zipWithIndex
        .map(_.swap)
        .toMap
    )

    val discretizeRDD = discretizeRDDstr.map(_.map{ case(v, idx2) => fromStrToIdx.value(idx2)(v) })
    rdd.unpersist(false)
    discretizeRDD
  }

  /*
   * Determine in which interval falls a value given a specific range. Left exclude Right include
   */
  val whichInterval = (d:Double, range:IndexedSeq[Double]) =>
  {
    var continue = true
    var bucketNumber = 0
    while( continue && d > range(bucketNumber) )
    {
      bucketNumber += 1
      if( bucketNumber == range.size ) continue = false
    }
    if ( bucketNumber == 0 ) (Double.NegativeInfinity,range(bucketNumber))
    else if ( bucketNumber == range.size ) (range(bucketNumber - 1), Double.PositiveInfinity)
    else (range(bucketNumber - 1), range(bucketNumber))
  }

  val divideByFeatures = (discretizedRDD:RDD[(Long, Array[Double], Double)], nbColumn:Int, nbFeatures:Int) =>
  {
    val sizeColumn = nbFeatures / nbColumn
    val shuffleFeats = Random.shuffle(0 to nbFeatures - 1).toArray
    val columnsOfFeats = (for( i <- 0 to nbColumn ) yield(shuffleFeats.slice(sizeColumn * i, sizeColumn * (i + 1)))).toArray

    val divideByFeatsRDD = if( nbColumn == 0 ) discretizedRDD.map{ case(id, vector, label) => (id, Array(vector), label) }
      else discretizedRDD.map{ case(id, vector, label) => (id, for( feats <- columnsOfFeats ) yield(for( feat <- feats ) yield(vector(feat))), label) }
    (divideByFeatsRDD, columnsOfFeats)
  }

}
