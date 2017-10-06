/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.hive

import com.esri.hadoop.hive.ST_AsText
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import org.apache.hadoop.io.BytesWritable
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialOperator.JoinQuery.JoinParams
import org.datasyslab.geospark.spatialRDD.SpatialRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, JoinedRow, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.execution.SparkPlan

trait SpatialJoin { self: SparkPlan =>

  val left: SparkPlan
  val right: SparkPlan
  val leftShape: Expression
  val rightShape: Expression
  val intersects: Boolean
  val extraCondition: Option[Expression]

  // Using lazy val to avoid serialization
  private lazy val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)

  @transient private lazy val boundCondition: (InternalRow => Boolean) = {
    if (extraCondition.isDefined) {
      // eval vs. apply
      newPredicate(extraCondition.get, left.output ++ right.output).eval _
    } else {
      (r: InternalRow) => true
    }
  }

  override def output: Seq[Attribute] = left.output ++ right.output

  protected def esriToJtsGeometry(input: Array[Byte]): Geometry = {
    val wkt = (new ST_AsText()).evaluate(new BytesWritable(input)).toString
    new WKTReader().read(wkt)
  }

  protected def toSpatialRDD(rdd: RDD[UnsafeRow],
                             shapeExpression: Expression): SpatialRDD[Geometry] = {

    val spatialRdd = new SpatialRDD[Geometry]
    spatialRdd.setRawSpatialRDD(
      rdd
        .map { x =>
          {
            // TODO Eliminate conversion between ESRI and JTS types
            val shape = esriToJtsGeometry(shapeExpression.eval(x).asInstanceOf[Array[Byte]])
            shape.setUserData(x.copy)
            shape
          }
        }
        .toJavaRDD())
    spatialRdd
  }

  def toSpatialRDDs(buildRdd: RDD[UnsafeRow],
                    buildExpr: Expression,
                    streamedRdd: RDD[UnsafeRow],
                    streamedExpr: Expression): (SpatialRDD[Geometry], SpatialRDD[Geometry]) =
    (toSpatialRDD(buildRdd, buildExpr), toSpatialRDD(streamedRdd, streamedExpr))

  override protected def doExecute(): RDD[InternalRow] = {
    val boundLeftShape = BindReferences.bindReference(leftShape, left.output)
    val boundRightShape = BindReferences.bindReference(rightShape, right.output)

    val leftResultsRaw = left.execute().asInstanceOf[RDD[UnsafeRow]]
    val rightResultsRaw = right.execute().asInstanceOf[RDD[UnsafeRow]]

    // By default, determine partitioning scheme based on sample of the right side of the join
    val partitionSide = sparkContext.conf.get("spark.sql.spatial.partitionSide", "right")

    // Allow the user to request re-partitioning of the side used to determine partitioning
    // scheme before sampling
    val numRawPartitions = sparkContext.conf.getInt("spark.sql.spatial.numRawPartitions", -1)

    val (leftResults, rightResults) =
      if (numRawPartitions > 0) {
        logInfo(s"Repartitioning prior to spatial join into $numRawPartitions partitions")
        if (partitionSide.equalsIgnoreCase("right")) {
          (leftResultsRaw, rightResultsRaw.repartition(numRawPartitions))
        } else {
          (leftResultsRaw.repartition(numRawPartitions), rightResultsRaw)
        }
      } else {
        (leftResultsRaw, rightResultsRaw)
      }

    logInfo("Number of partitions on the left: " + leftResults.partitions.size)
    logInfo("Number of partitions on the right: " + rightResults.partitions.size)

    val (leftShapes, rightShapes) =
      toSpatialRDDs(leftResults, boundLeftShape, rightResults, boundRightShape)

    val (first, second) =
      if (partitionSide.equalsIgnoreCase("right")) (rightShapes, leftShapes)
      else (leftShapes, rightShapes)

    first.analyze
    if (first.approximateTotalCount == 0) {
      // empty dataset; skip the join
      logInfo("One side of the spatial join is empty. Skipping the join.")
      sparkContext.emptyRDD
    } else {
      def getNumPartitions: Int = {
        val numPartitions = sparkContext.conf
          .getInt("spark.sql.spatial.numPartitions", first.rawSpatialRDD.partitions.size())

        // Make sure number of partitions doesn't exceed half of the number of records
        if (numPartitions * 2 > first.approximateTotalCount) {
          Math.floor(first.approximateTotalCount / 2).toInt
        } else {
          numPartitions
        }
      }

      val numPartitions = getNumPartitions

      logInfo(s"Found ${first.approximateTotalCount} objects spread over ${first.boundaryEnvelope}")

      logInfo(s"Making $numPartitions spatial partitions")
      first.spatialPartitioning(GridType.QUADTREE, numPartitions)
      second.spatialPartitioning(first.getPartitioner)

      val joinParams = new JoinParams(intersects, IndexType.RTREE)
      val matches = JoinQuery.spatialJoin(rightShapes, leftShapes, joinParams)

      matches.rdd.mapPartitions {
        _.map {
          case (l, r) =>
            val left = l.getUserData.asInstanceOf[UnsafeRow]
            val right = r.getUserData.asInstanceOf[UnsafeRow]

            joiner.join(left, right)
        }
      }

      matches.rdd.mapPartitionsWithIndexInternal { (index, iter) =>
        val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
        val filtered = if (extraCondition.isDefined) {
          val boundCondition = newPredicate(extraCondition.get, left.output ++ right.output)
          boundCondition.initialize(index)
          val joined = new JoinedRow

          iter.filter { r =>
            val left = r._1.getUserData.asInstanceOf[InternalRow]
            val right = r._2.getUserData.asInstanceOf[InternalRow]
            boundCondition.eval(joined(left, right))
          }
        } else {
          iter
        }
        filtered.map { r =>
          val left = r._1.getUserData.asInstanceOf[UnsafeRow]
          val right = r._2.getUserData.asInstanceOf[UnsafeRow]
          joiner.join(left, right)
        }
      }
    }
  }
}
