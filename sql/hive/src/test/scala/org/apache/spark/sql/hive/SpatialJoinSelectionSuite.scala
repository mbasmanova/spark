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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class SpatialJoinSelectionSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  val spatialFunctionNames = Seq(
    "ST_GeomFromText",
    "ST_Point",
    "ST_Contains",
    "ST_Intersects",
    "ST_Distance")

  private def withSpatialUdfs(f: => Unit): Unit = {
    try {
      spatialFunctionNames.foreach { name =>
        spark.sql(s"CREATE TEMPORARY FUNCTION $name AS 'com.esri.hadoop.hive.$name'")
      }

      f
    } finally {
      spatialFunctionNames.foreach { name =>
        spark.sql(s"DROP TEMPORARY FUNCTION $name")
      }
    }
  }

  private def getTable(plan: SparkPlan): HiveTableRelation = {
    val relations = plan.collect { case HiveTableScanExec(_, relation, _) => relation }
    assert(relations.size == 1, "Expected HiveTableScanExec, but got " + plan)
    relations(0)
  }

  private def getUdf(expression: Expression): HiveSimpleUDF = {
    val udfs = expression.collect { case udf: HiveSimpleUDF => udf }
    assert(udfs.size == 1, "Expected HiveSimpleUDF, but got " + expression)
    udfs(0)
  }

  test("spatial join") {
    withTable("points_a", "polygons") {
      withSpatialUdfs {
        sql("CREATE TABLE points(lon DOUBLE, lat DOUBLE, name STRING)")
        sql("CREATE TABLE polygons(city_id INT, geometry STRING)")

        def findJoin(plan: SparkPlan): SpatialJoinExec = {
          val joins = plan.collect { case join: SpatialJoinExec => join }
          assert(joins.size == 1, "Should use spatial join")
          joins(0)
        }

        def verifyPlan(plan: SparkPlan, intersects: Boolean): Unit = {
          val join = findJoin(plan)

          val leftRelation = getTable(join.left);
          val leftUdf = getUdf(join.leftShape)

          assert(leftRelation.tableMeta.identifier.table == "polygons")
          assert(leftUdf.name == "ST_GeomFromText")
          assert(leftUdf.references.filter(leftRelation.dataCols.contains(_)).isEmpty)

          val rightRelation = getTable(join.right);
          val rightUdf = getUdf(join.rightShape)
          assert(rightRelation.tableMeta.identifier.table == "points")
          assert(rightUdf.name == "ST_Point")
          assert(rightUdf.references.filter(rightRelation.dataCols.contains(_)).isEmpty)

          assert(join.intersects == intersects)
          assert(join.extraCondition == None)
        }

        verifyPlan(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.geometry
             |FROM points a, polygons b
             |WHERE ST_Contains(ST_GeomFromText(b.geometry), ST_Point(a.lon, a.lat))
         """.stripMargin).queryExecution.sparkPlan, false)

        // Swap tables
        verifyPlan(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.geometry
             |FROM polygons b, points a
             |WHERE ST_Contains(ST_GeomFromText(b.geometry), ST_Point(a.lon, a.lat))
         """.stripMargin).queryExecution.sparkPlan, false)

        verifyPlan(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.geometry
             |FROM points a, polygons b
             |WHERE ST_Intersects(ST_GeomFromText(b.geometry), ST_Point(a.lon, a.lat))
         """.stripMargin).queryExecution.sparkPlan, true)

        // Swap tables
        verifyPlan(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.geometry
             |FROM polygons b, points a
             |WHERE ST_Intersects(ST_GeomFromText(b.geometry), ST_Point(a.lon, a.lat))
         """.stripMargin).queryExecution.sparkPlan, true)
      }
    }
  }

  test("spatial distance join") {
    withTable("points_a", "points_b") {
      withSpatialUdfs {
        sql("CREATE TABLE points_a(lon DOUBLE, lat DOUBLE, name STRING)")
        sql("CREATE TABLE points_b(lon DOUBLE, lat DOUBLE, name STRING)")

        def findJoin(plan: SparkPlan): DistanceJoinExec = {
          val joins = plan.collect { case join: DistanceJoinExec => join }
          assert(joins.size == 1, "Should use spatial distance join")
          joins(0)
        }

        def verifyPlan(plan: SparkPlan, leftTableName: String, rightTableName: String): Unit = {
          val join = findJoin(plan)

          val leftRelation = getTable(join.left);
          val leftUdf = getUdf(join.leftShape)

          assert(leftRelation.tableMeta.identifier.table == leftTableName)
          assert(leftUdf.name == "ST_Point")
          assert(leftUdf.references.filter(leftRelation.dataCols.contains(_)).isEmpty)
          assert(join.radius.references.filter(leftRelation.dataCols.contains(_)).isEmpty)

          val rightRelation = getTable(join.right);
          val rightUdf = getUdf(join.rightShape)
          assert(rightRelation.tableMeta.identifier.table == rightTableName)
          assert(rightUdf.name == "ST_Point")
          assert(rightUdf.references.filter(rightRelation.dataCols.contains(_)).isEmpty)

          assert(join.extraCondition == None)
        }

        verifyPlan(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_a a, points_b b
             |WHERE ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1
           """.stripMargin).queryExecution.sparkPlan, "points_a", "points_b")

        // Swap tables
        verifyPlan(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_b b, points_a a
             |WHERE ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1
           """.stripMargin).queryExecution.sparkPlan, "points_a", "points_b")

        // Use expression over columns in 'a' for radius
        verifyPlan(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_a a, points_b b
             |WHERE ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1 * a.lon
           """.stripMargin).queryExecution.sparkPlan, "points_a", "points_b")

        // Swap tables
        verifyPlan(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_b b, points_a a
             |WHERE ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1 * a.lon
           """.stripMargin).queryExecution.sparkPlan, "points_a", "points_b")

        // Use expression over columns in 'b' for radius
        verifyPlan(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_a a, points_b b
             |WHERE ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1 * b.lon
           """.stripMargin).queryExecution.sparkPlan, "points_b", "points_a")

        // Swap tables
        verifyPlan(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_b b, points_a a
             |WHERE ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1 * b.lon
           """.stripMargin).queryExecution.sparkPlan, "points_b", "points_a")
      }
    }
  }
}
