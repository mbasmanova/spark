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

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{And, Expression, LessThan, LessThanOrEqual}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

//
//  * Plans `SpatialJoinExec` for inner joins on spatial relationships ST_Contains(a, b)
//  * and ST_Intersects(a, b).
//  *
//  * Plans `DistanceJoinExec` for inner joins on spatial relationship ST_Distance(a, b) < r.
//  */
object SpatialJoinSelection extends Strategy {

//  /**
//    * Returns true if specified expression has at least one reference and all its references
//    * map to the output of the specified plan.
//    */
  private def matches(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.find(plan.outputSet.contains(_)).isDefined &&
      expr.references.find(!plan.outputSet.contains(_)).isEmpty

  private def matchExpressionsToPlans(exprA: Expression,
                                      exprB: Expression,
                                      planA: LogicalPlan,
                                      planB: LogicalPlan): Option[(LogicalPlan, LogicalPlan)] =
    if (matches(exprA, planA) && matches(exprB, planB)) {
      Some((planA, planB))
    } else if (matches(exprA, planB) && matches(exprB, planA)) {
      Some((planB, planA))
    } else {
      None
    }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case Join(left, right, Inner, Some(HiveGenericUDF("ST_Contains", _, children))) =>
      // ST_Contains(a, b) - a contains b
      val a = children.head
      val b = children.tail.head

      matchExpressionsToPlans(a, b, left, right) match {
        case Some((planA, planB)) =>
          logInfo("Planning spatial join for 'contains' relationship")
          SpatialJoinExec(planLater(planA), planLater(planB), a, b, false) :: Nil
        case None =>
          logInfo(
            "Spatial join for ST_Contains with arguments not aligned " +
              "with join relations is not supported")
          Nil
      }

    case Join(left, right, Inner, Some(HiveGenericUDF("ST_Intersects", _, children))) =>
      // ST_Intersects(a, b) - a and b intersect
      val a = children.head
      val b = children.tail.head

      matchExpressionsToPlans(a, b, left, right) match {
        case Some((planA, planB)) =>
          logInfo("Planning spatial join for 'intersects' relationship")
          SpatialJoinExec(planLater(planA), planLater(planB), a, b, true) :: Nil
        case None =>
          logInfo(
            "Spatial join for ST_Intersects with arguments not aligned " +
              "with join relations is not supported")
          Nil
      }

    case Join(left,
              right,
              Inner,
              Some(LessThanOrEqual(HiveSimpleUDF("ST_Distance", _, children), radius))) =>
      // ST_Distance(a, b) <= radius
      planDistanceJoin(left, right, children, radius)

    case Join(left, right, Inner, Some(And(a, b))) =>
      a match {
        case LessThanOrEqual(HiveSimpleUDF("ST_Distance", _, children), radius) =>
          planDistanceJoin(left, right, children, radius, Some(b))
        case _ =>
          b match {
            case LessThanOrEqual(HiveSimpleUDF("ST_Distance", _, children), radius) =>
              planDistanceJoin(left, right, children, radius, Some(a))
            case _ => Nil
          }
      }

    case _ =>
      Nil
  }

  private def planDistanceJoin(left: LogicalPlan,
                               right: LogicalPlan,
                               children: Seq[Expression],
                               radius: Expression,
                               extraCondition: Option[Expression] = None): Seq[SparkPlan] = {
    val a = children.head
    val b = children.tail.head

    matchExpressionsToPlans(a, b, left, right) match {
      case Some((planA, planB)) =>
        if (radius.references.isEmpty || matches(radius, planA)) {
          logInfo("Planning spatial distance join")
          DistanceJoinExec(planLater(planA), planLater(planB), a, b, radius, extraCondition) :: Nil
        } else if (matches(radius, planB)) {
          logInfo("Planning spatial distance join")
          DistanceJoinExec(planLater(planB), planLater(planA), b, a, radius, extraCondition) :: Nil
        } else {
          logInfo(
            "Spatial distance join for ST_Distance with non-scalar radius " +
              "that is not a computation over just one side of the join is not supported")
          Nil
        }
      case None =>
        logInfo(
          "Spatial distance join for ST_Distance with arguments not " +
            "aligned with join relations is not supported")
        Nil
    }
  }
}
