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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

//
//  *  ST_Contains(left, right) - left contains right
//  *  or
//  *  ST_Intersects(left, right) - left and right intersect
//  *
//  * @param left left side of the join
//  * @param right right side of the join
//  * @param leftShape expression for the first argument of ST_Contains or ST_Intersects
//  * @param rightShape expression for the second argument of ST_Contains or ST_Intersects
//  * @param intersects boolean indicating whether spatial relationship is 'intersects' (true)
//  *                   or 'contains' (false)
//  */
case class SpatialJoinExec(left: SparkPlan,
                           right: SparkPlan,
                           leftShape: Expression,
                           rightShape: Expression,
                           intersects: Boolean,
                           extraCondition: Option[Expression] = None)
    extends BinaryExecNode
    with SpatialJoin
    with Logging {}
