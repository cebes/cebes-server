/* Copyright 2016 The Cebes Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, version 2.0 (the "License").
 * You may not use this work except in compliance with the License,
 * which is available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 * Created by phvu on 04/12/2016.
 */

package io.cebes.df.expressions

case class AddMonths(child: Expression, numMonths: Int) extends UnaryExpression

case class CurrentDate() extends LeafExpression

case class CurrentTimestamp() extends LeafExpression

case class DateFormatClass(child: Expression, format: String) extends UnaryExpression

case class DateAdd(child: Expression, days: Int) extends UnaryExpression

case class DateSub(child: Expression, days: Int) extends UnaryExpression

case class DateDiff(left: Expression, right: Expression) extends BinaryExpression

case class Year(child: Expression) extends UnaryExpression

case class Quarter(child: Expression) extends UnaryExpression

case class Month(child: Expression) extends UnaryExpression

case class DayOfMonth(child: Expression) extends UnaryExpression

case class DayOfYear(child: Expression) extends UnaryExpression

case class Hour(child: Expression) extends UnaryExpression

case class LastDay(child: Expression) extends UnaryExpression

case class Minute(child: Expression) extends UnaryExpression

case class MonthsBetween(left: Expression, right: Expression) extends BinaryExpression

case class NextDay(child: Expression, dayOfWeek: String) extends UnaryExpression

case class Second(child: Expression) extends UnaryExpression

case class WeekOfYear(child: Expression) extends UnaryExpression

case class FromUnixTime(child: Expression, format: String) extends UnaryExpression

case class UnixTimestamp(child: Expression, format: String) extends UnaryExpression

case class ToDate(child: Expression) extends UnaryExpression

case class TruncDate(child: Expression, format: String) extends UnaryExpression

case class FromUTCTimestamp(child: Expression, tz: String) extends UnaryExpression

case class ToUTCTimestamp(child: Expression, tz: String) extends UnaryExpression

case class TimeWindow(child: Expression, windowDuration: String,
                      slideDuration: String, startTime: String) extends UnaryExpression
