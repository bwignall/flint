/*
 *  Copyright 2017 TWO SIGMA OPEN SOURCE, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.twosigma.flint.sql.function.aggregate

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.math.{ abs, signum, sqrt }

/**
 * Calculates the weighted mean, weighted deviation, and weighted tstat.
 *
 * Takes every (sample, weight) pair and treats them as if they were written
 * (sign(weight) * sample, abs(weight)).
 *
 * Implemented based on
 * [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Weighted_incremental_algorithm Weighted incremental algorithm]] and
 * [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm Parallel algorithm]]
 * and replaces all "n" with corresponding "SumWeight"
 */

class InputSchema(val value: Double, val weight: Double)
//class BufferSchema extends Tuple[Long, Double, Double, Double, Double]
//class DataType extends Tuple[Double, Double, Double, Long]

class BufferSchema(
  val count: Long,
  val sumWeight: Double,
  val mean: Double,
  val sumSquareOfDiffFromMean: Double,
  val sumSquareOfWeights: Double
)

class DataType(
  val weightedMean: Double,
  val weightedStandardDeviation: Double,
  val weightedTstat: Double,
  val observationCount: Long
)

class WeightedMeanTest extends Aggregator[InputSchema, BufferSchema, DataType] {

  override def zero: BufferSchema = {
    new BufferSchema(0L, 0.0, 0.0, 0.0, 0.0)
  }

  override def reduce(
    buffer: BufferSchema,
    input: InputSchema
  ): BufferSchema = {
    val rawValue = input.value
    val rawWeight = input.weight

    // Move the sign from weight to value, keep weight non-negative
    val value = rawValue * signum(rawWeight)
    val weight = abs(rawWeight)

    val count = buffer.count
    val sumWeight = buffer.sumWeight
    val mean = buffer.mean
    val sumSquareOfDiffFromMean = buffer.sumSquareOfDiffFromMean
    val sumSquareOfWeights = buffer.sumSquareOfWeights

    val temp = weight + sumWeight
    val delta = value - mean
    val R = delta * weight / temp

    new BufferSchema(
      count + 1,
      temp,
      mean + R,
      sumSquareOfDiffFromMean + (sumWeight * delta * R),
      sumSquareOfWeights + (weight * weight)
    )
  }

  override def merge(
    buffer1: BufferSchema,
    buffer2: BufferSchema
  ): BufferSchema = {
    val count2 = buffer2.count

    if (count2 > 0) {
      val count1 = buffer1.count
      val sumWeight1 = buffer1.sumWeight
      val mean1 = buffer1.mean
      val sumSquareOfDiffFromMean1 = buffer1.sumSquareOfDiffFromMean
      val sumSquareOfWeights1 = buffer1.sumSquareOfWeights

      val sumWeight2 = buffer2.sumWeight
      val mean2 = buffer2.mean
      val sumSquareOfDiffFromMean2 = buffer2.sumSquareOfDiffFromMean
      val sumSquareOfWeights2 = buffer2.sumSquareOfWeights

      val delta = mean2 - mean1

      val new0 = count1 + count2
      val new1 = sumWeight1 + sumWeight2
      // This particular way to calculate mean is chosen based on
      // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
      val new2 =
        (sumWeight1 * mean1 + sumWeight2 * mean2) / (sumWeight1 + sumWeight2)
      val new3 = sumSquareOfDiffFromMean1 + sumSquareOfDiffFromMean2 +
        delta * delta * sumWeight1 * sumWeight2 / (sumWeight1 + sumWeight2)
      val new4 = sumSquareOfWeights1 + sumSquareOfWeights2

      new BufferSchema(new0, new1, new2, new3, new4)
    } else {
      buffer1
    }
  }

  override def finish(buffer: BufferSchema): DataType = {
    val count = buffer.count
    val sumWeight = buffer.sumWeight
    val mean = buffer.mean
    val sumSquareOfDiffFromMean = buffer.sumSquareOfDiffFromMean

    val variance = sumSquareOfDiffFromMean / sumWeight
    val stdDev = sqrt(variance)
    val tStat = sqrt(count.toDouble) * mean / stdDev

    new DataType(mean, stdDev, tStat, count)
  }

  override def bufferEncoder: Encoder[BufferSchema] = ExpressionEncoder()

  override def outputEncoder: Encoder[DataType] = ExpressionEncoder()

}
