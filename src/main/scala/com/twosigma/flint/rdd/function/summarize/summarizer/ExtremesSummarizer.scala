/*
 *  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
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

package com.twosigma.flint.rdd.function.summarize.summarizer

import scala.collection.mutable
import scala.collection.mutable.PriorityQueue
import scala.reflect.ClassTag

/**
 * Get the k largest elements defined by order
 */
case class ExtremesSummarizer[T](
  k: Int,
  implicit val tag: ClassTag[T],
  ordering: Ordering[T]
) extends FlippableSummarizer[T, mutable.PriorityQueue[T], Array[T]] {

  // To find the k largest, we use a min heap, so the order needs to be reversed.
  override def zero(): mutable.PriorityQueue[T] =
    mutable.PriorityQueue.empty(ordering.reverse)

  override def add(
    u: mutable.PriorityQueue[T],
    t: T
  ): mutable.PriorityQueue[T] = {
    if (u.isEmpty || ordering.gt(t, u.head)) {
      u.enqueue(t)
      while (u.size > k) {
        u.dequeue()
      }
    }

    u
  }

  override def merge(
    u1: mutable.PriorityQueue[T],
    u2: mutable.PriorityQueue[T]
  ): mutable.PriorityQueue[T] = {
    val u = u1 ++ u2
    // If there are more than n items after merge, keep the top k items
    while (u.size > k) {
      u.dequeue()
    }

    u
  }

  // Output elements in order defined by ordering
  override def render(u: mutable.PriorityQueue[T]): Array[T] =
    u.toArray.sorted(ordering)

}
