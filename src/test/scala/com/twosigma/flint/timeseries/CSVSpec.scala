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

package com.twosigma.flint.timeseries

import java.util.TimeZone

import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.sql.types._

class CSVSpec extends TimeSeriesSuite {

  "CSV" should "read a CSV file without header." in {
    withResource("/timeseries/csv/Price.csv") { source =>
      val expectedSchema = Schema("C1" -> IntegerType, "C2" -> DoubleType)
      val timeseriesRdd = CSV.from(
        sqlContext,
        "file://" + source,
        sorted = true,
        schema = expectedSchema
      )
      val ts = timeseriesRdd.collect()
      assert(timeseriesRdd.schema == expectedSchema)
      assert(ts(0).getAs[Long](TimeSeriesRDD.timeColumnName) == 1000L)
      assert(ts(0).getAs[Integer]("C1") == 7)
      assert(ts(0).getAs[Double]("C2") == 0.5)
      assert(ts.length == 12)
    }
  }

  it should "read a CSV file with header." in {
    withResource("/timeseries/csv/PriceWithHeader.csv") { source =>
      val expectedSchema =
        Schema("id" -> IntegerType, "price" -> DoubleType, "info" -> StringType)
      val timeseriesRdd = CSV.from(
        sqlContext,
        "file://" + source,
        header = true,
        sorted = true
      )
      val ts = timeseriesRdd.collect()

      assert(timeseriesRdd.schema == expectedSchema)
      assert(ts(0).getAs[Long](TimeSeriesRDD.timeColumnName) == 1000L)
      assert(ts(0).getAs[Integer]("id") == 7)
      assert(ts(0).getAs[Double]("price") == 0.5)
      assert(ts.length == 12)
    }
  }

  it should "read a CSV file with header and keep origin time column." in {
    withResource("/timeseries/csv/PriceWithHeader.csv") { source =>
      val expectedSchema = Schema(
        "time_" -> IntegerType,
        "id" -> IntegerType,
        "price" -> DoubleType,
        "info" -> StringType
      )
      val timeseriesRdd = CSV.from(
        sqlContext,
        "file://" + source,
        header = true,
        keepOriginTimeColumn = true,
        sorted = true
      )
      val ts = timeseriesRdd.collect()
      // we want to keep the time column first, but the order isn't guaranteed
      assert(timeseriesRdd.schema.fieldIndex(TimeSeriesRDD.timeColumnName) == 0)
      assert(timeseriesRdd.schema.fields.toSet == expectedSchema.fields.toSet)
      assert(ts(0).getAs[Long](TimeSeriesRDD.timeColumnName) == 1000L)
      assert(ts(0).getAs[Integer]("id") == 7)
      assert(ts(0).getAs[Double]("price") == 0.5)
      assert(ts.forall(_.getAs[String]("info") == "test"))
      assert(ts.length == 12)
    }
  }

  it should "read an unsorted CSV file with header" in {
    val ts1 = withResource("/timeseries/csv/PriceWithHeaderUnsorted.csv") {
      source =>
        val timeseriesRdd = CSV.from(
          sqlContext,
          "file://" + source,
          header = true,
          sorted = false
        )
        timeseriesRdd.collect()
    }
    val ts2 = withResource("/timeseries/csv/PriceWithHeader.csv") { source =>
      val timeseriesRdd =
        CSV.from(sqlContext, "file://" + source, header = true, sorted = true)
      timeseriesRdd.collect()
    }
    assert(ts1.length == ts2.length)
    assert(ts1.deep == ts2.deep)
  }

  it should "correctly convert SQL TimestampType with default format" in {
    withResource("/timeseries/csv/TimeStampsWithHeader.csv") { source =>
      val timeseriesRdd =
        CSV.from(sqlContext, "file://" + source, header = true, sorted = false)
      val first = timeseriesRdd.first()

      val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
      format.setTimeZone(TimeZone.getTimeZone("UTC"))

      assert(
        first.getAs[Long]("time") == format
          .parse("2008-01-02 00:00:00.000")
          .getTime * 1000000L
      )
    }
  }

  it should "correctly convert SQL TimestampType with default format and explicit schema" in {
    withResource("/timeseries/csv/TimeStampsWithHeader.csv") { source =>
      val expectedSchema = Schema("time" -> TimestampType)
      val timeseriesRdd =
        CSV.from(
          sqlContext,
          "file://" + source,
          header = true,
          sorted = false,
          schema = expectedSchema
        )
      val first = timeseriesRdd.first()

      val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
      format.setTimeZone(TimeZone.getTimeZone("UTC"))

      assert(
        first.getAs[Long]("time") == format
          .parse("2008-01-02 00:00:00.000")
          .getTime * 1000000L
      )
    }
  }

  it should "correctly convert SQL TimestampType with explicit format" in {
    withResource("/timeseries/csv/TimeStampsWithHeader.csv") { source =>
      val specifiedFormat = "yyyy-MM-dd HH:mm:ss.S"
      val timeseriesRdd = CSV.from(
        sqlContext,
        "file://" + source,
        header = true,
        sorted = false,
        dateFormat = specifiedFormat
      )
      val first = timeseriesRdd.first()

      val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
      format.setTimeZone(TimeZone.getTimeZone("UTC"))

      assert(
        first.getAs[Long]("time") == format
          .parse("2008-01-02 00:00:00.000")
          .getTime * 1000000L
      )
    }
  }

  it should "correctly convert SQL TimestampType with specified format and explicit schema, V2" in {
    withResource("/timeseries/csv/TimeStampsWithHeaderV2.csv") { source =>
      val expectedSchema = Schema("time" -> TimestampType)
      val specifiedFormat = "yyyy-MM-dd'T'HH:mm:ss.S"

      val timeseriesRdd =
        CSV.from(
          sqlContext,
          "file://" + source,
          header = true,
          sorted = false,
          schema = expectedSchema,
          dateFormat = specifiedFormat
        )
      val first = timeseriesRdd.first()

      val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
      format.setTimeZone(TimeZone.getTimeZone("UTC"))

      assert(
        first.getAs[Long]("time") == format
          .parse("2008-01-02 00:00:00.000")
          .getTime * 1000000L
      )
    }
  }

  it should "correctly convert SQL TimestampType with explicit format, V2" in {
    withResource("/timeseries/csv/TimeStampsWithHeaderV2.csv") { source =>
      val specifiedFormat = "yyyy-MM-dd'T'HH:mm:ss.S"

      val timeseriesRdd = CSV.from(
        sqlContext,
        "file://" + source,
        header = true,
        sorted = false,
        dateFormat = specifiedFormat
      )
      val first = timeseriesRdd.first()

      val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
      format.setTimeZone(TimeZone.getTimeZone("UTC"))

      assert(
        first.getAs[Long]("time") === format
          .parse("2008-01-02 00:00:00.000")
          .getTime * 1000000L
      )
    }
  }

  it should "correctly convert SQL TimestampType with specified format, V2" in {
    withResource("/timeseries/csv/TimeStampsWithHeaderV2.csv") { source =>
      val specifiedFormat = "yyyy-MM-dd'T'HH:mm:ss.S"
      val timeseriesRdd = CSV.from(
        sqlContext,
        "file://" + source,
        header = true,
        sorted = false,
        dateFormat = specifiedFormat
      )
      val first = timeseriesRdd.first()

      val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
      format.setTimeZone(TimeZone.getTimeZone("UTC"))

      assert(
        first.getAs[Long]("time") == format
          .parse("2008-01-02 00:00:00.000")
          .getTime * 1000000L
      )
    }
  }

  it should "correctly convert SQL TimestampType with specified format" in {
    withResource("/timeseries/csv/TimeStampsWithHeader2.csv") { source =>
      val specifiedFormat = "yyyyMMdd'T'HH:mm:ssZ"
      val timeseriesRdd = CSV.from(
        sqlContext,
        "file://" + source,
        header = true,
        sorted = false,
        dateFormat = specifiedFormat
      )
      val first = timeseriesRdd.first()

      val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
      format.setTimeZone(TimeZone.getTimeZone("UTC"))

      assert(
        first.getAs[Long]("time") == format
          .parse("2008-01-02 00:00:00.000")
          .getTime * 1000000L
      )
    }
  }

  it should "correctly parse datetimes" in {
    val specifiedFormat =
      new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    specifiedFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

    val lhs = specifiedFormat.parse("2008-01-02 00:00:00.000").getTime

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    format.setTimeZone(TimeZone.getTimeZone("UTC"))

    val rhs = format.parse("2008-01-02 00:00:00.000").getTime

    assert(lhs === rhs)
  }

  it should "correctly parse datetimes2" in {
    val specifiedFormat =
      new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S")
    specifiedFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

    val lhs = specifiedFormat.parse("2008-01-02T00:00:00.000").getTime

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    format.setTimeZone(TimeZone.getTimeZone("UTC"))

    val rhs = format.parse("2008-01-02 00:00:00.000").getTime

    assert(lhs === rhs)
  }
}
