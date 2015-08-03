package com.socrata.bq.server

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class SoQLDateFunctionsTest extends QueryTest {

  test("extract month") {
    val expected = ArrayBuffer(mutable.Buffer("1443657600000000"), ArrayBuffer("1454284800000000"), ArrayBuffer("1412121600000000"))
    queryAndCompare(s"SELECT (utc_usec_to_month(timestamp(timestamp))) FROM ${QueryTest.FULL_TABLE_NAME} LIMIT 3", Some(expected), 3)
  }

  test("extract day") {
    val expected = ArrayBuffer(mutable.Buffer[String]("1443744000000000"), ArrayBuffer("1456531200000000"), ArrayBuffer("1413936000000000"))
    queryAndCompare(s"SELECT utc_usec_to_day(timestamp(timestamp)) FROM ${QueryTest.FULL_TABLE_NAME} LIMIT 3", Some(expected), 3)
  }

  test("extract year") {
    val expected = ArrayBuffer(mutable.Buffer[String]("1420070400000000"), ArrayBuffer("1451606400000000"), ArrayBuffer("1388534400000000"))
    queryAndCompare(s"SELECT utc_usec_to_year(timestamp(timestamp)) FROM ${QueryTest.FULL_TABLE_NAME} LIMIT 3", Some(expected), 3)
  }

}
