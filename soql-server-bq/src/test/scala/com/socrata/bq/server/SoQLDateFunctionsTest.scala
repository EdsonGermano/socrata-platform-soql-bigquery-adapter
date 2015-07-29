package com.socrata.bq.server

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class SoQLDateFunctionsTest extends QueryTest {

  test("extract month") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("10"), mutable.Buffer("2"), mutable.Buffer("10"))
    queryAndCompare(s"SELECT MONTH(timestamp) FROM ${QueryTest.FULL_TABLE_NAME} LIMIT 3", Some(expected), 3)
  }

  test("extract hour") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("23"), mutable.Buffer("16"), mutable.Buffer("1"))
    queryAndCompare(s"SELECT HOUR(timestamp) FROM ${QueryTest.FULL_TABLE_NAME} LIMIT 3", Some(expected), 3)
  }

  test("extract minute") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("55"), mutable.Buffer("36"), mutable.Buffer("44"))
    queryAndCompare(s"SELECT MINUTE(timestamp) FROM ${QueryTest.FULL_TABLE_NAME} LIMIT 3", Some(expected), 3)
  }

}
