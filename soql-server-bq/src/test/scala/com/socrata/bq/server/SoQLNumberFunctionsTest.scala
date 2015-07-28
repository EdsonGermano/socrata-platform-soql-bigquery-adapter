package com.socrata.bq.server

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class SoQLNumberFunctionsTest extends QueryTest {

  test("c > x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("4914"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer > 4914", Some(expected), 1)
  }

  test("c >= x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("4914"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer >= 4914", Some(expected), 1)
  }

  test("c < x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("-4968"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer < -4900", Some(expected), 1)
  }

  test("c <= x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("-4846"), mutable.Buffer("-4968"),
      mutable.Buffer("-4878"), mutable.Buffer("-4845"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer <= -4968", Some(expected), 4)
  }

  test("c = x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("-4845"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer == -4845", Some(expected), 1)
  }

  test("c != x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer(""))
  }



}
