package com.socrata.bq.server

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class SoQLNumberFunctionsTest extends QueryTest {

  test("c > x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("4914"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer > 4913", Some(expected), 1)
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
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("-4968"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer <= -4968", Some(expected), 1)
  }

  test("c = x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("-4845"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer == -4845", Some(expected), 1)
  }

  test("c != x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("1826"), mutable.Buffer("-250"), mutable.Buffer("-2805"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer != -4845 LIMIT 3", Some(expected), 3)
  }

  test("c + x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("-4745"))
    queryAndCompare(s"SELECT integer + 100 FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer == -4845", Some(expected), 1)
  }

  test("c - x"){
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("-4945"))
    queryAndCompare(s"SELECT integer - 100 FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer == -4845", Some(expected), 1)
  }

  test("+c") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("4914"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer > +4845", Some(expected), 1)
  }

  test("-c") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("-4968"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer < -4900", Some(expected), 1)
  }

  test("c * x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("-4968"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer < -2450 * 2", Some(expected), 1)
  }

  test("c / x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("-4968"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer / 2 < -2450", Some(expected), 1)
  }

  test("c % x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("2663"), mutable.Buffer("1873"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer % 10 = 3", Some(expected), 2)
  }

}
