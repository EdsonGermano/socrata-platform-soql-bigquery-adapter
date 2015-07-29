package com.socrata.bq.server

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SoQLStringFunctionsTest extends QueryTest {

  test("c < x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("Aaron"), mutable.Buffer("Abdul"))
    queryAndCompare(s"SELECT string FROM ${QueryTest.FULL_TABLE_NAME} WHERE string < 'Alyssa'", Some(expected), 2)
  }

  test("c <= x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("Aaron"), mutable.Buffer("Alyssa"), mutable.Buffer("Abdul"))
    queryAndCompare(s"SELECT string FROM ${QueryTest.FULL_TABLE_NAME} WHERE string <= 'Alyssa'", Some(expected), 3)
  }

  test("c = x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("Alyssa"))
    queryAndCompare(s"SELECT string FROM ${QueryTest.FULL_TABLE_NAME} WHERE string = 'Alyssa'", Some(expected), 1)
  }

  test("c != x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("Aaron"), mutable.Buffer("Lacey"), mutable.Buffer("Ulysses"))
    queryAndCompare(s"SELECT string FROM ${QueryTest.FULL_TABLE_NAME} WHERE string != 'Alyssa' LIMIT 3", Some(expected), 3)
  }

  test("c > x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("Zia"))
    queryAndCompare(s"SELECT string FROM ${QueryTest.FULL_TABLE_NAME} WHERE string > 'Willa'", Some(expected), 1)
  }

  test("c >= x") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("Zia"), mutable.Buffer("Willa"))
    queryAndCompare(s"SELECT string FROM ${QueryTest.FULL_TABLE_NAME} WHERE string >= 'Willa'", Some(expected), 2)
  }

  test("lower(c)") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("alyssa"))
    queryAndCompare(s"SELECT lower(string) FROM ${QueryTest.FULL_TABLE_NAME} WHERE string = 'Alyssa'", Some(expected), 1)
  }

  test("upper(c)") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("ALYSSA"))
    queryAndCompare(s"SELECT upper(string) FROM ${QueryTest.FULL_TABLE_NAME} WHERE string = 'Alyssa'", Some(expected), 1)
  }

  test("starts_with(c, x)") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("Eugenia"), mutable.Buffer("Eve"))
    queryAndCompare(s"SELECT string FROM [test.dataset] WHERE string LIKE 'E%'", Some(expected), 2)
  }

  test("contains(x)") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("Leslie"))
    queryAndCompare(s"SELECT string FROM [test.dataset] WHERE string CONTAINS 'esl'", Some(expected), 1)
  }

}
