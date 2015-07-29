package com.socrata.bq.server

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SoQLBoolFunctionsTest extends QueryTest {

  test("and") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("4839"), mutable.Buffer("4914"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE boolean AND integer > 4700", Some(expected), 2)
  }

  test ("or") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("4839"), mutable.Buffer("-4968"), mutable.Buffer("4914"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE integer < -4900 OR integer > 4700", Some(expected), 3)
  }

  test ("not") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("4914"))
    queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} WHERE NOT integer != 4914", Some(expected), 1)
  }

}
