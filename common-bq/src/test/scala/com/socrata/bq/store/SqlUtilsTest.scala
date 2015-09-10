package com.socrata.bq.store

import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, FunSuite}
import org.scalacheck.{Arbitrary, Gen}

class SqlUtilsTest extends FunSuite with Matchers with PropertyChecks {

  test("backslash") {
    val e = SqlUtils.escapeString("hello \\ there \\ this is a \\ test")
    e should be ("hello \\\\ there \\\\ this is a \\\\ test")
  }

  test("single quote") {
    val e = SqlUtils.escapeString("Justin\'s carrot")
    e should be ("Justin\\'s carrot")
  }

  test("double quote") {
    val e = SqlUtils.escapeString("\"I live for food,\" said Justin")
    e should be ("\\\"I live for food,\\\" said Justin")
  }

  test("Special character: \\b") {
    val e = SqlUtils.escapeString("\b is a weird \b character\b")
    e should be ("\\b is a weird \\b character\\b")
  }

  test("Special character: \\f") {
    val e = SqlUtils.escapeString("\f is a weird \f character\f")
    e should be ("\\f is a weird \\f character\\f")
  }

  test("Special character: \\n") {
    val e = SqlUtils.escapeString("\n is a weird \n character\n")
    e should be ("\\n is a weird \\n character\\n")
  }

  test("Special character: \\r") {
    val e = SqlUtils.escapeString("\r is a weird \r character\r")
    e should be ("\\r is a weird \\r character\\r")
  }

  test("String literal w/o special chars, single quote, or double quote") {
    forAll(Gen.alphaStr) { s: String =>
      println(s)
      SqlUtils.escapeString(s) should be (s)
    }
  }

}
