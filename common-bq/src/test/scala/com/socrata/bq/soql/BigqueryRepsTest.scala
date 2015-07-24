package com.socrata.bq.soql

import com.socrata.soql.types._
import org.joda.time.DateTime
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.{Matchers, FunSuite}
import org.scalatest.prop.PropertyChecks

object DateTimes {
  private val dtGen = for {
    year <- Gen.choose(1, 3000)
    month <- Gen.choose(1, 12)
    day <- Gen.choose(1, 28)
    hour <- Gen.choose(0, 23)
    minute <- Gen.choose(0, 59)
    second <- Gen.choose(0, 59)
    ms <- Gen.choose(0, 999)
  } yield new DateTime(year, month, day, hour, minute, second, ms)

  implicit val valid: Arbitrary[DateTime] = Arbitrary(dtGen)
}

class BigqueryRepsTest extends FunSuite with Matchers with PropertyChecks {

  import DateTimes._

  test("SoQLBoolean") {
    val s = BigQueryRepFactory(SoQLBoolean).SoQL("true")
    s.typ should be (SoQLBoolean)
    s.asInstanceOf[SoQLBoolean].value should be (true)
    s should be (SoQLBoolean(true))
  }

  test("SoQLNumber") {
    val s = BigQueryRepFactory(SoQLNumber).SoQL("3.14")
    s.typ should be (SoQLNumber)
    s.asInstanceOf[SoQLNumber].value should be (new java.math.BigDecimal(3.14))
    s should be (SoQLNumber(new java.math.BigDecimal(3.14)))
  }

  test("SoQLText") {
    forAll { (str: String) =>
      val s = BigQueryRepFactory(SoQLText).SoQL(str)
      s.typ should be (SoQLText)
      s.asInstanceOf[SoQLText].value should be (str)
      s should be (SoQLText(str))

    }
  }

  test("SoQLFixedTimestamp") {
    forAll { (date: DateTime) =>
      println(date)
      val s = BigQueryRepFactory(SoQLFixedTimestamp).SoQL(date.getMillis + "000")
      s.typ should be (SoQLFixedTimestamp)
      s.asInstanceOf[SoQLFixedTimestamp].value should be (date)
      s should be (SoQLFixedTimestamp(date))
    }
  }

  test("SoQLDouble") {
    forAll { (d: Double) =>
      val s = BigQueryRepFactory(SoQLDouble).SoQL(d.toString)
      s.typ should be (SoQLDouble)
      s.asInstanceOf[SoQLDouble].value should be (d)
      s should be (SoQLDouble(d))
    }
  }
}
