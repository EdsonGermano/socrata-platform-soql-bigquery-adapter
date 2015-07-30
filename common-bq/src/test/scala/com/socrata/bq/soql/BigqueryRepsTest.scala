package com.socrata.bq.soql

import com.socrata.soql.types._
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.joda.time.{DateTimeZone, LocalDateTime, DateTime}
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

  implicit val validDt: Arbitrary[DateTime] = Arbitrary(dtGen)
}

class BigqueryRepsTest extends FunSuite with Matchers with PropertyChecks {

  import DateTimes._

  test("SoQLBoolean") {
    val s = BigQueryRepFactory(SoQLBoolean).SoQL(Seq("true"), 0)
    s.typ should be (SoQLBoolean)
    s.asInstanceOf[SoQLBoolean].value should be (true)
    s should be (SoQLBoolean(true))

    val s2 = BigQueryRepFactory(SoQLBoolean).SoQL(Seq("false"), 0)
    s2.typ should be (SoQLBoolean)
    s2.asInstanceOf[SoQLBoolean].value should be (false)
    s2 should be (SoQLBoolean(false))
  }

  test("SoQLNumber") {
    forAll { d : Double =>
      val s = BigQueryRepFactory(SoQLNumber).SoQL(Seq(d.toString), 0)
      s.typ should be (SoQLNumber)
      s.asInstanceOf[SoQLNumber].value should be (new java.math.BigDecimal(d))
      s should be (SoQLNumber(new java.math.BigDecimal(d)))
    }
  }

  test("SoQLText") {
    forAll { (str: String) =>
      val s = BigQueryRepFactory(SoQLText).SoQL(Seq(str), 0)
      s.typ should be (SoQLText)
      s.asInstanceOf[SoQLText].value should be (str)
      s should be (SoQLText(str))
    }
  }

  test("SoQLFixedTimestamp") {
    forAll { (date: DateTime) =>
      val s = BigQueryRepFactory(SoQLFixedTimestamp).SoQL(Seq(date.getMillis + "000"), 0)
      s.typ should be (SoQLFixedTimestamp)
      s.asInstanceOf[SoQLFixedTimestamp].value should be (date)
      s should be (SoQLFixedTimestamp(date))
    }
  }

  test("SoQLFloatingTimestamp") {
    forAll { (date: DateTime) =>
      val s = BigQueryRepFactory(SoQLFloatingTimestamp).SoQL(Seq(date.getMillis + "000"), 0)
      s.typ should be (SoQLFloatingTimestamp)
      s.asInstanceOf[SoQLFloatingTimestamp].value should be (new LocalDateTime(date.getMillis, DateTimeZone.UTC))
      s should be (SoQLFloatingTimestamp(new LocalDateTime(date.getMillis, DateTimeZone.UTC)))
    }
  }

  test("SoQLDouble") {
    forAll { (d: Double) =>
      val s = BigQueryRepFactory(SoQLDouble).SoQL(Seq(d.toString), 0)
      s.typ should be (SoQLDouble)
      s.asInstanceOf[SoQLDouble].value should be (d)
      s should be (SoQLDouble(d))
    }
  }

  test("SoQLPoint") {
    val geomFactory = new GeometryFactory()
    forAll { x: Tuple2[Double, Double] =>
      val s = BigQueryRepFactory(SoQLPoint).SoQL(Seq(x._1.toString, x._2.toString), 0)
      val point = geomFactory.createPoint(new Coordinate(x._1, x._2))
      s.typ should be (SoQLPoint)
      s.asInstanceOf[SoQLPoint].value should be (point)
      s should be (SoQLPoint(point))
    }
  }
}
