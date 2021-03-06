package com.socrata.bq.soql

import com.socrata.bq.soql.bqreps.MultiPolygonRep.BoundingBoxRep
import javax.xml.bind.DatatypeConverter.printBase64Binary
import com.socrata.soql.types._
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
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
  } yield new DateTime(year, month, day, hour, minute, second, ms, DateTimeZone.UTC)

  implicit val validDt: Arbitrary[DateTime] = Arbitrary(dtGen)
}

class BBQRepsTest extends FunSuite with Matchers with PropertyChecks {

  import DateTimes._

  test("SoQLBoolean") {
    val s = BBQRepFactory(SoQLBoolean).SoQL(Seq("true"))
    s.typ should be (SoQLBoolean)
    s.asInstanceOf[SoQLBoolean].value should be (true)
    s should be (SoQLBoolean(true))

    val s2 = BBQRepFactory(SoQLBoolean).SoQL(Seq("false"))
    s2.typ should be (SoQLBoolean)
    s2.asInstanceOf[SoQLBoolean].value should be (false)
    s2 should be (SoQLBoolean(false))
  }

  test("SoQLNumber") {
    forAll { d : Double =>
      val s = BBQRepFactory(SoQLNumber).SoQL(Seq(d.toString))
      val bd = new java.math.BigDecimal(d.toString)
      s.typ should be (SoQLNumber)
      s.asInstanceOf[SoQLNumber].value should be (bd)
      s should be (SoQLNumber(bd))
    }
  }

  test("SoQLMoney") {
    forAll { m : Double =>
      val s = BBQRepFactory(SoQLMoney).SoQL(Seq(m.toString))
      val bd = new java.math.BigDecimal(m.toString)
      s.typ should be (SoQLMoney)
      s.asInstanceOf[SoQLMoney].value should be (bd)
      s should be (SoQLMoney(bd))
    }
  }

  test("SoQLText") {
    forAll { (str: String) =>
      val s = BBQRepFactory(SoQLText).SoQL(Seq(str))
      s.typ should be (SoQLText)
      s.asInstanceOf[SoQLText].value should be (str)
      s should be (SoQLText(str))
    }
  }

  test("SoQLFixedTimestamp") {
    forAll { (date: DateTime) =>
      val s = BBQRepFactory(SoQLFixedTimestamp).SoQL(Seq(date.getMillis.toString))
      s.typ should be (SoQLFixedTimestamp)
      s.asInstanceOf[SoQLFixedTimestamp].value should be (date)
      s should be (SoQLFixedTimestamp(date))

    }
  }

  test("SoQLFloatingTimestamp") {
    forAll { (date: DateTime) =>
      val s = BBQRepFactory(SoQLFloatingTimestamp).SoQL(Seq(date.getMillis.toString))
      val ldt = new LocalDateTime(date.getMillis, DateTimeZone.UTC)
      s.typ should be (SoQLFloatingTimestamp)
      s.asInstanceOf[SoQLFloatingTimestamp].value should be (ldt)
      s should be (SoQLFloatingTimestamp(ldt))
    }
  }

  test("SoQLDouble") {
    forAll { (d: Double) =>
      val s = BBQRepFactory(SoQLDouble).SoQL(Seq(d.toString))
      s.typ should be (SoQLDouble)
      s.asInstanceOf[SoQLDouble].value should be (d)
      s should be (SoQLDouble(d))
    }
  }

  test("SoQLVersion") {
    forAll { (l: Long) =>
      val s = BBQRepFactory(SoQLVersion).SoQL(Seq(l.toString))
      s.typ should be (SoQLVersion)
      s.asInstanceOf[SoQLVersion].value should be (l)
      s should be (SoQLVersion(l))
    }
  }

  test("SoQLID") {
    forAll { (l: Long) =>
      val s = BBQRepFactory(SoQLID).SoQL(Seq(l.toString))
      s.typ should be (SoQLID)
      s.asInstanceOf[SoQLID].value should be (l)
      s should be (SoQLID(l))
    }
  }

  test("SoQLPoint") {
    val geomFactory = new GeometryFactory()
    forAll { x: Tuple2[Double, Double] =>
      val s = BBQRepFactory(SoQLPoint).SoQL(Seq(x._1.toString, x._2.toString))
      val point = geomFactory.createPoint(new Coordinate(x._2, x._1))
      s.typ should be (SoQLPoint)
      s.asInstanceOf[SoQLPoint].value should be (point)
      s should be (SoQLPoint(point))
    }
  }

  test("SoQLMultiPolygon") {
    val wkbWriter = new WKBWriter()
    val wkbReader = new WKBReader()
    val geomFactory = new GeometryFactory()
    forAll { x: Tuple4[Double, Double, Double, Double] =>
      val rect = geomFactory.createMultiPolygon(
        Array(geomFactory.createPolygon(geomFactory.createLinearRing(Array(
          new Coordinate(x._2, x._1),
          new Coordinate(x._2, x._3),
          new Coordinate(x._4, x._3),
          new Coordinate(x._4, x._1),
          new Coordinate(x._2, x._1)
        )))))
      val s = BBQRepFactory(SoQLMultiPolygon).SoQL(Seq(printBase64Binary(wkbWriter.write(rect))))

      s.typ should be (SoQLMultiPolygon)
      s.asInstanceOf[SoQLMultiPolygon].value should be (rect)
      s should be (SoQLMultiPolygon(rect))
    }
  }

  test("SoQLMultiPolygon (BoundingBoxRep)") {
    val wkbWriter = new WKBWriter()
    val geomFactory = new GeometryFactory()
    forAll { x: Tuple4[Double, Double, Double, Double] =>
      val rep = new BoundingBoxRep
      val s = rep.SoQL(Seq(x._1.toString, x._2.toString, x._3.toString, x._4.toString))
      val rect = geomFactory.createMultiPolygon(
        Array(geomFactory.createPolygon(geomFactory.createLinearRing(Array(
        new Coordinate(x._2, x._1),
        new Coordinate(x._2, x._3),
        new Coordinate(x._4, x._3),
        new Coordinate(x._4, x._1),
        new Coordinate(x._2, x._1)
      )))))

      s.typ should be (SoQLMultiPolygon)
      s.asInstanceOf[SoQLMultiPolygon].value should be (rect)
      s should be (SoQLMultiPolygon(rect))
    }
  }
}
