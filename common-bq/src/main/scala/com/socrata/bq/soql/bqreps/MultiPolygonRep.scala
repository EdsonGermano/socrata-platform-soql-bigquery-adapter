package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JString, JValue}
import com.socrata.bq.soql.{BigqueryType, BBQReadRep, BBQRep}
import com.socrata.soql.types._
import com.vividsolutions.jts.geom.{MultiPolygon, Coordinate, GeometryFactory}
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import javax.xml.bind.DatatypeConverter.{parseBase64Binary, printBase64Binary}

class MultiPolygonRep extends BBQRep[SoQLType, SoQLValue] {

  private val wkbWriter = new WKBWriter()
  private val wkbReader = new WKBReader()

  override def repType: SoQLType = SoQLMultiPolygon

  override val bigqueryType = BigqueryType.String

  // Values sent to and stored in BigQuery as well-known binary
  override def jvalue(value: SoQLValue): JValue = {
    val bytes = wkbWriter.write(value.asInstanceOf[SoQLMultiPolygon].value)
    JString(printBase64Binary(bytes))
  }

  override def SoQL(cols: Seq[String]): SoQLValue = {
    SoQLMultiPolygon(wkbReader.read(parseBase64Binary(cols.head)).asInstanceOf[MultiPolygon])
  }

  override val numColumns: Int = 1
}

object MultiPolygonRep {

  /**
   * A special case of the MultiPolygonRep for converting "extent" (aka bounding box) queries, which operate
   * on points and return a multiPolygon
   */
  class BoundingBoxRep extends BBQReadRep[SoQLType, SoQLValue] {

    private val geomFactory = new GeometryFactory()

    override def repType: SoQLType = SoQLMultiPolygon

    // Expects a series of points: [(minLat, minLong), (maxLat, maxLong)]
    override def SoQL(cols: Seq[String]): SoQLValue = {
      if (cols(0) == null || cols(1) == null || cols(2) == null || cols(3) == null) SoQLNull
      else {
        val minLat = cols(0).toDouble
        val minLong = cols(1).toDouble
        val maxLat = cols(2).toDouble
        val maxLong = cols(3).toDouble
        SoQLMultiPolygon(geomFactory.createMultiPolygon(
          Array(geomFactory.createPolygon(geomFactory.createLinearRing(Array(
            new Coordinate(minLong, minLat),
            new Coordinate(minLong, maxLat),
            new Coordinate(maxLong, maxLat),
            new Coordinate(maxLong, minLat),
            new Coordinate(minLong, minLat)
          ))))))
      }
    }

    override val numColumns: Int = 4
  }
}
