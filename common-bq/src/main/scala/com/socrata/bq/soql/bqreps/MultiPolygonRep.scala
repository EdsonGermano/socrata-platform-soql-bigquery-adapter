package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JString, JValue}
import com.socrata.bq.soql.{BigQueryReadRep, BigQueryRep}
import com.socrata.soql.types._
import com.vividsolutions.jts.geom.{MultiPolygon, Coordinate, GeometryFactory}
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}
import javax.xml.bind.DatatypeConverter.{parseBase64Binary, printBase64Binary}

class MultiPolygonRep extends BigQueryRep[SoQLType, SoQLValue] {

  private val wkbWriter = new WKBWriter()
  private val wkbReader = new WKBReader()

  override def repType: SoQLType = SoQLMultiPolygon

  override val bigqueryType: String = "STRING"

  override def jvalue(value: SoQLValue): JValue = {
    val bytes = wkbWriter.write(value.asInstanceOf[SoQLMultiPolygon].value)
    JString(printBase64Binary(bytes))
  }

  override def SoQL(row: Seq[String]): SoQLValue = {
    SoQLMultiPolygon(wkbReader.read(parseBase64Binary(row.head)).asInstanceOf[MultiPolygon])
  }

  override def numColumns: Int = 1
}

object MultiPolygonRep {
  class BoundingBoxRep extends BigQueryReadRep[SoQLType, SoQLValue] {

    private val geomFactory = new GeometryFactory()

    override def repType: SoQLType = SoQLMultiPolygon

    override def SoQL(row: Seq[String]): SoQLValue = {
      if (row(0) == null || row(1) == null || row(2) == null || row(3) == null) SoQLNull
      else {
        val minLat = row(0).toDouble
        val minLong = row(1).toDouble
        val maxLat = row(2).toDouble
        val maxLong = row(3).toDouble
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

    override def numColumns: Int = 4
  }
}
