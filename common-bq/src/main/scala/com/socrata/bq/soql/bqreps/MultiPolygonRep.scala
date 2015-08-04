package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.JValue
import com.socrata.bq.soql.BigQueryRep
import com.socrata.soql.types.{SoQLNull, SoQLMultiPolygon, SoQLType, SoQLValue}
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}

// We are not currently storing MultiPolygons in BigQuery, just using the SoQL conversion method
// in order to properly format results for SQL queries using 'extent' (used on Data Lens pages),
// which require a bounding box represented by a SoQLMultiPolygon 
class MultiPolygonRep extends BigQueryRep[SoQLType, SoQLValue] {

  private val geomFactory = new GeometryFactory()

  override def repType: SoQLType = SoQLMultiPolygon

  override val bigqueryType: String = "RECORD"

  override def jvalue(value: SoQLValue): JValue = ???

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
