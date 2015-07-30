package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JNumber, JObject, JNull, JValue}
import com.socrata.bq.soql.{BigQueryWriteRep, BigQueryReadRep}
import com.socrata.soql.types.{SoQLNull, SoQLPoint, SoQLValue, SoQLType}
import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate, Point}

class PointRep extends BigQueryReadRep[SoQLType, SoQLValue] with BigQueryWriteRep[SoQLType, SoQLValue] {

  val geomFactory = new GeometryFactory()

  override def repType: SoQLType = SoQLPoint

  override val bigqueryType: String = "RECORD"

  // Parses points return from big query in the form "long,lat"
  override def SoQL(row: Seq[String], index: Int): SoQLValue = {
    val x = row(index)
    val y = row(index+1)
    if (x == null || y == null) SoQLNull
    else {
      SoQLPoint(geomFactory.createPoint(new Coordinate(x.toDouble, y.toDouble)))
    }
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == null) JNull
    else JObject(Map(
      "lat" -> JNumber(value.asInstanceOf[SoQLPoint].value.getY),
      "long" -> JNumber(value.asInstanceOf[SoQLPoint].value.getX)
    ))
  }

  override def numColumns: Int = 2
}
