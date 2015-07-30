package com.socrata.bq.soql.bqreps

import collection.JavaConversions._

import com.rojoma.json.v3.ast.{JNumber, JObject, JNull, JValue}
import com.socrata.bq.soql.{BigQueryRep}
import com.socrata.soql.types.{SoQLNull, SoQLPoint, SoQLValue, SoQLType}
import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate, Point}
import com.google.api.services.bigquery.model.TableFieldSchema

class PointRep extends BigQueryRep[SoQLType, SoQLValue] {

  val geomFactory = new GeometryFactory()

  override def repType: SoQLType = SoQLPoint

  override val bigqueryType: String = "RECORD"

  // Parses points in the form "{long},{lat}"
  // Points aren't actually returned as "{long},{lat}" from bigquery, but we encode them to satisfy
  // BigQueryReadRep's interface.
  override def SoQL(row: Seq[String], index: Int): SoQLValue = {
    val x = row(index)
    val y = row(index+1)
    if (x == null || y == null) SoQLNull
    else {
      SoQLPoint(geomFactory.createPoint(new Coordinate(y.toDouble, x.toDouble)))
    }
  }

  override def bigqueryFieldSchema() = {
    new TableFieldSchema()
        .setType(bigqueryType)
        .setFields(List(
            new TableFieldSchema()
             .setName("long")
             .setType("FLOAT"),
            new TableFieldSchema()
             .setName("lat")
             .setType("FLOAT")
        ))
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
