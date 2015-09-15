package com.socrata.bq.soql.bqreps

import collection.JavaConversions._

import com.rojoma.json.v3.ast.{JNumber, JObject, JNull, JValue}
import com.socrata.bq.soql.BBQRep
import com.socrata.soql.types.{SoQLNull, SoQLPoint, SoQLValue, SoQLType}
import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate, Point}
import com.google.api.services.bigquery.model.TableFieldSchema

class PointRep extends BBQRep[SoQLType, SoQLValue] {

  val geomFactory = new GeometryFactory()

  override def repType: SoQLType = SoQLPoint

  override val bigqueryType: String = "RECORD"

  override def SoQL(row: Seq[String]): SoQLValue = {
    // Points are written to BigQuery as (lat, long), so reverse the order after
    // reading the data returned to match the GeoJSON format of (long, lat).
    val y = row(0)
    val x = row(1)
    if (x == null || y == null) SoQLNull
    else SoQLPoint(geomFactory.createPoint(new Coordinate(x.toDouble, y.toDouble)))
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
    // The SoQLValue is broken down into its latitude and longitude components, which are stored in separate
    // columns in BigQuery
    if (value == SoQLNull) JObject(Map(
      "lat" -> JNull,
      "long" -> JNull
    ))
    else JObject(Map(
      "lat" -> JNumber(value.asInstanceOf[SoQLPoint].value.getY),
      "long" -> JNumber(value.asInstanceOf[SoQLPoint].value.getX)
    ))
  }

  override def numColumns: Int = 2
}
