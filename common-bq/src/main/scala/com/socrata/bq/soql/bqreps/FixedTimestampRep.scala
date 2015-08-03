package com.socrata.bq.soql.bqreps

import com.google.api.services.bigquery.model.TableFieldSchema
import com.rojoma.json.v3.ast.{JObject, JString, JNull, JValue}
import com.socrata.bq.soql.BigQueryRep
import com.socrata.soql.types._
import org.joda.time.{DateTimeZone, DateTime}
import collection.JavaConversions._


class FixedTimestampRep extends BigQueryRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLFixedTimestamp

  override val bigqueryType: String = "RECORD"

  // TODO: this requires that the TIMESTAMP is extracted from BigQuery using TIMESTAMP_TO_USEC().
  override def SoQL(row: Seq[String]): SoQLValue = {
    if (row.head == null || row(1) == null) SoQLNull
    else SoQLFixedTimestamp(new DateTime(row.head.toLong / 1000, DateTimeZone.forID(row(1))))
  }

  override def bigqueryFieldSchema: TableFieldSchema = {
    new TableFieldSchema()
      .setType(bigqueryType)
      .setFields(List(
      new TableFieldSchema()
        .setName("ts")
        .setType("TIMESTAMP"),
      new TableFieldSchema()
        .setName("tz")
        .setType("STRING")
    ))
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else {
      val dateTime = value.asInstanceOf[SoQLFixedTimestamp].value
      JObject(Map(
        "ts" -> JString(SoQLFixedTimestamp.StringRep(dateTime)),
        "tz" -> JString(dateTime.getZone.getID)
      ))
    }
  }

  override def numColumns: Int = 2
}
