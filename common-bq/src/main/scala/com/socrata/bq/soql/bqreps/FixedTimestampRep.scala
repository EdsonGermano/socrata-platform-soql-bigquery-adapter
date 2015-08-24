package com.socrata.bq.soql.bqreps

import com.google.api.services.bigquery.model.TableFieldSchema
import com.rojoma.json.v3.ast.{JObject, JString, JNull, JValue}
import com.socrata.bq.soql.BigQueryRep
import com.socrata.soql.types._
import org.joda.time.{DateTimeZone, DateTime}
import collection.JavaConversions._


class FixedTimestampRep extends BigQueryRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLFixedTimestamp

  override val bigqueryType: String = "TIMESTAMP"

  // TODO: this requires that the TIMESTAMP is extracted from BigQuery using TIMESTAMP_TO_USEC().
  override def SoQL(row: Seq[String]): SoQLValue = {
    if (row.head == null) SoQLNull
    else SoQLFixedTimestamp(new DateTime(row.head.toDouble.toLong / 1000, DateTimeZone.UTC))
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else JString(SoQLFixedTimestamp.StringRep(value.asInstanceOf[SoQLFixedTimestamp].value))
  }

  override def numColumns: Int = 1
}
