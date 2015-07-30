package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JNull, JValue}
import com.socrata.bq.soql.{BigQueryWriteRep, BigQueryReadRep}
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLType}

class NullRep extends BigQueryReadRep[SoQLType, SoQLValue] with BigQueryWriteRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLNull

  override val bigqueryType: String = "STRING"

  override def SoQL(row: Seq[String], index: Int): SoQLValue = SoQLNull

  override def jvalue(value: SoQLValue): JValue = JNull

  override def numColumns: Int = 1
}
