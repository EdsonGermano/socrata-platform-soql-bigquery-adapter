package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JString, JNull, JValue}
import com.socrata.bq.soql.{BigQueryRep}
import com.socrata.soql.types.{SoQLID, SoQLValue, SoQLType}

class IDRep extends BigQueryRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLID

  override val bigqueryType: String = "INTEGER"

  override def SoQL(row: Seq[String], index: Int): SoQLValue = {
    // should not be null
    SoQLID(row(index).toLong)
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == null) JNull
    else JString(value.asInstanceOf[SoQLID].value.toString)
  }

  override def numColumns: Int = 1
}
