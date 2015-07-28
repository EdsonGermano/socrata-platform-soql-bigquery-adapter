package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JString, JNull, JValue}
import com.socrata.bq.soql.{BigQueryWriteRep, BigQueryReadRep}
import com.socrata.soql.types.{SoQLID, SoQLValue, SoQLType}

class IDRep extends BigQueryReadRep[SoQLType, SoQLValue] with BigQueryWriteRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLID

  override val bigqueryType: String = "INTEGER"

  override def SoQL(value: String): SoQLValue = {
    // should not be null
    SoQLID(value.toLong)
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == null) JNull
    else JString(value.asInstanceOf[SoQLID].value.toString)
  }
}
