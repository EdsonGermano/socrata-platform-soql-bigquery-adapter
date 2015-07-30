package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JBoolean, JValue, JNull}
import com.socrata.bq.soql.{BigQueryWriteRep, BigQueryReadRep}
import com.socrata.soql.types.{SoQLNull, SoQLBoolean, SoQLType, SoQLValue}

class BooleanRep extends BigQueryReadRep[SoQLType, SoQLValue] with BigQueryWriteRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLBoolean

  override val bigqueryType: String = "BOOLEAN"

  override def SoQL(value: String): SoQLValue = {
    if (value == null) SoQLNull
    else if (value == "true") SoQLBoolean(true)
    else SoQLBoolean(false)
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else JBoolean(value.asInstanceOf[SoQLBoolean].value)
  }

  override def numColumns(): Long = 1
}
