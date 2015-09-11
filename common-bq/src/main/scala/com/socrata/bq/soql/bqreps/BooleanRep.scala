package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JBoolean, JValue, JNull}
import com.socrata.bq.soql.BBQRep
import com.socrata.soql.types.{SoQLNull, SoQLBoolean, SoQLType, SoQLValue}

class BooleanRep extends BBQRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLBoolean

  override val bigqueryType: String = "BOOLEAN"

  override def SoQL(row: Seq[String]): SoQLValue = {
    row(0) match {
      case null => SoQLNull
      case "true" => SoQLBoolean(true)
      case _ => SoQLBoolean(false)
    }
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else JBoolean(value.asInstanceOf[SoQLBoolean].value)
  }

  override def numColumns: Int = 1
}
