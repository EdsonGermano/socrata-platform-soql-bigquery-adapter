package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JNull, JValue}
import com.socrata.bq.soql.{BigqueryType, BBQRep}
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLType}

class NullRep extends BBQRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLNull

  override val bigqueryType = BigqueryType.String

  override def SoQL(cols: Seq[String]): SoQLValue = SoQLNull

  override def jvalue(value: SoQLValue): JValue = JNull

  override val numColumns: Int = 1
}
