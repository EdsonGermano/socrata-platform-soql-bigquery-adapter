package com.socrata.bq.soql

import com.socrata.bq.soql.bqreps._
import com.socrata.soql.types._

class BigQueryRepFactory {

  private val RepFactory = Map[SoQLType, String => SoQLBigQueryReadRep[SoQLType, SoQLValue]](
    SoQLText -> (base => new TextRep(base))
  )

  def bqRep(givenType : SoQLType, base : String) : SoQLBigQueryReadRep[SoQLType, SoQLValue] = {
    RepFactory(givenType)(base)
  }
}
