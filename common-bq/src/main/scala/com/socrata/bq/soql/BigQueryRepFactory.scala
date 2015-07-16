package com.socrata.bq.soql

import com.socrata.bq.soql.bqreps._
import com.socrata.soql.types._

object BigQueryRepFactory {

  private val RepFactory = Map[SoQLType, SoQLBigQueryReadRep[SoQLType, SoQLValue]](
    SoQLText -> new TextRep,
    SoQLNumber -> new NumberLikeRep(SoQLNumber(_)),
    SoQLFixedTimestamp -> new FixedTimestampRep,
    SoQLDate -> new DateRep,
    SoQLDouble -> new DoubleRep
  )

  def bqRep(givenType : SoQLType) : SoQLBigQueryReadRep[SoQLType, SoQLValue] = {
    RepFactory(givenType)
  }
}
