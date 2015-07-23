package com.socrata.bq.soql

import com.socrata.bq.soql.bqreps._
import com.socrata.soql.types._

object BigQueryRepFactory {

  private val RepFactory = Map[SoQLType, BigQueryReadRep[SoQLType, SoQLValue]](
    SoQLText -> new TextRep,
    SoQLNumber -> new NumberRep,
    SoQLFixedTimestamp -> new FixedTimestampRep,
    SoQLDate -> new DateRep,    // Date may not be working correctly
    SoQLDouble -> new DoubleRep,
    SoQLBoolean -> new BooleanRep
  )

  def bqRep(givenType : SoQLType) : BigQueryReadRep[SoQLType, SoQLValue] = {
    RepFactory(givenType)
  }
}
