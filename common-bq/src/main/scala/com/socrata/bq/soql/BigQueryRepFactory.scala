package com.socrata.bq.soql

import com.socrata.bq.soql.bqreps._
import com.socrata.soql.types._

object BigQueryRepFactory {

  private val RepFactory = Map[SoQLType, BigQueryReadRep[SoQLType, SoQLValue] with BigQueryWriteRep[SoQLType, SoQLValue]](
    SoQLText -> new TextRep,
    SoQLNumber -> new NumberRep,
    SoQLFixedTimestamp -> new FixedTimestampRep,
    SoQLDate -> new DateRep,    // Date may not be working correctly
    SoQLDouble -> new DoubleRep,
    SoQLBoolean -> new BooleanRep
  )

  def apply(givenType : SoQLType) : BigQueryReadRep[SoQLType, SoQLValue] with BigQueryWriteRep[SoQLType, SoQLValue] = {
    RepFactory.getOrElse(givenType, new NullRep)
  }
}
