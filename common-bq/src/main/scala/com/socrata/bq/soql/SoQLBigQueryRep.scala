package com.socrata.bq.soql

import scala.collection.mutable

trait SoQLBigQueryRep[Type] {
  def repType: Type
  def base: String
}

trait SoQLBigQueryReadRep[Type, Value] extends SoQLBigQueryRep[Type] {
  def toSoQL(value : String) : Value
}
