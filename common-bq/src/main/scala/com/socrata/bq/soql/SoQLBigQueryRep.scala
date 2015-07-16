package com.socrata.bq.soql

import com.socrata.soql.types.SoQLType

import scala.collection.mutable

trait SoQLBigQueryRep[Type] {
  def repType: Type
//  def base: String
}

trait SoQLBigQueryReadRep[Type, Value] extends SoQLBigQueryRep[Type] {
  def SoQL(value : String) : Value
}

trait SoQLBigQueryWriteRep[Type, Value] extends SoQLBigQueryRep[Type] {
  def BigQueryType(soqlType : Type) : String
}
