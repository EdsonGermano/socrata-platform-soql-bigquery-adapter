package com.socrata.bq.soql

import com.rojoma.json.v3.ast.JValue

trait BigQueryRep[Type] {
  def repType: Type
//  def base: String
}

trait BigQueryReadRep[Type, Value] extends BigQueryRep[Type] {
  def SoQL(value : String) : Value
  def numColumns() : Long
}

trait BigQueryWriteRep[Type, Value] extends BigQueryRep[Type] {
  val bigqueryType : String
  def jvalue(value: Value) : JValue
}
