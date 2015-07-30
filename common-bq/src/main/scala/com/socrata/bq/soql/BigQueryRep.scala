package com.socrata.bq.soql

import com.rojoma.json.v3.ast.JValue
import com.google.api.services.bigquery.model.TableFieldSchema

trait BigQueryRepBase[Type] {
  def repType: Type
//  def base: String
}

trait BigQueryReadRep[Type, Value] extends BigQueryRepBase[Type] {
  def SoQL(row : Seq[String], index: Int) : Value
  def numColumns : Int
}

trait BigQueryWriteRep[Type, Value] extends BigQueryRepBase[Type] {
  val bigqueryType : String
  def bigqueryFieldSchema : TableFieldSchema = new TableFieldSchema().setType(bigqueryType)
  def jvalue(value: Value) : JValue
}

trait BigQueryRep[Type, Value] extends BigQueryReadRep[Type, Value] with BigQueryWriteRep[Type, Value]
