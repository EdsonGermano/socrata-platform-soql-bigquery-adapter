package com.socrata.bq.soql

import com.rojoma.json.v3.ast.JValue
import com.google.api.services.bigquery.model.TableFieldSchema

trait BBQRepBase[Type] {
  def repType: Type
}

trait BBQReadRep[Type, Value] extends BBQRepBase[Type] {
  def SoQL(row : Seq[String]) : Value
  def numColumns : Int
}

trait BBQWriteRep[Type, Value] extends BBQRepBase[Type] {
  val bigqueryType : String
  def bigqueryFieldSchema : TableFieldSchema = bigqueryType match {
    case "RECORD" => ???
    case _ => new TableFieldSchema().setType(bigqueryType)
  }
  def jvalue(value: Value) : JValue
}

trait BBQRep[Type, Value] extends BBQReadRep[Type, Value] with BBQWriteRep[Type, Value]
