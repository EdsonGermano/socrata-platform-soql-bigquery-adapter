package com.socrata.bq.soql

import com.rojoma.json.v3.ast.JValue
import com.google.api.services.bigquery.model.TableFieldSchema

trait BBQRepBase[Type] {
  def repType: Type
}

trait BBQReadRep[Type, Value] extends BBQRepBase[Type] {
  /**
   * Converts a sequence of columns in BigQuery to a value. Expects the length of the sequence to be `numColumns`
   */
  def SoQL(cols: Seq[String]) : Value

  /**
   * The number of columns in BigQuery to read
   */
  def numColumns: Int
}

trait BBQWriteRep[Type, Value] extends BBQRepBase[Type] {
  /**
   * Should be one of: INTEGER, STRING, BOOLEAN, FLOAT, TIMESTAMP, or RECORD
   */
  def bigqueryType: BigqueryType

  /**
   *  Table field schema to assist in constructing a table in BigQuery with support for this column
   *  This is automatically defined for columns of the type INTEGER, STRING, BOOLEAN, FLOAT, or TIMESTAMP.
   *
   *  NOTE: For RECORDS, this should be overridden to define the schema of the nested fields
   */
  def bigqueryFieldSchema: TableFieldSchema = bigqueryType match {
    case BigqueryType.Record =>
      throw new NotImplementedError("For RECORDS, this should be overridden to define the schema of the nested fields")
    case _ => new TableFieldSchema().setType(bigqueryType)
  }

  /**
   * Conversion for writing this column's value to JSON
   */
  def jvalue(value: Value): JValue
}

/**
 * Representation for a reading and writing a column in BigQuery
 */
trait BBQRep[Type, Value] extends BBQReadRep[Type, Value] with BBQWriteRep[Type, Value]


sealed abstract class BigqueryType(toString: String)

object BigqueryType {
  case object String extends BigqueryType("STRING")
  case object Integer extends BigqueryType("INTEGER")
  case object Float extends BigqueryType("FLOAT")
  case object Timestamp extends BigqueryType("TIMESTAMP")
  case object Boolean extends BigqueryType("BOOLEAN")
  case object Record extends BigqueryType("RECORD")
  implicit def stringify(typ: BigqueryType): String = typ.toString
}
