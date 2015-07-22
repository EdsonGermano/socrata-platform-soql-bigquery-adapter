package com.socrata.bq.store

import collection.JavaConversions._

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.simplearm.Managed
import com.socrata.soql.types._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, ColumnInfo => SecondaryColumnInfo, _}
import com.socrata.datacoordinator.id.{CopyId, ColumnId, UserColumnId}
import com.typesafe.scalalogging.slf4j.Logging
import org.postgresql.ds.PGSimpleDataSource

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.ByteArrayContent
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.google.api.services.bigquery.model._

trait JsonColumnCommonRep {
  val representedType: SoQLType
}

trait JsonColumnReadRep extends JsonColumnCommonRep {
  def fromJValue(input: JValue): Option[SoQLValue]
}

trait JsonColumnWriteRep extends JsonColumnCommonRep {
  def toJValue(value: SoQLValue): JValue
  protected def stdBadValue: Nothing = sys.error("Incorrect value passed to toJValue")
}

trait JsonColumnRep extends JsonColumnReadRep with JsonColumnWriteRep

class CodecBasedJsonColumnRep[TrueCV : JsonEncode : JsonDecode](val representedType: SoQLType, unwrapper: SoQLValue => TrueCV, wrapper: TrueCV => SoQLValue) extends JsonColumnRep {
  def fromJValue(input: JValue) =
    if(JNull == input) Some(SoQLNull)
    else JsonDecode[TrueCV].decode(input).right.toOption.map(wrapper)

  def toJValue(input: SoQLValue) =
    if(SoQLNull == input) JNull
    else JsonEncode[TrueCV].encode(unwrapper(input))
}

object TextRep extends CodecBasedJsonColumnRep[String](SoQLText, _.asInstanceOf[SoQLText].value, SoQLText(_))
object NumberRep extends CodecBasedJsonColumnRep[java.math.BigDecimal](SoQLNumber, _.asInstanceOf[SoQLNumber].value, SoQLNumber(_))


class BigqueryHandler(bigquery: Bigquery, bqProjectId: String, bqDatasetId: String) extends Logging {

  // Translate SoQLTypes to Bigquery column types.
  private def translateType(typ: SoQLType): String = {
    typ match {
      case SoQLText => "STRING"
      case SoQLNumber => "FLOAT"
      case SoQLFixedTimestamp => "TIMESTAMP"
      case SoQLDate => "TIMESTAMP"
      case SoQLDouble => "FLOAT"
      case SoQLBoolean => "BOOLEAN"
      case _ => "STRING"
    }
  }

  private def encode(value: SoQLValue): JValue = {
    value match {
      case SoQLNumber(value) => JNumber(value)
      case SoQLText(value) => JString(value)
      case _ => JNull
    }
  }

  private def makeTableReference(datasetInfo: DatasetInfo, copyInfo: SecondaryCopyInfo) = {
    val dsId = datasetInfo.internalName.replace('.', '_')
    val tableId = s"${dsId}_${copyInfo.copyNumber}"
    new TableReference()
        .setProjectId(bqProjectId)
        .setDatasetId(bqDatasetId)
        .setTableId(tableId)
  }

  private def makeColumnName(columnId: ColumnId, userColumnId: UserColumnId) = {
    val parts = userColumnId.underlying.split('-')
    s"u_${parts(0)}_${parts(1)}_${columnId.underlying}"
  }

  private def isUserColumn(info: SecondaryColumnInfo[SoQLType]) = info.id.underlying.charAt(0) != ':'

  private def makeColumnNameMap(soqlSchema: ColumnIdMap[SecondaryColumnInfo[SoQLType]]): ColumnIdMap[String] = {
    soqlSchema.filter( (id, info) => isUserColumn(info) ).transform( (id, info) => makeColumnName(id, info.id) )
  }

  private def makeTableSchema(userColumnInfo: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
                              columnNameMap: ColumnIdMap[String]): TableSchema = {
    // map over the values of userColumnInfo, converting to bigquery TableFieldSchema
    val fields = userColumnInfo.iterator.toList.sortBy(_._1.underlying).map { case (id, info) => {
      new TableFieldSchema()
          .setName(columnNameMap(id))
          .setType(translateType(info.typ))
    }}
    new TableSchema().setFields(fields)
  }

  def loadRows(ref: TableReference, rows: Seq[String]) {
    val config = new JobConfigurationLoad()
            .setDestinationTable(ref)
            .setSourceFormat("NEWLINE_DELIMITED_JSON")
            .setWriteDisposition("WRITE_TRUNCATE")

    val content = new ByteArrayContent("application/octet-stream", rows.mkString("\n").toCharArray.map(_.toByte))
    val insert = bigquery.jobs.insert(bqProjectId, new Job().setConfiguration(new JobConfiguration().setLoad(config)), content)
    insert.execute()
  }

  // Handle a resync event
  def handleResync(datasetInfo: DatasetInfo,
                   copyInfo: SecondaryCopyInfo,
                   schema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
                   rows: Managed[Iterator[ColumnIdMap[SoQLValue]]]): Unit = {
    // construct ref to table
    val columnNames: ColumnIdMap[String] = makeColumnNameMap(schema)
    val ref = makeTableReference(datasetInfo, copyInfo)
    val userSchema = schema.filter( (id, info) => isUserColumn(info) )
    val bqSchema = makeTableSchema(userSchema, columnNames)
    val table = new Table()
            .setTableReference(ref)
            .setSchema(bqSchema)

    try {
      bigquery.tables.insert(bqProjectId, bqDatasetId, table).execute()
    } catch {
      case e: GoogleJsonResponseException => {
        if (e.getDetails.getCode == 409) {
          // the table already exists
          // what should be done here?
        } else {
          throw e
        }
      }
    }
    for { iter <- rows } {
      val requests = 
        for {
          row: ColumnIdMap[SoQLValue] <- iter
        } yield {
          val rowMap = row.foldLeft(Map[String, JValue]()) { case (map, (id, value)) =>
            columnNames.get(id) match {
              case None => map
              case Some(name) => map + ((name, encode(value)))
            }
          }
          JsonUtil.renderJson(rowMap)
        }

      for { batch <- requests.grouped(10000) } {
        loadRows(ref, batch)
      }
    }
  }
}
