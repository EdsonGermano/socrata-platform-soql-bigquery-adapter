package com.socrata.bq.store

import collection.JavaConversions._

import com.rojoma.json.v3.ast._
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

  def encode(value: SoQLValue): JValue = {
    value match {
      case SoQLNumber(value: java.math.BigDecimal) => JNumber(value)
      case SoQLText(value) => JString(value)
      case _ => JNull
    }
  }

  def makeTableReference(datasetInfo: DatasetInfo, copyInfo: SecondaryCopyInfo) = {
    val dsId = datasetInfo.internalName.replace('.', '_')
    val tableId = s"${dsId}_${copyInfo.copyNumber}"
    new TableReference()
        .setProjectId(bqProjectId)
        .setDatasetId(bqDatasetId)
        .setTableId(tableId)
  }

  def makeColumnName(columnId: ColumnId, userColumnId: UserColumnId) = {
    val parts = userColumnId.underlying.split('-')
    s"u_${parts(0)}_${parts(1)}_${columnId.underlying}"
  }

  def isUserColumn(id: UserColumnId) = id.underlying.charAt(0) != ':'

  def makeColumnNameMap(soqlSchema: ColumnIdMap[SecondaryColumnInfo[SoQLType]]): ColumnIdMap[String] = {
    soqlSchema.filter( (id, info) => isUserColumn(info.id) ).transform( (id, info) => makeColumnName(id, info.id) )
  }

  def makeTableSchema(userColumnInfo: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
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

}
