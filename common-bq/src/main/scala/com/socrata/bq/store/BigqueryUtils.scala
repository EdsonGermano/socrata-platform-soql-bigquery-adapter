package com.socrata.bq.store

import java.sql.ResultSet

import collection.JavaConversions._

import com.socrata.bq.soql.BigQueryRepFactory

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.simplearm.util._
import com.rojoma.simplearm.Managed
import com.socrata.soql.types._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
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

class BigqueryUtils(dsInfo: DSInfo, bqProjectId: String) extends Logging {

  private val COPY_INFO_TABLE = "bbq_copy_info"

  private def parseDatasetId(datasetInternalName: String): Int = {
    datasetInternalName.split('.')(1).toInt
  }

  def makeTableName(datasetId: Long): String = {
    makeTableName("primus." + datasetId, getCopyNumber(datasetId))
  }

  def makeTableName(datasetInternalName: String): String = {
    makeTableName(datasetInternalName, getCopyNumber(parseDatasetId(datasetInternalName)))
  }

  def makeTableName(datasetInternalName: String, copyNumber: Long): String = {
    datasetInternalName.replace('.', '_') + "_" + copyNumber
  }

  def makeTableReference(bqDatasetId: String, datasetInfo: DatasetInfo, copyInfo: SecondaryCopyInfo) = {
    new TableReference()
        .setProjectId(bqProjectId)
        .setDatasetId(bqDatasetId)
        .setTableId(makeTableName(datasetInfo.internalName, copyInfo.copyNumber))
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
          .setType(BigQueryRepFactory(info.typ).bigqueryType)
    }}
    new TableSchema().setFields(fields)
  }

  def loadRows(bigquery: Bigquery, ref: TableReference, rows: Seq[String]) {
    val config = new JobConfigurationLoad()
            .setDestinationTable(ref)
            .setSourceFormat("NEWLINE_DELIMITED_JSON")
            .setWriteDisposition("WRITE_TRUNCATE")

    val content = new ByteArrayContent("application/octet-stream", rows.mkString("\n").toCharArray.map(_.toByte))
    val insert = bigquery.jobs.insert(bqProjectId, new Job().setConfiguration(new JobConfiguration().setLoad(config)), content)
    insert.execute()
  }

  def getCopyNumber(datasetId: Long): Long = {
    for (conn <- managed(getConnection())) {
      val query = s"SELECT copy_number FROM $COPY_INFO_TABLE WHERE dataset_id=$datasetId"
      val stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      val resultSet = stmt.executeQuery(query)
      if (resultSet.first()) {
        // result set has a row
        val copyNumber = resultSet.getInt("copy_number")
        return copyNumber
      }
    }
    0
  }

  def getDataVersion(datasetId: Long): Long = {
    for (conn <- managed(getConnection())) {
      val query = s"SELECT data_version FROM $COPY_INFO_TABLE WHERE dataset_id=$datasetId"
      val stmt = conn.createStatement()
      val resultSet = stmt.executeQuery(query)
      if (resultSet.first()) {
        // result set has a row
        val dataVersion = resultSet.getInt("data_version")
        return dataVersion
      }
    }
    0
  }

  def setCopyInfoEntry(datasetId: Long, copyInfo: SecondaryCopyInfo) = {
    for (conn <- managed(getConnection())) {
      val stmt = conn.createStatement()
      val (id, copyNumber, version) = (datasetId, copyInfo.copyNumber, copyInfo.dataVersion)
      val query = s"""
              |BEGIN;
              |LOCK TABLE ${COPY_INFO_TABLE} IN SHARE MODE;
              |UPDATE ${COPY_INFO_TABLE}
              |  SET (copy_number, data_version) = ('$copyNumber', '$version') WHERE dataset_id='$id';
              |INSERT INTO ${COPY_INFO_TABLE} (dataset_id, copy_number, data_version)
              |  SELECT $id, $copyNumber, $version
              |  WHERE NOT EXISTS ( SELECT 1 FROM bbq_copy_info WHERE dataset_id='$id' );
              |COMMIT;""".stripMargin.trim
      stmt.executeUpdate(query)
    }
  }
  
  private def getConnection() = dsInfo.dataSource.getConnection()
}
