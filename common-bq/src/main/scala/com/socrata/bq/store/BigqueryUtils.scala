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
    if (userColumnId.underlying.charAt(0) == ':') {
      // here we expect userColumnId.underlying to be like `:snake_case_identifier`
      val name = userColumnId.underlying.substring(1)
      s"s_${name}_${columnId.underlying}"
    } else {
      // here we expect userColumnId.underlying to be like `a2c4-1b3d`
      val name = userColumnId.underlying.replace('-', '_')
      s"u_${name}_${columnId.underlying}"
    }
  }

  def makeColumnNameMap(soqlSchema: ColumnIdMap[SecondaryColumnInfo[SoQLType]]): ColumnIdMap[String] = {
    soqlSchema.transform( (id, info) => makeColumnName(id, info.id) )
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

  def makeLoadJob(ref: TableReference) = {
    val config = new JobConfigurationLoad()
            .setDestinationTable(ref)
            .setSourceFormat("NEWLINE_DELIMITED_JSON")
    new Job().setConfiguration(new JobConfiguration().setLoad(config))
  }

  def getCopyNumber(datasetId: Long): Option[Long] = {
    for (conn <- managed(getConnection())) {
      val query = s"SELECT copy_number FROM $COPY_INFO_TABLE WHERE dataset_id=$datasetId"
      val stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      val resultSet = stmt.executeQuery(query)
      if (resultSet.first()) {
        // result set has a row
        val copyNumber = resultSet.getInt("copy_number")
        return Some(copyNumber)
      }
    }
    None
  }

  def getDataVersion(datasetId: Long): Option[Long] = {
    for (conn <- managed(getConnection())) {
      val query = s"SELECT data_version FROM $COPY_INFO_TABLE WHERE dataset_id=$datasetId"
      val stmt = conn.createStatement()
      val resultSet = stmt.executeQuery(query)
      if (resultSet.first()) {
        // result set has a row
        val dataVersion = resultSet.getInt("data_version")
        return Some(dataVersion)
      }
    }
    None
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
