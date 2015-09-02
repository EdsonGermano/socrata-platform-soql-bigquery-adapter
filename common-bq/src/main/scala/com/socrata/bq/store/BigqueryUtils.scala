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

class BigqueryUtils(dsInfo: DSInfo, bqProjectId: String) extends BigqueryUtilsBase {

  private val copyInfoTable = "bbq_copy_info_2"
  private val columnMapTable = "bbq_column_map"
  private val bbqCopyInfoCreateTableStatement = s"""
    |CREATE TABLE IF NOT EXISTS $copyInfoTable (
    |  dataset_id integer PRIMARY KEY,
    |  copy_number integer,
    |  data_version integer,
    |  obfuscation_key bytea
    |);
    """.stripMargin.trim

  private val bbqColumnMapCreateTableStatement = s"""
    |CREATE TABLE IF NOT EXISTS $columnMapTable (
    |  system_id bigint,
    |  dataset_id bigint,
    |  user_column_id string,
    |  type_name string,
    |  is_system_primary_key boolean,
    |  is_user_primary_key boolean,
    |  is_version boolean,
    |  PRIMARY KEY(dataset_id, system_id)
    |);
    """.stripMargin.trim

  def makeTableReference(bqDatasetId: String, datasetInfo: DatasetInfo, copyInfo: SecondaryCopyInfo) = 
    super.makeTableReference(bqProjectId, bqDatasetId, datasetInfo, copyInfo)

    
  def getCopyNumber(datasetId: Long): Option[Long] = {
    for (conn <- managed(getConnection())) {
      conn.createStatement().execute(bbqCopyInfoCreateTableStatement)
      val query = s"SELECT copy_number FROM $copyInfoTable WHERE dataset_id=$datasetId;"
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
      conn.createStatement().execute(bbqCopyInfoCreateTableStatement)
      val query = s"SELECT data_version FROM $copyInfoTable WHERE dataset_id=$datasetId;"
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

  def getObfuscationKey(datasetId: Long): Option[Array[Byte]] = {
    for (conn <- managed(getConnection())) {
      conn.createStatement().execute(bbqCopyInfoCreateTableStatement)
      val query = s"SELECT obfuscation_key FROM $copyInfoTable WHERE dataset_id=$datasetId;"
      val stmt = conn.createStatement()
      val resultSet = stmt.executeQuery(query)
      if (resultSet.next()) {
        // result set has a row
        val obfuscationKey = resultSet.getBytes("obfuscation_key")
        return Some(obfuscationKey)
      }
    }
    None
  }

  def setMetadataEntry(datasetId: Long, copyInfo: SecondaryCopyInfo, obfuscationKey: Array[Byte]) = {
    for (conn <- managed(getConnection())) {
      val (id, copyNumber, version) = (datasetId, copyInfo.copyNumber, copyInfo.dataVersion)
      val stmt = conn.prepareStatement(s"""
        |BEGIN;
        |$bbqCopyInfoCreateTableStatement
        |LOCK TABLE ${copyInfoTable} IN SHARE MODE;
        |UPDATE ${copyInfoTable}
        |  SET (copy_number, data_version, obfuscation_key) = ('$copyNumber', '$version', ?) WHERE dataset_id='$id';
        |INSERT INTO ${copyInfoTable} (dataset_id, copy_number, data_version, obfuscation_key)
        |  SELECT $id, $copyNumber, $version, ?
        |  WHERE NOT EXISTS ( SELECT 1 FROM ${copyInfoTable} WHERE dataset_id='$id' );
        |COMMIT;""".stripMargin.trim)
      stmt.setBytes(1, obfuscationKey)
      stmt.setBytes(2, obfuscationKey)
      stmt.executeUpdate()
    }
  }

  def setSchema(datasetId: Long, schema: ColumnIdMap[SecondaryColumnInfo[SoQLType]]) = {
    for (conn <- managed(getConnection())) {
      val stmt = conn.prepareStatement(
      // TODO: Set the schema
        s"""
           |
           |
         """.stripMargin.trim)
    }
  }
  
  private def getConnection() = dsInfo.dataSource.getConnection()
}

object BigqueryUtils extends BigqueryUtilsBase

class BigqueryUtilsBase extends Logging {

  def parseDatasetId(datasetInternalName: String): Long = {
    datasetInternalName.split('.')(1).toLong
  }

  def makeTableName(datasetInternalName: String, copyNumber: Long): String = {
    datasetInternalName.replace('.', '_') + "_" + copyNumber
  }

  def makeTableReference(bqProjectId: String, bqDatasetId: String, datasetInfo: DatasetInfo, copyInfo: SecondaryCopyInfo) = {
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

  def makeTableSchema(schema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
                      columnNameMap: ColumnIdMap[String]): TableSchema = {
    // map over the values of schema, converting to bigquery TableFieldSchema
    val fields = schema.iterator.toList.sortBy(_._1.underlying).map { case (id, info) => {
      BigQueryRepFactory(info.typ)
          .bigqueryFieldSchema
          .setName(columnNameMap(id))
    }}
    new TableSchema().setFields(fields)
  }

  def makeLoadJob(ref: TableReference, truncate: Boolean = false) = {
    val config = new JobConfigurationLoad()
            .setDestinationTable(ref)
            .setSourceFormat("NEWLINE_DELIMITED_JSON")
    if (truncate) config.setWriteDisposition("TRUNCATE")
    new Job().setConfiguration(new JobConfiguration().setLoad(config))
  }

}
