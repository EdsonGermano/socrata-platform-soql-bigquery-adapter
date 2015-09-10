package com.socrata.bq.store

import com.rojoma.simplearm.util._
import com.socrata.soql.types._
import com.socrata.datacoordinator.util.collection.{UserColumnIdMap, ColumnIdMap}
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.id._
import com.socrata.bq.soql.BigQueryRepFactory
import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.truth.metadata.{Schema}
import com.socrata.datacoordinator.util.NullCache
import com.socrata.soql.environment.TypeName

import collection.JavaConversions._
import java.sql.{ResultSet, Timestamp}
import com.typesafe.scalalogging.slf4j.Logging
import com.google.api.services.bigquery.model._
import org.joda.time.DateTime

case class BBQColumnInfo(userColumnId: UserColumnId, soqlTypeName: String) {
  val typ = SoQLType.typesByName(new TypeName(soqlTypeName))
}

case class BBQDatasetInfo(datasetId: Long, copyNumber: Long, dataVersion: Long, lastModified: DateTime, locale: String, obfuscationKey: Array[Byte])

class BigqueryUtils(dsInfo: DSInfo, bqProjectId: String) extends BigqueryMetadataHandler(dsInfo) with BigqueryUtilsBase {
  def makeTableReference(bqDatasetId: String, datasetInfo: DatasetInfo, copyInfo: CopyInfo) =
    super.makeTableReference(bqProjectId, bqDatasetId, datasetInfo, copyInfo)
}

protected abstract class BigqueryMetadataHandler(dsInfo: DSInfo) extends BigqueryUtilsBase {
  private val copyInfoTable = "bbq_copy_info_2"
  private val columnMapTable = "bbq_column_map"
  private val bbqCopyInfoCreateTableStatement = s"""
    |CREATE TABLE IF NOT EXISTS $copyInfoTable (
    |  dataset_id bigint PRIMARY KEY,
    |  copy_number bigint,
    |  data_version bigint,
    |  last_modified timestamp with time zone,
    |  locale character varying(40),
    |  obfuscation_key bytea
    |);
    """.stripMargin.trim

  private val bbqColumnMapCreateTableStatement = s"""
    |CREATE TABLE IF NOT EXISTS $columnMapTable (
    |  system_id bigint,
    |  dataset_id bigint,
    |  user_column_id character varying(40),
    |  type_name character varying(40),
    |  is_system_primary_key boolean,
    |  is_user_primary_key boolean,
    |  is_version boolean,
    |  PRIMARY KEY(dataset_id, system_id)
    |);
    """.stripMargin.trim

  private val schemaHasher = new BBQSchemaHasher[SoQLType, Nothing](SoQLTypeContext.typeNamespace.userTypeForType, NullCache)

  private def getMetadataEntry(datasetId: Long): Option[BBQDatasetInfo] = {
    for (conn <- managed(getConnection())) {
      conn.createStatement().execute(bbqCopyInfoCreateTableStatement)
      val query = s"SELECT copy_number, data_version, locale, last_modified, obfuscation_key FROM $copyInfoTable WHERE dataset_id=?;"
      val stmt = conn.prepareStatement(query)
      stmt.setLong(1, datasetId)
      val resultSet = stmt.executeQuery()
      if (resultSet.next()) {
        // result set has a row
        val copyNumber = resultSet.getLong("copy_number")
        val dataVersion = resultSet.getLong("data_version")
        val lastModified = new DateTime(resultSet.getTimestamp("last_modified").getTime)
        val locale = resultSet.getString("locale")
        val obfuscationKey = resultSet.getBytes("obfuscation_key")
        return Some(new BBQDatasetInfo(datasetId, copyNumber, dataVersion, lastModified, locale, obfuscationKey))
      }
    }
    None
  }

  def getMetadataEntry(datasetInfo: DatasetInfo): Option[BBQDatasetInfo] =
    getMetadataEntry(parseDatasetId(datasetInfo.internalName))

  def getMetadataEntry(datasetId: DatasetId): Option[BBQDatasetInfo] =
    getMetadataEntry(datasetId.underlying)

  def getMetadataEntry(datasetInternalName: String): Option[BBQDatasetInfo] =
    getMetadataEntry(parseDatasetId(datasetInternalName))

  def setMetadataEntry(datasetInfo: DatasetInfo, copyInfo: CopyInfo) = {
    val id = parseDatasetId(datasetInfo.internalName)
    val (copyNumber, version, lastModified) = (copyInfo.copyNumber, copyInfo.dataVersion, copyInfo.lastModified)
    val (locale, obfuscationKey) = (datasetInfo.localeName, datasetInfo.obfuscationKey)
    for (conn <- managed(getConnection())) {
      val query = s"""
          |BEGIN;
          |$bbqCopyInfoCreateTableStatement
          |LOCK TABLE $copyInfoTable IN SHARE MODE;
          |UPDATE $copyInfoTable
          |  SET (copy_number, data_version, last_modified, locale, obfuscation_key) = ('$copyNumber', '$version', ?, '$locale', ?)
          |  WHERE dataset_id='$id';
          |INSERT INTO $copyInfoTable (dataset_id, copy_number, data_version, last_modified, locale, obfuscation_key)
          |  SELECT '$id', '$copyNumber', '$version', ?, '$locale', ?
          |  WHERE NOT EXISTS ( SELECT 1 FROM $copyInfoTable WHERE dataset_id='$id' );
          |COMMIT;""".stripMargin.trim
      val stmt = conn.prepareStatement(query)
      val ts = new Timestamp(lastModified.getMillis)
      stmt.setTimestamp(1, ts)
      stmt.setTimestamp(3, ts)
      stmt.setBytes(2, datasetInfo.obfuscationKey)
      stmt.setBytes(4, datasetInfo.obfuscationKey)
      stmt.executeUpdate()
    }
  }

  def setSchema(datasetId: Long, schema: ColumnIdMap[ColumnInfo[SoQLType]]): Unit = {
    for (conn <- managed(getConnection())) {
      val stmt = conn.createStatement()

      // Delete the currently stored schema for this dataset
      val delete =
        s"""
           |BEGIN;
           |$bbqColumnMapCreateTableStatement
           |LOCK TABLE $columnMapTable IN SHARE MODE;
           |DELETE FROM $columnMapTable WHERE dataset_id = '$datasetId';
         """.stripMargin.trim
      stmt.execute(delete)

      // Add the new schema
      val update = conn.prepareStatement(s"INSERT INTO $columnMapTable VALUES (?, ?, ?, ?, ?, ?, ?)")
      schema.foreach { (columnId, columnInfo) =>
        update.setLong(1, columnId.underlying)
        update.setLong(2, datasetId)
        update.setString(3, columnInfo.id.underlying)
        update.setString(4, columnInfo.typ.name.toString())
        update.setBoolean(5, columnInfo.isSystemPrimaryKey)
        update.setBoolean(6, columnInfo.isUserPrimaryKey)
        update.setBoolean(7, columnInfo.isVersion)

        update.execute()
      }

      conn.createStatement().execute("COMMIT;")
    }
  }

  def setSchema(datasetInfo: DatasetInfo, schema: ColumnIdMap[ColumnInfo[SoQLType]]): Unit =
    setSchema(parseDatasetId(datasetInfo.internalName), schema)

  def setSchema(datasetInternalName: String, schema: ColumnIdMap[ColumnInfo[SoQLType]]): Unit =
    setSchema(parseDatasetId(datasetInternalName), schema)

  def getSchema(datasetInternalName: String): Option[Schema] = {
    getSchema(parseDatasetId(datasetInternalName))
  }

  def getSchema(datasetId: Long): Option[Schema] = {
    for (conn <- managed(getConnection())) {
      val stmt = conn.createStatement()
      val query =
        s"""
           |SELECT system_id, user_column_id, type_name, is_system_primary_key, is_user_primary_key, is_version
           |FROM $columnMapTable
           |WHERE dataset_id='$datasetId';
         """.stripMargin.trim
      val resultSet = stmt.executeQuery(query)
      var userColumnIdMap = UserColumnIdMap.empty[TypeName]
      var columnIds = Seq.empty[(UserColumnId, SoQLType)]
      var systemPrimaryKey: UserColumnId = new UserColumnId(":id") // in case we aren't storing the system pk
      var userPrimaryKey: Option[UserColumnId] = None

      while (resultSet.next()) {
        val userColumnId = resultSet.getString("user_column_id")
        val typeName = new TypeName(resultSet.getString("type_name"))
        val soqlType = SoQLType.typesByName.getOrElse(typeName, SoQLNull)
        val isSystemPrimaryKey = resultSet.getBoolean("is_system_primary_key")
        val isUserPrimaryKey = resultSet.getBoolean("is_user_primary_key")

        // Build up the user column id map and the column id map
        userColumnIdMap += (new UserColumnId(userColumnId), typeName)
        columnIds :+= (new UserColumnId(userColumnId), soqlType)

        if (isSystemPrimaryKey)
          systemPrimaryKey = new UserColumnId(userColumnId)

        if (isUserPrimaryKey)
          userPrimaryKey = Some(new UserColumnId(userColumnId))
      }

      // Ensure that we built up a user column id map before attempting to construct the schema
      if (!userColumnIdMap.isEmpty) {
        val (version, locale) = getMetadataEntry(datasetId) match {
          case Some(info) => (info.dataVersion, info.locale)
          case None       => (0L, "en_US")
        }
        val pk = userPrimaryKey.getOrElse(systemPrimaryKey)
        val hash = schemaHasher.schemaHash(datasetId, version, columnIds, pk, locale)
        val schema = new Schema(hash, userColumnIdMap, systemPrimaryKey, locale)
        return Some(schema)
      }
    }
    None
  }

  def getUserToSystemColumnMap(datasetId: Long): Option[Map[UserColumnId, ColumnId]] = {
    for (conn <- managed(getConnection())) {
      val stmt = conn.createStatement()
      val query = s"""SELECT system_id, user_column_id FROM $columnMapTable WHERE dataset_id='$datasetId';"""
      val resultSet = stmt.executeQuery(query)
      val systemToUserColumnMap = scala.collection.mutable.Map[UserColumnId, ColumnId]()

      while (resultSet.next()) {
        val columnId = resultSet.getInt("system_id")
        val userColumnId = resultSet.getString("user_column_id")
        systemToUserColumnMap += new UserColumnId(userColumnId) -> new ColumnId(columnId) 
      }
      if (systemToUserColumnMap.nonEmpty) {
        return Some(systemToUserColumnMap.toMap)
      }
    }
    None
  }

  private def getConnection() = dsInfo.dataSource.getConnection()
}

object BigqueryUtils extends BigqueryUtilsBase

protected trait BigqueryUtilsBase extends Logging {

  def parseDatasetId(datasetInternalName: String): Long = {
    datasetInternalName.split('.')(1).toLong
  }

  def makeTableName(datasetInternalName: String, copyNumber: Long): String = {
    datasetInternalName.replace('.', '_') + "_" + copyNumber
  }

  // The string identifier for a dataset/table combination used in query strings
  def makeFullTableIdentifier(datasetId: String, datasetInternalName: String, copyNumber: Long): String = {
    val tableName = makeTableName(datasetInternalName, copyNumber)
    s"[$datasetId.$tableName]"
  }

  def makeTableReference(bqProjectId: String, bqDatasetId: String, datasetInfo: DatasetInfo, copyInfo: CopyInfo) = {
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

  def makeColumnNameMap(soqlSchema: ColumnIdMap[ColumnInfo[SoQLType]]): ColumnIdMap[String] = {
    soqlSchema.transform( (id, info) => makeColumnName(id, info.id) )
  }

  def makeTableSchema(schema: ColumnIdMap[ColumnInfo[SoQLType]],
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
