package com.socrata.bq.store

import com.rojoma.simplearm.util._
import com.socrata.soql.types._
import com.socrata.datacoordinator.util.collection.{UserColumnIdMap, ColumnIdMap}
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.id._
import com.socrata.bq.soql.BBQRepFactory
import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.truth.metadata.Schema
import com.socrata.datacoordinator.util.NullCache
import com.socrata.soql.environment.TypeName

import collection.JavaConversions._
import java.sql.Timestamp
import com.typesafe.scalalogging.slf4j.Logging
import com.google.api.services.bigquery.model._
import org.joda.time.DateTime

case class BBQColumnInfo(userColumnId: UserColumnId, soqlTypeName: String) {
  val typ = SoQLType.typesByName(new TypeName(soqlTypeName))
}

/**
 * A BBQDatasetInfo corresponds to a row in the copy info table
 */
case class BBQDatasetInfo(datasetId: Long, copyNumber: Long, dataVersion: Long, lastModified: DateTime, locale: String, obfuscationKey: Array[Byte])

object BBQCommon extends BBQCommonBase

/**
 * BBQCommon includes methods for accessing metadata for the Bigquery service
 * It is instantiated with a project id for brevity in makeTableReference
 */
class BBQCommon(dsInfo: DSInfo, bqProjectId: String) extends BBQCommonBase {
  private val copyInfoTable = "bbq_copy_info"
  private val columnMapTable = "bbq_column_map"
  private val schemaHasher =
    new BBQSchemaHasher[SoQLType, Nothing](SoQLTypeContext.typeNamespace.userTypeForType, NullCache) // TODO: Use a real cache

  // Ensure tables exist.
  for (conn <- managed(getConnection)) {
    val stmt = conn.createStatement()
    stmt.execute(s"""
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
    """.stripMargin.trim)
    stmt.execute(s"""
      |CREATE TABLE IF NOT EXISTS $copyInfoTable (
      |  dataset_id bigint PRIMARY KEY,
      |  copy_number bigint,
      |  data_version bigint,
      |  last_modified timestamp with time zone,
      |  locale character varying(40),
      |  obfuscation_key bytea
      |);
    """.stripMargin.trim)
  }

  /**
    * Creates a reference to a table in bigquery
   */
  def makeTableReference(bqDatasetId: String, datasetInfo: DatasetInfo, copyInfo: CopyInfo) =
  super.makeTableReference(bqProjectId, bqDatasetId, datasetInfo, copyInfo)


  /**
   * Looks up metadata for a dataset and returns a BBQDatasetInfo object
   */
  private def getMetadataEntry(datasetId: Long): Option[BBQDatasetInfo] = {
    for (conn <- managed(getConnection)) {
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

  /**
   * Retrieves a BBQDatasetInfo object containing metadata for a dataset
   */
  def getMetadataEntry(datasetInfo: DatasetInfo): Option[BBQDatasetInfo] =
    getMetadataEntry(parseDatasetId(datasetInfo.internalName))

  def getMetadataEntry(datasetId: DatasetId): Option[BBQDatasetInfo] =
    getMetadataEntry(datasetId.underlying)

  def getMetadataEntry(datasetInternalName: String): Option[BBQDatasetInfo] =
    getMetadataEntry(parseDatasetId(datasetInternalName))

  /**
   * Writes metadata for a dataset to the copy info table.
   */
  def setMetadataEntry(datasetInfo: DatasetInfo, copyInfo: CopyInfo) = {
    val id = parseDatasetId(datasetInfo.internalName)
    val (copyNumber, version, lastModified) = (copyInfo.copyNumber, copyInfo.dataVersion, copyInfo.lastModified)
    val (locale, obfuscationKey) = (datasetInfo.localeName, datasetInfo.obfuscationKey)
    for (conn <- managed(getConnection)) {
      // Naive attempt at a write-locked upsert.
      val query = s"""
          |BEGIN;
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
      stmt.setBytes(2, obfuscationKey)
      stmt.setBytes(4, obfuscationKey)
      stmt.executeUpdate()
    }
  }

  /**
   * Writes schema info for a dataset to the column map table
   */
  def setSchema(datasetId: Long, schema: ColumnIdMap[ColumnInfo[SoQLType]]): Unit = {
    for (conn <- managed(getConnection)) {
      val stmt = conn.createStatement()

      // Delete the currently stored schema for this dataset
      val delete =
        s"""
           |BEGIN;
           |LOCK TABLE $columnMapTable IN SHARE MODE;
           |DELETE FROM $columnMapTable WHERE dataset_id='$datasetId';
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

  /**
   * Retrieves the schema for a dataset
   */
  def getSchema(datasetInternalName: String): Option[Schema] = {
    getSchema(parseDatasetId(datasetInternalName))
  }

  def getSchema(datasetId: Long): Option[Schema] = {
    for (conn <- managed(getConnection)) {
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

  /**
   * Converts the schema for the given dataset to a mapping of user column id to physical column name.
   *
   * Example:
   *
   * Schema
   *    system_id  user_column_id
   *    0          :created_at
   *    1          dhla-89ak
   *    2          a9dj-23ox
   *
   * Map(
   *    :created_at -> s_created_at_0
   *    dhla-89ak -> u_dhla-89ak_1
   *    a9dj-23ox -> u_a9dj_23ox_2
   * )
   *
   * @param datasetInternalName
   * @return
   */
  def getUserToSystemColumnMap(datasetInternalName: String): Option[Map[UserColumnId, String]] = {
    val datasetId = parseDatasetId(datasetInternalName)
    for (conn <- managed(getConnection)) {
      val stmt = conn.createStatement()
      val query = s"""SELECT system_id, user_column_id FROM $columnMapTable WHERE dataset_id='$datasetId';"""
      val resultSet = stmt.executeQuery(query)
      val systemToUserColumnMap = scala.collection.mutable.Map[UserColumnId, String]()

      while (resultSet.next()) {
        val columnId = new ColumnId(resultSet.getInt("system_id"))
        val userColumnId = new UserColumnId(resultSet.getString("user_column_id"))
        systemToUserColumnMap += userColumnId -> makeColumnName(columnId, userColumnId)
      }
      if (systemToUserColumnMap.nonEmpty) {
        return Some(systemToUserColumnMap.toMap)
      }
    }
    None
  }

  /**
   * A connection to the Postgres database that stores metadata
   */
  private def getConnection = dsInfo.dataSource.getConnection
}

/**
 * Utility methods for creating columns and tables, and loading data in bigquery
 */
protected trait BBQCommonBase extends Logging {

  /**
   * Extracts a dataset id from a dataset internal name like "primus.1"
   */
  def parseDatasetId(datasetInternalName: String): Long = {
    datasetInternalName.split('.')(1).toLong
  }

  /**
   * Converts a dataset internal name to a copy-unique table name for bigquery
   */
  def makeTableName(datasetInternalName: String, copyNumber: Long): String = {
    datasetInternalName.replace('.', '_') + "_" + copyNumber
  }

  /**
   * The string identifier for a dataset/table combination used in query strings
   * e.g. SELECT * FROM [datasetId.tableName] ...
   */
  def makeFullTableIdentifier(datasetId: String, datasetInternalName: String, copyNumber: Long): String = {
    val tableName = makeTableName(datasetInternalName, copyNumber)
    s"[$datasetId.$tableName]"
  }

  /**
   * Constructs a bigquery TableReference object referring to the bigquery table for the specified copy
   */
  def makeTableReference(bqProjectId: String, bqDatasetId: String, datasetInfo: DatasetInfo, copyInfo: CopyInfo) = {
    new TableReference()
        .setProjectId(bqProjectId)
        .setDatasetId(bqDatasetId)
        .setTableId(makeTableName(datasetInfo.internalName, copyInfo.copyNumber))
  }

  /**
   * Converts a set of truth column information to a bigquery column identifier
   */
  def makeColumnName(columnId: ColumnId, userColumnId: UserColumnId): String = {
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

  /**
   * Converts a schema map to a name map, applying makeColumnName to each column
   */
  def makeColumnNameMap(soqlSchema: ColumnIdMap[ColumnInfo[SoQLType]]): ColumnIdMap[String] = {
    soqlSchema.transform( (id, info) => makeColumnName(id, info.id) )
  }

  /**
   * Constructs a bigquery TableSchema from a schema map and a corresponding column name map
   */
  def makeTableSchema(schema: ColumnIdMap[ColumnInfo[SoQLType]],
                      columnNameMap: ColumnIdMap[String]): TableSchema = {
    // map over the values of schema, converting to bigquery TableFieldSchema
    val fields = schema.iterator.toList.sortBy(_._1.underlying).map { case (id, info) =>
      BBQRepFactory(info.typ)
          .bigqueryFieldSchema
          .setName(columnNameMap(id))
    }
    new TableSchema().setFields(fields)
  }

  /**
   * Constructs a bigquery load job referencing the specified table
   */
  def makeLoadJob(ref: TableReference, truncate: Boolean = false) = {
    val config = new JobConfigurationLoad()
            .setDestinationTable(ref)
            .setSourceFormat("NEWLINE_DELIMITED_JSON")
    if (truncate) config.setWriteDisposition("TRUNCATE")
    new Job().setConfiguration(new JobConfiguration().setLoad(config))
  }
}
