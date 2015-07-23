package com.socrata.bq.store

import collection.JavaConversions._
import java.sql.{Connection, DriverManager, ResultSet}

import com.rojoma.json.v3.ast._
import com.rojoma.simplearm.util._
import com.rojoma.simplearm.Managed
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.soql.types._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, ColumnInfo => SecondaryColumnInfo, _}
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.id.{DatasetId, CopyId, ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.universe.sql.PostgresCopyIn
import com.socrata.thirdparty.typesafeconfig.C3P0Propertizer
import com.typesafe.config.Config
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

// scalastyle:off
class BBQSecondary(config: Config) extends Secondary[SoQLType, SoQLValue] with Logging {
  private val PROJECT_ID = config.getString("project-id")
  private val BQ_DATASET_ID = config.getString("dataset-id")
  private val COPY_INFO_TABLE = "bbq_copy_info"
  private val TRANSPORT = new NetHttpTransport()
  private val JSON_FACTORY = new JacksonFactory()

  val bigquery = {
    var credential: GoogleCredential = GoogleCredential.getApplicationDefault()
    if (credential.createScopedRequired) {
      credential = credential.createScoped(BigqueryScopes.all)
    }
    new Bigquery.Builder(TRANSPORT, JSON_FACTORY, credential).setApplicationName("BBQ Secondary").build()
  }

  val handler = new BigqueryHandler(bigquery, PROJECT_ID, BQ_DATASET_ID)

  // called on graceful shutdown
  override def shutdown(): Unit = {
    logger.info("shutdown called")
  }

  override def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = {
    logger.info("dropDataset called")
  }

  override def snapshots(datasetInternalName: String, cookie: Cookie): Set[Long] = ???

  override def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Cookie): Cookie = {
    logger.info(s"dropCopy called on ${datasetInternalName}/${copyNumber}")
    cookie
  }

  override def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long = {
    val datasetId = parseDatasetId(datasetInternalName)
    getCopyNumber(datasetId)
  }

  override def wantsWorkingCopies: Boolean = false

  override def currentVersion(datasetInternalName: String, cookie: Cookie): Long = {
    val datasetId = parseDatasetId(datasetInternalName)
    getDataVersion(datasetId)
  }

  override def resync(datasetInfo: DatasetInfo,
                      copyInfo: SecondaryCopyInfo,
                      schema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
                      cookie: Secondary.Cookie,
                      rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
                      rollups: Seq[RollupInfo]): Secondary.Cookie = {
    logger.info(s"resyncing ${datasetInfo.internalName}@${copyInfo.systemId.underlying}/${copyInfo.dataVersion}/${copyInfo.copyNumber}")
    val datasetId = parseDatasetId(datasetInfo.internalName)
    // construct ref to table
    val columnNames: ColumnIdMap[String] = handler.makeColumnNameMap(schema)
    val ref = handler.makeTableReference(datasetInfo, copyInfo)
    val userSchema = schema.filter( (id, info) => handler.isUserColumn(info.id) )
    val bqSchema = handler.makeTableSchema(userSchema, columnNames)
    val table = new Table()
            .setTableReference(ref)
            .setSchema(bqSchema)

    try {
      bigquery.tables.insert(PROJECT_ID, BQ_DATASET_ID, table).execute()
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
              case Some(name) => map + ((name, handler.encode(value)))
            }
          }
          JsonUtil.renderJson(rowMap)
        }

      for { batch <- requests.grouped(10000) } {
        handler.loadRows(ref, batch)
      }
    }
    setCopyInfoEntry(datasetId, copyInfo)
    cookie
  }



  override def version(datasetInfo: DatasetInfo,
              dataVersion: Long,
              cookie: Cookie,
              events: Iterator[Event[SoQLType, SoQLValue]]): Cookie = {
    val internalName = datasetInfo.internalName
    logger.info(s"version called for dataset ${internalName}@${dataVersion}")

    events.foreach { event =>
      event match {
        case WorkingCopyPublished => {
          logger.info(">> got publish event; throwing resync")
          throw new ResyncSecondaryException("resync from WorkingCopyPublished")
        }
        case _ => ()
      }
    }

    cookie
  }

  private def parseDatasetId(datasetInternalName: String) = {
    datasetInternalName.split('.')(1).toInt
  }

  private def getCopyNumber(datasetId: Int): Int = {
    for (conn <- managed(getConnection())) {
      val query = "SELECT copy_number FROM $COPY_INFO_TABLE WHERE dataset_id=$datasetId"
      val stmt = conn.createStatement()
      val resultSet = stmt.executeQuery(query)
      if (resultSet.first()) {
        // result set has a row
        val copyNumber = resultSet.getInt("copy_number")
        return copyNumber
      }
    }
    0
  }

  private def getDataVersion(datasetId: Int): Int = {
    for (conn <- managed(getConnection())) {
      val query = "SELECT data_version FROM $COPY_INFO_TABLE WHERE dataset_id=$datasetId"
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

  private def setCopyInfoEntry(datasetId: Int, copyInfo: SecondaryCopyInfo) = {
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

  private val dsInfo = dataSourceFromConfig(new DataSourceConfig(config, "database"))

  private def getConnection() = dsInfo.dataSource.getConnection()

  private def dataSourceFromConfig(config: DataSourceConfig): DSInfo = {
    val dataSource = new PGSimpleDataSource
    dataSource.setServerName(config.host)
    dataSource.setPortNumber(config.port)
    dataSource.setDatabaseName(config.database)
    dataSource.setUser(config.username)
    dataSource.setPassword(config.password)
    dataSource.setApplicationName(config.applicationName)
    new DSInfo(dataSource, PostgresCopyIn)
  }
}
