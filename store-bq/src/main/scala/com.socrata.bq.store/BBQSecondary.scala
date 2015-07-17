package com.socrata.bq.store

import collection.JavaConversions._
import java.sql.{Connection, DriverManager, ResultSet}

import com.rojoma.simplearm.util._
import com.rojoma.simplearm.Managed
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, _}
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.id.{DatasetId, CopyId}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo, LifecycleStage => TruthLifecycleStage,
                                                   ColumnInfo, DatasetCopyContext}
import com.socrata.datacoordinator.secondary.{ColumnInfo => SecondaryColumnInfo}
import com.socrata.datacoordinator.truth.universe.sql.{PostgresCopyIn, C3P0WrappedPostgresCopyIn}
import com.socrata.thirdparty.typesafeconfig.C3P0Propertizer
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.Logging
import org.postgresql.ds.PGSimpleDataSource

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.google.api.services.bigquery.model._

// scalastyle:off
class BBQSecondary(config: Config) extends Secondary[SoQLType, SoQLValue] with Logging {
  private val PROJECT_ID = "numeric-zoo-99418"
  private val PROJECT_NUMBER = "1093450707280"
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
    val res = getCopyNumber(datasetId)
    logger.info(s"$res")
    res
  }

  override def wantsWorkingCopies: Boolean = {
    logger.info("wantsWorkingCopies accessed")
    false
  }

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
    logger.info(s"resync ${datasetInfo.internalName}@${copyInfo.systemId.underlying}/${copyInfo.dataVersion}/${copyInfo.copyNumber}")
    val datasetId = parseDatasetId(datasetInfo.internalName)
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

  private def getCopyInfoEntry(datasetId: Int): (Int, Int) = {
    var result = (0, 0)
    for (conn <- managed(getConnection())) {
      val query = "SELECT copy_number, data_version FROM $COPY_INFO_TABLE WHERE dataset_id=$datasetId"
      val stmt = conn.createStatement()
      val resultSet = stmt.executeQuery(query)
      if (resultSet.first()) {
        // result set has a row
        val copyNumber = resultSet.getInt("copy_number")
        val dataVersion = resultSet.getInt("data_version")
        result = (copyNumber, dataVersion)
      }
    }
    result
  }

  private def getCopyNumber(datasetId: Int) = getCopyInfoEntry(datasetId)._1

  private def getDataVersion(datasetId: Int) = getCopyInfoEntry(datasetId)._2

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
