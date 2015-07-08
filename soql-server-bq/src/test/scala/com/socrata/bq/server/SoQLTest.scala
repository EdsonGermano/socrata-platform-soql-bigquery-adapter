package com.socrata.bq.server

import com.socrata.bq.store.PGSecondaryTestBase
import com.socrata.bq.store.PGSecondaryUtil._
import com.socrata.bq.query.PGQueryTestBase


abstract class SoQLTest extends PGSecondaryTestBase with PGQueryServerDatabaseTestBase with PGQueryTestBase {

  override def beforeAll = {
    createDatabases()
    withDb() { conn =>
      importDataset(conn)
    }
  }

  override def afterAll = {
    withPgu() { pgu =>
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val tableName = pgu.datasetMapReader.latest(datasetInfo).dataTableName
      dropDataset(pgu, truthDatasetId)
      cleanupDroppedTables(pgu)
      hasDataTables(pgu.conn, tableName, datasetInfo) should be (false)
    }
    super.afterAll
  }
}
