package com.socrata.pg.store

import com.rojoma.json.util.JsonUtil
import com.socrata.pg.server.{QueryServerTest, QueryServer}
import com.socrata.pg.Schema
import scala.io.Source
import scala.language.reflectiveCalls
import com.socrata.datacoordinator.common.DataSourceConfig

class SchemaTest extends PGSecondaryTestBase {
  import com.socrata.pg.store.PGSecondaryUtil._
  import Schema._

  test("schema json codec") {
    withPgu() { pgu =>
      val f = columnsCreatedFixture
      f.pgs._version(pgu, f.datasetInfo, f.dataVersion+1, None, f.events.iterator)
      val dsConfig = new DataSourceConfig(config, "test-database")
      val qs = new QueryServerTest(dsConfig, pgu)
      val schema = qs.latestSchema(testInternalName).get
      val schemaj = JsonUtil.renderJson(schema)
      val schemaRoundTrip = JsonUtil.parseJson[com.socrata.datacoordinator.truth.metadata.Schema](schemaj).get
      schema should be (schemaRoundTrip)
      val expected = Source.fromURL(getClass.getResource("/fixtures/schema.json"))
      val expectedSchema = JsonUtil.readJson[com.socrata.datacoordinator.truth.metadata.Schema](expected.reader()).get
      schema should be (expectedSchema)
    }
  }


}
