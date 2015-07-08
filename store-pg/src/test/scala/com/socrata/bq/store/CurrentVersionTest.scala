package com.socrata.bq.store

import scala.language.reflectiveCalls

class CurrentVersionTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  import PGSecondaryUtil._

  test("handle CurrentVersion") {
    withPgu() { pgu =>
      val f = columnsCreatedFixture

      var version = f.dataVersion
      f.events foreach { e =>
        version = version + 1
        f.pgs._version(pgu, f.datasetInfo, version, None, Iterator(e))
      }

      f.pgs._currentVersion(pgu, testInternalName, None) shouldEqual version
    }
  }


}