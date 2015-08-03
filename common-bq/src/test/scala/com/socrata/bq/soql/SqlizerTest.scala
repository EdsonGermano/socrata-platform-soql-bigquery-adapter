package com.socrata.bq.soql

import org.scalatest.{Matchers, FunSuite}
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.bq.soql.SqlizerContext._
import com.socrata.bq.store.PostgresUniverseCommon
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types._
import com.socrata.soql.types.obfuscation.CryptProvider


class SqlizerTest extends FunSuite with Matchers {

  import SqlizerTest._

  test("string literal with quotes") {
    val soql = "select 'there is a '' quote'"
    val BQSql(sql, setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT ? FROM t1")
    setParams.length should be (1)
    setParams should be (Seq("'there is a ' quote'"))
  }

  test("field in (x, y...)") {
    val soql = "select case_number where case_number in ('ha001', 'ha002', 'ha003') order by case_number offset 1 limit 2"
    val BQSql(sql, setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT case_number FROM t1 WHERE (case_number in(?,?,?)) ORDER BY case_number LIMIT 2 OFFSET 1")
    setParams.length should be (3)
    setParams should be (Seq("'ha001'", "'ha002'", "'ha003'"))
  }

  test("field in (x, y...) ci") {
    val soql = "select case_number where case_number in ('ha001', 'ha002', 'ha003') order by case_number limit 2"
    val BQSql(sql, setParams) = sqlize(soql, CaseInsensitive)
    sql should be ("SELECT case_number FROM t1 WHERE (upper(case_number) in(?,?,?)) ORDER BY case_number LIMIT 2")
    setParams.length should be (3)
    setParams should be (Seq("'HA001'", "'HA002'", "'HA003'"))
  }

  test("as implicit alias order by") {
    val soql = "select sum(id) order by sum(id) limit 2"
    val BQSql(sql, setParams) = sqlize(soql, CaseInsensitive)
    sql should be ("SELECT (sum(id)) AS _sum_id__ FROM t1 ORDER BY _sum_id__ LIMIT 2")
    setParams.length should be (0)
  }

  test("as implicit alias no forward alias reference") {
    val soql = "select sum(id) group by id limit 2"
    val BQSql(sql, setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (sum(id)) AS _sum_id__ FROM t1 GROUP BY id LIMIT 2")
  }

  test("extent") {
    val soql = "select extent(point)"
    val BQSql(sql, setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (MIN(point.lat), MIN(point.long), MAX(point.lat), MAX(point.long)) FROM t1")
    setParams.length should be (0)
  }

  test("expr and expr") {
    val soql = "select id where id = 1 and case_number = 'cn001'"
    val BQSql(sql, setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT id FROM t1 WHERE ((id = ?) and (case_number = ?))")
    setParams.length should be (2)
    setParams should be (Seq("1", "'cn001'"))
  }

  test("expr and expr ci") {
    val soql = "select id where id = 1 and case_number = 'cn001'"
    val BQSql(sql, setParams) = sqlize(soql, CaseInsensitive)
    sql should be ("SELECT id FROM t1 WHERE ((id = ?) and (upper(case_number) = ?))")
    setParams.length should be (2)
    setParams should be (Seq("1", "'CN001'"))
  }

  test("point conversion") {
    val soql = "select point limit 2"
    val BQSql(sql, setParams) = sqlize(soql, CaseInsensitive)
    sql should be ("SELECT point.lat, point.long FROM t1 LIMIT 2")
    setParams.length should be (0)
  }

  test("starts_with has automatic suffix %") {
    val soql = "select id where starts_with(case_number, 'cn')"
    val BQSql(sql, setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT id FROM t1 WHERE (case_number like ?)")
    setParams.length should be (1)
    setParams should be (Seq("'cn%'"))
  }

  test("starts_with has automatic suffix % ci") {
    val soql = "select id where starts_with(case_number, 'cn')"
    val BQSql(sql, setParams) = sqlize(soql, CaseInsensitive)
    sql should be ("SELECT id FROM t1 WHERE (upper(case_number) like ?)")
    setParams.length should be (1)
    setParams should be (Seq("'CN%'"))
  }

  test("explicit like") {
    val soql = "select case_number where case_number like '%10%'"
    val BQSql(sql, setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT case_number FROM t1 WHERE (case_number like ?)")
    setParams should be (Seq("'%10%'"))
    setParams.length should be (1)
  }

  test("between") {
    val soql = "select id where id between 1 and 9"
    val BQSql(sql, setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT id FROM t1 WHERE (id between ? and ?)")
    setParams.length should be (2)
    setParams should be (Seq("1", "9"))
  }

  test("select count(*)") {
    val soql = "select count(*)"
    val BQSql(sql, setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (count(*)) AS _count____ FROM t1")
    setParams.length should be (0)
  }

  test("select aggregate functions") {
    val soql = "select count(id), avg(id), min(id), max(id), sum(id)"
    val BQSql(sql, setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (count(id)) AS _count_id__,(avg(id)) AS _avg_id__,(min(id)) AS _min_id__,(max(id)) AS " +
      "_max_id__,(sum(id)) AS _sum_id__ FROM t1")
    setParams.length should be (0)
  }

  test("bounding box") {
    val soql = "select point where within_box(point, -60, 40, -90, 50)"
    val BQSql(sql, setParams) = sqlize(soql, CaseSensitive)
    setParams should be (Seq("60", "90", "40", "50"))
    sql should be ("SELECT point.lat, point.long FROM t1 WHERE (point.lat > (-?) AND point.lat < (-?) AND point.long > ? AND point" +
      ".long < ?)")
  }

  test("within circle") {
    val soql = "select point where within_circle(point, 45.535, -123.424, 500)"
    val BQSql(sql, setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT point.lat, point.long FROM t1 WHERE (((ACOS(SIN(? * PI() / 180) * SIN((point.lat/1000) * PI() / 180) " +
      "+ COS(? * PI() / 180) * COS((point.lat/1000) * PI() / 180) * COS(((-?) - (point.long/1000)) * PI() / 180)) * " +
      "180 / PI()) * 60 * 1.1515) < ?)")
    setParams should be (Seq("45.535", "45.535", "123.424", "500"))
  }
}

object SqlizerTest {

  import Sqlizer._

  private val cryptProvider = new CryptProvider(CryptProvider.generateKey())
  val sqlCtx = Map[SqlizerContext, Any](
    SqlizerContext.IdRep -> new SoQLID.StringRep(cryptProvider),
    SqlizerContext.VerRep -> new SoQLVersion.StringRep(cryptProvider)
  )

  private def sqlize(soql: String, caseSensitivity: CaseSensitivity): BQSql = {
    val allColumnReps = columnInfos.map(PostgresUniverseCommon.repForIndex(_))
    val analysis: SoQLAnalysis[UserColumnId, SoQLType] = SoQLAnalyzerHelper.analyzeSoQL(soql, datasetCtx, idMap)
    (analysis, "t1", allColumnReps).sql(
      Map.empty[UserColumnId,
      SqlColumnRep[SoQLType, SoQLValue]],
      Seq.empty,
      sqlCtx + (SqlizerContext.CaseSensitivity -> caseSensitivity),
      passThrough)
  }

  private val idMap =  (cn: ColumnName) => new UserColumnId(cn.caseFolded)

  private val passThrough = (s: String) => s

  private val columnMap = Map(
    ColumnName(":id") -> (1, SoQLID),
    ColumnName(":version") -> (2, SoQLVersion),
    ColumnName(":created_at") -> (3, SoQLFixedTimestamp),
    ColumnName(":updated_at") -> (4, SoQLFixedTimestamp),
    ColumnName("id") -> (5, SoQLNumber),
    ColumnName("case_number") -> (6, SoQLText),
    ColumnName("primary_type") -> (7, SoQLText),
    ColumnName("year") -> (8, SoQLNumber),
    ColumnName("arrest") -> (9, SoQLBoolean),
    ColumnName("updated_on") -> (10, SoQLFloatingTimestamp),
    ColumnName("object") -> (11, SoQLObject),
    ColumnName("array") -> (12, SoQLArray),
    ColumnName("point") -> (13, SoQLPoint),
    ColumnName("multiline") -> (14, SoQLMultiLine),
    ColumnName("multipolygon") -> (15, SoQLMultiPolygon),
    ColumnName("polygon") -> (16, SoQLPolygon),
    ColumnName("line") -> (17, SoQLLine),
    ColumnName("multipoint") -> (18, SoQLMultiPoint)
  )

  private val columnInfos = columnMap.foldLeft(Seq.empty[ColumnInfo[SoQLType]]) { (acc, colNameAndType) => colNameAndType match {
    case (columnName: ColumnName, (id, typ)) =>
      val cinfo = new com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType](
        null,
        new ColumnId(id),
        new UserColumnId(columnName.caseFolded),
        typ,
        columnName.caseFolded,
        typ == SoQLID,
        false, // isUserKey
        typ == SoQLVersion
      )(SoQLTypeContext.typeNamespace, null)
      acc :+ cinfo
  }}

  private val datasetCtx: DatasetContext[SoQLType] = new DatasetContext[SoQLType] {
    val schema = new OrderedMap[ColumnName, SoQLType](columnMap,  columnMap.keys.toVector)
  }
}
